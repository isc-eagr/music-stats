package library;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;

/**
 * Standalone crawler to scrape Vato's Personal Countdown from classic.atrl.net
 * and output a SQL script for loading into the pc_countdown_entry table.
 *
 * Only the original 2003-2009 run is included (stops after 2009-09-15).
 * No database connection is needed — output is a SQL file for review.
 *
 * Usage: Run main() from IDE or via:
 *   mvnw exec:java -Dexec.mainClass="library.PersonalCuntdownCrawler"
 */
public class PersonalCuntdownCrawler {

    // Forum configuration
    private static final String BASE_URL = "https://classic.atrl.net/forums/printthread.php?t=36&page=";
    private static final int TOTAL_PAGES = 357;
    private static final long RATE_LIMIT_MS = 800;
    private static final String STOP_DATE = "2009-09-15"; // stop parsing after this date
    private static final String OUTPUT_FILE = "pc_crawl_output.sql";
    private static final String TARGET_USER = "Vato";
    private static final String USER_AGENT = "MusicStatsApp/1.0 (cuntdown-crawler)";

    // Month name -> number map
    private static final Map<String, Integer> MONTHS = new LinkedHashMap<>();
    static {
        String[] names = {"january","february","march","april","may","june","july","august","september","october","november","december"};
        for (int i = 0; i < names.length; i++) MONTHS.put(names[i], i + 1);
    }

    // Date patterns for countdown headers in post bodies
    // Pattern 1: "Monday, January 6th, 2003" or "January 6th, 2003"
    private static final Pattern DATE_WITH_YEAR = Pattern.compile(
        "(?:(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday),?\\s+)?" +
        "(january|february|march|april|may|june|july|august|september|october|november|december)\\s+" +
        "(\\d{1,2})(?:st|nd|rd|th)?[,:]?\\s+(\\d{4})",
        Pattern.CASE_INSENSITIVE
    );
    // Pattern 2: "February 17:" or "Tuesday, March 11" (no year — fallback to post timestamp year)
    private static final Pattern DATE_NO_YEAR = Pattern.compile(
        "(?:(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday),?\\s+)?" +
        "(january|february|march|april|may|june|july|august|september|october|november|december)\\s+" +
        "(\\d{1,2})(?:st|nd|rd|th)?\\s*:?\\s*$",
        Pattern.CASE_INSENSITIVE
    );
    // Post timestamp pattern (e.g., "2/17/2003 7:35 PM")
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("(\\d{1,2})/(\\d{1,2})/(\\d{4})");
    // Numeric date pattern: "08.21.04" or "08.21.2004" (MM.DD.YY American format; DD.MM.YY if first part > 12)
    // Used by Vato from roughly Aug 2004 through mid-2006
    private static final Pattern DATE_NUMERIC = Pattern.compile("^(\\d{1,2})\\.(\\d{1,2})\\.(\\d{2,4})$");
    // vBulletin smiley tokens rendered as text in printthread mode: :randy:, :lol:, etc.
    private static final Pattern SMILEY_TOKEN = Pattern.compile(":[a-zA-Z0-9_]+:");

    // Entry pattern: "10. Artist - Song" or "1. Artist - Song"
    private static final Pattern ENTRY_PATTERN = Pattern.compile("^#?0?(\\d{1,2})[.):]\\s+(.+?)\\s+-\\s+(.+)$");
    // Album / stats strip patterns
    private static final Pattern ALBUM_PIPE    = Pattern.compile("\\s*\\|.*$");
    private static final Pattern STATS_SUFFIX  = Pattern.compile("\\s*\\(stats\\)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PAREN_ALBUM   = Pattern.compile("\\s*\\([^)]*album[^)]*\\)\\s*$", Pattern.CASE_INSENSITIVE);
    // Strips inline chart-stats annotations like (DEBUT/1/10), (NE), (2/5), (RE) etc.
    // Matches closing parens whose content is ONLY uppercase letters, digits, slashes, asterisks.
    private static final Pattern STATS_PARENS  = Pattern.compile("\\s*\\([A-Z0-9/*]+\\)\\s*$");
    // Strips mixed-case per-entry stats like (32nd day / Peak: 1[x4]) or (1st day / DEBUT / Peak: 10)
    // Matches any trailing paren that contains " / " — the separator used in these stats blocks.
    private static final Pattern STATS_SLASH_PARENS = Pattern.compile("\\s*\\([^)]*\\s+/\\s+[^)]*\\)\\s*$");

    // Section header detection
    private static final Set<String> CLOSECALL_HEADERS = new HashSet<>(Arrays.asList(
        "close calls", "close call", "elite four", "the elite four", "bubbling under", "close calls:"
    ));
    private static final Set<String> MAIN_HEADERS = new HashSet<>(Arrays.asList(
        "the top 10", "top 10", "gym leaders", "the gym leaders", "the countdown", "top ten", "the top ten"
    ));
    private static final Set<String> IGNORE_HEADERS = new HashSet<>(Arrays.asList(
        "past #1s", "past #1's", "past #1", "fall-offs", "fall offs", "fall-off", "falloffs",
        "retirements", "retired", "the hall of fame", "hall of fame",
        "past number ones", "past number one"
    ));

    // Unnumbered close call line: "Artist - Song" with no leading position number.
    // Anchored to avoid matching section headers; requires at least 3 chars before " - ".
    private static final Pattern UNNUMBERED_CC_PATTERN = Pattern.compile(
        "^([^\\d#!\\[].{2,}?)\\s+-\\s+(.+)$"
    );
    private static final Pattern RETIRED_SUFFIX = Pattern.compile("(?i)\\s*\\(?RETIRED[^)]*\\)?\\s*$");
    private static final Pattern OLD_SKOOL_SUFFIX = Pattern.compile("(?i)\\s*\\(?OL['’]?\\s*SKOOL[^)]*\\)?\\s*$");
    private static final Pattern DAYS_AT_PEAK_SUFFIX = Pattern.compile("(?i)\\s*\\d*\\s*DAYS AT #\\d+\\)?\\s*$");

    // Section state enum
    private enum Section { MAIN, CLOSECALLS, IGNORE, IDLE }

    // Raw entry from scraping
    static class RawEntry {
        String chartDate;
        int position;
        String artistName;
        String songTitle;
        boolean isClosecall;

        RawEntry(String chartDate, int position, String artistName, String songTitle, boolean isClosecall) {
            this.chartDate = chartDate;
            this.position = position;
            this.artistName = artistName;
            this.songTitle = songTitle;
            this.isClosecall = isClosecall;
        }
    }

    // Stats
    private int pagesFetched = 0;
    private int vatoPostsFound = 0;
    private int countdownDatesParsed = 0;
    private int totalEntries = 0;
    private int errorCount = 0;
    private boolean stopped = false;

    // All raw entries collected
    private final List<RawEntry> allEntries = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        new PersonalCuntdownCrawler().run();
    }

    private void run() throws Exception {
        System.out.println("=== Personal Cuntdown Crawler ===");
        System.out.println("Target: " + BASE_URL + "N  (pages 1-" + TOTAL_PAGES + ")");
        System.out.println("Stop date: " + STOP_DATE);
        System.out.println();

        for (int page = 1; page <= TOTAL_PAGES; page++) {
            if (stopped) break;

            String url = BASE_URL + page;
            System.out.printf("[Page %3d/%d] Fetching...", page, TOTAL_PAGES);

            try {
                String html = fetchPage(url);
                Document doc = Jsoup.parse(html);
                boolean shouldStop = processPage(doc, page);
                System.out.printf(" done  (total entries so far: %d)%n", totalEntries);
                if (shouldStop) {
                    System.out.println("  >> Stop condition reached (date past " + STOP_DATE + ")");
                    stopped = true;
                    break;
                }
            } catch (Exception e) {
                System.out.printf(" ERROR: %s%n", e.getMessage());
                errorCount++;
            }

            if (page < TOTAL_PAGES && !stopped) {
                Thread.sleep(RATE_LIMIT_MS);
            }
            pagesFetched++;
        }

        System.out.println();
        System.out.println("=== Crawl complete. Building SQL... ===");
        buildSql();
        printStats();
    }

    /**
     * Fetch a page with User-Agent header, follow redirects.
     */
    private String fetchPage(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setRequestProperty("Accept", "text/html,application/xhtml+xml");
        conn.setConnectTimeout(15_000);
        conn.setReadTimeout(30_000);
        conn.setInstanceFollowRedirects(true);

        try (InputStream is = conn.getInputStream()) {
            // Detect charset from Content-Type header; forum pages are typically ISO-8859-1
            String contentType = conn.getContentType();
            Charset charset = StandardCharsets.ISO_8859_1;
            if (contentType != null) {
                Matcher cm = Pattern.compile("charset=([\\w-]+)", Pattern.CASE_INSENSITIVE).matcher(contentType);
                if (cm.find()) {
                    try { charset = Charset.forName(cm.group(1)); } catch (Exception ignored) {}
                }
            }
            return new String(is.readAllBytes(), charset);
        }
    }

    /**
     * Process a single forum page. Returns true if stop condition was reached.
     *
     * vBulletin 3 printthread structure (no id attributes on post tables!):
     *   <table class="tborder">          — one per post
     *     <td class="page">
     *       <table>                      — inner header
     *         <td style="font-size:14pt">USERNAME</td>
     *         <td class="smallfont" align="right">TIMESTAMP</td>
     *       </table>
     *       <hr />
     *       <div>post body</div>         — may be preceded by a title div for the OP
     *     </td>
     *   </table>
     */
    private boolean processPage(Document doc, int pageNum) {
        Elements postTables = doc.select("table.tborder");

        for (Element postTable : postTables) {
            // Username: <td style="font-size:14pt">
            Element usernameTd = postTable.select("td[style*=font-size:14pt]").first();
            if (usernameTd == null) continue;
            String username = usernameTd.text().trim();
            if (!TARGET_USER.equals(username)) continue;

            // Timestamp: <td class="smallfont" align="right">
            Element timestampTd = postTable.select("td.smallfont[align=right]").first();
            String timestamp = (timestampTd != null) ? timestampTd.text().trim() : "";

            // Post body: td.page after stripping inner header table and <hr>
            Element pageTd = postTable.select("td.page").first();
            if (pageTd == null) continue;
            pageTd.select("table").remove();
            pageTd.select("hr").remove();

            boolean stop = parseVatoPost(pageTd, timestamp);
            if (stop) return true;
        }
        return false;
    }

    /**
     * Parse a Vato post body for countdown entries.
     * Returns true if the stop condition is reached.
     *
     * Key insight: Jsoup's .text() normalises whitespace, collapsing our \n insertions.
     * Instead we work on the raw HTML string: replace <br> with \n, strip other tags,
     * then decode HTML entities with Parser.unescapeEntities (no whitespace normalisation).
     */
    private boolean parseVatoPost(Element msgDiv, String timestamp) {
        vatoPostsFound++;

        int fallbackYear = extractYearFromTimestamp(timestamp);

        // Work on raw HTML string so we control newlines precisely
        String bodyHtml = msgDiv.html();

        // Strip quote blocks before any other processing
        bodyHtml = bodyHtml.replaceAll("(?is)<div[^>]*class=\"quote\".*?</div>", "");
        bodyHtml = bodyHtml.replaceAll("(?is)<blockquote.*?</blockquote>", "");

        // Strip img tags (2007 Photobucket era)
        bodyHtml = bodyHtml.replaceAll("(?i)<img[^>]*/>", "");
        bodyHtml = bodyHtml.replaceAll("(?i)<img[^>]*>", "");

        // Strip anchor tags whose text is a URL (Photobucket link text prefixed before entries
        // on pages 272+: <a href="...">http://img.photobucket.com/...</a>10. Artist - Song)
        bodyHtml = bodyHtml.replaceAll("(?i)<a\\s[^>]*>https?://[^<]*</a>", "");

        // Replace <br> variants with newline
        bodyHtml = bodyHtml.replaceAll("(?i)<br\\s*/?>" , "\n");

        // Replace block-level tag boundaries with newline so entries stay on own lines
        bodyHtml = bodyHtml.replaceAll("(?i)</?(?:div|p|li|tr|td|thead|tbody|table|hr|h[1-6])[^>]*>", "\n");

        // Strip all remaining HTML tags
        bodyHtml = bodyHtml.replaceAll("<[^>]+>", "");

        // Decode HTML entities (&amp; -> &, &#039; -> ', etc.) without any whitespace normalisation
        String bodyText = Parser.unescapeEntities(bodyHtml, false);

        String[] lines = bodyText.split("\n");

        String currentDate = null;
        Section section = Section.IDLE;

        for (String rawLine : lines) {
            String line = rawLine.trim();
            if (line.isEmpty()) continue;

            // Strip vBulletin smiley tokens (e.g. :randy:) — rendered as text in printthread mode
            String lineForDate = SMILEY_TOKEN.matcher(line).replaceAll("").trim();

            // Check for countdown date on this line
            String foundDate = parseCountdownDate(lineForDate, fallbackYear);
            if (foundDate != null) {
                // Stop condition fires for ANY date past cutoff (including post-2009 dates)
                if (foundDate.compareTo(STOP_DATE) > 0) {
                    return true;
                }
                if (isValidCountdownDate(foundDate)) {
                    if (currentDate == null || !currentDate.equals(foundDate)) {
                        currentDate = foundDate;
                        countdownDatesParsed++;
                    }
                    section = Section.MAIN; // default section after date header
                }
                continue;
            }

            // Check for section headers
            String lineLower = line.toLowerCase().replaceAll("[^a-z0-9 '#]", "").trim();
            if (CLOSECALL_HEADERS.contains(lineLower)) {
                section = Section.CLOSECALLS;
                continue;
            }
            if (MAIN_HEADERS.contains(lineLower)) {
                section = Section.MAIN;
                continue;
            }
            if (IGNORE_HEADERS.contains(lineLower)) {
                section = Section.IGNORE;
                continue;
            }
            // Also check substrings for partial matches
            if (containsAny(lineLower, "close calls", "elite four", "bubbling under")) {
                section = Section.CLOSECALLS;
                continue;
            }
            if (containsAny(lineLower, "past #1", "fall-off", "fall offs", "hall of fame", "retirements")) {
                section = Section.IGNORE;
                continue;
            }
            if (containsAny(lineLower, "top 10", "gym leaders", "the countdown")) {
                section = Section.MAIN;
                continue;
            }

            // Skip if no date established or in ignore mode
            if (currentDate == null || section == Section.IDLE || section == Section.IGNORE) {
                continue;
            }

            // Try to parse as a numbered entry
            Matcher m = ENTRY_PATTERN.matcher(line);
            if (m.matches()) {
                int pos = Integer.parseInt(m.group(1));
                String artist = m.group(2).trim();
                String song = cleanSongTitle(m.group(3).trim());

                if (!artist.isEmpty() && !song.isEmpty()) {
                    // Auto-switch CLOSECALLS -> MAIN when the line contains a stats annotation
                    // ("Nth day / Peak: X"). This handles posts where Vato listed unnumbered
                    // close calls first, then the numbered main countdown with no explicit header.
                    if (section == Section.CLOSECALLS && line.contains(" / ")) {
                        section = Section.MAIN;
                    }
                    boolean isCC = (section == Section.CLOSECALLS);
                    allEntries.add(new RawEntry(currentDate, pos, artist, song, isCC));
                    totalEntries++;
                }
            } else if (section == Section.CLOSECALLS) {
                // Capture unnumbered close call lines: "Artist - Song" (no leading position number).
                Matcher cc = UNNUMBERED_CC_PATTERN.matcher(line);
                if (cc.matches()) {
                    String artist = cc.group(1).trim();
                    String song = cleanSongTitle(cc.group(2).trim());
                    if (!artist.isEmpty() && !song.isEmpty()) {
                        allEntries.add(new RawEntry(currentDate, 0, artist, song, true));
                        totalEntries++;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Try to parse a countdown date from a line.
     * Returns a YYYY-MM-DD string or null.
     */
    private String parseCountdownDate(String line, int fallbackYear) {
        // Try pattern with year first
        Matcher m1 = DATE_WITH_YEAR.matcher(line);
        if (m1.find()) {
            int month = MONTHS.getOrDefault(m1.group(1).toLowerCase(), -1);
            int day = Integer.parseInt(m1.group(2));
            int year = Integer.parseInt(m1.group(3));
            if (month > 0 && day >= 1 && day <= 31) {
                return String.format("%04d-%02d-%02d", year, month, day);
            }
        }

        // Try pattern without year (use fallback year)
        Matcher m2 = DATE_NO_YEAR.matcher(line);
        if (m2.find()) {
            int month = MONTHS.getOrDefault(m2.group(1).toLowerCase(), -1);
            int day = Integer.parseInt(m2.group(2));
            int year = (fallbackYear > 0) ? fallbackYear : 2003;
            if (month > 0 && day >= 1 && day <= 31) {
                return String.format("%04d-%02d-%02d", year, month, day);
            }
        }

        // Try numeric date pattern: MM.DD.YY or DD.MM.YY (when first part > 12)
        Matcher m3 = DATE_NUMERIC.matcher(line.trim());
        if (m3.matches()) {
            int a = Integer.parseInt(m3.group(1));
            int b = Integer.parseInt(m3.group(2));
            int rawYear = Integer.parseInt(m3.group(3));
            int year = (rawYear < 100) ? 2000 + rawYear : rawYear;
            int month, day;
            if (a > 12) {
                // First part can't be a month — must be DD.MM.YY
                day = a; month = b;
            } else {
                // Assume MM.DD.YY (Vato's standard American format)
                month = a; day = b;
            }
            if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
                return String.format("%04d-%02d-%02d", year, month, day);
            }
        }

        return null;
    }

    /**
     * Validate that a parsed date is within the 2003-2009 range.
     */
    private boolean isValidCountdownDate(String date) {
        return date.compareTo("2003-01-01") >= 0 && date.compareTo("2009-12-31") <= 0;
    }

    /**
     * Extract year from a post timestamp string like "2/17/2003 7:35 PM".
     */
    private int extractYearFromTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) return -1;
        Matcher m = TIMESTAMP_PATTERN.matcher(timestamp);
        if (m.find()) {
            return Integer.parseInt(m.group(3));
        }
        // Also try 4-digit year directly
        Pattern yearOnly = Pattern.compile("\\b(200[3-9])\\b");
        Matcher ym = yearOnly.matcher(timestamp);
        if (ym.find()) return Integer.parseInt(ym.group(1));
        return -1;
    }

    /**
     * Clean a song title by stripping album info and stats suffixes.
     */
    private String cleanSongTitle(String song) {
        song = ALBUM_PIPE.matcher(song).replaceFirst("");
        song = STATS_SUFFIX.matcher(song).replaceFirst("");
        song = PAREN_ALBUM.matcher(song).replaceFirst("");
        song = STATS_PARENS.matcher(song).replaceFirst("");
        song = STATS_SLASH_PARENS.matcher(song).replaceFirst("");
        if (RETIRED_SUFFIX.matcher(song).find()) {
            song = RETIRED_SUFFIX.matcher(song).replaceFirst("");
        }
        if (OLD_SKOOL_SUFFIX.matcher(song).find()) {
            song = OLD_SKOOL_SUFFIX.matcher(song).replaceFirst("");
        }
        if (DAYS_AT_PEAK_SUFFIX.matcher(song).find()) {
            song = DAYS_AT_PEAK_SUFFIX.matcher(song).replaceFirst("");
        }
        return song.trim();
    }

    /**
     * Build and write the SQL output file.
     * Writes a single-table import for pc_countdown_entry only.
     */
    private void buildSql() throws Exception {
        // Sort entries chronologically, then by section, then position
        List<RawEntry> sortedEntries = new ArrayList<>(allEntries);
        sortedEntries.sort(Comparator.comparing((RawEntry e) -> e.chartDate)
                                     .thenComparingInt(e -> e.isClosecall ? 1 : 0)
                                     .thenComparingInt(e -> e.position));

        System.out.println("Total entries to write: " + sortedEntries.size());

        try (PrintWriter pw = new PrintWriter(new FileWriter(OUTPUT_FILE, StandardCharsets.UTF_8))) {
            pw.println("-- Personal Cuntdown SQL import");
            pw.println("-- Generated by PersonalCuntdownCrawler");
            pw.println("-- Pages fetched:          " + pagesFetched);
            pw.println("-- Vato posts found:        " + vatoPostsFound);
            pw.println("-- Countdown dates parsed:  " + countdownDatesParsed);
            pw.println("-- Total entries:           " + totalEntries);
            pw.println("-- Errors:                  " + errorCount);
            pw.println();
            pw.println("-- ================================================================");
            pw.println("-- STEP 1: Import raw countdown entries into pc_countdown_entry.");
            pw.println("-- ================================================================");
            pw.println();
            pw.println("BEGIN TRANSACTION;");
            pw.println();
            pw.println("DELETE FROM pc_countdown_entry;");
            pw.println();

            for (RawEntry e : sortedEntries) {
                pw.printf("INSERT INTO pc_countdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES (%s, %d, %s, %s, %d, NULL);%n",
                    sqlStr(e.chartDate),
                    e.position,
                    sqlStr(e.artistName),
                    sqlStr(e.songTitle),
                    e.isClosecall ? 1 : 0);
            }

            pw.println();
            pw.println("COMMIT;");
        }

        System.out.println("SQL written to: " + OUTPUT_FILE);
    }

    /**
     * Escape a string for safe SQL single-quoted value.
     */
    private String sqlStr(String s) {
        if (s == null) return "NULL";
        return "'" + s.replace("'", "''") + "'";
    }

    /**
     * Check if a string contains any of the given substrings.
     */
    private boolean containsAny(String text, String... subs) {
        for (String sub : subs) {
            if (text.contains(sub)) return true;
        }
        return false;
    }

    private void printStats() {
        System.out.println();
        System.out.println("=== Final Stats ===");
        System.out.println("Pages fetched:          " + pagesFetched);
        System.out.println("Vato posts found:       " + vatoPostsFound);
        System.out.println("Countdown dates parsed: " + countdownDatesParsed);
        System.out.println("Total entries:          " + totalEntries);
        System.out.println("Errors:                 " + errorCount);
        System.out.println("Output file:            " + OUTPUT_FILE);
    }
}
