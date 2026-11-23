package library.service;

import library.dto.ChartHistoryDTO;
import library.dto.ChartHistoryViewDTO;
import library.dto.ChartHistoryViewDTO.Calendar;
import library.dto.ChartHistoryViewDTO.CalendarCell;
import library.dto.ChartHistoryViewDTO.CalendarGrid;
import library.dto.ChartHistoryViewDTO.CalendarItem;
import library.dto.ChartHistoryViewDTO.CalendarYear;
import library.dto.ChartHistoryViewDTO.HitCard;
import library.dto.ChartHistoryViewDTO.HitSection;
import library.dto.ChartHistoryViewDTO.Segment;
import library.dto.ChartHistoryViewDTO.SpreadRow;
import library.dto.ChartHistoryViewDTO.Tile;
import library.dto.ChartHistoryViewDTO.TileStat;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Builds the Chart History tab of the detail pages from the chart data the controllers already load
 * (no queries of its own): chart tiles, the "Where they peaked" spread, the hit cards per run chart
 * and the Seasonal + Yearly calendar.
 */
@Service
public class DetailChartHistoryService {

    static final int WEEKLY_SONG_SCALE = 20;
    static final int WEEKLY_ALBUM_SCALE = 10;
    static final int BB_SCALE = 100;
    static final int COUNTDOWN_SCALE = 10;
    private static final String[] SEASONS = {"Winter", "Spring", "Summer", "Fall"};

    /** One seasonal or yearly chart placement. note: "feat. on X" / "by X", or null. */
    public record Placement(String name, String href, String periodKey, int position, String note) {
    }

    record Charts(List<HitCard> weeklySongs, List<HitCard> weeklyAlbums,
                  List<HitCard> bb, List<HitCard> pc, List<HitCard> trl,
                  List<Placement> seasonalSongs, List<Placement> yearlySongs,
                  List<Placement> seasonalAlbums, List<Placement> yearlyAlbums, boolean singleSong) {
    }

    /** bb / pc / trl rows may carry owner_id, owner_name and featured_on (the toggle-scoped artist queries). */
    public ChartHistoryViewDTO forArtist(int artistId, List<ChartHistoryDTO> songs, List<ChartHistoryDTO> albums,
                                         List<Map<String, Object>> seasonalSongs, List<Map<String, Object>> yearlySongs,
                                         List<Map<String, Object>> seasonalAlbums, List<Map<String, Object>> yearlyAlbums,
                                         List<Map<String, Object>> bb, List<Map<String, Object>> pc,
                                         List<Map<String, Object>> trl) {
        return build(new Charts(weeklyCards(songs), weeklyCards(albums),
                recapCards(bb, "bb", artistId), recapCards(pc, "pc", artistId), recapCards(trl, "trl", artistId),
                placements(seasonalSongs, "song"), placements(yearlySongs, "song"),
                placements(seasonalAlbums, "album"), placements(yearlyAlbums, "album"), false));
    }

    public ChartHistoryViewDTO forAlbum(int albumId, String albumName, List<ChartHistoryDTO> albumHistory,
                                        List<Map<String, Object>> albumSongs,
                                        List<Map<String, Object>> seasonalAlbum, List<Map<String, Object>> yearlyAlbum,
                                        List<Map<String, Object>> seasonalSongs, List<Map<String, Object>> yearlySongs,
                                        List<Map<String, Object>> bb, List<Map<String, Object>> pc,
                                        List<Map<String, Object>> trl) {
        String albumHref = "/albums/" + albumId;
        return build(new Charts(albumSongCards(albumSongs, albumId), weeklyCards(albumHistory),
                recapCards(bb, "bb", null), recapCards(pc, "pc", null), recapCards(trl, "trl", null),
                placements(seasonalSongs, "song"), placements(yearlySongs, "song"),
                itemPlacements(seasonalAlbum, albumName, albumHref), itemPlacements(yearlyAlbum, albumName, albumHref), false));
    }

    public ChartHistoryViewDTO forSong(int songId, String songName, Integer albumId, List<ChartHistoryDTO> songHistory,
                                       List<Map<String, Object>> seasonal, List<Map<String, Object>> yearly,
                                       Map<String, Object> bbStats, Map<String, Object> pcStats,
                                       Map<String, Object> trlStats) {
        String songHref = "/songs/" + songId;
        return build(new Charts(weeklyCards(songHistory), List.of(),
                songStatsCard(bbStats, "bb", songId, songName, albumId),
                songStatsCard(pcStats, "pc", songId, songName, albumId),
                songStatsCard(trlStats, "trl", songId, songName, albumId),
                itemPlacements(seasonal, songName, songHref), itemPlacements(yearly, songName, songHref),
                List.of(), List.of(), true));
    }

    ChartHistoryViewDTO build(Charts c) {
        List<Tile> tiles = new ArrayList<>();
        List<HitCard> weekly = concat(c.weeklySongs(), c.weeklyAlbums());
        if (!weekly.isEmpty()) {
            tiles.add(cardTile("weekly", "Weekly", "weekly", c.weeklySongs(), c.weeklyAlbums(), "week", "weekly", c.singleSong()));
        }
        List<Placement> seasonal = concat(c.seasonalSongs(), c.seasonalAlbums());
        if (!seasonal.isEmpty()) {
            tiles.add(placementTile("seasonal", "Seasonal", seasonal, "season", c.singleSong()));
        }
        List<Placement> yearly = concat(c.yearlySongs(), c.yearlyAlbums());
        if (!yearly.isEmpty()) {
            tiles.add(placementTile("yearly", "Yearly", yearly, "year", c.singleSong()));
        }
        if (!c.bb().isEmpty()) {
            tiles.add(cardTile("bb", "Hot 100", "bb", c.bb(), List.of(), "week", "bb", c.singleSong()));
        }
        if (!c.pc().isEmpty()) {
            tiles.add(cardTile("pc", "Vato's Cuntdown", "pc", c.pc(), List.of(), "day", "pc", c.singleSong()));
        }
        if (!c.trl().isEmpty()) {
            tiles.add(cardTile("trl", "TRL", "trl", c.trl(), List.of(), "day", "trl", c.singleSong()));
        }

        // No "Where they peaked" spread on the song page: every bar would be a single entry.
        List<SpreadRow> spread = new ArrayList<>();
        List<SpreadRow> periodSpread = new ArrayList<>();
        if (!c.singleSong()) {
            addSpread(spread, "Weekly songs", "weekly", c.weeklySongs());
            addSpread(spread, "Weekly albums", "weekly", c.weeklyAlbums());
            addSpread(spread, "Hot 100", "bb", c.bb());
            addSpread(spread, "Vato's Cuntdown", "pc", c.pc());
            addSpread(spread, "TRL", "trl", c.trl());

            addPeriodSpread(periodSpread, "Seasonal songs", c.seasonalSongs());
            addPeriodSpread(periodSpread, "Seasonal albums", c.seasonalAlbums());
            addPeriodSpread(periodSpread, "Yearly songs", c.yearlySongs());
            addPeriodSpread(periodSpread, "Yearly albums", c.yearlyAlbums());
        }

        List<HitSection> sections = new ArrayList<>();
        if (!weekly.isEmpty()) {
            sections.add(new HitSection("weekly", "Weekly hits", "wks", WEEKLY_SONG_SCALE, WEEKLY_ALBUM_SCALE,
                    "/charts/weekly/{key}", sorted(c.weeklySongs()), sorted(c.weeklyAlbums())));
        }
        addRecapSection(sections, "bb", "Hot 100 hits", "wks", BB_SCALE, "/misc/billboard-hot-100/recaps?date={key}", c.bb());
        addRecapSection(sections, "pc", "Vato's Cuntdown hits", "days", COUNTDOWN_SCALE, "/misc/vatos-cuntdown/recaps?date={key}", c.pc());
        addRecapSection(sections, "trl", "TRL hits", "days", COUNTDOWN_SCALE, "/misc/trl/recaps?date={key}", c.trl());

        Calendar calendar = new Calendar(grid("songs", c.seasonalSongs(), c.yearlySongs()),
                grid("albums", c.seasonalAlbums(), c.yearlyAlbums()));
        return new ChartHistoryViewDTO(tiles, spread, periodSpread, sections, calendar);
    }

    // ── Tiles and spread ──

    /**
     * Run chart tile: best peak with the weeks (or days) spent there across every entry that reached it,
     * then songs / albums, total time on the chart and how many entries hit #1 (top 10 when none did).
     * singleSong (song page): only the time on the chart, since the counts would always read 1.
     */
    private Tile cardTile(String key, String name, String tone, List<HitCard> songs, List<HitCard> albums,
                          String timeUnit, String target, boolean singleSong) {
        List<HitCard> cards = concat(songs, albums);
        int best = cards.stream().mapToInt(HitCard::peak).min().orElse(0);
        int atBest = cards.stream().filter(card -> card.peak() == best).mapToInt(card -> intOr(card.atPeak())).sum();
        List<TileStat> stats = new ArrayList<>();
        if (!singleSong && !songs.isEmpty()) stats.add(new TileStat(number(songs.size()), plural(songs.size(), "song")));
        if (!singleSong && !albums.isEmpty()) stats.add(new TileStat(number(albums.size()), plural(albums.size(), "album")));
        int time = cards.stream().mapToInt(HitCard::total).sum();
        stats.add(new TileStat(number(time), plural(time, timeUnit)));
        if (!singleSong) stats.add(hitsStat(cards.stream().map(HitCard::peak).toList()));
        return new Tile(key, name, tone, best, atBest, plural(atBest, timeUnit) + " at #" + best, stats, target);
    }

    /**
     * Seasonal / yearly tile: best placement with how many times it was reached, periods, entries, #1s.
     * singleSong (song page): no entries count, since it always equals the periods.
     */
    private Tile placementTile(String key, String name, List<Placement> placements, String periodUnit, boolean singleSong) {
        int best = placements.stream().mapToInt(Placement::position).min().orElse(0);
        int atBest = (int) placements.stream().filter(p -> p.position() == best).count();
        Set<String> periods = new HashSet<>();
        placements.forEach(p -> periods.add(p.periodKey()));
        List<TileStat> stats = new ArrayList<>();
        stats.add(new TileStat(number(periods.size()), plural(periods.size(), periodUnit)));
        if (!singleSong) stats.add(new TileStat(number(placements.size()), plural(placements.size(), "entry")));
        stats.add(hitsStat(placements.stream().map(Placement::position).toList()));
        return new Tile(key, name, "weekly", best, atBest, plural(atBest, "time") + " at #" + best, stats, "calendar");
    }

    /** Entries that reached #1, or the top 10 when none did. */
    private static TileStat hitsStat(List<Integer> peaks) {
        long ones = peaks.stream().filter(p -> p == 1).count();
        if (ones > 0) {
            return new TileStat(number(ones), "at #1");
        }
        return new TileStat(number(peaks.stream().filter(p -> p <= 10).count()), "top 10");
    }

    private static String number(long value) {
        return String.format(java.util.Locale.ENGLISH, "%,d", value);
    }

    /** Unit word for a count: 1 week, 2 weeks, 3 entries (the count itself is shown separately). */
    private static String plural(long n, String unit) {
        String counted = count(n, unit);
        return counted.substring(counted.indexOf(' ') + 1);
    }

    private void addSpread(List<SpreadRow> rows, String name, String tone, List<HitCard> cards) {
        addSpreadRow(rows, name, tone, cards.stream().map(HitCard::peak).toList());
    }

    /** One count per song or album: its best placement across the periods. */
    private void addPeriodSpread(List<SpreadRow> rows, String name, List<Placement> placements) {
        Map<String, Integer> best = new java.util.LinkedHashMap<>();
        for (Placement p : placements) {
            String key = p.href() != null ? p.href() : String.valueOf(p.name());
            best.merge(key, p.position(), Math::min);
        }
        addSpreadRow(rows, name, "weekly", List.copyOf(best.values()));
    }

    private void addSpreadRow(List<SpreadRow> rows, String name, String tone, List<Integer> peaks) {
        if (peaks.isEmpty()) {
            return;
        }
        int[] counts = new int[4];
        peaks.forEach(peak -> counts[bandIndex(peak)]++);
        String[] bands = {"one", "top5", "top10", "rest"};
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            if (counts[i] > 0) {
                segments.add(new Segment(bands[i], counts[i]));
            }
        }
        rows.add(new SpreadRow(name, tone, segments, peaks.size()));
    }

    private void addRecapSection(List<HitSection> sections, String key, String title, String unit, int scale,
                                 String periodHref, List<HitCard> cards) {
        if (!cards.isEmpty()) {
            sections.add(new HitSection(key, title, unit, scale, scale, periodHref, sorted(cards), List.of()));
        }
    }

    static int bandIndex(int position) {
        if (position <= 1) return 0;
        if (position <= 5) return 1;
        if (position <= 10) return 2;
        return 3;
    }

    static String band(int position) {
        return switch (bandIndex(position)) {
            case 0 -> "one";
            case 1 -> "top5";
            case 2 -> "top10";
            default -> "rest";
        };
    }

    // ── Calendar ──

    CalendarGrid grid(String view, List<Placement> seasonal, List<Placement> yearly) {
        if (seasonal.isEmpty() && yearly.isEmpty()) {
            return null;
        }
        TreeMap<Integer, List<List<Placement>>> byYear = new TreeMap<>();
        for (Placement p : seasonal) {
            String[] parts = p.periodKey() == null ? new String[0] : p.periodKey().split("-");
            Integer year = parts.length == 2 ? parseYear(parts[0]) : null;
            int season = parts.length == 2 ? seasonIndex(parts[1]) : -1;
            if (year != null && season >= 0) {
                cellsFor(byYear, year).get(season).add(p);
            }
        }
        for (Placement p : yearly) {
            Integer year = parseYear(p.periodKey());
            if (year != null) {
                cellsFor(byYear, year).get(4).add(p);
            }
        }
        if (byYear.isEmpty()) {
            return null;
        }
        String prefix = view.substring(0, 1);
        List<CalendarYear> years = new ArrayList<>();
        String selectedId = null;
        int selectedPos = Integer.MAX_VALUE;
        int seasonsCharted = 0;
        int numberOnes = 0;
        for (int year = byYear.firstKey(); year <= byYear.lastKey(); year++) {
            List<List<Placement>> lists = byYear.containsKey(year) ? byYear.get(year) : emptyCells();
            List<CalendarCell> cells = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                List<CalendarItem> items = lists.get(i).stream()
                        .sorted(Comparator.comparingInt(Placement::position).thenComparing(Placement::name, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)))
                        .map(p -> new CalendarItem(p.name(), p.href(), p.position(), p.note()))
                        .toList();
                String id = prefix + "-" + year + "-" + i;
                boolean season = i < 4;
                String label = season ? SEASONS[i] + " " + year : year + " year-end";
                String href = season ? "/charts/seasonal/" + year + "-" + SEASONS[i] : "/charts/yearly/" + year;
                String band = items.isEmpty() ? "none" : band(items.get(0).position());
                cells.add(new CalendarCell(id, label, season ? "season" : "year", href, band, items));
                if (!items.isEmpty()) {
                    if (season) seasonsCharted++;
                    if (items.get(0).position() == 1) numberOnes++;
                    // Best placement wins; on ties the later cell (more recent) wins
                    if (items.get(0).position() <= selectedPos) {
                        selectedPos = items.get(0).position();
                        selectedId = id;
                    }
                }
            }
            years.add(new CalendarYear(year, cells));
        }
        return new CalendarGrid(view, years, selectedId, seasonsCharted, numberOnes);
    }

    private static List<List<Placement>> cellsFor(TreeMap<Integer, List<List<Placement>>> byYear, int year) {
        return byYear.computeIfAbsent(year, y -> emptyCells());
    }

    private static List<List<Placement>> emptyCells() {
        List<List<Placement>> cells = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cells.add(new ArrayList<>());
        }
        return cells;
    }

    private static int seasonIndex(String season) {
        for (int i = 0; i < SEASONS.length; i++) {
            if (SEASONS[i].equalsIgnoreCase(season)) return i;
        }
        return -1;
    }

    private static Integer parseYear(String value) {
        if (value == null || !value.matches("\\d{4}")) return null;
        return Integer.parseInt(value);
    }

    // ── Card conversion ──

    List<HitCard> weeklyCards(List<ChartHistoryDTO> history) {
        if (history == null) return List.of();
        List<HitCard> cards = new ArrayList<>();
        for (ChartHistoryDTO d : history) {
            if (d.getId() == null || d.getPeakPosition() == null) continue;
            boolean album = "album".equals(d.getChartType());
            String cover;
            String fallback = null;
            if (album) {
                cover = albumImage(d.getId());
            } else if (d.isHasImage()) {
                cover = songImage(d.getId());
                fallback = d.getAlbumId() != null ? albumImage(d.getAlbumId()) : null;
            } else {
                cover = d.getAlbumId() != null ? albumImage(d.getAlbumId()) : null;
            }
            String sub = null;
            String subHref = null;
            if (d.getSourceArtistName() != null && (d.isFeaturedOn() || d.isFromGroup())) {
                sub = (d.isFeaturedOn() ? "feat. on " : "by ") + d.getSourceArtistName();
                subHref = d.getSourceArtistId() != null ? "/artists/" + d.getSourceArtistId() : null;
            } else if (d.getReleaseDate() != null) {
                sub = "Released " + d.getReleaseDate();
            }
            cards.add(new HitCard(album ? "album" : "song", d.getId(), d.getName(),
                    (album ? "/albums/" : "/songs/") + d.getId(), sub, subHref, cover, fallback, initials(d.getName()),
                    d.getPeakPosition(), intOr(d.getTotalWeeks()), d.getWeeksAtPeak(),
                    d.getDebutDate(), d.getDebutWeek() != null ? "/charts/weekly/" + d.getDebutWeek() : null,
                    d.getPeakDate(), d.getPeakWeek() != null ? "/charts/weekly/" + d.getPeakWeek() : null,
                    (album ? "/charts/album/" : "/charts/song/") + d.getId() + "/run",
                    d.getInItunes(), d.getTotalWeekBreakdownItems()));
        }
        return cards;
    }

    List<HitCard> albumSongCards(List<Map<String, Object>> albumSongs, int albumId) {
        if (albumSongs == null) return List.of();
        List<HitCard> cards = new ArrayList<>();
        for (Map<String, Object> s : albumSongs) {
            Integer peak = toInt(s.get("peakPosition"));
            Integer id = toInt(s.get("id"));
            if (!Boolean.TRUE.equals(s.get("charted")) || peak == null || id == null) continue;
            String name = str(s.get("name"));
            String debutWeek = str(s.get("debutWeek"));
            String peakWeek = str(s.get("peakWeek"));
            cards.add(new HitCard("song", id, name, "/songs/" + id, null, null,
                    songImage(id), albumImage(albumId), initials(name),
                    peak, intOr(toInt(s.get("totalWeeks"))), toInt(s.get("weeksAtPeak")),
                    str(s.get("debutDate")), debutWeek != null ? "/charts/weekly/" + debutWeek : null,
                    str(s.get("peakDate")), peakWeek != null ? "/charts/weekly/" + peakWeek : null,
                    "/charts/song/" + id + "/run", s.get("inItunes") instanceof Boolean b ? b : null, null));
        }
        return cards;
    }

    /**
     * Hot 100 / Cuntdown / TRL rows (song_id, song_name, weeks_on_chart or days_on_countdown, ...). On the artist
     * page (pageArtistId set) a song owned by someone else gets "feat. on X" (featured_on = 1) or "by X" (a group).
     */
    List<HitCard> recapCards(List<Map<String, Object>> rows, String chart, Integer pageArtistId) {
        if (rows == null) return List.of();
        boolean bb = "bb".equals(chart);
        List<HitCard> cards = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Integer id = toInt(r.get("song_id"));
            Integer peak = toInt(r.get("peak_position"));
            if (id == null || peak == null) continue;
            String name = str(r.get("song_name"));
            String debut = str(r.get("debut_date"));
            String peakDate = str(r.get("peak_date"));
            Integer ownerId = toInt(r.get("owner_id"));
            String ownerName = str(r.get("owner_name"));
            String sub = null;
            String subHref = null;
            if (pageArtistId != null && ownerId != null && ownerName != null && !ownerId.equals(pageArtistId)) {
                Integer featured = toInt(r.get("featured_on"));
                sub = (featured != null && featured == 1 ? "feat. on " : "by ") + ownerName;
                subHref = "/artists/" + ownerId;
            }
            cards.add(new HitCard("song", id, name, "/songs/" + id, sub, subHref, songImage(id), null, initials(name),
                    peak, intOr(toInt(r.get(bb ? "weeks_on_chart" : "days_on_countdown"))),
                    toInt(r.get(bb ? "weeks_at_peak" : "days_at_peak")),
                    displayDate(debut), recapHref(chart, debut), displayDate(peakDate), recapHref(chart, peakDate),
                    "/charts/" + chart + "/song/" + id + "/run", null, null));
        }
        return cards;
    }

    /** The song page's own Hot 100 / Cuntdown / TRL stats (weeksOnChart or daysOnCountdown, ...). */
    List<HitCard> songStatsCard(Map<String, Object> stats, String chart, int songId, String songName, Integer albumId) {
        if (stats == null) return List.of();
        Integer peak = toInt(stats.get("peakPosition"));
        if (peak == null) return List.of();
        boolean bb = "bb".equals(chart);
        String debut = str(stats.get("debutDate"));
        String peakDate = str(stats.get("peakDate"));
        return List.of(new HitCard("song", songId, songName, "/songs/" + songId, null, null,
                songImage(songId), albumId != null ? albumImage(albumId) : null, initials(songName),
                peak, intOr(toInt(stats.get(bb ? "weeksOnChart" : "daysOnCountdown"))),
                toInt(stats.get(bb ? "weeksAtPeak" : "daysAtPeak")),
                displayDate(debut), recapHref(chart, debut), displayDate(peakDate), recapHref(chart, peakDate),
                "/charts/" + chart + "/song/" + songId + "/run", null, null));
    }

    /** Artist / album song placements (songId+songName or albumId+albumName, featuredOn, fromGroup). */
    List<Placement> placements(List<Map<String, Object>> rows, String kind) {
        if (rows == null) return List.of();
        boolean song = "song".equals(kind);
        List<Placement> result = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Integer position = toInt(r.get("position"));
            Integer id = toInt(r.get(song ? "songId" : "albumId"));
            if (position == null) continue;
            String note = null;
            String source = str(r.get("sourceArtistName"));
            if (source != null && Boolean.TRUE.equals(r.get("featuredOn"))) {
                note = "feat. on " + source;
            } else if (source != null && Boolean.TRUE.equals(r.get("fromGroup"))) {
                note = "by " + source;
            }
            result.add(new Placement(str(r.get(song ? "songName" : "albumName")),
                    id != null ? (song ? "/songs/" : "/albums/") + id : null,
                    str(r.get("periodKey")), position, note));
        }
        return result;
    }

    /** Placements of the page's own item (position + periodKey only). */
    List<Placement> itemPlacements(List<Map<String, Object>> rows, String name, String href) {
        if (rows == null) return List.of();
        List<Placement> result = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Integer position = toInt(r.get("position"));
            if (position != null) {
                result.add(new Placement(name, href, str(r.get("periodKey")), position, null));
            }
        }
        return result;
    }

    // ── Helpers ──

    private static List<HitCard> sorted(List<HitCard> cards) {
        return cards.stream()
                .sorted(Comparator.comparingInt(HitCard::peak)
                        .thenComparing((HitCard c) -> intOr(c.atPeak()), Comparator.reverseOrder())
                        .thenComparing(HitCard::total, Comparator.reverseOrder())
                        .thenComparing(HitCard::name, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)))
                .toList();
    }

    private static <T> List<T> concat(List<T> a, List<T> b) {
        List<T> all = new ArrayList<>(a);
        all.addAll(b);
        return all;
    }

    static String count(long n, String unit) {
        boolean consonantY = unit.length() > 1 && unit.endsWith("y") && "aeiou".indexOf(unit.charAt(unit.length() - 2)) < 0;
        String plural = consonantY ? unit.substring(0, unit.length() - 1) + "ies" : unit + "s";
        return n + " " + (n == 1 ? unit : plural);
    }

    static String initials(String name) {
        if (name == null || name.isBlank()) return "-";
        StringBuilder sb = new StringBuilder();
        for (String word : name.trim().split("\\s+")) {
            int cp = word.codePoints().filter(Character::isLetterOrDigit).findFirst().orElse(-1);
            if (cp >= 0) sb.appendCodePoint(Character.toUpperCase(cp));
            if (sb.length() >= 2) break;
        }
        return sb.isEmpty() ? "-" : sb.toString();
    }

    /** yyyy-MM-dd to dd/MM/yyyy; anything else is returned as is. */
    static String displayDate(String value) {
        if (value == null || value.isBlank()) return null;
        String day = value.length() > 10 ? value.substring(0, 10) : value;
        if (day.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return day.substring(8, 10) + "/" + day.substring(5, 7) + "/" + day.substring(0, 4);
        }
        return value;
    }

    private static String recapHref(String chart, String date) {
        if (date == null) return null;
        String base = switch (chart) {
            case "bb" -> "/misc/billboard-hot-100/recaps";
            case "pc" -> "/misc/vatos-cuntdown/recaps";
            default -> "/misc/trl/recaps";
        };
        return base + "?date=" + URLEncoder.encode(date, StandardCharsets.UTF_8);
    }

    private static String songImage(int id) {
        return "/songs/" + id + "/image?thumbnail=true";
    }

    private static String albumImage(int id) {
        return "/albums/" + id + "/image?thumbnail=true";
    }

    private static int intOr(Integer value) {
        return value != null ? value : 0;
    }

    private static Integer toInt(Object value) {
        return value instanceof Number n ? n.intValue() : null;
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
