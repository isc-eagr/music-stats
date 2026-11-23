package library.service;

import library.service.CatalogDetailService.BreakdownRow;
import library.service.CatalogDetailService.Dimension;
import library.service.CatalogDetailService.GenderRow;
import library.service.CatalogDetailService.MixCard;
import library.service.CatalogDetailService.MixRow;
import library.service.CatalogDetailService.PeerRow;
import library.service.CatalogDetailService.Rhythm;
import library.service.CatalogDetailService.Stream;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Detail page of a timeframe (day, week, month, season, year, decade). Same layout as the catalog detail pages,
 * with sections that only make sense for a period: who won each slice, streaks, sessions and runs, discoveries,
 * comebacks and farewells. Each section adapts its step and thresholds to the period type.
 */
@Service
public class TimeframeDetailService {

    /** A session ends when the next play is more than this many minutes away. */
    static final int SESSION_GAP_MINUTES = 25;
    /** Same artist back to back at least this many times counts as a run. */
    static final int RUN_MIN = 8;
    private static final int TOP_SIZE = 50;
    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter HM = DateTimeFormatter.ofPattern("HH:mm");
    private static final Map<String, List<String>> STEPS = Map.of(
            "decades", List.of("years"), "years", List.of("months", "weeks"), "seasons", List.of("weeks"),
            "months", List.of("days"), "weeks", List.of("days"), "days", List.of("hours"));
    private static final Map<String, List<String>> SLICES = Map.of(
            "decades", List.of("years", "months", "weeks"), "years", List.of("months", "weeks", "days"),
            "seasons", List.of("months", "weeks", "days"), "months", List.of("weeks", "days"),
            "weeks", List.of("days", "sessions"), "days", List.of("sessions", "hours"));
    private static final Map<String, Integer> COMEBACK_DAYS = Map.of(
            "decades", 730, "years", 365, "seasons", 182, "months", 182, "weeks", 90, "days", 90);
    private static final List<Dimension> MIX_DIMS = List.of(Dimension.LANGUAGE, Dimension.ETHNICITY, Dimension.COUNTRY);
    private static final Map<Dimension, String> MIX_COLORS = Map.of(
            Dimension.LANGUAGE, "#a78bfa", Dimension.ETHNICITY, "#fbbf24", Dimension.COUNTRY, "#34d399");

    public record NavLink(String label, String href) {}
    /** One hero standing: a big rank ({@code top} = podium, drawn gold), what it ranks against and a short note. */
    public record Standing(String value, String label, String note, boolean top) {}
    public record ChartLink(String text, String href) {}
    /** Hero tile with an optional note under the value ({@code tone} up, down or empty). */
    public record Tile(String label, String value, String note, String tone, String href) {}
    public record Pill(String label, String plays, String time, int malePct, boolean best, String href, String title) {}
    /** One step of the Across card (months, weeks, days, hours): plays and time streams with a pill per slice. */
    public record Step(String id, String label, Stream plays, Stream time, List<Pill> pills) {}
    /** Session strip under the hours of a Day page; positions are percentages of the day. */
    public record Segment(double left, double width, String label, String title) {}
    public record Tally(String label, int male, int female, int ties, int malePct, String note) {}
    public record Tick(double left, String kind, String title) {}
    /** A streak: inside the period ({@code left}/{@code width} on the period timeline) or around it ({@code marker} on the streak). */
    public record Streak(String label, String kind, int days, String range, String note, double left, double width, Double marker) {}
    public record Streaks(boolean around, List<Streak> streaks, List<Tick> ticks, List<String> axis) {}
    public record SessionRow(String range, int plays, String length, int barPct, String href) {}
    public record Run(String title, String sub, int plays, String href, String img) {}
    public record Sessions(String count, String perDay, String longest, String longestNote, String longestHref,
                           List<SessionRow> rows, List<Run> runs) {}
    public record TopRow(int rank, String name, String sub, String plays, int barPct, String gender, String href, String img) {}
    public record TopColumn(String title, String note, List<TopRow> all, List<TopRow> male, List<TopRow> female) {}
    public record Discovery(String name, String gender, String first, String plays, String since, int insidePct,
                            boolean faded, String href, String img) {}
    public record Discoveries(String note, boolean showSince, List<Discovery> artists, List<Discovery> songs) {}
    public record Comeback(String name, String gender, String sub, String gap, int gapPx, String plays, String href, String img) {}
    public record Farewell(String name, String gender, String sub, String lifetime, String href, String img) {}
    public record DateBar(String label, int height, int femalePct, boolean self, String href, String title) {}

    public record Detail(TimeframePeriod period, String name, String eyebrow, String sub, String theme, String listLabel,
                         NavLink prev, NavLink next, List<NavLink> partOf, String containsLabel, List<NavLink> contains,
                         List<Standing> standing, List<Tile> tiles, List<GenderRow> gender, List<BreakdownRow> genres,
                         List<MixCard> mixes, List<Tally> tallies, List<Step> steps, List<Segment> sessionStrip,
                         Streaks streaks, Sessions sessions, String topNote, ChartLink chart, List<TopColumn> tops,
                         Discoveries discoveries, List<Comeback> comebacks, String comebackNote, List<Farewell> farewells,
                         String peersCaption, String peersNote, List<PeerRow> byPlays, List<PeerRow> byMale, List<DateBar> sameDate,
                         Rhythm rhythm, boolean showWeek, boolean showMonth, long plays, boolean inProgress) {}

    private final JdbcTemplate jdbcTemplate;

    public TimeframeDetailService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** One play with what every section needs; {@code edge} plays (the hours around the period) only extend sessions. */
    private record P(LocalDateTime at, int song, int artist, Integer album, long len, int gender, int artistGender,
                     Map<Dimension, String> keys, boolean edge) {
        LocalDate day() {
            return at.toLocalDate();
        }
    }

    private record Session(LocalDateTime start, LocalDateTime end, List<P> plays) {
        long minutes() {
            return ChronoUnit.MINUTES.between(start, end);
        }
    }

    public Optional<Detail> build(String type, String key) {
        return build(type, key, LocalDate.now());
    }

    /** Builds the page as seen on {@code today}: the current period is "in progress", future periods have no page. */
    public Optional<Detail> build(String type, String key, LocalDate today) {
        Optional<TimeframePeriod> parsed = TimeframePeriod.parse(type, key);
        if (parsed.isEmpty() || parsed.get().from().isAfter(today)) return Optional.empty();
        TimeframePeriod period = parsed.get();
        boolean inProgress = period.contains(today);
        LocalDate last = inProgress ? today : period.to();
        long elapsed = ChronoUnit.DAYS.between(period.from(), last) + 1;

        List<P> all = plays(period);
        List<P> rows = all.stream().filter(p -> !p.edge).toList();
        Map<LocalDate, long[]> daily = daily();
        Map<Dimension, Map<String, String>> names = lookupNames();
        Map<Integer, Integer> genderIds = genderIds();

        long plays = rows.size(), time = 0;
        long[] playsBy = new long[3], timeBy = new long[3];
        List<Set<Integer>> artistsBy = sets(), albumsBy = sets(), songsBy = sets();
        Set<Integer> artists = new HashSet<>(), albums = new HashSet<>(), songs = new HashSet<>();
        Set<LocalDate> daysPlayed = new HashSet<>();
        for (P p : rows) {
            time += p.len;
            artists.add(p.artist);
            songs.add(p.song);
            if (p.album != null) albums.add(p.album);
            daysPlayed.add(p.day());
            if (p.gender < 0) continue;
            playsBy[p.gender]++;
            timeBy[p.gender] += p.len;
            artistsBy.get(p.gender).add(p.artist);
            songsBy.get(p.gender).add(p.song);
            if (p.album != null) albumsBy.get(p.gender).add(p.album);
        }
        String theme = playsBy[0] > playsBy[1] ? "gender-male" : playsBy[1] > playsBy[0] ? "gender-female" : "";
        String range = "listenedDateFrom=" + period.from() + "&listenedDateTo=" + period.to() + "&periodLabel=" + enc(period.name());

        // Gender breakdown (same five columns as the catalog pages), genres and the mixes
        String[][] lists = {{"Artists", "artists"}, {"Albums", "albums"}, {"Songs", "songs"}};
        long[][] counts = {sizes(artistsBy), sizes(albumsBy), sizes(songsBy)};
        List<GenderRow> gender = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int[] s = CatalogDetailService.shares(counts[i]);
            String base = "/" + lists[i][1] + "?" + range + "&view=table&genderMode=includes&gender=";
            gender.add(new GenderRow(lists[i][0], fmt(counts[i][0]), fmt(counts[i][1]), s[0], s[2], s[1],
                    genderIds.containsKey(0) ? base + genderIds.get(0) : null, genderIds.containsKey(1) ? base + genderIds.get(1) : null));
        }
        int[] ps = CatalogDetailService.shares(playsBy), ts = CatalogDetailService.shares(timeBy);
        gender.add(new GenderRow("Plays", fmt(playsBy[0]), fmt(playsBy[1]), ps[0], ps[2], ps[1], null, null));
        gender.add(new GenderRow("Time", TimeFormatUtils.formatTime(timeBy[0]), TimeFormatUtils.formatTime(timeBy[1]), ts[0], ts[2], ts[1], null, null));
        List<BreakdownRow> genres = breakdown(rows, Dimension.GENRE, names, range);
        List<MixCard> mixes = new ArrayList<>();
        for (Dimension d : MIX_DIMS) mixes.add(mix(rows, d, names, range, plays));

        // Slices, sessions and the Across card
        List<Session> sessions = sessions(all, period);
        List<Step> steps = new ArrayList<>();
        for (String step : STEPS.get(type)) steps.add(step(rows, period, step, last));
        List<Tally> tallies = new ArrayList<>();
        for (String slice : SLICES.get(type)) tallies.add(tally(rows, sessions, period, slice));
        List<Segment> strip = "days".equals(type) ? strip(sessions, period) : List.of();

        Streaks streaks = streaks(period, daily, last);
        Sessions sessionCard = sessionCard(sessions, period, daysPlayed.size());

        // Lifetime numbers of the period's artists: discoveries, comebacks, farewells
        Map<Integer, Object[]> artistNames = artistInfo(artists);
        Map<Integer, String[]> life = artistLife(period);
        Map<Integer, Long> inside = rows.stream().collect(Collectors.groupingBy(P::artist, Collectors.counting()));
        Map<Integer, LocalDateTime> firstInside = new HashMap<>();
        for (P p : rows) firstInside.putIfAbsent(p.artist, p.at);

        List<TopColumn> tops = tops(rows, artistNames);
        long topMale = top(rows, P::artist, p -> p.artistGender, TOP_SIZE).stream().filter(e -> e.getValue()[1] == 0).count();
        int topCount = Math.min(TOP_SIZE, artists.size());
        String topNote = topCount == 0 ? null : topMale + " of your top " + topCount + " artists are male";

        Discoveries discoveries = discoveries(period, rows, life, inside, artistNames, inProgress);
        List<Comeback> comebacks = comebacks(period, life, inside, firstInside, artistNames);
        List<Farewell> farewells = inProgress ? List.of() : farewells(period, life, artistNames, today);

        // Peers of the same type (same season for seasons) from the daily totals
        Map<String, long[]> peerTotals = new HashMap<>();
        Map<String, TimeframePeriod> peerPeriods = new HashMap<>();
        for (var e : daily.entrySet()) {
            TimeframePeriod p = TimeframePeriod.of(type, e.getKey());
            if (!p.season().equals(period.season())) continue;
            peerPeriods.putIfAbsent(p.key(), p);
            long[] t = peerTotals.computeIfAbsent(p.key(), k -> new long[4]);
            for (int i = 0; i < 4; i++) t[i] += e.getValue()[i];
        }
        List<PeerRow> byPlays = new ArrayList<>(), byMale = new ArrayList<>();
        int[] ranks = peers(peerTotals, peerPeriods, key, byPlays, byMale);
        TimeframePeriod current = TimeframePeriod.of(type, today);
        boolean currentIsPeer = peerTotals.containsKey(current.key());
        String peersCaption = "Where " + period.name() + " sits among all " + peerTotals.size() + " " + period.plural();
        String peersNote = currentIsPeer ? current.name() + " still in progress" : "All time";

        List<DateBar> sameDate = "days".equals(type) ? sameDate(period, daily) : List.of();

        // Hero
        TimeframePeriod comparison = period.comparison();
        LocalDate compEnd = inProgress ? comparison.from().plusDays(elapsed - 1) : comparison.to();
        long[] compTotals = sum(daily, comparison.from(), compEnd);
        long[] compDistinct = distinctCounts(comparison.from(), compEnd);
        String compName = comparison.name() + (inProgress ? " to date" : "");
        List<Tile> tiles = tiles(period, plays, time, artists.size(), albums.size(), songs.size(),
                discoveries.artists().size(), compTotals, compDistinct, compName, range);
        List<Standing> standing = standing(period, ranks, peerTotals.size(), playsBy, inProgress);

        String sub;
        if ("days".equals(type)) {
            sub = rows.isEmpty() ? "No plays this day" : "First play " + rows.get(0).at.format(HM) + " · last play "
                    + rows.get(rows.size() - 1).at.format(HM);
        } else {
            sub = period.from().format(DMY) + " – " + (inProgress ? "today " + today.format(DMY) + " · day " + elapsed + " of "
                    + period.days() : period.to().format(DMY) + " · " + period.days() + " days");
        }
        String eyebrow = "days".equals(type)
                ? "Day · " + period.from().getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH)
                : period.typeLabel() + (inProgress ? " · in progress" : " · listened");
        TimeframePeriod prev = period.previous();
        TimeframePeriod next = period.next();
        NavLink prevLink = new NavLink(prev.name(), prev.href());
        NavLink nextLink = next.from().isAfter(today) ? null : new NavLink(next.name(), next.href());

        Rhythm rhythm = rhythm(rows);
        // When you play it skips what the Across card already shows or what would be mostly blank: Day of week on a
        // week (its days are the Across step), Month of year below a decade (a year's months are its Across step, a
        // season or month only fills a few bars). Day pages hide the whole card.
        boolean showWeek = !"weeks".equals(type);
        boolean showMonth = "decades".equals(type);
        return Optional.of(new Detail(period, period.name(), eyebrow, sub, theme, capitalize(type), prevLink, nextLink,
                partOf(period), "days".equals(type) ? "Same date" : "Contains", contains(period, daily, today),
                standing, tiles, gender, genres, mixes, tallies, steps, strip, streaks, sessionCard, topNote, chartLink(period), tops,
                discoveries, comebacks, comebackNote(type), farewells, peersCaption, peersNote, trim(byPlays), trim(byMale),
                sameDate, rhythm, showWeek, showMonth, plays, inProgress));
    }

    // ── Data ──

    private List<P> plays(TimeframePeriod period) {
        String keys = String.join(", ", java.util.Arrays.stream(Dimension.values()).map(d -> d.songExpr + " AS k_" + d.param).toList());
        LocalDateTime start = period.from().atStartOfDay(), end = period.to().plusDays(1).atStartOfDay();
        LocalDateTime headFrom = start.minusHours(6), tailTo = end.plusHours(6);
        return jdbcTemplate.query("SELECT p.play_date AS d, s.id AS sid, s.artist_id AS aid, s.album_id AS alid,"
                + " COALESCE(s.length_seconds, 0) AS len, gn.name AS gname, ag.name AS agname, " + keys
                + " FROM Play p JOIN Song s ON s.id = p.song_id JOIN Artist ar ON ar.id = s.artist_id"
                + " LEFT JOIN Album al ON al.id = s.album_id"
                + " LEFT JOIN Gender gn ON gn.id = COALESCE(s.override_gender_id, ar.gender_id)"
                + " LEFT JOIN Gender ag ON ag.id = ar.gender_id"
                + " WHERE p.play_date >= ? AND p.play_date < ? ORDER BY p.play_date, p.id", (rs, i) -> {
                    LocalDateTime at = parse(rs.getString("d"));
                    Map<Dimension, String> k = new HashMap<>();
                    for (Dimension d : Dimension.values()) k.put(d, rs.getString("k_" + d.param));
                    int album = rs.getInt("alid");
                    Integer albumId = rs.wasNull() ? null : album;
                    boolean edge = at.isBefore(start) || !at.isBefore(end);
                    return new P(at, rs.getInt("sid"), rs.getInt("aid"), albumId, rs.getLong("len"),
                            CatalogDetailService.bucket(rs.getString("gname")), CatalogDetailService.bucket(rs.getString("agname")), k, edge);
                }, sqlTime(headFrom), sqlTime(tailTo));
    }

    /** Plays, male plays, female plays and seconds per day, over every play ever (the initial load day left out). */
    private Map<LocalDate, long[]> daily() {
        Map<LocalDate, long[]> out = new TreeMap<>();
        jdbcTemplate.query("SELECT SUBSTR(p.play_date, 1, 10) AS d, gn.name AS gname, COUNT(*) AS n,"
                + " SUM(COALESCE(s.length_seconds, 0)) AS secs FROM Play p JOIN Song s ON s.id = p.song_id"
                + " JOIN Artist ar ON ar.id = s.artist_id LEFT JOIN Gender gn ON gn.id = COALESCE(s.override_gender_id, ar.gender_id)"
                + " WHERE p.play_date IS NOT NULL GROUP BY 1, 2", (RowCallbackHandler) rs -> {
                    String d = rs.getString("d");
                    if (d == null || d.length() < 10 || d.equals(MaleFunFactsService.EXCLUDED_DAY)) return;
                    LocalDate day;
                    try {
                        day = LocalDate.parse(d);
                    } catch (RuntimeException e) {
                        return;
                    }
                    long[] v = out.computeIfAbsent(day, x -> new long[4]);
                    long n = rs.getLong("n");
                    int g = CatalogDetailService.bucket(rs.getString("gname"));
                    v[0] += n;
                    if (g == 0) v[1] += n;
                    if (g == 1) v[2] += n;
                    v[3] += rs.getLong("secs");
                });
        return out;
    }

    /** Per artist of the period: {first play, last play, lifetime plays, last play before the period, plays after it}. */
    private Map<Integer, String[]> artistLife(TimeframePeriod period) {
        String from = period.from().toString(), after = period.to().plusDays(1).toString();
        Map<Integer, String[]> out = new HashMap<>();
        jdbcTemplate.query("SELECT s.artist_id AS aid, MIN(p.play_date) AS f, MAX(p.play_date) AS l, COUNT(*) AS n,"
                + " MAX(CASE WHEN p.play_date < ? THEN p.play_date END) AS prev,"
                + " SUM(CASE WHEN p.play_date >= ? THEN 1 ELSE 0 END) AS later"
                + " FROM Play p JOIN Song s ON s.id = p.song_id WHERE s.artist_id IN (SELECT DISTINCT s2.artist_id FROM Play p2"
                + " JOIN Song s2 ON s2.id = p2.song_id WHERE p2.play_date >= ? AND p2.play_date < ?) GROUP BY s.artist_id",
                (RowCallbackHandler) rs -> out.put(rs.getInt("aid"), new String[]{rs.getString("f"), rs.getString("l"),
                        rs.getString("n"), rs.getString("prev"), rs.getString("later")}), from, after, from, after);
        return out;
    }

    private long[] distinctCounts(LocalDate from, LocalDate to) {
        return jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT s.artist_id) AS a, COUNT(DISTINCT s.album_id) AS al,"
                + " COUNT(DISTINCT s.id) AS so FROM Play p JOIN Song s ON s.id = p.song_id WHERE p.play_date >= ? AND p.play_date < ?",
                (rs, i) -> new long[]{rs.getLong("a"), rs.getLong("al"), rs.getLong("so")}, from.toString(), to.plusDays(1).toString());
    }

    /** Artist id to {name, gender bucket}. */
    private Map<Integer, Object[]> artistInfo(Collection<Integer> ids) {
        Map<Integer, Object[]> out = new HashMap<>();
        if (ids.isEmpty()) return out;
        jdbcTemplate.query("SELECT ar.id, ar.name, gn.name AS gname FROM Artist ar LEFT JOIN Gender gn ON gn.id = ar.gender_id",
                (RowCallbackHandler) rs -> {
                    if (ids.contains(rs.getInt("id"))) {
                        out.put(rs.getInt("id"), new Object[]{rs.getString("name"), CatalogDetailService.bucket(rs.getString("gname"))});
                    }
                });
        return out;
    }

    private Map<Dimension, Map<String, String>> lookupNames() {
        Map<Dimension, Map<String, String>> names = new HashMap<>();
        for (Dimension d : Dimension.values()) {
            if (d.byName()) continue;
            Map<String, String> m = new HashMap<>();
            jdbcTemplate.query("SELECT id, name FROM " + d.table, (RowCallbackHandler) rs -> m.put(rs.getString("id"), rs.getString("name")));
            names.put(d, m);
        }
        return names;
    }

    private Map<Integer, Integer> genderIds() {
        Map<Integer, Integer> ids = new HashMap<>();
        jdbcTemplate.query("SELECT id, name FROM Gender ORDER BY id", (RowCallbackHandler) rs -> {
            int b = CatalogDetailService.bucket(rs.getString("name"));
            if (b == 0 || b == 1) ids.putIfAbsent(b, rs.getInt("id"));
        });
        return ids;
    }

    // ── Breakdowns ──

    private static final class Agg {
        final String key;
        long plays, time;
        final long[] by = new long[3];

        Agg(String key) {
            this.key = key;
        }
    }

    private static List<Agg> aggregate(List<P> rows, Dimension by) {
        Map<String, Agg> map = new LinkedHashMap<>();
        for (P p : rows) {
            String k = p.keys.get(by);
            Agg a = map.computeIfAbsent(k == null ? "\u0000" : k, x -> new Agg(k));
            a.plays++;
            a.time += p.len;
            if (p.gender >= 0) a.by[p.gender]++;
        }
        return map.values().stream().sorted(Comparator.comparing((Agg a) -> a.key == null).thenComparingLong(a -> -a.plays)).toList();
    }

    private static List<BreakdownRow> breakdown(List<P> rows, Dimension dim, Map<Dimension, Map<String, String>> names, String range) {
        List<Agg> aggs = aggregate(rows, dim);
        long max = aggs.stream().mapToLong(a -> a.plays).max().orElse(0);
        List<BreakdownRow> out = new ArrayList<>();
        for (Agg a : aggs) {
            int[] s = CatalogDetailService.shares(a.by);
            out.add(new BreakdownRow(label(dim, a.key, names), songsHref(range, dim, a.key), a.key == null, fmt(a.plays),
                    TimeFormatUtils.formatTime(a.time), s[0], s[2], pct(a.plays, max)));
        }
        return out;
    }

    private static MixCard mix(List<P> rows, Dimension dim, Map<Dimension, Map<String, String>> names, String range, long plays) {
        List<Agg> aggs = aggregate(rows, dim);
        long max = aggs.stream().mapToLong(a -> a.plays).max().orElse(0);
        List<MixRow> out = new ArrayList<>();
        for (Agg a : aggs) {
            out.add(new MixRow(label(dim, a.key, names), songsHref(range, dim, a.key), a.key == null,
                    CatalogDetailService.share(a.plays, plays), CatalogDetailService.shares(a.by)[0], pct(a.plays, max)));
        }
        return new MixCard(dim.plural, MIX_COLORS.get(dim), out);
    }

    // ── Across the period and who won each slice ──

    /** Slice key of a play for a step: a sub period key, or the hour for "hours". */
    private static String sliceKey(P p, String step) {
        return "hours".equals(step) ? String.valueOf(p.at.getHour()) : TimeframePeriod.of(step, p.day()).key();
    }

    private static Map<String, long[][]> bySlice(List<P> rows, String step) {
        Map<String, long[][]> out = new HashMap<>();
        Map<LocalDate, String> cache = new HashMap<>();
        for (P p : rows) {
            String k = "hours".equals(step) ? sliceKey(p, step) : cache.computeIfAbsent(p.day(), d -> TimeframePeriod.of(step, d).key());
            long[][] v = out.computeIfAbsent(k, x -> new long[2][3]);
            int g = p.gender < 0 ? 2 : p.gender;
            v[0][g]++;
            v[1][g] += p.len;
        }
        return out;
    }

    private static Step step(List<P> rows, TimeframePeriod period, String step, LocalDate last) {
        Map<String, long[][]> data = bySlice(rows, step);
        List<String> keys = new ArrayList<>(), labels = new ArrayList<>(), hrefs = new ArrayList<>(), names = new ArrayList<>();
        if ("hours".equals(step)) {
            for (int h = 0; h < 24; h++) {
                keys.add(String.valueOf(h));
                labels.add(String.valueOf(h));
                hrefs.add(null);
                names.add(String.format("%02d:00", h));
            }
        } else {
            for (TimeframePeriod c : period.children(step)) {
                if (c.from().isAfter(last)) break;
                keys.add(c.key());
                labels.add(c.shortName(period.type()));
                hrefs.add(c.href());
                names.add(c.name());
            }
        }
        List<long[]> playsSeries = new ArrayList<>(), timeSeries = new ArrayList<>();
        long best = 0;
        for (String k : keys) {
            long[][] v = data.getOrDefault(k, new long[2][3]);
            playsSeries.add(v[0]);
            timeSeries.add(v[1]);
            best = Math.max(best, v[0][0] + v[0][1] + v[0][2]);
        }
        List<Pill> pills = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            long[] pv = playsSeries.get(i), tv = timeSeries.get(i);
            long total = pv[0] + pv[1] + pv[2];
            int male = total == 0 ? 0 : CatalogDetailService.shares(pv)[0];
            String time = TimeFormatUtils.formatHoursMinutes(tv[0] + tv[1] + tv[2]);
            pills.add(new Pill(labels.get(i), fmt(total), time,
                    male, best > 0 && total == best, total == 0 ? null : hrefs.get(i),
                    names.get(i) + ": " + fmt(total) + " plays" + (total == 0 ? "" : " · " + time + " listened · " + male + "% of plays by male artists")));
        }
        return new Step(step, capitalize(step), CatalogDetailService.paths(playsSeries), CatalogDetailService.paths(timeSeries), pills);
    }

    private static Tally tally(List<P> rows, List<Session> sessions, TimeframePeriod period, String slice) {
        List<String[]> labelled = new ArrayList<>();   // label, male, female
        if ("sessions".equals(slice)) {
            for (Session s : sessions) {
                long m = s.plays.stream().filter(p -> p.gender == 0).count(), f = s.plays.stream().filter(p -> p.gender == 1).count();
                labelled.add(new String[]{s.start.format(HM) + " – " + s.end.format(HM), String.valueOf(m), String.valueOf(f)});
            }
        } else {
            Map<String, long[][]> data = bySlice(rows, slice);
            if ("hours".equals(slice)) {
                for (int h = 0; h < 24; h++) {
                    long[][] v = data.get(String.valueOf(h));
                    if (v != null) labelled.add(new String[]{String.format("%02d:00", h), String.valueOf(v[0][0]), String.valueOf(v[0][1])});
                }
            } else {
                for (TimeframePeriod c : period.children(slice)) {
                    long[][] v = data.get(c.key());
                    if (v != null) labelled.add(new String[]{c.shortName(period.type()), String.valueOf(v[0][0]), String.valueOf(v[0][1])});
                }
            }
        }
        int male = 0, female = 0, ties = 0;
        List<String> femaleWon = new ArrayList<>();
        String closest = null;
        double closestGap = Double.MAX_VALUE;
        int closestMale = 0;
        for (String[] s : labelled) {
            long m = Long.parseLong(s[1]), f = Long.parseLong(s[2]);
            if (m + f == 0) continue;
            if (m > f) male++;
            else if (f > m) {
                female++;
                femaleWon.add(s[0]);
            } else ties++;
            double gap = Math.abs(m - f) / (double) (m + f);
            if (gap < closestGap) {
                closestGap = gap;
                closest = s[0];
                closestMale = (int) Math.round(m * 100.0 / (m + f));
            }
        }
        String note;
        if (female > 0 && female <= 6) note = "female: " + String.join(" · ", femaleWon);
        else if (closest == null) note = "no plays";
        else if (female == 0 && ties == 0 && closestMale == 100) note = "every one 100% male";
        else note = "closest: " + closest + " · " + closestMale + "% male";
        int won = male + female;
        return new Tally(capitalize(slice), male, female, ties, won == 0 ? 0 : (int) Math.round(male * 100.0 / won), note);
    }

    private static List<Segment> strip(List<Session> sessions, TimeframePeriod period) {
        LocalDateTime dayStart = period.from().atStartOfDay();
        List<Segment> out = new ArrayList<>();
        for (Session s : sessions) {
            double left = ChronoUnit.MINUTES.between(dayStart, s.start) * 100.0 / 1440;
            double width = Math.max(0.6, Math.min(100 - left, s.minutes() * 100.0 / 1440));
            out.add(new Segment(round1(left), round1(width), String.valueOf(s.plays.size()),
                    s.start.format(HM) + " – " + s.end.format(HM) + " · " + s.plays.size() + " plays"));
        }
        return out;
    }

    // ── Streaks ──

    private record Span(LocalDate start, LocalDate end) {
        int days() {
            return (int) ChronoUnit.DAYS.between(start, end) + 1;
        }
    }

    private static boolean played(Map<LocalDate, long[]> daily, LocalDate d) {
        long[] v = daily.get(d);
        return v != null && v[0] > 0;
    }

    private static boolean maleWon(Map<LocalDate, long[]> daily, LocalDate d) {
        long[] v = daily.get(d);
        return v != null && v[1] > v[2];
    }

    private static Span spanAround(Map<LocalDate, long[]> daily, LocalDate d, java.util.function.BiPredicate<Map<LocalDate, long[]>, LocalDate> ok) {
        if (!ok.test(daily, d)) return null;
        LocalDate a = d, b = d;
        while (ok.test(daily, a.minusDays(1))) a = a.minusDays(1);
        while (ok.test(daily, b.plusDays(1))) b = b.plusDays(1);
        return new Span(a, b);
    }

    private static Span longestInside(Map<LocalDate, long[]> daily, LocalDate from, LocalDate to,
                                      java.util.function.BiPredicate<Map<LocalDate, long[]>, LocalDate> ok) {
        Span best = null;
        LocalDate start = null;
        for (LocalDate d = from; !d.isAfter(to); d = d.plusDays(1)) {
            if (ok.test(daily, d)) {
                if (start == null) start = d;
                Span s = new Span(start, d);
                if (best == null || s.days() > best.days()) best = s;
            } else start = null;
        }
        return best;
    }

    static Streaks streaks(TimeframePeriod period, Map<LocalDate, long[]> daily, LocalDate last) {
        boolean around = "days".equals(period.type()) || "weeks".equals(period.type());
        List<Streak> out = new ArrayList<>();
        if (around) {
            for (int i = 0; i < 2; i++) {
                java.util.function.BiPredicate<Map<LocalDate, long[]>, LocalDate> ok = i == 0 ? TimeframeDetailService::played : TimeframeDetailService::maleWon;
                Span best = null;
                for (LocalDate d = period.from(); !d.isAfter(last); d = d.plusDays(1)) {
                    Span s = spanAround(daily, d, ok);
                    if (s != null && (best == null || s.days() > best.days())) best = s;
                }
                String label = i == 0 ? "Listening streak" : "Male-won streak";
                if (best == null) {
                    out.add(new Streak(label, i == 0 ? "listen" : "male", 0, null,
                            i == 0 ? "No plays, no streak" : "No male-won streak touches this " + period.typeLabel().toLowerCase(Locale.ROOT), 0, 0, null));
                    continue;
                }
                LocalDate mark = best.start().isAfter(period.from()) ? best.start() : period.from();
                double marker = best.days() <= 1 ? 50 : ChronoUnit.DAYS.between(best.start(), mark) * 100.0 / (best.days() - 1);
                long inPeriod = ChronoUnit.DAYS.between(mark, best.end().isBefore(last) ? best.end() : last) + 1;
                String note = "days".equals(period.type()) ? "This day is day " + (ChronoUnit.DAYS.between(best.start(), period.from()) + 1)
                        : "Covers " + inPeriod + " of the week's " + (ChronoUnit.DAYS.between(period.from(), last) + 1) + " days";
                out.add(new Streak(label + " · " + best.days() + (best.days() == 1 ? " day" : " days"), i == 0 ? "listen" : "male",
                        best.days(), best.start().format(DMY) + " → " + best.end().format(DMY), note, 0, 0, round1(marker)));
            }
            return new Streaks(true, out, List.of(), List.of());
        }
        double total = period.days();
        Span listen = longestInside(daily, period.from(), last, TimeframeDetailService::played);
        Span male = longestInside(daily, period.from(), last, TimeframeDetailService::maleWon);
        Span[] spans = {listen, male};
        for (int i = 0; i < 2; i++) {
            Span s = spans[i];
            String label = i == 0 ? "Listening streak" : "Male-won streak";
            String kind = i == 0 ? "listen" : "male";
            if (s == null) {
                out.add(new Streak(label, kind, 0, null, "None in this period", 0, 0, null));
                continue;
            }
            java.util.function.BiPredicate<Map<LocalDate, long[]>, LocalDate> ok = i == 0 ? TimeframeDetailService::played : TimeframeDetailService::maleWon;
            String note = s.start().format(DateTimeFormatter.ofPattern("dd/MM")) + " → " + s.end().format(DateTimeFormatter.ofPattern("dd/MM"));
            if (s.end().equals(last) && ok.test(daily, last.plusDays(1))) note += ", kept going after";
            else if (s.end().equals(last) && last.equals(period.to())) note += ", unbroken at period end";
            double left = ChronoUnit.DAYS.between(period.from(), s.start()) * 100.0 / total;
            out.add(new Streak(label, kind, s.days(), s.start().format(DMY) + " → " + s.end().format(DMY), note,
                    round1(left), round1(s.days() * 100.0 / total), null));
        }
        List<Tick> female = new ArrayList<>(), silent = new ArrayList<>();
        for (LocalDate d = period.from(); !d.isAfter(last); d = d.plusDays(1)) {
            double left = round1(ChronoUnit.DAYS.between(period.from(), d) * 100.0 / total);
            long[] v = daily.get(d);
            if (v == null || v[0] == 0) silent.add(new Tick(left, "silent", d.format(DMY) + " · no plays"));
            else if (v[2] > v[1]) female.add(new Tick(left, "female", d.format(DMY) + " · female won"));
        }
        List<Tick> ticks = new ArrayList<>();
        if (female.size() <= 40) ticks.addAll(female);
        if (silent.size() <= 40) ticks.addAll(silent);
        List<String> axis = new ArrayList<>();
        DateTimeFormatter f = switch (period.type()) {
            case "decades" -> DateTimeFormatter.ofPattern("yyyy");
            case "months" -> DateTimeFormatter.ofPattern("dd/MM");
            default -> DateTimeFormatter.ofPattern("MMM", Locale.ENGLISH);
        };
        for (int i = 0; i < 5; i++) axis.add(period.from().plusDays(Math.round((period.days() - 1) * i / 4.0)).format(f));
        return new Streaks(false, out, ticks, axis);
    }

    // ── Sessions and runs ──

    /** Sessions that start inside the period; edge plays only extend a session that began inside. */
    static List<Session> sessions(List<P> all, TimeframePeriod period) {
        List<Session> out = new ArrayList<>();
        List<P> current = new ArrayList<>();
        for (P p : all) {
            if (p.at.toLocalDate().toString().equals(MaleFunFactsService.EXCLUDED_DAY)) continue;
            if (!current.isEmpty() && ChronoUnit.MINUTES.between(current.get(current.size() - 1).at, p.at) > SESSION_GAP_MINUTES) {
                close(current, period, out);
                current = new ArrayList<>();
            }
            current.add(p);
        }
        close(current, period, out);
        return out;
    }

    private static void close(List<P> plays, TimeframePeriod period, List<Session> out) {
        if (plays.isEmpty() || !period.contains(plays.get(0).day())) return;
        out.add(new Session(plays.get(0).at, plays.get(plays.size() - 1).at, plays));
    }

    private record RunData(int artist, List<P> plays) {}

    private static List<RunData> runs(List<Session> sessions) {
        List<RunData> out = new ArrayList<>();
        for (Session s : sessions) {
            List<P> current = new ArrayList<>();
            for (P p : s.plays) {
                if (!current.isEmpty() && current.get(0).artist != p.artist) {
                    if (current.size() >= RUN_MIN) out.add(new RunData(current.get(0).artist, current));
                    current = new ArrayList<>();
                }
                current.add(p);
            }
            if (current.size() >= RUN_MIN) out.add(new RunData(current.get(0).artist, current));
        }
        out.sort(Comparator.comparingInt((RunData r) -> -r.plays.size()));
        return out;
    }

    private Sessions sessionCard(List<Session> sessions, TimeframePeriod period, int daysPlayed) {
        if (sessions.isEmpty()) return null;
        String type = period.type();
        Session longest = sessions.stream().max(Comparator.comparingLong(Session::minutes).thenComparingInt(s -> s.plays.size())).orElseThrow();
        boolean oneDay = "days".equals(type);
        String longestNote = (oneDay ? "" : longest.start.toLocalDate().format(DMY) + " ") + longest.start.format(HM) + " → "
                + (longest.end.toLocalDate().equals(longest.start.toLocalDate()) ? "" : longest.end.toLocalDate().format(DateTimeFormatter.ofPattern("dd/MM")) + " ")
                + longest.end.format(HM) + " · " + longest.plays.size() + " plays";
        int listed = switch (type) {
            case "days" -> sessions.size();
            case "weeks" -> 5;
            case "months", "seasons" -> 3;
            default -> 0;
        };
        List<Session> shown = oneDay ? sessions : sessions.stream()
                .sorted(Comparator.comparingLong(Session::minutes).reversed().thenComparingInt(s -> -s.plays.size())).limit(listed).toList();
        long maxMinutes = shown.stream().mapToLong(Session::minutes).max().orElse(0);
        List<SessionRow> rows = new ArrayList<>();
        for (Session s : shown) {
            String range = (oneDay ? "" : s.start.toLocalDate().format(DateTimeFormatter.ofPattern("dd/MM")) + " ") + s.start.format(HM) + " – " + s.end.format(HM);
            rows.add(new SessionRow(range, s.plays.size(), duration(s.minutes()), maxMinutes == 0 ? 3 : (int) Math.max(3, Math.round(s.minutes() * 100.0 / maxMinutes)),
                    oneDay ? null : TimeframePeriod.of("days", s.start.toLocalDate()).href()));
        }
        List<RunData> runData = runs(sessions);
        List<RunData> runShown = "days".equals(type) || "weeks".equals(type) ? runData.stream().limit(10).toList() : runData.stream().limit(1).toList();
        Map<Integer, Object[]> names = artistInfo(runShown.stream().map(RunData::artist).toList());
        List<Run> runs = new ArrayList<>();
        for (RunData r : runShown) {
            P first = r.plays.get(0), lastPlay = r.plays.get(r.plays.size() - 1);
            String album = wholeAlbum(r);
            String when = (oneDay ? "" : first.at.toLocalDate().format(DMY) + " · ") + first.at.format(HM) + " – " + lastPlay.at.format(HM);
            String title = String.valueOf(names.getOrDefault(r.artist, new Object[]{"Unknown"})[0]);
            runs.add(new Run(album != null ? album + " · " + title : title,
                    (album != null ? "whole album · " : "") + r.plays.size() + " in a row · " + when, r.plays.size(),
                    oneDay ? "/artists/" + r.artist : TimeframePeriod.of("days", first.day()).href(),
                    album != null ? image("/albums/" + first.album) : image("/artists/" + r.artist)));
        }
        return new Sessions(fmt(sessions.size()), daysPlayed == 0 ? "-" : String.format(Locale.US, "%.1f a day", sessions.size() / (double) daysPlayed),
                duration(longest.minutes()), longestNote, oneDay ? null : TimeframePeriod.of("days", longest.start.toLocalDate()).href(), rows, runs);
    }

    /** Album name when a run plays every song of one album, null otherwise. */
    private String wholeAlbum(RunData r) {
        Integer album = r.plays.get(0).album;
        if (album == null || r.plays.stream().anyMatch(p -> !album.equals(p.album))) return null;
        long distinct = r.plays.stream().map(P::song).distinct().count();
        Integer songs = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM Song WHERE album_id = ?", Integer.class, album);
        if (songs == null || songs < 3 || distinct < songs) return null;
        return jdbcTemplate.queryForObject("SELECT name FROM Album WHERE id = ?", String.class, album);
    }

    // ── Top lists ──

    /** Ids by plays (ties by first play), each with {plays, gender bucket}. */
    private static List<Map.Entry<Integer, long[]>> top(List<P> rows, Function<P, Integer> id, Function<P, Integer> gender, int limit) {
        Map<Integer, long[]> map = new LinkedHashMap<>();
        for (P p : rows) {
            Integer k = id.apply(p);
            if (k == null) continue;
            long[] v = map.computeIfAbsent(k, x -> new long[]{0, gender.apply(p)});
            v[0]++;
        }
        return map.entrySet().stream().sorted(Comparator.comparingLong(e -> -e.getValue()[0])).limit(limit).toList();
    }

    private List<TopColumn> tops(List<P> rows, Map<Integer, Object[]> artistNames) {
        Map<Integer, String[]> albumNames = new HashMap<>(), songNames = new HashMap<>();
        Set<Integer> albumIds = rows.stream().map(P::album).filter(java.util.Objects::nonNull).collect(Collectors.toSet());
        Set<Integer> songIds = rows.stream().map(P::song).collect(Collectors.toSet());
        if (!albumIds.isEmpty()) {
            jdbcTemplate.query("SELECT al.id, al.name, ar.name AS artist FROM Album al JOIN Artist ar ON ar.id = al.artist_id", (RowCallbackHandler) rs -> {
                if (albumIds.contains(rs.getInt("id"))) albumNames.put(rs.getInt("id"), new String[]{rs.getString("name"), rs.getString("artist")});
            });
        }
        if (!songIds.isEmpty()) {
            jdbcTemplate.query("SELECT s.id, s.name, ar.name AS artist FROM Song s JOIN Artist ar ON ar.id = s.artist_id", (RowCallbackHandler) rs -> {
                if (songIds.contains(rs.getInt("id"))) songNames.put(rs.getInt("id"), new String[]{rs.getString("name"), rs.getString("artist")});
            });
        }
        List<TopColumn> out = new ArrayList<>();
        out.add(column("Artists", rows, P::artist, p -> p.artistGender,
                id -> new String[]{String.valueOf(artistNames.getOrDefault(id, new Object[]{"Unknown"})[0]), null}, "/artists/"));
        out.add(column("Albums", rows, P::album, p -> p.artistGender,
                id -> albumNames.getOrDefault(id, new String[]{"Unknown", null}), "/albums/"));
        out.add(column("Songs", rows, P::song, p -> p.gender,
                id -> songNames.getOrDefault(id, new String[]{"Unknown", null}), "/songs/"));
        return out;
    }

    private static TopColumn column(String title, List<P> rows, Function<P, Integer> id, Function<P, Integer> gender,
                                    Function<Integer, String[]> name, String hrefBase) {
        List<List<TopRow>> lists = new ArrayList<>();
        for (int filter = -1; filter <= 1; filter++) {
            final int g = filter;
            List<P> subset = g < 0 ? rows : rows.stream().filter(p -> gender.apply(p) == g).toList();
            List<Map.Entry<Integer, long[]>> top = top(subset, id, gender, TOP_SIZE);
            long max = top.isEmpty() ? 0 : top.get(0).getValue()[0];
            List<TopRow> list = new ArrayList<>();
            for (int i = 0; i < top.size(); i++) {
                var e = top.get(i);
                String[] n = name.apply(e.getKey());
                list.add(new TopRow(i + 1, n[0], n[1], fmt(e.getValue()[0]), pct(e.getValue()[0], max), genderCode((int) e.getValue()[1]),
                        hrefBase + e.getKey(), image(hrefBase + e.getKey())));
            }
            lists.add(list);
        }
        long femaleTop10 = lists.get(0).stream().limit(10).filter(r -> r.gender().equals("f")).count();
        String note = femaleTop10 == 0 ? "no women in the top 10" : femaleTop10 + " female in the top 10";
        return new TopColumn(title, note, lists.get(0), lists.get(1), lists.get(2));
    }

    // ── Discoveries, comebacks, farewells ──

    private Discoveries discoveries(TimeframePeriod period, List<P> rows, Map<Integer, String[]> life, Map<Integer, Long> inside,
                                    Map<Integer, Object[]> names, boolean inProgress) {
        boolean oneDay = "days".equals(period.type());
        List<Discovery> artists = new ArrayList<>();
        long[] byGender = new long[3];
        for (var e : life.entrySet()) {
            LocalDateTime first = parse(e.getValue()[0]);
            if (!period.contains(first.toLocalDate())) continue;
            long in = inside.getOrDefault(e.getKey(), 0L), later = Long.parseLong(e.getValue()[4]);
            Object[] info = names.getOrDefault(e.getKey(), new Object[]{"Unknown", -1});
            int g = (int) info[1];
            if (g >= 0) byGender[g]++;
            artists.add(new Discovery(String.valueOf(info[0]), genderCode(g), oneDay ? first.format(HM) : first.toLocalDate().format(DMY),
                    fmt(in), inProgress ? null : fmt(later), (int) Math.round(in * 100.0 / Math.max(1, in + later)),
                    !inProgress && later < in * 0.25, "/artists/" + e.getKey(), image("/artists/" + e.getKey())));
        }
        artists.sort(Comparator.comparingLong((Discovery d) -> -Long.parseLong(d.plays().replace(",", ""))).thenComparing(Discovery::name));
        List<Discovery> songs = new ArrayList<>();
        if (oneDay || "weeks".equals(period.type())) {
            Map<Integer, Long> songPlays = rows.stream().collect(Collectors.groupingBy(P::song, Collectors.counting()));
            List<Object[]> firsts = jdbcTemplate.query("SELECT p.song_id AS sid, MIN(p.play_date) AS f, s.name AS sname, ar.name AS aname,"
                    + " gn.name AS gname FROM Play p JOIN Song s ON s.id = p.song_id JOIN Artist ar ON ar.id = s.artist_id"
                    + " LEFT JOIN Gender gn ON gn.id = COALESCE(s.override_gender_id, ar.gender_id)"
                    + " WHERE p.song_id IN (SELECT DISTINCT song_id FROM Play WHERE play_date >= ? AND play_date < ?)"
                    + " GROUP BY p.song_id HAVING MIN(p.play_date) >= ?",
                    (rs, i) -> new Object[]{rs.getInt("sid"), rs.getString("f"), rs.getString("sname"), rs.getString("aname"),
                            CatalogDetailService.bucket(rs.getString("gname"))},
                    period.from().toString(), period.to().plusDays(1).toString(), period.from().toString());
            for (Object[] f : firsts) {
                LocalDateTime at = parse((String) f[1]);
                long n = songPlays.getOrDefault((Integer) f[0], 0L);
                songs.add(new Discovery(f[2] + " · " + f[3], genderCode((int) f[4]), oneDay ? at.format(HM) : at.toLocalDate().format(DMY),
                        fmt(n), null, 100, false, "/songs/" + f[0], image("/songs/" + f[0])));
            }
            songs.sort(Comparator.comparing(Discovery::first));
            if (songs.size() > 20) songs = songs.subList(0, 20);
        }
        String note = artists.size() + (artists.size() == 1 ? " artist" : " artists") + " first played"
                + (byGender[0] + byGender[1] + byGender[2] == 0 ? "" : " · " + byGender[0] + " male · " + byGender[1] + " female"
                + (byGender[2] > 0 ? " · " + byGender[2] + " other" : ""))
                + (songs.isEmpty() ? "" : " · " + songs.size() + (songs.size() == 1 ? " first-ever song" : " first-ever songs"));
        return new Discoveries(note, !inProgress, artists.size() > TOP_SIZE ? artists.subList(0, TOP_SIZE) : artists, songs);
    }

    private static List<Comeback> comebacks(TimeframePeriod period, Map<Integer, String[]> life, Map<Integer, Long> inside,
                                            Map<Integer, LocalDateTime> firstInside, Map<Integer, Object[]> names) {
        int threshold = COMEBACK_DAYS.get(period.type());
        boolean oneDay = "days".equals(period.type());
        List<Object[]> found = new ArrayList<>();
        for (var e : life.entrySet()) {
            String prev = e.getValue()[3];
            LocalDateTime back = firstInside.get(e.getKey());
            if (prev == null || back == null) continue;
            LocalDateTime before = parse(prev);
            long gap = ChronoUnit.DAYS.between(before.toLocalDate(), back.toLocalDate());
            if (gap >= threshold) found.add(new Object[]{e.getKey(), before, back, gap});
        }
        found.sort(Comparator.comparingLong(o -> -(long) o[3]));
        long maxGap = found.isEmpty() ? 1 : (long) found.get(0)[3];
        List<Comeback> out = new ArrayList<>();
        for (Object[] o : found.subList(0, Math.min(30, found.size()))) {
            int id = (int) o[0];
            Object[] info = names.getOrDefault(id, new Object[]{"Unknown", -1});
            LocalDateTime before = (LocalDateTime) o[1], back = (LocalDateTime) o[2];
            long gap = (long) o[3];
            out.add(new Comeback(String.valueOf(info[0]), genderCode((int) info[1]),
                    "last " + before.toLocalDate().format(DMY) + " → back " + back.toLocalDate().format(DMY) + (oneDay ? " · " + back.format(HM) : ""),
                    gapLabel(gap), (int) Math.max(4, Math.round(gap * 60.0 / maxGap)), fmt(inside.getOrDefault(id, 0L)),
                    "/artists/" + id, image("/artists/" + id)));
        }
        return out;
    }

    private static String comebackNote(String type) {
        int days = COMEBACK_DAYS.get(type);
        return "back after " + (days >= 730 ? "2+ years" : days >= 365 ? "a year or more" : days >= 182 ? "6+ months" : "90+ days") + " away";
    }

    private static List<Farewell> farewells(TimeframePeriod period, Map<Integer, String[]> life, Map<Integer, Object[]> names, LocalDate today) {
        List<Object[]> found = new ArrayList<>();
        for (var e : life.entrySet()) {
            LocalDate lastPlay = parse(e.getValue()[1]).toLocalDate();
            if (!period.contains(lastPlay) || ChronoUnit.DAYS.between(lastPlay, today) < 365) continue;
            found.add(new Object[]{e.getKey(), lastPlay, Long.parseLong(e.getValue()[2])});
        }
        found.sort(Comparator.comparingLong(o -> -(long) o[2]));
        List<Farewell> out = new ArrayList<>();
        for (Object[] o : found.subList(0, Math.min(30, found.size()))) {
            Object[] info = names.getOrDefault((int) o[0], new Object[]{"Unknown", -1});
            out.add(new Farewell(String.valueOf(info[0]), genderCode((int) info[1]), "last play " + ((LocalDate) o[1]).format(DMY),
                    fmt((long) o[2]), "/artists/" + o[0], image("/artists/" + o[0])));
        }
        return out;
    }

    // ── Peers ──

    /** Fills both ranked lists; returns {rank by plays, rank by male share} of the page's period (0 when it has no plays). */
    private static int[] peers(Map<String, long[]> totals, Map<String, TimeframePeriod> periods, String self,
                               List<PeerRow> byPlays, List<PeerRow> byMale) {
        List<Map.Entry<String, long[]>> sorted = totals.entrySet().stream().filter(e -> e.getValue()[0] > 0)
                .sorted(Comparator.comparingLong((Map.Entry<String, long[]> e) -> -e.getValue()[0]).thenComparing(Map.Entry::getKey)).toList();
        long max = sorted.isEmpty() ? 0 : sorted.get(0).getValue()[0];
        int[] ranks = new int[2];
        for (int i = 0; i < sorted.size(); i++) {
            var e = sorted.get(i);
            long[] v = e.getValue();
            int[] s = CatalogDetailService.shares(new long[]{v[1], v[2], v[0] - v[1] - v[2]});
            boolean me = e.getKey().equals(self);
            if (me) ranks[0] = i + 1;
            TimeframePeriod p = periods.get(e.getKey());
            byPlays.add(new PeerRow(i + 1, p.name(), p.href(), fmt(v[0]), s[0], s[2], pct(v[0], max), me));
        }
        List<PeerRow> male = byPlays.stream().sorted(Comparator.comparingInt(PeerRow::malePct).reversed().thenComparing(PeerRow::rank)).toList();
        for (int i = 0; i < male.size(); i++) {
            PeerRow r = male.get(i);
            if (r.self()) ranks[1] = i + 1;
            byMale.add(new PeerRow(i + 1, r.name(), r.href(), r.plays(), r.malePct(), r.otherPct(), r.barPct(), r.self()));
        }
        return ranks;
    }

    /** Long peer lists keep the top 50 and the rows around the page's own period, with a gap row (null href) between. */
    static List<PeerRow> trim(List<PeerRow> rows) {
        if (rows.size() <= 60) return rows;
        int self = -1;
        for (int i = 0; i < rows.size(); i++) if (rows.get(i).self()) self = i;
        List<PeerRow> out = new ArrayList<>(rows.subList(0, 50));
        if (self >= 50) {
            int from = Math.max(50, self - 5), to = Math.min(rows.size(), self + 6);
            if (from > 50) out.add(new PeerRow(0, "", null, "", 0, 0, 0, false));
            out.addAll(rows.subList(from, to));
        }
        return out;
    }

    private static List<DateBar> sameDate(TimeframePeriod period, Map<LocalDate, long[]> daily) {
        String md = period.key().substring(5);
        List<Map.Entry<LocalDate, long[]>> days = daily.entrySet().stream()
                .filter(e -> e.getKey().toString().substring(5).equals(md) && e.getValue()[0] > 0).toList();
        long max = days.stream().mapToLong(e -> e.getValue()[0]).max().orElse(0);
        List<DateBar> out = new ArrayList<>();
        for (var e : days) {
            long[] v = e.getValue();
            int female = (int) Math.round(v[2] * 100.0 / v[0]);
            out.add(new DateBar("'" + String.valueOf(e.getKey().getYear()).substring(2), pct(v[0], max), female,
                    e.getKey().equals(period.from()), TimeframePeriod.of("days", e.getKey()).href(),
                    e.getKey().format(DMY) + ": " + fmt(v[0]) + " plays · " + Math.round(v[1] * 100.0 / v[0]) + "% male"));
        }
        return out;
    }

    // ── Hero ──

    private List<Tile> tiles(TimeframePeriod period, long plays, long time, int artists, int albums, int songs, int discovered,
                             long[] comp, long[] compDistinct, String compName, String range) {
        List<Tile> out = new ArrayList<>();
        out.add(delta("Artists", fmt(artists), artists, compDistinct[0], "", "/artists?" + range + "&view=table"));
        out.add(delta("Albums", fmt(albums), albums, compDistinct[1], "", "/albums?" + range + "&view=table"));
        out.add(delta("Songs", fmt(songs), songs, compDistinct[2], "", "/songs?" + range + "&view=table"));
        out.add(delta("Plays", fmt(plays), plays, comp[0], comp[0] > 0 ? " vs " + compName : "", null));
        out.add(delta("Time listened", hours(time), time, comp[3], "", null));
        out.add(new Tile("New artists", fmt(discovered), "first play ever", "", discovered == 0 ? null : "#tf-discoveries"));
        String released = "releaseDateMode=between&releaseDateFrom=" + period.from().format(DMY) + "&releaseDateTo="
                + period.to().format(DMY) + "&sortby=release_date&sortdir=asc&view=table";
        out.add(releaseTile("Album releases", releases(period, "SELECT g.name FROM Album a JOIN Artist ar ON ar.id = a.artist_id"
                + " LEFT JOIN Gender g ON g.id = ar.gender_id WHERE DATE(a.release_date) BETWEEN ? AND ?"), "/albums?" + released));
        out.add(releaseTile("Song releases", releases(period, "SELECT g.name FROM Song s JOIN Artist ar ON ar.id = s.artist_id"
                + " LEFT JOIN Album alb ON alb.id = s.album_id LEFT JOIN Gender g ON g.id = COALESCE(s.override_gender_id, ar.gender_id)"
                + " WHERE DATE(COALESCE(s.release_date, alb.release_date)) BETWEEN ? AND ?"), "/songs?" + released));
        return out;
    }

    /** Total, male and female counts of the catalog items whose release date falls inside the period. */
    private long[] releases(TimeframePeriod period, String sql) {
        long[] out = new long[3];
        jdbcTemplate.query(sql, rs -> {
            out[0]++;
            int g = CatalogDetailService.bucket(rs.getString(1));
            if (g == 0 || g == 1) out[g + 1]++;
        }, period.from().toString(), period.to().toString());
        return out;
    }

    private static Tile releaseTile(String label, long[] counts, String href) {
        String note = counts[0] == 0 ? null : fmt(counts[1]) + " male · " + fmt(counts[2]) + " female";
        return new Tile(label, fmt(counts[0]), note, "", counts[0] == 0 ? null : href);
    }

    private static Tile delta(String label, String value, long now, long before, String suffix, String href) {
        if (before <= 0) return new Tile(label, value, null, "", href);
        double change = (now - before) * 100.0 / before;
        String text = (change >= 0 ? "▲ " : "▼ ") + String.format(Locale.US, "%.1f%%", Math.abs(change)) + suffix;
        return new Tile(label, value, text, change >= 0 ? "up" : "down", href);
    }

    /**
     * The two standings in the hero: rank among its peers by plays and by male share. Everything else the old hero
     * chips said (in progress, who won, days played, the day's streak) already lives in the eyebrow, tiles or cards.
     */
    private static List<Standing> standing(TimeframePeriod period, int[] ranks, int peerCount, long[] playsBy, boolean inProgress) {
        List<Standing> out = new ArrayList<>();
        String noun = period.plural();
        String topShare = peerCount > 50 && ranks[0] > 0 ? "top " + Math.max(1, Math.round(ranks[0] * 100.0 / peerCount)) + "%" : null;
        if (ranks[0] > 0) {
            out.add(new Standing("#" + fmt(ranks[0]), "of " + fmt(peerCount) + " " + noun + " by plays" + (inProgress ? " so far" : ""),
                    topShare, ranks[0] <= 3));
        }
        long total = playsBy[0] + playsBy[1] + playsBy[2];
        if (ranks[1] > 0 && total > 0) {
            out.add(new Standing("#" + fmt(ranks[1]), "of " + fmt(peerCount) + " by male share",
                    CatalogDetailService.shares(playsBy)[0] + "% male plays", ranks[1] <= 3));
        }
        return out;
    }

    /** The period's own song chart (#1 song, draft or final), shown as a link on the Top of card. */
    private ChartLink chartLink(TimeframePeriod period) {
        String chartType = switch (period.type()) {
            case "weeks" -> "weekly";
            case "seasons" -> "seasonal";
            case "years" -> "yearly";
            default -> null;
        };
        if (chartType == null) return null;
        List<Object[]> chart = jdbcTemplate.query("SELECT ch.is_finalized AS fin, s.name AS song FROM Chart ch"
                + " LEFT JOIN ChartEntry e ON e.chart_id = ch.id AND e.position = 1 LEFT JOIN Song s ON s.id = e.song_id"
                + " WHERE ch.chart_type = 'song' AND ch.period_type = ? AND ch.period_key = ?",
                (rs, i) -> new Object[]{rs.getInt("fin") == 1, rs.getString("song")}, chartType, period.key());
        if (chart.isEmpty()) return null;
        boolean draft = !"weekly".equals(chartType) && !(boolean) chart.get(0)[0];
        String song = (String) chart.get(0)[1];
        String text = (song == null ? capitalize(chartType) + " chart" : capitalize(chartType) + " chart #1 · " + song) + (draft ? " (draft)" : "");
        return new ChartLink(text, "/charts/" + chartType + "/" + period.key());
    }

    private static List<NavLink> partOf(TimeframePeriod period) {
        List<String> parents = switch (period.type()) {
            case "days" -> List.of("decades", "years", "seasons", "months", "weeks");
            case "weeks" -> List.of("decades", "years", "months");
            case "months" -> List.of("decades", "years", "seasons");
            case "seasons", "years" -> List.of("decades", "years");
            default -> List.of();
        };
        List<NavLink> out = new ArrayList<>();
        for (String t : parents) {
            if (t.equals(period.type())) continue;
            LocalDate anchor = "seasons".equals(period.type()) && "years".equals(t) ? period.to() : period.from();
            TimeframePeriod p = TimeframePeriod.of(t, anchor);
            out.add(new NavLink(p.name(), p.href()));
        }
        return out;
    }

    private static List<NavLink> contains(TimeframePeriod period, Map<LocalDate, long[]> daily, LocalDate today) {
        List<NavLink> out = new ArrayList<>();
        if ("days".equals(period.type())) {
            String md = period.key().substring(5);
            daily.keySet().stream().filter(d -> d.toString().substring(5).equals(md) && !d.equals(period.from()))
                    .sorted(Comparator.reverseOrder()).limit(5)
                    .forEach(d -> out.add(new NavLink(d.format(DMY), TimeframePeriod.of("days", d).href())));
            return out;
        }
        List<String> childTypes = switch (period.type()) {
            case "decades" -> List.of("years");
            case "years" -> List.of("seasons", "months");
            case "seasons" -> List.of("months");
            case "months" -> List.of("weeks");
            default -> List.of("days");
        };
        for (String t : childTypes) {
            for (TimeframePeriod c : period.children(t)) {
                if (c.from().isAfter(today)) break;
                if ("seasons".equals(t) && !c.key().startsWith(period.key())) continue;
                out.add(new NavLink(c.shortName(period.type()), c.href()));
            }
        }
        return out;
    }

    private static Rhythm rhythm(List<P> rows) {
        long[] hours = new long[24], week = new long[7], months = new long[12];
        long excluded = 0;
        for (P p : rows) {
            if (p.day().toString().equals(MaleFunFactsService.EXCLUDED_DAY)) {
                excluded++;
                continue;
            }
            hours[p.at.getHour()]++;
            week[p.at.getDayOfWeek().getValue() - 1]++;
            months[p.at.getMonthValue() - 1]++;
        }
        return CatalogDetailService.rhythm(hours, week, months, excluded);
    }

    // ── Helpers ──

    private static long[] sum(Map<LocalDate, long[]> daily, LocalDate from, LocalDate to) {
        long[] out = new long[4];
        for (LocalDate d = from; !d.isAfter(to); d = d.plusDays(1)) {
            long[] v = daily.get(d);
            if (v == null) continue;
            for (int i = 0; i < 4; i++) out[i] += v[i];
        }
        return out;
    }

    private static List<Set<Integer>> sets() {
        return List.of(new HashSet<>(), new HashSet<>(), new HashSet<>());
    }

    private static long[] sizes(List<Set<Integer>> sets) {
        return new long[]{sets.get(0).size(), sets.get(1).size(), sets.get(2).size()};
    }

    static LocalDateTime parse(String playDate) {
        String s = playDate.length() >= 16 ? playDate.substring(0, 16) : playDate.substring(0, 10) + " 00:00";
        return LocalDateTime.parse(s.replace(' ', 'T'));
    }

    private static String sqlTime(LocalDateTime t) {
        return t.toLocalDate() + " " + t.toLocalTime().format(HM);
    }

    static String duration(long minutes) {
        if (minutes < 60) return minutes + "m";
        return minutes / 60 + "h" + (minutes % 60 == 0 ? "" : " " + minutes % 60 + "m");
    }

    static String gapLabel(long days) {
        if (days >= 365) return String.format(Locale.US, "%.1f yrs", days / 365.25);
        if (days >= 60) return Math.round(days / 30.44) + " mo";
        return days + " days";
    }

    /** Time tile value: minutes under an hour, whole hours with separators above. */
    static String hours(long seconds) {
        if (seconds < 3600) return seconds / 60 + "m";
        return fmt(Math.round(seconds / 3600.0)) + "h";
    }

    private static String genderCode(int bucket) {
        return bucket == 0 ? "m" : bucket == 1 ? "f" : "o";
    }


    private static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }

    private static double round1(double v) {
        return Math.round(v * 10) / 10.0;
    }

    private static int pct(long v, long max) {
        return max == 0 ? 0 : (int) Math.max(v > 0 ? 2 : 0, Math.round(v * 100.0 / max));
    }

    private static String label(Dimension d, String key, Map<Dimension, Map<String, String>> names) {
        if (key == null) return "Not set";
        if (d.byName()) return key;
        return names.get(d).getOrDefault(key, "Unknown");
    }

    private static String songsHref(String range, Dimension by, String key) {
        String row = key == null ? by.param + "Mode=isnull" : by.param + "=" + enc(key) + "&" + by.param + "Mode=includes";
        return "/songs?" + range + "&" + row + "&view=table";
    }

    /** Thumbnail of an artist, album or song page ({@code /songs/{id}/image} already falls back to the album cover). */
    static String image(String href) {
        return href + "/image?thumbnail=true";
    }

    private static String fmt(long n) {
        return String.format(Locale.US, "%,d", n);
    }

    private static String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
