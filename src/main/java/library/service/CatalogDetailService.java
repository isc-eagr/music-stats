package library.service;

import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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

/**
 * Detail page for the secondary catalogs (genre, subgenre, language, ethnicity, country): hero totals,
 * gender breakdown, a breakdown by another catalog, plays over the years with the yearly rank, where the
 * item sits among its peers, when you play it (hour, weekday, month) and the mix of the other catalogs.
 *
 * Every attribute follows the override cascade (song, then album, then artist) the list pages use.
 */
@Service
public class CatalogDetailService {

    /** A secondary catalog: its URL, list filter param and the SQL that resolves a song, album or artist to it. */
    public enum Dimension {
        GENRE("genre", "Genre", "Genres", "genres", "Genre",
                "COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id)"),
        SUBGENRE("subgenre", "Subgenre", "Subgenres", "subgenres", "SubGenre",
                "COALESCE(s.override_subgenre_id, al.override_subgenre_id, ar.subgenre_id)"),
        LANGUAGE("language", "Language", "Languages", "languages", "Language",
                "COALESCE(s.override_language_id, al.override_language_id, ar.language_id)"),
        ETHNICITY("ethnicity", "Ethnicity", "Ethnicities", "ethnicities", "Ethnicity",
                "COALESCE(s.override_ethnicity_id, ar.ethnicity_id)"),
        COUNTRY("country", "Country", "Countries", "countries", null, "ar.country");

        public final String param;
        public final String label;
        public final String plural;
        public final String path;
        final String table;
        final String songExpr;

        Dimension(String param, String label, String plural, String path, String table, String songExpr) {
            this.param = param;
            this.label = label;
            this.plural = plural;
            this.path = path;
            this.table = table;
            this.songExpr = songExpr;
        }

        /** Country has no lookup table: the name is the key. */
        boolean byName() {
            return table == null;
        }

        String detailHref(String key) {
            return "/" + path + "/" + (byName() ? enc(key) : key);
        }
    }

    /** Which catalog gets the big breakdown table and which ones get a mix card, per page catalog. */
    private static final Map<Dimension, Dimension> BIG = Map.of(
            Dimension.GENRE, Dimension.SUBGENRE, Dimension.SUBGENRE, Dimension.COUNTRY,
            Dimension.LANGUAGE, Dimension.GENRE, Dimension.ETHNICITY, Dimension.GENRE, Dimension.COUNTRY, Dimension.GENRE);
    private static final Map<Dimension, List<Dimension>> MIXES = Map.of(
            Dimension.GENRE, List.of(Dimension.LANGUAGE, Dimension.COUNTRY, Dimension.ETHNICITY),
            Dimension.SUBGENRE, List.of(Dimension.LANGUAGE, Dimension.ETHNICITY),
            Dimension.LANGUAGE, List.of(Dimension.COUNTRY, Dimension.ETHNICITY, Dimension.SUBGENRE),
            Dimension.ETHNICITY, List.of(Dimension.LANGUAGE, Dimension.COUNTRY, Dimension.SUBGENRE),
            Dimension.COUNTRY, List.of(Dimension.LANGUAGE, Dimension.ETHNICITY, Dimension.SUBGENRE));
    private static final Map<Dimension, String> MIX_COLORS = Map.of(
            Dimension.LANGUAGE, "#a78bfa", Dimension.COUNTRY, "#34d399", Dimension.ETHNICITY, "#fbbf24",
            Dimension.SUBGENRE, "#22d3ee", Dimension.GENRE, "#fb923c");

    static final String[] WEEKDAYS = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
    static final String[] WEEKDAY_NAMES = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
    static final String[] MONTHS = {"January", "February", "March", "April", "May", "June", "July", "August",
            "September", "October", "November", "December"};

    public record Stat(String label, String value) {}
    public record ListLink(String label, String value, String href) {}
    /** One gender column: counts, shares (male, other, female add up to 100) and the filtered list links. */
    public record GenderRow(String label, String male, String female, int malePct, int otherPct, int femalePct,
                            String maleHref, String femaleHref) {}
    /** {@code href} opens the Songs table filtered by the page item and this row (unset rows filter on an empty value). */
    public record BreakdownRow(String name, String href, boolean unset, String plays, String time, int malePct, int otherPct, int barPct) {}
    /** Plays or time per year stacked by gender, as SVG paths over a 1000 x 220 box. */
    public record Stream(String male, String other, String female, String note) {}
    /** One year under the chart: rank among peers and this item's value, by plays and by time (null rank = no plays). */
    public record YearPill(int year, String label, Integer rank, Integer timeRank, String plays, String time,
                           String playsTitle, String timeTitle) {}
    public record PeerRow(int rank, String name, String href, String plays, int malePct, int otherPct, int barPct, boolean self) {}
    public record Bar(String value, String label, int height, boolean best, String title) {}
    /** A labelled band behind a group of bars; {@code left} and {@code width} are CSS lengths. */
    public record Band(String name, int share, String left, String width, String icon) {}
    public record Rhythm(List<Band> hourBands, List<Bar> hours, String hourNote,
                         List<Band> weekBands, List<Bar> week, String weekNote,
                         List<Band> monthBands, List<Bar> months, String monthNote, String excludedNote) {}
    public record MixRow(String name, String href, boolean unset, String share, int malePct, int barPct) {}
    public record MixCard(String title, String color, List<MixRow> rows) {}

    public record Detail(Dimension dimension, String key, String name, String typeLabel, String parentName,
                         String parentHref, String theme, String rankLabel, String shareLabel, String firstPlay,
                         String lastPlay, List<Stat> stats, List<ListLink> links, List<GenderRow> gender,
                         String breakdownTitle, List<BreakdownRow> breakdown, Stream playsStream, Stream timeStream,
                         List<YearPill> years, List<PeerRow> byPlays, List<PeerRow> byMale, Rhythm rhythm,
                         List<MixCard> mixes, long plays) {}

    private final JdbcTemplate jdbcTemplate;

    public CatalogDetailService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final String SONG_FROM = " FROM Song s JOIN Artist ar ON ar.id = s.artist_id"
            + " LEFT JOIN Album al ON al.id = s.album_id"
            + " LEFT JOIN Gender gn ON gn.id = COALESCE(s.override_gender_id, ar.gender_id) ";

    /** Gender buckets the list pages use: names with Male (not Female), names with Female, anything else. */
    static int bucket(String genderName) {
        if (genderName == null) return -1;
        String n = genderName.toLowerCase(Locale.ROOT);
        if (n.contains("female")) return 1;
        if (n.contains("male")) return 0;
        return 2;
    }

    public Optional<Detail> build(Dimension dim, String rawKey) {
        String key = normalizeKey(dim, rawKey);
        if (key == null) return Optional.empty();
        Map<Dimension, Map<String, String>> names = new HashMap<>();
        for (Dimension d : Dimension.values()) {
            if (d.byName()) continue;
            Map<String, String> m = new HashMap<>();
            jdbcTemplate.query("SELECT id, name FROM " + d.table, (RowCallbackHandler) rs -> m.put(rs.getString("id"), rs.getString("name")));
            names.put(d, m);
        }
        String name;
        String parentName = null, parentHref = null;
        if (dim.byName()) {
            Integer found = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM Artist WHERE country = ?", Integer.class, key);
            if (found == null || found == 0) return Optional.empty();
            name = key;
        } else {
            name = names.get(dim).get(key);
            if (name == null) return Optional.empty();
        }
        Object keyParam = dim.byName() ? key : (Object) Integer.parseInt(key);

        // Songs of this catalog with their plays, gender and the other catalogs they resolve to
        String others = String.join(", ", java.util.Arrays.stream(Dimension.values())
                .map(d -> d.songExpr + " AS k_" + d.param).toList());
        List<SongRow> songs = jdbcTemplate.query("SELECT s.id, s.artist_id, s.album_id, COALESCE(s.length_seconds, 0) AS len, gn.name AS gname, "
                + others + ", (SELECT COUNT(*) FROM Play p WHERE p.song_id = s.id) AS plays" + SONG_FROM
                + "WHERE " + dim.songExpr + " = ?", (rs, i) -> {
                    Map<Dimension, String> keys = new HashMap<>();
                    for (Dimension d : Dimension.values()) keys.put(d, rs.getString("k_" + d.param));
                    int albumId = rs.getInt("album_id");
                    Integer album = rs.wasNull() ? null : albumId;
                    return new SongRow(rs.getInt("artist_id"), album, rs.getLong("len"), bucket(rs.getString("gname")),
                            rs.getLong("plays"), keys);
                }, keyParam);

        long[] playsBy = new long[3], timeBy = new long[3], songsBy = new long[3];
        long plays = 0, time = 0;
        List<Set<Integer>> artistsBy = List.of(new HashSet<>(), new HashSet<>(), new HashSet<>());
        List<Set<Integer>> albumsBy = List.of(new HashSet<>(), new HashSet<>(), new HashSet<>());
        Set<Integer> allArtists = new HashSet<>(), allAlbums = new HashSet<>();
        for (SongRow s : songs) {
            long t = s.len * (s.plays == 0 ? 1 : s.plays);
            plays += s.plays;
            time += t;
            allArtists.add(s.artistId);
            if (s.albumId != null) allAlbums.add(s.albumId);
            if (s.gender >= 0) {
                playsBy[s.gender] += s.plays;
                timeBy[s.gender] += t;
                songsBy[s.gender]++;
                artistsBy.get(s.gender).add(s.artistId);
                if (s.albumId != null) albumsBy.get(s.gender).add(s.albumId);
            }
        }
        long[] artistCounts = {artistsBy.get(0).size(), artistsBy.get(1).size(), artistsBy.get(2).size()};
        long[] albumCounts = {albumsBy.get(0).size(), albumsBy.get(1).size(), albumsBy.get(2).size()};
        long artistTotal = allArtists.size(), albumTotal = allAlbums.size();
        if (dim == Dimension.GENRE) {
            // The genre list counts artists by their own genre and albums by the album cascade
            long[][] a = countByGender("SELECT gn.name AS gname, COUNT(DISTINCT ar.id) AS n FROM Artist ar LEFT JOIN Gender gn ON gn.id = ar.gender_id"
                    + " WHERE ar.genre_id = ? GROUP BY gn.name", keyParam);
            long[][] b = countByGender("SELECT gn.name AS gname, COUNT(DISTINCT al.id) AS n FROM Album al JOIN Artist ar ON ar.id = al.artist_id"
                    + " LEFT JOIN Gender gn ON gn.id = ar.gender_id WHERE COALESCE(al.override_genre_id, ar.genre_id) = ? GROUP BY gn.name", keyParam);
            artistCounts = a[0];
            artistTotal = a[1][0];
            albumCounts = b[0];
            albumTotal = b[1][0];
        }

        String theme = playsBy[0] > playsBy[1] ? "gender-male" : playsBy[1] > playsBy[0] ? "gender-female" : "";
        Map<Integer, Integer> genderIds = genderIds();
        String filter = dim.param + "=" + enc(key) + "&" + dim.param + "Mode=includes";
        String[][] listDefs = {{"Artists", "artists"}, {"Albums", "albums"}, {"Songs", "songs"}};
        long[] listTotals = {artistTotal, albumTotal, songs.size()};
        List<ListLink> links = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            links.add(new ListLink(listDefs[i][0], fmt(listTotals[i]), "/" + listDefs[i][1] + "?" + filter + "&view=table"));
        }
        long[][] counts = {artistCounts, albumCounts, songsBy};
        List<GenderRow> gender = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            long[] c = counts[i];
            int[] pct = shares(c);
            String base = "/" + listDefs[i][1] + "?" + filter + "&view=table&genderMode=includes&gender=";
            gender.add(new GenderRow(listDefs[i][0], fmt(c[0]), fmt(c[1]), pct[0], pct[2], pct[1],
                    genderIds.containsKey(0) ? base + genderIds.get(0) : null,
                    genderIds.containsKey(1) ? base + genderIds.get(1) : null));
        }
        int[] pp = shares(playsBy), tp = shares(timeBy);
        gender.add(new GenderRow("Plays", fmt(playsBy[0]), fmt(playsBy[1]), pp[0], pp[2], pp[1], null, null));
        gender.add(new GenderRow("Time", TimeFormatUtils.formatTime(timeBy[0]), TimeFormatUtils.formatTime(timeBy[1]), tp[0], tp[2], tp[1], null, null));

        // Breakdown table and mix cards from the same song rows
        Dimension bigDim = BIG.get(dim);
        List<BreakdownRow> breakdown = new ArrayList<>();
        List<Agg> bigAggs = aggregate(songs, bigDim);
        long bigMax = bigAggs.isEmpty() ? 0 : bigAggs.get(0).plays;
        for (Agg a : bigAggs) {
            int[] s = shares(a.playsBy);
            breakdown.add(new BreakdownRow(label(bigDim, a.key, names), songsHref(filter, bigDim, a.key), a.key == null,
                    fmt(a.plays), TimeFormatUtils.formatTime(a.time), s[0], s[2], pct(a.plays, bigMax)));
        }
        List<MixCard> mixes = new ArrayList<>();
        for (Dimension md : MIXES.get(dim)) {
            List<Agg> aggs = aggregate(songs, md);
            long max = aggs.isEmpty() ? 0 : aggs.get(0).plays;
            List<MixRow> rows = new ArrayList<>();
            for (Agg a : aggs) {
                if (a.plays == 0) continue;
                rows.add(new MixRow(label(md, a.key, names), songsHref(filter, md, a.key), a.key == null,
                        share(a.plays, plays), shares(a.playsBy)[0], pct(a.plays, max)));
            }
            mixes.add(new MixCard(md.plural, MIX_COLORS.get(md), rows));
        }

        // Every peer's plays per year and gender: yearly ranks, peer standings and this item's stream
        Map<String, long[]> peerTotals = new HashMap<>();
        Map<Integer, Map<String, Long>> playsByYear = new TreeMap<>(), timeByYear = new TreeMap<>();
        Map<Integer, long[][]> selfYears = new TreeMap<>();
        jdbcTemplate.query("SELECT " + dim.songExpr + " AS k, CAST(strftime('%Y', p.play_date) AS INTEGER) AS y, gn.name AS gname,"
                + " COUNT(*) AS n, SUM(COALESCE(s.length_seconds, 0)) AS secs FROM Play p JOIN Song s ON s.id = p.song_id"
                + " JOIN Artist ar ON ar.id = s.artist_id LEFT JOIN Album al ON al.id = s.album_id"
                + " LEFT JOIN Gender gn ON gn.id = COALESCE(s.override_gender_id, ar.gender_id)"
                + " WHERE " + dim.songExpr + " IS NOT NULL GROUP BY 1, 2, 3", (RowCallbackHandler) rs -> {
                    String k = rs.getString("k");
                    int y = rs.getInt("y");
                    boolean hasYear = !rs.wasNull();
                    int g = bucket(rs.getString("gname"));
                    long n = rs.getLong("n");
                    long[] t = peerTotals.computeIfAbsent(k, x -> new long[4]);
                    t[3] += n;
                    if (g >= 0) t[g] += n;
                    if (!hasYear) return;
                    long secs = rs.getLong("secs");
                    playsByYear.computeIfAbsent(y, x -> new HashMap<>()).merge(k, n, Long::sum);
                    timeByYear.computeIfAbsent(y, x -> new HashMap<>()).merge(k, secs, Long::sum);
                    if (k.equals(key) && g >= 0) {
                        long[][] v = selfYears.computeIfAbsent(y, x -> new long[2][3]);
                        v[0][g] += n;
                        v[1][g] += secs;
                    }
                });

        List<YearPill> years = new ArrayList<>();
        List<long[]> yearPlays = new ArrayList<>(), yearTime = new ArrayList<>();
        if (!selfYears.isEmpty()) {
            int first = ((TreeMap<Integer, long[][]>) selfYears).firstKey(), last = ((TreeMap<Integer, long[][]>) selfYears).lastKey();
            for (int y = first; y <= last; y++) {
                long[][] v = selfYears.getOrDefault(y, new long[2][3]);
                yearPlays.add(v[0]);
                yearTime.add(v[1]);
                long yp = v[0][0] + v[0][1] + v[0][2], yt = v[1][0] + v[1][1] + v[1][2];
                Integer pr = rankIn(playsByYear.getOrDefault(y, Map.of()), key, dim, names);
                Integer tr = rankIn(timeByYear.getOrDefault(y, Map.of()), key, dim, names);
                String among = " among " + dim.plural.toLowerCase(Locale.ROOT);
                years.add(new YearPill(y, "'" + String.valueOf(y).substring(2), pr, tr, compactCount(yp), compactTime(yt),
                        pr == null ? y + ": no plays" : y + ": #" + pr + among + " · " + fmt(yp) + " plays",
                        tr == null ? y + ": no plays" : y + ": #" + tr + among + " · " + TimeFormatUtils.formatTime(yt) + " listened"));
            }
        }
        Stream playsStream = stream(yearPlays, years, false);
        Stream timeStream = stream(yearTime, years, true);

        List<Map.Entry<String, long[]>> peers = peerTotals.entrySet().stream().filter(e -> e.getValue()[3] > 0)
                .sorted(Comparator.comparingLong((Map.Entry<String, long[]> e) -> -e.getValue()[3])
                        .thenComparing(e -> label(dim, e.getKey(), names), String.CASE_INSENSITIVE_ORDER)).toList();
        long peerMax = peers.isEmpty() ? 0 : peers.get(0).getValue()[3];
        List<PeerRow> byPlays = new ArrayList<>();
        int selfRank = 0;
        for (int i = 0; i < peers.size(); i++) {
            var e = peers.get(i);
            int[] s = shares(e.getValue());
            boolean self = e.getKey().equals(key);
            if (self) selfRank = i + 1;
            byPlays.add(new PeerRow(i + 1, label(dim, e.getKey(), names), dim.detailHref(e.getKey()), fmt(e.getValue()[3]),
                    s[0], s[2], pct(e.getValue()[3], peerMax), self));
        }
        List<PeerRow> sortedByMale = byPlays.stream().sorted(Comparator.comparingInt(PeerRow::malePct).reversed()
                .thenComparing(PeerRow::rank)).toList();
        List<PeerRow> byMale = new ArrayList<>();
        for (int i = 0; i < sortedByMale.size(); i++) {
            PeerRow r = sortedByMale.get(i);
            byMale.add(new PeerRow(i + 1, r.name(), r.href(), r.plays(), r.malePct(), r.otherPct(), r.barPct(), r.self()));
        }

        Long allPlays = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM Play WHERE song_id IS NOT NULL", Long.class);
        String rankLabel = selfRank == 0 ? null : "#" + selfRank + " of " + peers.size() + " " + dim.plural.toLowerCase(Locale.ROOT);
        String shareLabel = allPlays == null || allPlays == 0 || plays == 0 ? null : share(plays, allPlays) + "% of all your plays";

        String[] span = jdbcTemplate.queryForObject("SELECT MIN(p.play_date) AS f, MAX(p.play_date) AS l FROM Play p JOIN Song s ON s.id = p.song_id"
                + " JOIN Artist ar ON ar.id = s.artist_id LEFT JOIN Album al ON al.id = s.album_id WHERE " + dim.songExpr + " = ?",
                (rs, i) -> new String[]{rs.getString("f"), rs.getString("l")}, keyParam);
        String firstPlay = span[0] == null ? null : DetailPlaysService.displayDate(span[0].substring(0, 10));
        String lastPlay = span[1] == null ? null : DetailPlaysService.displayDate(span[1].substring(0, 10));
        String perDay = "-";
        if (span[0] != null && span[1] != null) {
            long days = ChronoUnit.DAYS.between(LocalDate.parse(span[0].substring(0, 10)), LocalDate.parse(span[1].substring(0, 10))) + 1;
            perDay = String.format(Locale.US, "%.1f", plays / (double) Math.max(1, days));
        }
        List<Stat> stats = List.of(new Stat("Plays", fmt(plays)), new Stat("Time", TimeFormatUtils.formatTime(time)), new Stat("Avg / day", perDay));

        String typeLabel = dim.label;
        if (dim == Dimension.GENRE) {
            Integer subs = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM SubGenre WHERE parent_genre_id = ?", Integer.class, keyParam);
            if (subs != null && subs > 0) typeLabel += " · " + subs + (subs == 1 ? " subgenre" : " subgenres");
        } else if (dim == Dimension.SUBGENRE) {
            List<String[]> parent = jdbcTemplate.query("SELECT g.id, g.name FROM SubGenre sg JOIN Genre g ON g.id = sg.parent_genre_id WHERE sg.id = ?",
                    (rs, i) -> new String[]{rs.getString("id"), rs.getString("name")}, keyParam);
            if (!parent.isEmpty()) {
                parentName = parent.get(0)[1];
                parentHref = Dimension.GENRE.detailHref(parent.get(0)[0]);
            }
        }

        return Optional.of(new Detail(dim, key, name, typeLabel, parentName, parentHref, theme, rankLabel, shareLabel,
                firstPlay, lastPlay, stats, links, gender, bigDim.plural, breakdown, playsStream, timeStream, years,
                byPlays, byMale, rhythm(dim, keyParam), mixes, plays));
    }

    /** Ids as their canonical number, country names as given; null when the key cannot name an item. */
    static String normalizeKey(Dimension dim, String rawKey) {
        if (rawKey == null || rawKey.isBlank()) return null;
        if (dim.byName()) return rawKey;
        try {
            return String.valueOf(Integer.parseInt(rawKey.trim()));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private record SongRow(int artistId, Integer albumId, long len, int gender, long plays, Map<Dimension, String> keys) {}

    private static final class Agg {
        final String key;
        long plays, time;
        final long[] playsBy = new long[3];

        Agg(String key) {
            this.key = key;
        }
    }

    /** Song rows grouped by another catalog, most played first, unset values last. */
    private static List<Agg> aggregate(List<SongRow> songs, Dimension by) {
        Map<String, Agg> map = new LinkedHashMap<>();
        for (SongRow s : songs) {
            String k = s.keys.get(by);
            Agg a = map.computeIfAbsent(k == null ? "\u0000" : k, x -> new Agg(k));
            a.plays += s.plays;
            a.time += s.len * (s.plays == 0 ? 1 : s.plays);
            if (s.gender >= 0) a.playsBy[s.gender] += s.plays;
        }
        return map.values().stream().sorted(Comparator.comparing((Agg a) -> a.key == null)
                .thenComparingLong(a -> -a.plays).thenComparingLong(a -> -a.time)).toList();
    }

    private long[][] countByGender(String sql, Object keyParam) {
        long[] by = new long[3];
        long[] total = new long[1];
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            int g = bucket(rs.getString("gname"));
            long n = rs.getLong("n");
            total[0] += n;
            if (g >= 0) by[g] += n;
        }, keyParam);
        return new long[][]{by, total};
    }

    /** Bucket (0 male, 1 female) to the Gender id the list filters take. */
    private Map<Integer, Integer> genderIds() {
        Map<Integer, Integer> ids = new HashMap<>();
        jdbcTemplate.query("SELECT id, name FROM Gender ORDER BY id", (RowCallbackHandler) rs -> {
            int b = bucket(rs.getString("name"));
            if (b == 0 || b == 1) ids.putIfAbsent(b, rs.getInt("id"));
        });
        return ids;
    }

    private static Integer rankIn(Map<String, Long> year, String key, Dimension dim, Map<Dimension, Map<String, String>> names) {
        if (!year.containsKey(key)) return null;
        long mine = year.get(key);
        String myName = label(dim, key, names);
        int rank = 1;
        for (var e : year.entrySet()) {
            if (e.getKey().equals(key)) continue;
            if (e.getValue() > mine || (e.getValue() == mine && label(dim, e.getKey(), names).compareToIgnoreCase(myName) < 0)) rank++;
        }
        return rank;
    }

    /** Male bottom, other in the middle, female on top; x spreads the years over 1000, y over 220. */
    static Stream stream(List<long[]> perYear, List<YearPill> years, boolean time) {
        if (perYear.isEmpty()) return new Stream("", "", "", null);
        Stream paths = paths(perYear);
        int n = perYear.size();
        int peak = 0;
        for (int i = 1; i < n; i++) {
            long[] v = perYear.get(i), p = perYear.get(peak);
            if (v[0] + v[1] + v[2] > p[0] + p[1] + p[2]) peak = i;
        }
        long[] pv = perYear.get(peak);
        long peakTotal = pv[0] + pv[1] + pv[2];
        String note = peakTotal == 0 ? null : "Peak year " + years.get(peak).year() + " with "
                + (time ? TimeFormatUtils.formatTime(peakTotal) + " listened" : fmt(peakTotal) + " plays");
        return new Stream(paths.male(), paths.other(), paths.female(), note);
    }

    /** Stacked areas (male bottom, other, female on top) for values {male, female, other} spread over a 1000 x 220 box. */
    static Stream paths(List<long[]> values) {
        if (values.isEmpty()) return new Stream("", "", "", null);
        long max = values.stream().mapToLong(v -> v[0] + v[1] + v[2]).max().orElse(0);
        int n = values.size();
        double[] x = new double[n], y0 = new double[n], y1 = new double[n], y2 = new double[n];
        for (int i = 0; i < n; i++) {
            long[] v = values.get(i);
            x[i] = n == 1 ? 500 : i * 1000.0 / (n - 1);
            y0[i] = 220 - scale(v[0], max);
            y1[i] = 220 - scale(v[0] + v[2], max);
            y2[i] = 220 - scale(v[0] + v[1] + v[2], max);
        }
        double[] base = new double[n];
        java.util.Arrays.fill(base, 220);
        return new Stream(area(x, base, y0), area(x, y0, y1), area(x, y1, y2), null);
    }

    /** Short count for the year pills: exact under 1,000, then 1.2k / 12k. */
    public static String compactCount(long n) {
        if (n < 1000) return String.valueOf(n);
        if (n < 10000) return String.format(Locale.US, "%.1fk", n / 1000.0).replace(".0k", "k");
        return Math.round(n / 1000.0) + "k";
    }

    /** Short time for the year pills: minutes under an hour, hours under 1,000, days after that. */
    public static String compactTime(long seconds) {
        if (seconds < 3600) return (seconds / 60) + "m";
        long hours = Math.round(seconds / 3600.0);
        return hours < 1000 ? hours + "h" : Math.round(seconds / 86400.0) + "d";
    }

    private static double scale(long v, long max) {
        return max == 0 ? 0 : v * 205.0 / max;
    }

    /** Closed path between a lower and an upper line. */
    private static String area(double[] x, double[] lower, double[] upper) {
        StringBuilder sb = new StringBuilder();
        boolean single = x.length == 1;
        double[] xs = single ? new double[]{480, 520} : x;
        double[] lo = single ? new double[]{lower[0], lower[0]} : lower;
        double[] up = single ? new double[]{upper[0], upper[0]} : upper;
        for (int i = 0; i < xs.length; i++) sb.append(i == 0 ? "M" : " L").append(num(xs[i])).append(',').append(num(up[i]));
        for (int i = xs.length - 1; i >= 0; i--) sb.append(" L").append(num(xs[i])).append(',').append(num(lo[i]));
        return sb.append(" Z").toString();
    }

    private static String num(double v) {
        return String.format(Locale.US, "%.1f", v);
    }

    /** Hour of day, day of week and month of year, leaving out the initial Last.fm load like the Plays tab. */
    Rhythm rhythm(Dimension dim, Object keyParam) {
        return rhythm(" FROM Play p JOIN Song s ON s.id = p.song_id JOIN Artist ar ON ar.id = s.artist_id"
                + " LEFT JOIN Album al ON al.id = s.album_id WHERE " + dim.songExpr + " = ? AND", keyParam);
    }

    /** Same panels over every play in the library, matched or not (the Home page). */
    public Rhythm globalRhythm() {
        return rhythm(" FROM Play p WHERE");
    }

    private Rhythm rhythm(String fromWhere, Object... params) {
        long[] hours = new long[24], week = new long[7], months = new long[12];
        long[] excluded = new long[1];
        jdbcTemplate.query("SELECT CAST(strftime('%H', p.play_date) AS INTEGER) AS h, CAST(strftime('%w', p.play_date) AS INTEGER) AS w,"
                + " CAST(strftime('%m', p.play_date) AS INTEGER) AS m, p.play_date LIKE '" + MaleFunFactsService.EXCLUDED_DAY + "%' AS ex,"
                + " COUNT(*) AS n" + fromWhere + " strftime('%H', p.play_date) IS NOT NULL"
                + " GROUP BY 1, 2, 3, 4", (RowCallbackHandler) rs -> {
                    long n = rs.getLong("n");
                    if (rs.getBoolean("ex")) {
                        excluded[0] += n;
                        return;
                    }
                    hours[rs.getInt("h")] += n;
                    week[(rs.getInt("w") + 6) % 7] += n;
                    months[rs.getInt("m") - 1] += n;
                }, params);
        return rhythm(hours, week, months, excluded[0]);
    }

    static Rhythm rhythm(long[] hours, long[] week, long[] months, long excluded) {
        long hSum = sum(hours), hMax = max(hours);
        String[] bandNames = {"Night", "Morning", "Afternoon", "Evening"};
        List<Band> hourBands = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            long part = 0;
            for (int h = i * 6; h < i * 6 + 6; h++) part += hours[h];
            hourBands.add(new Band(bandNames[i], hSum == 0 ? 0 : (int) Math.round(part * 100.0 / hSum), (i * 25) + "%", null, null));
        }
        int peakHour = indexOfMax(hours);
        List<Bar> hourBars = new ArrayList<>();
        for (int h = 0; h < 24; h++) {
            hourBars.add(new Bar(null, null, hMax == 0 ? 0 : (int) Math.max(hours[h] > 0 ? 2 : 0, Math.round(hours[h] * 110.0 / hMax)),
                    hMax > 0 && h == peakHour, String.format("%02d:00 · %s plays", h, fmt(hours[h]))));
        }
        String hourNote = hSum == 0 ? "No listen times recorded"
                : String.format(Locale.US, "Peak hour %02d:00 · %.1f%% of plays", peakHour, hours[peakHour] * 100.0 / hSum);

        long wSum = sum(week);
        List<Band> weekBands = List.of(
                columnBand("Weekdays", 0, 5, 7, part(week, 0, 5), wSum, null),
                columnBand("Weekend", 5, 2, 7, part(week, 5, 7), wSum, null));
        List<Bar> weekBars = columnBars(week, WEEKDAYS, WEEKDAY_NAMES);
        int peakDay = indexOfMax(week);
        String weekNote = wSum == 0 ? "No plays yet"
                : String.format(Locale.US, "Peak day %s · %.1f%% of plays", WEEKDAY_NAMES[peakDay], week[peakDay] * 100.0 / wSum);

        long mSum = sum(months);
        long winter = months[0] + months[1] + months[11];
        List<Band> monthBands = List.of(
                columnBand("Winter", 0, 2, 12, winter, mSum, "winter"),
                columnBand("Spring", 2, 3, 12, part(months, 2, 5), mSum, "spring"),
                columnBand("Summer", 5, 3, 12, part(months, 5, 8), mSum, "summer"),
                columnBand("Fall", 8, 3, 12, part(months, 8, 11), mSum, "fall"),
                columnBand(null, 11, 1, 12, 0, mSum, "winter"));
        String[] initials = {"J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"};
        List<Bar> monthBars = columnBars(months, initials, MONTHS);
        int peakMonth = indexOfMax(months);
        String monthNote = mSum == 0 ? "No plays yet"
                : String.format(Locale.US, "Peak month %s · %.1f%% of plays", MONTHS[peakMonth], months[peakMonth] * 100.0 / mSum);

        String excludedNote = excluded == 0 ? null : "Leaves out " + fmt(excluded) + " plays from "
                + DetailPlaysService.displayDate(MaleFunFactsService.EXCLUDED_DAY) + ", the initial Last.fm load without real listen dates.";
        return new Rhythm(hourBands, hourBars, hourNote, weekBands, weekBars, weekNote, monthBands, monthBars, monthNote, excludedNote);
    }

    /** Band over {@code count} of {@code columns} bar columns 6px apart, the layout of the Plays tab month bars. */
    private static Band columnBand(String name, int first, int count, int columns, long part, long total, String icon) {
        int gaps = (columns - 1) * 6;
        String left = "calc((100% - " + gaps + "px) * " + first + " / " + columns + " + " + (first * 6) + "px)";
        String width = "calc((100% - " + gaps + "px) * " + count + " / " + columns + " + " + ((count - 1) * 6) + "px)";
        return new Band(name, total == 0 ? 0 : (int) Math.round(part * 100.0 / total), left, width, icon);
    }

    private static List<Bar> columnBars(long[] values, String[] labels, String[] names) {
        long max = max(values);
        int best = indexOfMax(values);
        List<Bar> bars = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            bars.add(new Bar(values[i] == 0 ? "" : fmt(values[i]), labels[i],
                    max == 0 || values[i] == 0 ? 0 : (int) Math.max(2, Math.round(values[i] * 92.0 / max)),
                    max > 0 && i == best, names[i] + ": " + fmt(values[i]) + " plays"));
        }
        return bars;
    }

    private static long part(long[] v, int from, int to) {
        long s = 0;
        for (int i = from; i < to; i++) s += v[i];
        return s;
    }

    private static long sum(long[] v) {
        return part(v, 0, v.length);
    }

    private static long max(long[] v) {
        long m = 0;
        for (long x : v) m = Math.max(m, x);
        return m;
    }

    private static int indexOfMax(long[] v) {
        int best = 0;
        for (int i = 1; i < v.length; i++) if (v[i] > v[best]) best = i;
        return best;
    }

    /** Male, female and other shares that add up to 100; values from the first three buckets. */
    static int[] shares(long[] v) {
        long total = v[0] + v[1] + v[2];
        if (total == 0) return new int[]{0, 0, 0};
        int m = (int) Math.round(v[0] * 100.0 / total);
        int f = (int) Math.round(v[1] * 100.0 / total);
        f = Math.min(f, 100 - m);
        return new int[]{m, f, 100 - m - f};
    }

    private static int pct(long v, long max) {
        return max == 0 ? 0 : (int) Math.max(v > 0 ? 2 : 0, Math.round(v * 100.0 / max));
    }

    /** Share of a whole as text: one decimal under 10%, none above, "<0.1" for tiny ones. */
    static String share(long part, long whole) {
        if (whole == 0) return "0";
        double p = part * 100.0 / whole;
        if (p > 0 && p < 0.1) return "<0.1";
        return p < 10 ? String.format(Locale.US, "%.1f", p) : String.valueOf(Math.round(p));
    }

    /** Songs table filtered by the page item ({@code filter}) and the {@code by} value, or by {@code by} being empty. */
    private static String songsHref(String filter, Dimension by, String key) {
        String row = key == null ? by.param + "Mode=isnull" : by.param + "=" + enc(key) + "&" + by.param + "Mode=includes";
        return "/songs?" + filter + "&" + row + "&view=table";
    }

    private static String label(Dimension d, String key, Map<Dimension, Map<String, String>> names) {
        if (key == null) return "Not set";
        if (d.byName()) return key;
        return names.get(d).getOrDefault(key, "Unknown");
    }

    private static String fmt(long n) {
        return String.format(Locale.US, "%,d", n);
    }

    private static String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
