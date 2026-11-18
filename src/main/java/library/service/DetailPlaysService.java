package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Data for the Plays tab of the artist, album and song detail pages: the over-time chart (per month,
 * running total, calendar), plays by year, listening rhythm, latest plays and the overall rank moves.
 *
 * Plays follow the page's scope (an artist's Main / Groups / Featured toggles). Ranks always use the
 * catalog definition shown on the General tab: an artist's own songs, an album's songs, the song itself.
 */
@Service
public class DetailPlaysService {
    public enum Kind { ARTIST, ALBUM, SONG }

    static final int LATEST_PLAYS = 10;
    static final int RANK_MOVES = 25;

    private final JdbcTemplate jdbcTemplate;

    public DetailPlaysService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Which plays count for the page: a SQL predicate over {@code Play p JOIN Song s}. */
    public record Scope(Kind kind, int id, String predicate, List<Object> params, Set<Integer> groupIds) {}

    public Scope artistScope(int artistId, List<Integer> groupIds, boolean includeMain, boolean includeFeatured) {
        List<Object> params = new ArrayList<>();
        List<String> parts = new ArrayList<>();
        if (includeMain) {
            parts.add("s.artist_id = ?");
            params.add(artistId);
        }
        if (groupIds != null && !groupIds.isEmpty()) {
            parts.add("s.artist_id IN (" + String.join(",", groupIds.stream().map(id -> "?").toList()) + ")");
            params.addAll(groupIds);
        }
        if (includeFeatured) {
            parts.add("EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id AND sfa.artist_id = ?)");
            params.add(artistId);
        }
        String predicate = parts.isEmpty() ? "0 = 1" : "(" + String.join(" OR ", parts) + ")";
        return new Scope(Kind.ARTIST, artistId, predicate, params, groupIds == null ? Set.of() : Set.copyOf(groupIds));
    }

    public Scope albumScope(int albumId) {
        return new Scope(Kind.ALBUM, albumId, "s.album_id = ?", List.of(albumId), Set.of());
    }

    public Scope songScope(int songId) {
        return new Scope(Kind.SONG, songId, "s.id = ?", List.of(songId), Set.of());
    }

    public record TopSong(int id, String name, long plays) {}

    /**
     * One row of Plays by year. {@code barPct} sizes the plays bar against the busiest year, {@code shades}
     * colors the Jan to Dec strip against the busiest month overall, {@code heights} draws the opened
     * year's month bars (px, against that year's best month).
     */
    public record YearRow(int year, long plays, long primary, long legacy, String share, Integer genderRank,
                          Integer overallRank, List<Long> months, List<TopSong> topSongs, Integer change,
                          boolean partial, int barPct, List<String> shades, List<Integer> heights) {
        public long bestMonthPlays() {
            return months.stream().mapToLong(Long::longValue).max().orElse(0);
        }

        /** 1 to 12, or 0 when the year has no plays. */
        public int bestMonth() {
            return bestMonthPlays() == 0 ? 0 : months.indexOf(bestMonthPlays()) + 1;
        }

        public long activeMonths() {
            return months.stream().filter(v -> v > 0).count();
        }
    }

    public record LatestPlay(String date, String time, int songId, String songName, Integer albumId,
                             String albumName, String sourceLabel) {}

    /**
     * Everything the tab renders. {@code chart} goes to the page script as JSON: daily totals, plays by
     * hour per year, milestones and releases.
     */
    public record PlaysTab(long total, long primary, long legacy, List<YearRow> years,
                           List<LatestPlay> latest, Map<String, Object> chart) {}

    /** {@code yearRanks} comes from {@link #yearRanks}, which the General tab's rank-by-year tiles use too. */
    public PlaysTab build(Scope scope, Map<Integer, Integer[]> yearRanks) {
        String from = " FROM Play p JOIN Song s ON s.id = p.song_id WHERE " + scope.predicate() + " ";
        Object[] params = scope.params().toArray();

        TreeMap<String, long[]> days = new TreeMap<>();
        jdbcTemplate.query("SELECT date(p.play_date) AS day, COUNT(*) AS n, "
                + "SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) AS legacy" + from
                + "AND date(p.play_date) IS NOT NULL GROUP BY 1 ORDER BY 1", (RowCallbackHandler) rs ->
                days.put(rs.getString("day"), new long[]{rs.getLong("n"), rs.getLong("legacy")}), params);

        Map<Integer, long[]> monthsByYear = new TreeMap<>();
        Map<Integer, long[]> legacyByYear = new HashMap<>();
        long total = 0, legacy = 0;
        for (var e : days.entrySet()) {
            int year = Integer.parseInt(e.getKey().substring(0, 4));
            int month = Integer.parseInt(e.getKey().substring(5, 7));
            monthsByYear.computeIfAbsent(year, y -> new long[12])[month - 1] += e.getValue()[0];
            legacyByYear.computeIfAbsent(year, y -> new long[1])[0] += e.getValue()[1];
            total += e.getValue()[0];
            legacy += e.getValue()[1];
        }
        long maxMonth = monthsByYear.values().stream().flatMapToLong(java.util.Arrays::stream).max().orElse(0);

        Map<Integer, List<TopSong>> topSongs = scope.kind() == Kind.SONG ? Map.of() : topSongsByYear(from, params);
        int currentYear = LocalDate.now(ZoneId.of("America/Mexico_City")).getYear();
        long maxYear = monthsByYear.values().stream().mapToLong(m -> java.util.Arrays.stream(m).sum()).max().orElse(0);
        List<YearRow> years = new ArrayList<>();
        if (!monthsByYear.isEmpty()) {
            int first = ((TreeMap<Integer, long[]>) monthsByYear).firstKey();
            int last = ((TreeMap<Integer, long[]>) monthsByYear).lastKey();
            Long previous = null;
            for (int year = first; year <= last; year++) {
                long[] months = monthsByYear.getOrDefault(year, new long[12]);
                long plays = java.util.Arrays.stream(months).sum();
                long best = java.util.Arrays.stream(months).max().orElse(0);
                long yearLegacy = legacyByYear.getOrDefault(year, new long[1])[0];
                Integer change = previous == null || previous == 0 ? null
                        : (int) Math.round((plays - previous) * 100.0 / previous);
                Integer[] rank = yearRanks.getOrDefault(year, new Integer[2]);
                List<String> shades = new ArrayList<>();
                List<Integer> heights = new ArrayList<>();
                for (long v : months) {
                    shades.add(v == 0 || maxMonth == 0 ? "0.05"
                            : String.format(java.util.Locale.US, "%.2f", 0.12 + 0.88 * Math.sqrt((double) v / maxMonth)));
                    heights.add(v == 0 || best == 0 ? 0 : (int) Math.max(2, Math.round(v * 92.0 / best)));
                }
                years.add(new YearRow(year, plays, plays - yearLegacy, yearLegacy,
                        total == 0 ? "0.0" : String.format(java.util.Locale.US, "%.1f", plays * 100.0 / total),
                        rank[1], rank[0], java.util.Arrays.stream(months).boxed().toList(),
                        topSongs.getOrDefault(year, List.of()), change, year == currentYear,
                        maxYear == 0 ? 0 : (int) Math.round(plays * 100.0 / maxYear), shades, heights));
                previous = plays;
            }
            java.util.Collections.reverse(years);
        }

        Map<String, Object> chart = new LinkedHashMap<>();
        chart.put("kind", scope.kind().name().toLowerCase());
        chart.put("days", days.entrySet().stream().map(e -> List.of(e.getKey(), e.getValue()[0])).toList());
        chart.put("hours", hoursByYear(from, params));
        chart.put("milestones", milestones(from, params, scope.kind(), total));
        chart.put("releases", releases(scope));
        chart.put("excludedDay", MaleFunFactsService.EXCLUDED_DAY);
        return new PlaysTab(total, total - legacy, legacy, years, latest(params, scope), chart);
    }

    /** Milestone play numbers: artists 50, 100, 200, 500, 1,000 then every 1,000; albums every 100 up to 1,000, then every 500; songs every 50. */
    static List<Integer> milestoneSteps(Kind kind, long total) {
        List<Integer> steps = new ArrayList<>();
        switch (kind) {
            case ARTIST -> {
                for (int n : new int[]{50, 100, 200, 500}) if (n <= total) steps.add(n);
                for (long n = 1000; n <= total; n += 1000) steps.add((int) n);
            }
            case ALBUM -> {
                for (int n = 100; n <= Math.min(1000, total); n += 100) steps.add(n);
                for (long n = 1500; n <= total; n += 500) steps.add((int) n);
            }
            case SONG -> {
                for (long n = 50; n <= total; n += 50) steps.add((int) n);
            }
        }
        return steps;
    }

    private List<Map<String, Object>> milestones(String from, Object[] params, Kind kind, long total) {
        List<Integer> steps = milestoneSteps(kind, total);
        if (steps.isEmpty()) return List.of();
        List<Object> args = new ArrayList<>(List.of(params));
        args.addAll(steps);
        List<Map<String, Object>> result = new ArrayList<>();
        jdbcTemplate.query("SELECT rn, play_date, song_name FROM (SELECT ROW_NUMBER() OVER (ORDER BY p.play_date, p.id) AS rn, "
                + "p.play_date, s.name AS song_name" + from + ") WHERE rn IN ("
                + String.join(",", steps.stream().map(n -> "?").toList()) + ") ORDER BY rn", (RowCallbackHandler) rs -> {
            String date = rs.getString("play_date");
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("n", rs.getInt("rn"));
            m.put("day", date == null ? null : date.substring(0, 10));
            m.put("song", rs.getString("song_name"));
            result.add(m);
        }, args.toArray());
        return result;
    }

    private Map<String, long[]> hoursByYear(String from, Object[] params) {
        Map<String, long[]> hours = new TreeMap<>();
        jdbcTemplate.query("SELECT strftime('%Y', p.play_date) AS y, CAST(strftime('%H', p.play_date) AS INTEGER) AS h, COUNT(*) AS n"
                + from + "AND strftime('%H', p.play_date) IS NOT NULL AND p.play_date NOT LIKE '"
                + MaleFunFactsService.EXCLUDED_DAY + "%' GROUP BY 1, 2", (RowCallbackHandler) rs ->
                hours.computeIfAbsent(rs.getString("y"), y -> new long[24])[rs.getInt("h")] = rs.getLong("n"), params);
        return hours;
    }

    private Map<Integer, List<TopSong>> topSongsByYear(String from, Object[] params) {
        Map<Integer, List<TopSong>> result = new HashMap<>();
        jdbcTemplate.query("SELECT CAST(strftime('%Y', p.play_date) AS INTEGER) AS y, s.id, s.name, COUNT(*) AS n" + from
                + "AND strftime('%Y', p.play_date) IS NOT NULL GROUP BY 1, s.id ORDER BY 1, n DESC, s.name", (RowCallbackHandler) rs -> {
            List<TopSong> list = result.computeIfAbsent(rs.getInt("y"), y -> new ArrayList<>());
            if (list.size() < 3) list.add(new TopSong(rs.getInt("id"), rs.getString("name"), rs.getLong("n")));
        }, params);
        return result;
    }

    private List<Map<String, Object>> releases(Scope scope) {
        String sql = switch (scope.kind()) {
            case ARTIST -> "SELECT name, release_date FROM Album WHERE artist_id = ? AND release_date IS NOT NULL ORDER BY release_date";
            case ALBUM -> "SELECT name, release_date FROM Album WHERE id = ? AND release_date IS NOT NULL";
            case SONG -> "SELECT s.name, COALESCE(s.release_date, al.release_date) AS release_date FROM Song s "
                    + "LEFT JOIN Album al ON al.id = s.album_id WHERE s.id = ? AND COALESCE(s.release_date, al.release_date) IS NOT NULL";
        };
        List<Map<String, Object>> result = new ArrayList<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            String date = rs.getString("release_date");
            if (date != null && date.length() >= 10) result.add(Map.of("name", rs.getString("name"), "day", date.substring(0, 10)));
        }, scope.id());
        return result;
    }

    private List<LatestPlay> latest(Object[] params, Scope scope) {
        return jdbcTemplate.query("SELECT p.play_date, s.id, s.name, s.artist_id, al.id AS album_id, al.name AS album_name, ar.name AS artist_name"
                + " FROM Play p JOIN Song s ON s.id = p.song_id JOIN Artist ar ON ar.id = s.artist_id LEFT JOIN Album al ON al.id = s.album_id"
                + " WHERE " + scope.predicate() + " AND p.play_date IS NOT NULL ORDER BY p.play_date DESC, p.id DESC LIMIT " + LATEST_PLAYS,
                (rs, i) -> {
                    String date = rs.getString("play_date");
                    String label = null;
                    if (scope.kind() == Kind.ARTIST && rs.getInt("artist_id") != scope.id()) {
                        label = (scope.groupIds().contains(rs.getInt("artist_id")) ? "With " : "Feat. on ") + rs.getString("artist_name");
                    }
                    int rawAlbumId = rs.getInt("album_id");
                    Integer albumId = rs.wasNull() ? null : rawAlbumId;
                    return new LatestPlay(displayDate(date.substring(0, 10)),
                            date.length() >= 16 ? date.substring(11, 16) : "", rs.getInt("id"), rs.getString("name"),
                            albumId, rs.getString("album_name"), label);
                }, params);
    }

    /** Ranking definition per catalog: the item key, the join that ties a play to it, and its gender. */
    private record Ranking(String table, String key, String join, String gender, String name) {}

    private static Ranking ranking(Kind kind) {
        return switch (kind) {
            case ARTIST -> new Ranking("Artist e", "e.id", "JOIN Song s ON s.artist_id = e.id", "e.gender_id", "e.name");
            case ALBUM -> new Ranking("Album e JOIN Artist ar ON ar.id = e.artist_id", "e.id",
                    "JOIN Song s ON s.album_id = e.id", "ar.gender_id", "e.name");
            case SONG -> new Ranking("Song s JOIN Artist ar ON ar.id = s.artist_id", "s.id", "",
                    "COALESCE(s.override_gender_id, ar.gender_id)", "s.name");
        };
    }

    /** Year -> [overall rank, gender rank] among the items with plays that year (same ordering as the General tab). */
    public Map<Integer, Integer[]> yearRanks(Kind kind, int id) {
        Ranking r = ranking(kind);
        String sql = """
                WITH year_song_plays AS MATERIALIZED (
                    SELECT strftime('%Y', play_date) AS year, song_id, COUNT(*) AS plays FROM Play GROUP BY year, song_id
                ), totals AS (
                    SELECT {key} AS id, {gender} AS gender_id, ysp.year, SUM(ysp.plays) AS plays
                    FROM {table} {join} JOIN year_song_plays ysp ON ysp.song_id = s.id
                    GROUP BY {key}, ysp.year
                )
                SELECT year, overall_rank, gender_rank FROM (
                    SELECT id, year,
                           ROW_NUMBER() OVER (PARTITION BY year ORDER BY plays DESC, id) AS overall_rank,
                           CASE WHEN gender_id IS NOT NULL
                                THEN ROW_NUMBER() OVER (PARTITION BY year, gender_id ORDER BY plays DESC, id) END AS gender_rank
                    FROM totals
                ) WHERE id = ? AND year IS NOT NULL
                """.replace("{key}", r.key()).replace("{gender}", r.gender())
                .replace("{table}", r.table()).replace("{join}", r.join());
        Map<Integer, Integer[]> result = new LinkedHashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> result.put(Integer.parseInt(rs.getString("year")),
                new Integer[]{rs.getInt("overall_rank"), (Integer) rs.getObject("gender_rank")}), id);
        return result;
    }

    /** Year -> overall rank, the shape the General tab's rank-by-year tiles use. */
    public static Map<Integer, Integer> overallByYear(Map<Integer, Integer[]> yearRanks) {
        Map<Integer, Integer> result = new LinkedHashMap<>();
        yearRanks.forEach((year, ranks) -> result.put(year, ranks[0]));
        return result;
    }

    public record RankMove(String date, boolean up, int from, int to, int otherId, String otherName, String otherGender) {}

    public record RankMoves(Integer rank, long ups, long downs, List<RankMove> moves) {}

    /**
     * The latest overall rank changes against every other item of the same catalog, newest first.
     * Replays daily running totals: the item passes another when its total goes above the other's, and
     * falls when another goes above it (an equal total keeps whoever was ahead).
     */
    public RankMoves rankMoves(Kind kind, int id) {
        Ranking r = ranking(kind);
        Map<Integer, Object[]> info = new HashMap<>();
        jdbcTemplate.query("SELECT " + r.key() + " AS id, " + r.name() + " AS name, " + r.gender() + " AS gender_id FROM " + r.table(),
                (RowCallbackHandler) rs -> info.put(rs.getInt("id"), new Object[]{rs.getString("name"), (Integer) rs.getObject("gender_id")}),
                new Object[0]);
        TreeMap<String, Map<Integer, Long>> byDay = new TreeMap<>();
        jdbcTemplate.query("SELECT " + r.key() + " AS id, date(p.play_date) AS day, COUNT(*) AS n FROM " + r.table() + " " + r.join()
                + " JOIN Play p ON p.song_id = s.id WHERE date(p.play_date) IS NOT NULL GROUP BY 1, 2", (RowCallbackHandler) rs ->
                byDay.computeIfAbsent(rs.getString("day"), d -> new HashMap<>()).put(rs.getInt("id"), rs.getLong("n")), new Object[0]);
        return replay(id, byDay, info);
    }

    /** Rank moves from per-day play counts ({@code day -> item -> plays}); {@code info} holds each item's name and gender id. */
    static RankMoves replay(int id, TreeMap<String, Map<Integer, Long>> byDay, Map<Integer, Object[]> info) {
        Map<Integer, Long> totals = new HashMap<>();
        Map<Integer, Boolean> ahead = new HashMap<>();
        int behind = 0;
        List<RankMove> moves = new ArrayList<>();
        for (var day : byDay.entrySet()) {
            long selfBefore = totals.getOrDefault(id, 0L);
            day.getValue().forEach((key, n) -> totals.merge(key, n, Long::sum));
            long selfNow = totals.getOrDefault(id, 0L);
            if (selfNow == 0) continue;
            Collection<Integer> toCheck = day.getValue().containsKey(id) ? totals.keySet() : day.getValue().keySet();
            List<Integer> passed = new ArrayList<>();
            List<Integer> passedBy = new ArrayList<>();
            for (int other : toCheck) {
                if (other == id) continue;
                long otherNow = totals.get(other);
                Boolean was = ahead.get(other);
                boolean now = selfNow > otherNow || (selfNow == otherNow && Boolean.TRUE.equals(was));
                if (was != null && was != now) (now ? passed : passedBy).add(other);
                else if (was == null && selfBefore > 0 && !now) passedBy.add(other);
                if (was == null || was != now) behind += now ? (was == null ? 0 : -1) : 1;
                ahead.put(other, now);
            }
            if (passed.isEmpty() && passedBy.isEmpty()) continue;
            int rank = 1 + behind - passedBy.size() + passed.size();
            String date = displayDate(day.getKey());
            passedBy.sort(java.util.Comparator.comparingLong(totals::get));
            passed.sort(java.util.Comparator.comparingLong((Integer o) -> totals.get(o)).reversed());
            for (int other : passedBy) moves.add(move(date, false, rank, ++rank, other, info));
            for (int other : passed) moves.add(move(date, true, rank, --rank, other, info));
        }
        int rank = 1 + behind;
        List<RankMove> latest = new ArrayList<>(moves.subList(Math.max(0, moves.size() - RANK_MOVES), moves.size()));
        java.util.Collections.reverse(latest);
        long ups = latest.stream().filter(RankMove::up).count();
        return new RankMoves(totals.containsKey(id) ? rank : null, ups, latest.size() - ups, latest);
    }

    /** yyyy-MM-dd to dd/MM/yyyy. */
    static String displayDate(String isoDay) {
        return isoDay.substring(8, 10) + "/" + isoDay.substring(5, 7) + "/" + isoDay.substring(0, 4);
    }

    private static RankMove move(String date, boolean up, int from, int to, int other, Map<Integer, Object[]> info) {
        Object[] o = info.getOrDefault(other, new Object[]{"Unknown", null});
        Integer gender = (Integer) o[1];
        String g = Integer.valueOf(2).equals(gender) ? "male" : Integer.valueOf(1).equals(gender) ? "female" : "other";
        return new RankMove(date, up, from, to, other, (String) o[0], g);
    }
}
