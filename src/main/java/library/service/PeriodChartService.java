package library.service;

import library.dto.ChartEntryDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extra columns and the summary strip of the Seasonal / Yearly chart page (charts/period-chart.html),
 * for both the finalized view and the draft editor:
 * plays inside the period, the item's best run on the Weekly chart, and every other seasonal or
 * yearly chart it made. Songs use their own ids, albums theirs.
 */
@Service
public class PeriodChartService {

    private static final DateTimeFormatter DISPLAY_DATE = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private final JdbcTemplate jdbcTemplate;

    public PeriodChartService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** One placement on another seasonal or yearly chart. */
    public record Appearance(String periodType, String periodKey, int position) {
        public String link() {
            return "/charts/" + periodType + "/" + periodKey;
        }

        public boolean seasonal() {
            return "seasonal".equals(periodType);
        }

        public String label() {
            return seasonal() ? SeasonalWallService.seasonLabel(periodKey) : periodKey;
        }

        /** Seasons of {@code year} drop the year ("Fall"); other seasons and years keep it. */
        public String labelFor(String year) {
            if (seasonal() && year != null && periodKey.startsWith(year + "-")) {
                return periodKey.substring(year.length() + 1);
            }
            return label();
        }
    }

    public record ItemStats(int plays, Integer weeklyPeak, int weeklyWeeks, List<Appearance> yearly, List<Appearance> seasons) {
        static final ItemStats EMPTY = new ItemStats(0, null, 0, List.of(), List.of());

        /** The best seasonal placement (lowest position, earliest on ties), or null. */
        public Appearance bestSeason() {
            return seasons.stream().min(Comparator.comparingInt(Appearance::position)).orElse(null);
        }

        public String weeklyBand() {
            if (weeklyPeak == null) return "co-band-4";
            if (weeklyPeak == 1) return "co-band-1";
            if (weeklyPeak <= 5) return "co-band-2";
            return weeklyPeak <= 10 ? "co-band-3" : "co-band-4";
        }
    }

    /** The #1 entry, the gender split of the counted entries and the artist with the most entries. */
    public record Summary(ChartEntryDTO top, ItemStats topStats, int male, int female, int other,
                          String leadArtist, Integer leadArtistId, int leadCount, int leadBest, String leadBestTitle) {
        public int total() {
            return male + female + other;
        }

        public double malePercent() {
            return total() == 0 ? 0 : male * 100.0 / total();
        }

        public double femalePercent() {
            return total() == 0 ? 0 : female * 100.0 / total();
        }
    }

    /** Stats for every listed item, keyed by song or album id. */
    public Map<Integer, ItemStats> stats(String chartType, List<ChartEntryDTO> entries, String periodType, String periodKey,
                                         String startDate, String endDate) {
        boolean album = "album".equals(chartType);
        List<Integer> ids = entries.stream()
                .map(album ? ChartEntryDTO::getAlbumId : ChartEntryDTO::getSongId)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .toList();
        return statsForIds(chartType, ids, periodType, periodKey, startDate, endDate);
    }

    public Map<Integer, ItemStats> statsForIds(String chartType, List<Integer> ids, String periodType, String periodKey,
                                         String startDate, String endDate) {
        Map<Integer, ItemStats> out = new HashMap<>();
        if (ids.isEmpty()) return out;
        boolean album = "album".equals(chartType);
        String idCol = album ? "album_id" : "song_id";
        String in = ids.stream().map(id -> "?").collect(Collectors.joining(","));

        Map<Integer, Integer> plays = new HashMap<>();
        if (startDate != null && endDate != null) {
            String playSql = album
                    ? "SELECT s.album_id AS id, COUNT(*) AS n FROM Play p INNER JOIN Song s ON s.id = p.song_id "
                      + "WHERE s.album_id IN (" + in + ") AND DATE(p.play_date) >= ? AND DATE(p.play_date) <= ? GROUP BY s.album_id"
                    : "SELECT p.song_id AS id, COUNT(*) AS n FROM Play p "
                      + "WHERE p.song_id IN (" + in + ") AND DATE(p.play_date) >= ? AND DATE(p.play_date) <= ? GROUP BY p.song_id";
            List<Object> args = new ArrayList<>(ids);
            args.add(startDate);
            args.add(endDate);
            jdbcTemplate.query(playSql, rs -> { plays.put(rs.getInt("id"), rs.getInt("n")); }, args.toArray());
        }

        Map<Integer, int[]> weekly = new HashMap<>();
        jdbcTemplate.query("SELECT ce." + idCol + " AS id, MIN(ce.position) AS peak, COUNT(*) AS weeks "
                        + "FROM ChartEntry ce INNER JOIN Chart c ON c.id = ce.chart_id "
                        + "WHERE c.period_type = 'weekly' AND c.chart_type = ? AND ce." + idCol + " IN (" + in + ") GROUP BY ce." + idCol,
                rs -> { weekly.put(rs.getInt("id"), new int[]{rs.getInt("peak"), rs.getInt("weeks")}); },
                prepend(chartType, ids).toArray());

        Map<Integer, List<Appearance>> appearances = new HashMap<>();
        List<Object> args = prepend(chartType, ids);
        args.add(periodType);
        args.add(periodKey);
        jdbcTemplate.query("SELECT ce." + idCol + " AS id, c.period_type, c.period_key, ce.position "
                        + "FROM ChartEntry ce INNER JOIN Chart c ON c.id = ce.chart_id "
                        + "WHERE c.period_type IN ('seasonal', 'yearly') AND c.chart_type = ? AND ce." + idCol + " IN (" + in + ") "
                        + "AND NOT (c.period_type = ? AND c.period_key = ?) "
                        + "ORDER BY c.period_start_date, c.period_type",
                rs -> {
                    appearances.computeIfAbsent(rs.getInt("id"), k -> new ArrayList<>())
                            .add(new Appearance(rs.getString("period_type"), rs.getString("period_key"), rs.getInt("position")));
                }, args.toArray());

        for (Integer id : ids) {
            int[] run = weekly.get(id);
            List<Appearance> all = appearances.getOrDefault(id, List.of());
            out.put(id, new ItemStats(plays.getOrDefault(id, 0),
                    run == null ? null : run[0], run == null ? 0 : run[1],
                    all.stream().filter(a -> !a.seasonal()).toList(),
                    all.stream().filter(Appearance::seasonal).toList()));
        }
        return out;
    }

    /** Gender of one song (override aware) or album (its artist), for rows added in the editor. */
    public Integer genderId(String chartType, int id) {
        String sql = "album".equals(chartType)
                ? "SELECT ar.gender_id FROM Album a INNER JOIN Artist ar ON ar.id = a.artist_id WHERE a.id = ?"
                : "SELECT COALESCE(s.override_gender_id, ar.gender_id) FROM Song s INNER JOIN Artist ar ON ar.id = s.artist_id WHERE s.id = ?";
        List<Integer> rows = jdbcTemplate.query(sql, (rs, n) -> {
            int value = rs.getInt(1);
            return rs.wasNull() ? null : value;
        }, id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** Summary of the counted entries (seasonal bubbling-under songs are never passed in). */
    public Summary summary(List<ChartEntryDTO> entries, Map<Integer, ItemStats> stats, boolean album) {
        if (entries == null || entries.isEmpty()) return null;
        ChartEntryDTO top = entries.stream().min(Comparator.comparing(ChartEntryDTO::getPosition)).orElseThrow();
        int male = 0;
        int female = 0;
        int other = 0;
        Map<Integer, List<ChartEntryDTO>> byArtist = new LinkedHashMap<>();
        for (ChartEntryDTO entry : entries) {
            Integer gender = entry.getGenderId();
            if (gender != null && gender == 2) male++;
            else if (gender != null && gender == 1) female++;
            else other++;
            byArtist.computeIfAbsent(entry.getArtistId(), k -> new ArrayList<>()).add(entry);
        }
        List<ChartEntryDTO> lead = byArtist.values().stream()
                .max(Comparator.<List<ChartEntryDTO>>comparingInt(List::size)
                        .thenComparing(list -> list.stream().mapToInt(ChartEntryDTO::getPosition).min().orElse(999), Comparator.reverseOrder()))
                .orElse(List.of(top));
        ChartEntryDTO leadBest = lead.stream().min(Comparator.comparing(ChartEntryDTO::getPosition)).orElse(top);
        Integer topId = album ? top.getAlbumId() : top.getSongId();
        return new Summary(top, stats.getOrDefault(topId, ItemStats.EMPTY), male, female, other,
                leadBest.getArtistName(), leadBest.getArtistId(), lead.size(), leadBest.getPosition(),
                album ? leadBest.getAlbumName() : leadBest.getSongName());
    }

    /** 2025-09-01 to 01/09/2025; anything unparsable comes back as is. */
    public static String displayDate(String isoDate) {
        if (isoDate == null || isoDate.length() < 10) return isoDate;
        try {
            return LocalDate.parse(isoDate.substring(0, 10)).format(DISPLAY_DATE);
        } catch (RuntimeException e) {
            return isoDate;
        }
    }

    private static List<Object> prepend(Object first, List<Integer> ids) {
        List<Object> args = new ArrayList<>();
        args.add(first);
        args.addAll(ids);
        return args;
    }
}
