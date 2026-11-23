package library.service;

import library.dto.ChartOverviewRows.SongRow;
import library.service.ChartOverviewService.Chart;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Runs at #1 for the Number Ones pages of every chart (Weekly songs and albums, Hot 100, TRL, Vato's Cuntdown).
 * A run is a stretch of consecutive chart periods (weeks or days) with the same item at #1.
 */
@Service
public class NumberOneService {

    /** What a row shows for one item. {@code gender} is "male", "female" or null. */
    public record Display(String title, String artistName, Integer artistId, String gender,
                          String link, String coverUrl, String coverHoverUrl) {
    }

    /** One chart period's #1: its first and last day (ISO) and the item's identity key. */
    public record TopEntry(String startDate, String endDate, String key) {
    }

    /** One run at #1. Dates are ISO; {@code runNumber} counts this item's runs from its first. */
    public record Run(String key, Display display, String startDate, String endDate,
                      int length, int runNumber, int cumulative, int total) {

        public String startDisplay() {
            return displayDate(startDate);
        }

        public String endDisplay() {
            return displayDate(endDate);
        }

        public String year() {
            return startDate == null || startDate.length() < 4 ? "" : startDate.substring(0, 4);
        }
    }

    private static final Display UNKNOWN = new Display("Unknown", "Unknown", null, null, null, null, null);

    private final JdbcTemplate jdbcTemplate;
    private final ChartOverviewService chartOverviewService;

    public NumberOneService(JdbcTemplate jdbcTemplate, ChartOverviewService chartOverviewService) {
        this.jdbcTemplate = jdbcTemplate;
        this.chartOverviewService = chartOverviewService;
    }

    /** Every #1 run on the chart, most recent first. Albums exist on the weekly chart only. */
    public List<Run> runs(Chart chart, String type) {
        Map<String, Display> displays = new HashMap<>();
        List<TopEntry> entries = switch (chart) {
            case WEEKLY -> "album".equals(type) ? weeklyAlbums(displays) : weeklySongs(displays);
            case HOT100 -> externalEntries(chart, displays, """
                    SELECT chart_date, song_id, artist_name, song_title, NULL AS debut_id
                    FROM billboard_hot100_entry WHERE position = 1 ORDER BY chart_date, id
                    """);
            case TRL -> externalEntries(chart, displays, """
                    SELECT chart_date, NULL AS song_id, artist_name, song_title, debut_id
                    FROM trl_chart_entry WHERE position = 1 ORDER BY chart_date, id
                    """);
            case CUNTDOWN -> externalEntries(chart, displays, """
                    SELECT chart_date, song_id, artist_name, song_title, NULL AS debut_id
                    FROM vatos_cuntdown_entry WHERE position = 1 AND is_close_call = 0 ORDER BY chart_date, id
                    """);
        };
        List<Run> runs = new ArrayList<>();
        for (Run run : buildRuns(entries)) {
            runs.add(new Run(run.key(), displays.getOrDefault(run.key(), UNKNOWN), run.startDate(), run.endDate(),
                    run.length(), run.runNumber(), run.cumulative(), run.total()));
        }
        return runs;
    }

    /**
     * Groups chronological #1 entries into runs (one entry per chart period; a second entry on the same
     * period is ignored) and returns them most recent first, with run number, cumulative and total length.
     */
    public static List<Run> buildRuns(List<TopEntry> entries) {
        List<Run> chronological = new ArrayList<>();
        Map<String, Integer> cumulative = new HashMap<>();
        Map<String, Integer> runCount = new HashMap<>();
        String lastPeriod = null;
        TopEntry first = null;
        TopEntry last = null;
        int length = 0;
        for (TopEntry entry : entries) {
            if (entry.key() == null || entry.startDate() == null || entry.startDate().equals(lastPeriod)) continue;
            lastPeriod = entry.startDate();
            if (last != null && last.key().equals(entry.key())) {
                last = entry;
                length++;
                continue;
            }
            if (first != null) chronological.add(close(first, last, length, cumulative, runCount));
            first = entry;
            last = entry;
            length = 1;
        }
        if (first != null) chronological.add(close(first, last, length, cumulative, runCount));

        List<Run> runs = new ArrayList<>(chronological.size());
        for (int i = chronological.size() - 1; i >= 0; i--) {
            Run run = chronological.get(i);
            runs.add(new Run(run.key(), null, run.startDate(), run.endDate(), run.length(), run.runNumber(),
                    run.cumulative(), cumulative.get(run.key())));
        }
        return runs;
    }

    private static Run close(TopEntry first, TopEntry last, int length, Map<String, Integer> cumulative, Map<String, Integer> runCount) {
        int total = cumulative.merge(first.key(), length, Integer::sum);
        int number = runCount.merge(first.key(), 1, Integer::sum);
        return new Run(first.key(), null, first.startDate(), last.endDate(), length, number, total, 0);
    }

    // ---------------------------------------------------------------- weekly chart

    private List<TopEntry> weeklySongs(Map<String, Display> displays) {
        List<TopEntry> entries = new ArrayList<>();
        jdbcTemplate.query("""
                SELECT s.id, s.name, ar.name AS artist_name, ar.id AS artist_id, ar.gender_id, s.album_id,
                       (CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) AS has_image,
                       (CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END) AS album_has_image,
                       c.period_start_date, c.period_end_date
                FROM ChartEntry ce
                JOIN Chart c ON ce.chart_id = c.id
                JOIN Song s ON ce.song_id = s.id
                JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                WHERE c.chart_type = 'song' AND c.period_type = 'weekly' AND ce.position = 1
                ORDER BY c.period_start_date
                """, rs -> {
            String key = String.valueOf(rs.getInt("id"));
            Integer albumId = intOrNull(rs.getObject("album_id"));
            boolean songImage = rs.getInt("has_image") == 1;
            boolean albumImage = albumId != null && rs.getInt("album_has_image") == 1;
            String songImageUrl = songImage ? "/songs/" + key + "/image?thumbnail=true" : null;
            String albumImageUrl = albumImage ? "/albums/" + albumId + "/image?thumbnail=true" : null;
            displays.putIfAbsent(key, new Display(rs.getString("name"), rs.getString("artist_name"), rs.getInt("artist_id"),
                    genderFromId(intOrNull(rs.getObject("gender_id"))), "/songs/" + key,
                    albumImageUrl != null ? albumImageUrl : songImageUrl, albumImageUrl != null ? songImageUrl : null));
            entries.add(new TopEntry(rs.getString("period_start_date"), rs.getString("period_end_date"), key));
        });
        return entries;
    }

    private List<TopEntry> weeklyAlbums(Map<String, Display> displays) {
        List<TopEntry> entries = new ArrayList<>();
        jdbcTemplate.query("""
                SELECT al.id, al.name, ar.name AS artist_name, ar.id AS artist_id, ar.gender_id,
                       (CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END) AS has_image,
                       c.period_start_date, c.period_end_date
                FROM ChartEntry ce
                JOIN Chart c ON ce.chart_id = c.id
                JOIN Album al ON ce.album_id = al.id
                JOIN Artist ar ON al.artist_id = ar.id
                WHERE c.chart_type = 'album' AND c.period_type = 'weekly' AND ce.position = 1
                ORDER BY c.period_start_date
                """, rs -> {
            String key = String.valueOf(rs.getInt("id"));
            displays.putIfAbsent(key, new Display(rs.getString("name"), rs.getString("artist_name"), rs.getInt("artist_id"),
                    genderFromId(intOrNull(rs.getObject("gender_id"))), "/albums/" + key,
                    rs.getInt("has_image") == 1 ? "/albums/" + key + "/image?thumbnail=true" : null, null));
            entries.add(new TopEntry(rs.getString("period_start_date"), rs.getString("period_end_date"), key));
        });
        return entries;
    }

    /** Gender ids follow the Gender lookup: 1 female, 2 male. */
    static String genderFromId(Integer genderId) {
        if (genderId == null) return null;
        return genderId == 2 ? "male" : genderId == 1 ? "female" : null;
    }

    // ---------------------------------------------------------------- Hot 100, TRL, Vato's Cuntdown

    /**
     * Daily (TRL, Cuntdown) or weekly (Hot 100) #1 entries. Linked songs, TRL debuts and unlinked names are
     * looked up in the overview rows so the page shows the same names, gender and covers as the Chart Overview.
     */
    private List<TopEntry> externalEntries(Chart chart, Map<String, Display> displays, String sql) {
        Map<String, Display> known = new HashMap<>();
        for (SongRow row : chartOverviewService.songRows(chart, new LinkedMultiValueMap<>())) {
            Display display = new Display(row.title(), row.artistName(), row.artistId(), row.gender(),
                    row.songId() == null ? null : "/songs/" + row.songId(), row.coverUrl(), row.coverHoverUrl());
            if (row.runKey() != null) known.putIfAbsent(chart == Chart.TRL ? "d:" + row.runKey() : row.runKey(), display);
            if (row.songId() != null) known.putIfAbsent(String.valueOf(row.songId()), display);
            known.putIfAbsent(nameKey(row.artistName(), row.title()), display);
        }
        List<TopEntry> entries = new ArrayList<>();
        jdbcTemplate.query(sql, rs -> {
            Integer songId = intOrNull(rs.getObject("song_id"));
            Integer debutId = intOrNull(rs.getObject("debut_id"));
            String artist = rs.getString("artist_name");
            String title = rs.getString("song_title");
            String key = songId != null ? String.valueOf(songId) : debutId != null ? "d:" + debutId : nameKey(artist, title);
            if (!displays.containsKey(key)) {
                Display display = known.get(key);
                if (display == null) display = known.get(nameKey(artist, title));
                displays.put(key, display != null ? display
                        : new Display(title == null ? "Unknown" : title, artist == null ? "Unknown" : artist, null, null, null, null, null));
            }
            String date = rs.getString("chart_date");
            entries.add(new TopEntry(date, date, key));
        });
        return entries;
    }

    private static String nameKey(String artist, String title) {
        return "n:" + normalize(artist) + "||" + normalize(title);
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private static String displayDate(String isoDate) {
        if (isoDate == null || isoDate.isBlank()) return "";
        try {
            LocalDate date = LocalDate.parse(isoDate.length() > 10 ? isoDate.substring(0, 10) : isoDate);
            return String.format("%02d/%02d/%d", date.getDayOfMonth(), date.getMonthValue(), date.getYear());
        } catch (RuntimeException e) {
            return isoDate;
        }
    }

    private static Integer intOrNull(Object value) {
        return value instanceof Number number ? number.intValue() : null;
    }
}
