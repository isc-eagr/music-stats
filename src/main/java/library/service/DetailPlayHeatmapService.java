package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class DetailPlayHeatmapService {
    private static final DateTimeFormatter DISPLAY_DATE = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private final JdbcTemplate jdbcTemplate;

    public DetailPlayHeatmapService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public YearHeatmap forArtist(int artistId, List<Integer> groupIds, boolean includeMain,
                                 boolean includeFeatured, Integer requestedYear) {
        List<Object> params = new ArrayList<>();
        List<String> scope = new ArrayList<>();
        if (includeMain) {
            scope.add("s.artist_id = ?");
            params.add(artistId);
        }
        if (groupIds != null && !groupIds.isEmpty()) {
            String placeholders = String.join(",", groupIds.stream().map(id -> "?").toList());
            scope.add("s.artist_id IN (" + placeholders + ")");
            params.addAll(groupIds);
        }
        if (includeFeatured) {
            scope.add("EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id AND sfa.artist_id = ?)");
            params.add(artistId);
        }
        String predicate = scope.isEmpty() ? "0 = 1" : "(" + String.join(" OR ", scope) + ")";
        Integer genderId = jdbcTemplate.queryForObject("SELECT gender_id FROM Artist WHERE id = ?", Integer.class, artistId);
        return build(predicate, params, genderClass(genderId), requestedYear);
    }

    public YearHeatmap forAlbum(int albumId, Integer requestedYear) {
        Integer genderId = jdbcTemplate.queryForObject("""
                SELECT ar.gender_id FROM Album al JOIN Artist ar ON ar.id = al.artist_id WHERE al.id = ?
                """, Integer.class, albumId);
        return build("s.album_id = ?", List.of(albumId), genderClass(genderId), requestedYear);
    }

    public YearHeatmap forSong(int songId, Integer requestedYear) {
        Integer genderId = jdbcTemplate.queryForObject("""
                SELECT COALESCE(s.override_gender_id, ar.gender_id)
                FROM Song s JOIN Artist ar ON ar.id = s.artist_id WHERE s.id = ?
                """, Integer.class, songId);
        return build("s.id = ?", List.of(songId), genderClass(genderId), requestedYear);
    }

    private YearHeatmap build(String scopePredicate, List<Object> scopeParams, String gender, Integer requestedYear) {
        List<Integer> years = jdbcTemplate.queryForList("""
                SELECT DISTINCT CAST(strftime('%Y', p.play_date) AS INTEGER)
                FROM Play p JOIN Song s ON s.id = p.song_id
                WHERE {scope} AND strftime('%Y', p.play_date) BETWEEN '0001' AND '9998'
                ORDER BY 1 DESC
                """.replace("{scope}", scopePredicate), Integer.class, scopeParams.toArray());
        int currentYear = LocalDate.now(ZoneId.of("America/Mexico_City")).getYear();
        if (years.isEmpty()) {
            years = new ArrayList<>(List.of(currentYear));
        }
        int year = requestedYear != null && requestedYear >= 1 && requestedYear <= 9998
                ? requestedYear : years.getFirst();
        if (!years.contains(year)) {
            years = new ArrayList<>(years);
            years.add(year);
            years.sort(java.util.Comparator.reverseOrder());
        }

        LocalDate start = LocalDate.of(year, 1, 1);
        List<Object> countParams = new ArrayList<>(scopeParams);
        countParams.add(start.toString());
        countParams.add(start.plusYears(1).toString());
        var counts = new java.util.HashMap<LocalDate, Long>();
        jdbcTemplate.query("""
                SELECT date(p.play_date) AS day, COUNT(*) AS total
                FROM Play p JOIN Song s ON s.id = p.song_id
                WHERE {scope} AND p.play_date >= ? AND p.play_date < ? AND date(p.play_date) IS NOT NULL
                GROUP BY date(p.play_date)
                """.replace("{scope}", scopePredicate), (RowCallbackHandler) rs ->
                        counts.put(LocalDate.parse(rs.getString("day")), rs.getLong("total")), countParams.toArray());

        long max = counts.values().stream().mapToLong(Long::longValue).max().orElse(0);
        int offset = start.getDayOfWeek().getValue() - 1;
        List<HeatmapDay> days = new ArrayList<>();
        for (int i = 0; i < start.lengthOfYear(); i++) {
            LocalDate date = start.plusDays(i);
            long total = counts.getOrDefault(date, 0L);
            int intensity = total == 0 ? 0 : (int) Math.ceil(4.0 * total / max);
            String label = date.format(DISPLAY_DATE) + ": " + total + (total == 1 ? " play" : " plays");
            days.add(new HeatmapDay(date, total, intensity, (offset + i) / 7 + 2,
                    date.getDayOfWeek().getValue() + 1, label));
        }
        return new YearHeatmap(year, years, days, days.stream().mapToLong(HeatmapDay::total).sum(),
                counts.size(), max, (offset + start.lengthOfYear() + 6) / 7, gender);
    }

    private String genderClass(Integer genderId) {
        if (Integer.valueOf(2).equals(genderId)) return "male";
        if (Integer.valueOf(1).equals(genderId)) return "female";
        return "other";
    }

    public record HeatmapDay(LocalDate date, long total, int intensity, int column, int row, String label) {}

    public record YearHeatmap(int year, List<Integer> years, List<HeatmapDay> days, long totalPlays,
                              int activeDays, long maxPlays, int weeks, String gender) {}
}
