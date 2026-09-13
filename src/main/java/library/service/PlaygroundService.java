package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

@Service
public class PlaygroundService {
    private static final DateTimeFormatter DISPLAY_DATE = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private final JdbcTemplate jdbcTemplate;

    public PlaygroundService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Integer> getYears() {
        TreeSet<Integer> years = new TreeSet<>();
        years.add(LocalDate.now(ZoneId.of("America/Mexico_City")).getYear());
        years.addAll(jdbcTemplate.queryForList("""
                SELECT DISTINCT CAST(strftime('%Y', play_date) AS INTEGER)
                FROM Play WHERE strftime('%Y', play_date) BETWEEN '0001' AND '9998'
                """, Integer.class));
        return new ArrayList<>(years.descendingSet());
    }

    public YearHeatmap getHeatmap(int year) {
        if (year < 1 || year > 9998) {
            throw new IllegalArgumentException("Year must be between 1 and 9998");
        }
        LocalDate start = LocalDate.of(year, 1, 1);
        Map<LocalDate, long[]> counts = new HashMap<>();
        // Stored dates are already Mexico City local time. Include unmatched plays in totals.
        jdbcTemplate.query("""
                SELECT date(p.play_date) AS day, COUNT(*) AS total,
                       SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN 1 ELSE 0 END) AS male,
                       SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN 1 ELSE 0 END) AS female
                FROM Play p
                LEFT JOIN Song s ON s.id = p.song_id
                LEFT JOIN Artist ar ON ar.id = s.artist_id
                WHERE p.play_date >= ? AND p.play_date < ? AND date(p.play_date) IS NOT NULL
                GROUP BY date(p.play_date)
                """, rs -> {
            counts.put(LocalDate.parse(rs.getString("day")),
                    new long[]{rs.getLong("total"), rs.getLong("male"), rs.getLong("female")});
        }, start.toString(), start.plusYears(1).toString());

        long max = counts.values().stream().mapToLong(c -> c[0]).max().orElse(0);
        List<HeatmapDay> days = new ArrayList<>();
        int offset = start.getDayOfWeek().getValue() - 1;
        for (int i = 0; i < start.lengthOfYear(); i++) {
            LocalDate date = start.plusDays(i);
            long[] count = counts.getOrDefault(date, new long[3]);
            long other = count[0] - count[1] - count[2];
            String gender = count[0] == 0 ? "no-plays"
                    : count[1] > count[2] && count[1] > other ? "male"
                    : count[2] > count[1] && count[2] > other ? "female" : "neutral";
            String purity = count[0] > 0 && count[1] == count[0] ? "all-male"
                    : count[0] > 0 && count[2] == count[0] ? "all-female" : "mixed";
            int intensity = count[0] == 0 ? 0 : (int) Math.ceil(4.0 * count[0] / max);
            String label = date.format(DISPLAY_DATE) + ": " + count[0] + " plays; "
                    + count[1] + " male, " + count[2] + " female, " + other + " other/unknown";
            days.add(new HeatmapDay(date, count[0], count[1], count[2], other, gender, purity, intensity,
                    (offset + i) / 7 + 2, date.getDayOfWeek().getValue() + 1, label));
        }
        return new YearHeatmap(year, days, days.stream().mapToLong(HeatmapDay::total).sum(),
                counts.size(), max, (offset + start.lengthOfYear() + 6) / 7);
    }

    public record HeatmapDay(LocalDate date, long total, long male, long female, long other,
                             String gender, String purity, int intensity, int column, int row, String label) {}

    public record YearHeatmap(int year, List<HeatmapDay> days, long totalPlays,
                              int activeDays, long maxPlays, int weeks) {}
}
