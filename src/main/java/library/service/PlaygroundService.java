package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

@Service
public class PlaygroundService {
    private static final DateTimeFormatter DISPLAY_DATE = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter MONTH_NAME = DateTimeFormatter.ofPattern("MMMM", Locale.ENGLISH);
    private final JdbcTemplate jdbcTemplate;

    public PlaygroundService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Integer> getYears() {
        TreeSet<Integer> years = new TreeSet<>();
        years.add(LocalDate.now(ZoneId.of("America/Mexico_City")).getYear());
        jdbcTemplate.query("""
                SELECT DISTINCT SUBSTR(play_date, 1, 4) AS play_year
                FROM Play
                WHERE play_date IS NOT NULL
                  AND SUBSTR(play_date, 1, 4) BETWEEN '0001' AND '9998'
                  AND strftime('%Y', play_date) BETWEEN '0001' AND '9998'
                """, rs -> {
            years.add(Integer.parseInt(rs.getString("play_year")));
        });
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
        int[] monthFirstColumns = new int[12];
        int monthColumns = 0;
        StringBuilder monthTracks = new StringBuilder();
        for (int month = 1; month <= 12; month++) {
            LocalDate monthStart = start.withMonth(month);
            monthFirstColumns[month - 1] = monthColumns + 2;
            int monthOffset = monthStart.getDayOfWeek().getValue() - 1;
            int monthWeeks = (monthOffset + monthStart.lengthOfMonth() + 6) / 7;
            monthColumns += monthWeeks;
            monthTracks.append(" repeat(").append(monthWeeks).append(", minmax(0, 1fr))");
            if (month < 12) {
                monthColumns++;
                monthTracks.append(" minmax(0, 0.4fr)");
            }
        }
        for (int i = 0; i < start.lengthOfYear(); i++) {
            LocalDate date = start.plusDays(i);
            long[] count = counts.getOrDefault(date, new long[3]);
            long other = count[0] - count[1] - count[2];
            String gender = count[0] == 0 ? "no-plays"
                    : count[1] > count[2] && count[1] > other ? "male"
                    : count[2] > count[1] && count[2] > other ? "female" : "neutral";
            String purity = count[0] > 0 && count[1] == count[0] ? "all-male"
                    : count[0] > 0 && count[2] == count[0] ? "all-female" : "mixed";
            long leaderPlays = gender.equals("male") ? count[1] : gender.equals("female") ? count[2] : 0;
            int leaderPercent = count[0] == 0 ? 0 : (int) Math.round(100.0 * leaderPlays / count[0]);
            int genderShade = leaderPlays == 0 ? 0 : leaderPercent <= 55 ? 1
                    : leaderPercent <= 65 ? 2 : leaderPercent <= 75 ? 3
                    : leaderPercent < 90 ? 4 : 5;
            int intensity = count[0] == 0 ? 0 : (int) Math.ceil(4.0 * count[0] / max);
            String label = date.format(DISPLAY_DATE) + ": " + count[0] + " plays; "
                    + count[1] + " male, " + count[2] + " female, " + other + " other/unknown"
                    + (leaderPlays > 0 ? "; " + leaderPercent + "% " + gender : "");
            int monthOffset = date.withDayOfMonth(1).getDayOfWeek().getValue() - 1;
            int monthColumn = monthFirstColumns[date.getMonthValue() - 1]
                    + (monthOffset + date.getDayOfMonth() - 1) / 7;
            days.add(new HeatmapDay(date, count[0], count[1], count[2], other, gender, purity,
                    leaderPercent, genderShade, intensity,
                    (offset + i) / 7 + 2, monthColumn, date.getDayOfWeek().getValue() + 1, label));
        }
        return new YearHeatmap(year, days, days.stream().mapToLong(HeatmapDay::total).sum(),
                counts.size(), max, (offset + start.lengthOfYear() + 6) / 7,
                monthColumns, monthTracks.toString().trim());
    }

    public record HeatmapDay(LocalDate date, long total, long male, long female, long other,
                             String gender, String purity, int leaderPercent, int genderShade,
                             int intensity, int column, int monthColumn, int row, String label) {}

    public record YearHeatmap(int year, List<HeatmapDay> days, long totalPlays,
                              int activeDays, long maxPlays, int weeks, int monthColumns, String monthTracks) {
        public List<HeatmapMonth> months() {
            return java.util.stream.IntStream.rangeClosed(1, 12).mapToObj(month ->
                    new HeatmapMonth(LocalDate.of(year, month, 1).format(MONTH_NAME),
                            days.stream().filter(day -> day.date().getMonthValue() == month).toList()))
                    .toList();
        }
    }

    public record HeatmapMonth(String name, List<HeatmapDay> days) {}
}
