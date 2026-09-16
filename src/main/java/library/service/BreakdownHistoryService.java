package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.DayOfWeek;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class BreakdownHistoryService {
    private final JdbcTemplate jdbc;

    public BreakdownHistoryService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public History getHistory(String breakdown, String measure, String mode, String interval, Integer year) {
        if (!List.of("gender", "genre", "ethnicity", "language").contains(breakdown)
                || !List.of("artists", "albums", "songs", "plays", "time").contains(measure)
                || !List.of("cumulative", "period").contains(mode)
                || !List.of("week", "month", "quarter", "year").contains(interval)
                || (year != null && (year < 1 || year > 9998))) {
            throw new IllegalArgumentException("Invalid breakdown history options");
        }
        LocalDate today = LocalDate.now(ZoneId.of("America/Mexico_City"));
        LocalDate end = year == null || year == today.getYear() ? today
                : LocalDate.of(year, 12, 31);
        if (end.isAfter(today)) end = today;
        LocalDate requestedStart = year == null ? null : LocalDate.of(year, 1, 1);
        if (requestedStart != null && requestedStart.isAfter(end)) {
            return new History(List.of(), List.of(), List.of());
        }
        boolean cumulative = mode.equals("cumulative");
        boolean catalog = List.of("artists", "albums", "songs").contains(measure);
        String entity = switch (measure) {
            case "artists" -> "ar.id";
            case "albums" -> "al.id";
            default -> "s.id";
        };
        String categoryId = categoryId(breakdown, measure);
        String category = breakdown.equals("gender")
                ? "CASE WHEN " + categoryId + " = 2 THEN 'Male' WHEN " + categoryId
                    + " = 1 THEN 'Female' ELSE 'Other / unknown' END"
                : "COALESCE(NULLIF(TRIM(lookup.name), ''), 'Unknown')";
        String lookup = breakdown.equals("gender") ? ""
                : " LEFT JOIN " + breakdown + " lookup ON lookup.id = " + categoryId;
        // Artists and albums use their own classification. Song overrides apply only to songs and plays.
        String artistJoin = measure.equals("albums") ? "al.artist_id" : "s.artist_id";
        String source = """
                FROM Play p
                LEFT JOIN Song s ON s.id = p.song_id
                LEFT JOIN Album al ON al.id = s.album_id
                LEFT JOIN Artist ar ON ar.id = %s
                %s
                WHERE p.play_date < ? AND date(p.play_date) BETWEEN '0001-01-01' AND ?
                """.formatted(artistJoin, lookup);
        List<Object> params = new ArrayList<>(List.of(end.plusDays(1).toString(), end.toString()));
        if (!cumulative && requestedStart != null) {
            source += " AND p.play_date >= ?";
            params.add(requestedStart.toString());
        }
        if (catalog) source += " AND " + entity + " IS NOT NULL";
        String events;
        if (catalog && cumulative) {
            // One first-play event per entity, before applying the visible range.
            events = "SELECT MIN(date(p.play_date)) AS day, " + category
                    + " AS category, 1 AS amount " + source + " GROUP BY " + entity;
        } else {
            String amount = catalog ? "COUNT(DISTINCT " + entity + ")"
                    : measure.equals("time") ? "SUM(MAX(COALESCE(s.length_seconds, 0), 0))" : "COUNT(*)";
            // Distinct catalog items must be counted over the whole bucket, not summed from daily counts.
            events = "SELECT " + bucketSql("date(p.play_date)", interval) + " AS day, "
                    + category + " AS category, " + amount + " AS amount " + source
                    + " GROUP BY day, category";
        }
        String sql = "WITH events AS (" + events + ") SELECT " + bucketSql("day", interval)
                + " AS bucket, category, SUM(amount) AS amount FROM events GROUP BY bucket, category ORDER BY bucket";
        List<Event> rows = jdbc.query(sql, (rs, i) -> new Event(LocalDate.parse(rs.getString("bucket")),
                rs.getString("category"), rs.getLong("amount")), params.toArray());
        if (rows.isEmpty() && requestedStart == null) return new History(List.of(), List.of(), List.of());
        boolean weekly = interval.equals("week");
        int months = interval.equals("year") ? 12 : interval.equals("quarter") ? 3 : 1;
        LocalDate start = requestedStart != null ? requestedStart : rows.getFirst().date();
        // Align buckets before clipping the first displayed period to the requested start.
        if (weekly) start = start.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        else start = start.withMonth(((start.getMonthValue() - 1) / months) * months + 1).withDayOfMonth(1);
        int size = weekly
                ? (int) ChronoUnit.WEEKS.between(start, end.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))) + 1
                : (int) (ChronoUnit.MONTHS.between(start, end.withDayOfMonth(1)) / months) + 1;
        if (size > 2400) throw new IllegalArgumentException("Choose a year or a larger interval (maximum 2400 periods)");
        Map<String, long[]> values = new LinkedHashMap<>();
        if (breakdown.equals("gender")) {
            for (String name : List.of("Male", "Female", "Other / unknown")) values.put(name, new long[size]);
        }
        for (Event row : rows) {
            long[] series = values.computeIfAbsent(row.category(), key -> new long[size]);
            int index = row.date().isBefore(start) ? 0
                    : weekly ? (int) (ChronoUnit.DAYS.between(start, row.date()) / 7)
                    : (int) (ChronoUnit.MONTHS.between(start, row.date()) / months);
            if (index < size) series[index] += row.amount();
        }
        if (cumulative) {
            for (long[] series : values.values()) {
                for (int i = 1; i < size; i++) series[i] += series[i - 1];
            }
        }
        List<Period> periods = new ArrayList<>();
        List<Long> totals = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            LocalDate from = weekly ? start.plusWeeks(i) : start.plusMonths((long) i * months);
            LocalDate to = weekly ? from.plusDays(6) : from.plusMonths(months).minusDays(1);
            LocalDate visibleFrom = requestedStart != null && from.isBefore(requestedStart) ? requestedStart : from;
            periods.add(new Period(visibleFrom.toString(), (to.isAfter(end) ? end : to).toString()));
            long total = 0;
            for (long[] series : values.values()) total += series[i];
            totals.add(total);
        }
        var series = values.entrySet().stream().map(entry -> new Series(entry.getKey(), entry.getValue()));
        if (!breakdown.equals("gender")) series = series.sorted(Comparator.comparing(Series::name, String.CASE_INSENSITIVE_ORDER));
        return new History(periods, series.toList(), totals);
    }

    private String categoryId(String breakdown, String measure) {
        if (measure.equals("artists")) return "ar." + breakdown + "_id";
        boolean inheritedThroughAlbum = breakdown.equals("genre") || breakdown.equals("language");
        if (measure.equals("albums")) return inheritedThroughAlbum
                ? "COALESCE(al.override_" + breakdown + "_id, ar." + breakdown + "_id)"
                : "ar." + breakdown + "_id";
        return "COALESCE(s.override_" + breakdown + "_id, "
                + (inheritedThroughAlbum ? "al.override_" + breakdown + "_id, " : "")
                + "ar." + breakdown + "_id)";
    }

    private String bucketSql(String day, String interval) {
        return switch (interval) {
            case "week" -> "date(" + day + ", 'weekday 0', '-6 days')";
            case "year" -> "strftime('%Y-01-01', " + day + ")";
            case "quarter" -> "printf('%s-%02d-01', strftime('%Y', " + day
                    + "), ((CAST(strftime('%m', " + day + ") AS INTEGER) - 1) / 3) * 3 + 1)";
            default -> "strftime('%Y-%m-01', " + day + ")";
        };
    }

    private record Event(LocalDate date, String category, long amount) {}
    public record Period(String start, String end) {}
    public record Series(String name, long[] values) {}
    public record History(List<Period> periods, List<Series> series, List<Long> totals) {}
}
