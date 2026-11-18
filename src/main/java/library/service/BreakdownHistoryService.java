package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.DayOfWeek;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Share-over-time histories built from first plays (cumulative) or per-period activity.
 * {@link #getHistory} powers Playground > Breakdown over time; {@link #getCatalogHistory} powers the
 * "Share over time" chart above the catalog list pages, one series per listed item plus the rest.
 */
@Service
public class BreakdownHistoryService {
    public static final int MAX_CATALOG_KEYS = 100;
    private static final List<String> CATALOG_MEASURES = List.of("artists", "albums", "songs", "plays", "time");
    private static final Map<String, List<String>> CATALOG_ALLOWED_MEASURES = Map.of(
            "artists", List.of("plays", "time", "albums", "songs"),
            "albums", List.of("plays", "time", "songs"),
            "songs", List.of("plays", "time"));
    private static final Map<String, String> CATALOG_BREAKDOWN = Map.of(
            "genres", "genre", "subgenres", "subgenre", "languages", "language",
            "ethnicities", "ethnicity", "genders", "gender");
    private static final Map<String, String> LOOKUP_TABLE = Map.of(
            "genre", "Genre", "subgenre", "SubGenre", "language", "Language",
            "ethnicity", "Ethnicity", "gender", "Gender");

    private final JdbcTemplate jdbc;

    public BreakdownHistoryService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public History getHistory(String breakdown, String measure, String mode, String interval, Integer year) {
        if (!List.of("gender", "genre", "ethnicity", "language").contains(breakdown)
                || !CATALOG_MEASURES.contains(measure)) {
            throw new IllegalArgumentException("Invalid breakdown history options");
        }
        Window window = window(mode, interval, year);
        if (window == null) return new History(List.of(), List.of(), List.of());
        String categoryId = categoryId(breakdown, measure);
        String category = breakdown.equals("gender")
                ? "CASE WHEN " + categoryId + " = 2 THEN 'Male' WHEN " + categoryId
                    + " = 1 THEN 'Female' ELSE 'Other / unknown' END"
                : "COALESCE(NULLIF(TRIM(lookup.name), ''), 'Unknown')";
        String lookup = breakdown.equals("gender") ? ""
                : " LEFT JOIN " + breakdown + " lookup ON lookup.id = " + categoryId;
        List<Event> rows = events(category, List.of(), lookup, "", List.of(), measure, mode, interval, window);
        Map<String, long[]> seed = new LinkedHashMap<>();
        if (breakdown.equals("gender")) {
            for (String name : List.of("Male", "Female", "Other / unknown")) seed.put(name, null);
        }
        Buckets buckets = bucket(rows, seed, mode, interval, window);
        if (buckets == null) return new History(List.of(), List.of(), List.of());
        var series = buckets.values().entrySet().stream().map(entry -> new Series(entry.getKey(), entry.getValue()));
        if (!breakdown.equals("gender")) series = series.sorted(Comparator.comparing(Series::name, String.CASE_INSENSITIVE_ORDER));
        return new History(buckets.periods(), series.toList(), buckets.totals());
    }

    /**
     * One series per requested key (an artist, album or song id, a lookup id, or a country name),
     * in the requested order, plus {@code rest} for everything else so the shares add up to the total.
     * Artist plays count songs where the artist is the main artist, so shares never overlap.
     * {@code scopeJson} (a JSON array of keys) limits everything to the list's current filters; null means the whole library.
     */
    public CatalogHistory getCatalogHistory(String catalog, List<String> keys, String measure, String mode,
                                            String interval, Integer year, String scopeJson) {
        List<String> allowed = CATALOG_ALLOWED_MEASURES.getOrDefault(catalog,
                CATALOG_BREAKDOWN.containsKey(catalog) || catalog.equals("countries") ? CATALOG_MEASURES : null);
        List<String> requested = keys == null ? List.of() : keys.stream()
                .filter(key -> key != null && !key.isBlank()).map(String::trim).distinct().toList();
        if (allowed == null || !allowed.contains(measure) || requested.size() > MAX_CATALOG_KEYS) {
            throw new IllegalArgumentException("Invalid share history options");
        }
        Window window = window(mode, interval, year);
        if (window == null || requested.isEmpty()) {
            return new CatalogHistory(List.of(), List.of(), new long[0], List.of(), scopeJson != null);
        }
        String key = catalogKey(catalog, measure);
        String placeholders = String.join(", ", Collections.nCopies(requested.size(), "?"));
        // Everything outside the requested keys lands in '' (the rest).
        String category = "CASE WHEN CAST(" + key + " AS TEXT) IN (" + placeholders + ") THEN CAST(" + key + " AS TEXT) ELSE '' END";
        String scope = scopeJson == null ? "" : " AND CAST(" + key + " AS TEXT) IN (SELECT CAST(value AS TEXT) FROM json_each(?))";
        List<Event> rows = events(category, requested, "", scope, scopeJson == null ? List.of() : List.of(scopeJson),
                measure, mode, interval, window);
        Map<String, long[]> seed = new LinkedHashMap<>();
        requested.forEach(requestedKey -> seed.put(requestedKey, null));
        seed.put("", null);
        Buckets buckets = bucket(rows, seed, mode, interval, window);
        if (buckets == null) return new CatalogHistory(List.of(), List.of(), new long[0], List.of(), scopeJson != null);
        Map<String, Label> labels = labels(catalog, requested);
        List<CatalogSeries> series = requested.stream().map(requestedKey -> {
            Label label = labels.get(requestedKey);
            return new CatalogSeries(requestedKey, label == null ? requestedKey : label.name(),
                    label == null ? null : label.subtitle(), label == null ? null : label.genderId(),
                    buckets.values().get(requestedKey));
        }).toList();
        return new CatalogHistory(buckets.periods(), series, buckets.values().get(""), buckets.totals(), scopeJson != null);
    }

    /** Visible date window; null when the requested year lies entirely in the future. */
    private Window window(String mode, String interval, Integer year) {
        if (!List.of("cumulative", "period").contains(mode)
                || !List.of("week", "month", "quarter", "year").contains(interval)
                || (year != null && (year < 1 || year > 9998))) {
            throw new IllegalArgumentException("Invalid breakdown history options");
        }
        LocalDate today = LocalDate.now(ZoneId.of("America/Mexico_City"));
        LocalDate end = year == null || year == today.getYear() ? today : LocalDate.of(year, 12, 31);
        if (end.isAfter(today)) end = today;
        LocalDate requestedStart = year == null ? null : LocalDate.of(year, 1, 1);
        if (requestedStart != null && requestedStart.isAfter(end)) return null;
        return new Window(requestedStart, end);
    }

    private List<Event> events(String category, List<String> categoryParams, String lookup, String scope,
                               List<Object> scopeParams, String measure, String mode, String interval, Window window) {
        boolean cumulative = mode.equals("cumulative");
        boolean catalog = List.of("artists", "albums", "songs").contains(measure);
        String entity = switch (measure) {
            case "artists" -> "ar.id";
            case "albums" -> "al.id";
            default -> "s.id";
        };
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
        // The category expression sits in the SELECT list, so its parameters come first.
        List<Object> params = new ArrayList<>(categoryParams);
        params.add(window.end().plusDays(1).toString());
        params.add(window.end().toString());
        if (!cumulative && window.requestedStart() != null) {
            source += " AND p.play_date >= ?";
            params.add(window.requestedStart().toString());
        }
        if (catalog) source += " AND " + entity + " IS NOT NULL";
        source += scope;
        params.addAll(scopeParams);
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
        return jdbc.query(sql, (rs, i) -> new Event(LocalDate.parse(rs.getString("bucket")),
                rs.getString("category"), rs.getLong("amount")), params.toArray());
    }

    /** Spreads events over aligned periods; seeded series keep their order and always exist. */
    private Buckets bucket(List<Event> rows, Map<String, long[]> seed, String mode, String interval, Window window) {
        LocalDate requestedStart = window.requestedStart();
        LocalDate end = window.end();
        if (rows.isEmpty() && requestedStart == null) return null;
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
        seed.keySet().forEach(name -> values.put(name, new long[size]));
        for (Event row : rows) {
            long[] series = values.computeIfAbsent(row.category(), key -> new long[size]);
            int index = row.date().isBefore(start) ? 0
                    : weekly ? (int) (ChronoUnit.DAYS.between(start, row.date()) / 7)
                    : (int) (ChronoUnit.MONTHS.between(start, row.date()) / months);
            if (index < size) series[index] += row.amount();
        }
        if (mode.equals("cumulative")) {
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
        return new Buckets(periods, values, totals);
    }

    private String catalogKey(String catalog, String measure) {
        return switch (catalog) {
            case "artists" -> "ar.id";
            case "albums" -> "al.id";
            case "songs" -> "s.id";
            case "countries" -> "NULLIF(TRIM(ar.country), '')";
            default -> categoryId(CATALOG_BREAKDOWN.get(catalog), measure);
        };
    }

    private String categoryId(String breakdown, String measure) {
        if (measure.equals("artists")) return "ar." + breakdown + "_id";
        boolean inheritedThroughAlbum = List.of("genre", "subgenre", "language").contains(breakdown);
        if (measure.equals("albums")) return inheritedThroughAlbum
                ? "COALESCE(al.override_" + breakdown + "_id, ar." + breakdown + "_id)"
                : "ar." + breakdown + "_id";
        return "COALESCE(s.override_" + breakdown + "_id, "
                + (inheritedThroughAlbum ? "al.override_" + breakdown + "_id, " : "")
                + "ar." + breakdown + "_id)";
    }

    /** Display name, an optional second line (the artist for albums and songs) and the gender used for colors. */
    private Map<String, Label> labels(String catalog, List<String> keys) {
        Map<String, Label> labels = new HashMap<>();
        if (catalog.equals("countries")) return labels;
        List<Long> ids = keys.stream().filter(key -> key.matches("\\d{1,18}")).map(Long::valueOf).toList();
        if (ids.isEmpty()) return labels;
        String in = String.join(", ", Collections.nCopies(ids.size(), "?"));
        String sql = switch (catalog) {
            case "artists" -> "SELECT id, name, NULL AS subtitle, gender_id FROM Artist WHERE id IN (" + in + ")";
            case "albums" -> "SELECT al.id, al.name, ar.name AS subtitle, ar.gender_id FROM Album al"
                    + " JOIN Artist ar ON ar.id = al.artist_id WHERE al.id IN (" + in + ")";
            case "songs" -> "SELECT s.id, s.name, ar.name AS subtitle, COALESCE(s.override_gender_id, ar.gender_id) AS gender_id"
                    + " FROM Song s JOIN Artist ar ON ar.id = s.artist_id WHERE s.id IN (" + in + ")";
            case "genders" -> "SELECT id, name, NULL AS subtitle, id AS gender_id FROM Gender WHERE id IN (" + in + ")";
            default -> "SELECT id, name, NULL AS subtitle, NULL AS gender_id FROM "
                    + LOOKUP_TABLE.get(CATALOG_BREAKDOWN.get(catalog)) + " WHERE id IN (" + in + ")";
        };
        jdbc.query(sql, rs -> {
            Object gender = rs.getObject("gender_id");
            labels.put(rs.getString("id"), new Label(rs.getString("name"), rs.getString("subtitle"),
                    gender == null ? null : ((Number) gender).intValue()));
        }, ids.toArray());
        return labels;
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
    private record Window(LocalDate requestedStart, LocalDate end) {}
    private record Buckets(List<Period> periods, Map<String, long[]> values, List<Long> totals) {}
    private record Label(String name, String subtitle, Integer genderId) {}
    public record Period(String start, String end) {}
    public record Series(String name, long[] values) {}
    public record History(List<Period> periods, List<Series> series, List<Long> totals) {}
    public record CatalogSeries(String key, String name, String subtitle, Integer genderId, long[] values) {}
    /** filtered: totals and rest cover only what matches the list's current filters. */
    public record CatalogHistory(List<Period> periods, List<CatalogSeries> series, long[] rest, List<Long> totals,
                                 boolean filtered) {}
}
