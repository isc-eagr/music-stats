package library.service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * One timeframe (day, week, month, season, year or decade) with the same keys and date ranges as the Timeframes
 * list pages: weeks follow SQLite's %W numbering (week 01 starts on the year's first Monday, the days before it
 * belong to the previous year's last week), Winter runs from December of the previous year to February.
 */
public record TimeframePeriod(String type, String key, LocalDate from, LocalDate to) {

    public static final List<String> TYPES = List.of("days", "weeks", "months", "seasons", "years", "decades");
    static final String[] SEASONS = {"Winter", "Spring", "Summer", "Fall"};
    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public static Optional<TimeframePeriod> parse(String type, String key) {
        if (type == null || key == null) return Optional.empty();
        try {
            TimeframePeriod p = switch (type) {
                case "days" -> of(type, LocalDate.parse(key));
                case "weeks" -> {
                    String[] parts = key.split("-W");
                    if (parts.length != 2) yield null;
                    int year = Integer.parseInt(parts[0]), week = Integer.parseInt(parts[1]);
                    if (week < 1) yield null;
                    LocalDate start = firstMonday(year).plusWeeks(week - 1L);
                    yield start.getYear() == year ? of(type, start) : null;
                }
                case "months" -> of(type, YearMonth.parse(key).atDay(1));
                case "seasons" -> {
                    String[] parts = key.split("-");
                    if (parts.length != 2) yield null;
                    int year = Integer.parseInt(parts[0]);
                    int idx = List.of(SEASONS).indexOf(parts[1]);
                    if (idx < 0) yield null;
                    yield of(type, idx == 0 ? LocalDate.of(year, 1, 1) : LocalDate.of(year, idx * 3, 1));
                }
                case "years" -> of(type, LocalDate.of(Integer.parseInt(key), 1, 1));
                case "decades" -> {
                    if (!key.endsWith("s")) yield null;
                    int start = Integer.parseInt(key.substring(0, key.length() - 1));
                    yield start % 10 == 0 ? of(type, LocalDate.of(start, 1, 1)) : null;
                }
                default -> null;
            };
            if (p == null || !p.key.equals(key) || p.from.getYear() < 1900 || p.to.getYear() > 2200) return Optional.empty();
            return Optional.of(p);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /** The period of {@code type} that holds {@code date}. */
    public static TimeframePeriod of(String type, LocalDate date) {
        return switch (type) {
            case "days" -> new TimeframePeriod(type, date.toString(), date, date);
            case "weeks" -> {
                LocalDate first = firstMonday(date.getYear());
                if (date.isBefore(first)) yield of(type, LocalDate.of(date.getYear() - 1, 12, 31));
                int week = (int) (ChronoUnit.DAYS.between(first, date) / 7) + 1;
                LocalDate start = first.plusWeeks(week - 1L);
                yield new TimeframePeriod(type, String.format("%d-W%02d", date.getYear(), week), start, start.plusDays(6));
            }
            case "months" -> {
                YearMonth ym = YearMonth.from(date);
                yield new TimeframePeriod(type, ym.toString(), ym.atDay(1), ym.atEndOfMonth());
            }
            case "seasons" -> {
                int m = date.getMonthValue();
                int year = m == 12 ? date.getYear() + 1 : date.getYear();
                int idx = m == 12 || m <= 2 ? 0 : m <= 5 ? 1 : m <= 8 ? 2 : 3;
                LocalDate start = idx == 0 ? LocalDate.of(year - 1, 12, 1) : LocalDate.of(year, idx * 3, 1);
                LocalDate end = idx == 0 ? YearMonth.of(year, 2).atEndOfMonth() : start.plusMonths(3).minusDays(1);
                yield new TimeframePeriod(type, year + "-" + SEASONS[idx], start, end);
            }
            case "years" -> new TimeframePeriod(type, String.valueOf(date.getYear()), LocalDate.of(date.getYear(), 1, 1),
                    LocalDate.of(date.getYear(), 12, 31));
            case "decades" -> {
                int start = date.getYear() / 10 * 10;
                yield new TimeframePeriod(type, start + "s", LocalDate.of(start, 1, 1), LocalDate.of(start + 9, 12, 31));
            }
            default -> throw new IllegalArgumentException("Unknown timeframe " + type);
        };
    }

    static LocalDate firstMonday(int year) {
        return LocalDate.of(year, 1, 1).with(TemporalAdjusters.nextOrSame(DayOfWeek.MONDAY));
    }

    /** Page title: 21/03/2025, Week 11, 2025, March 2025, Winter 2025, 2024, 2010s. */
    public String name() {
        return switch (type) {
            case "days" -> from.format(DMY);
            case "weeks" -> "Week " + Integer.parseInt(key.substring(key.indexOf("-W") + 2)) + ", " + key.substring(0, 4);
            case "months" -> monthName(from) + " " + from.getYear();
            case "seasons" -> season() + " " + key.substring(0, 4);
            default -> key;
        };
    }

    /** Label inside a bigger period: 21, Mon 17, W11, Mar, Winter, 2014, 2010s. */
    public String shortName(String within) {
        return switch (type) {
            case "days" -> "weeks".equals(within) ? from.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.ENGLISH) + " " + from.getDayOfMonth()
                    : "months".equals(within) ? String.valueOf(from.getDayOfMonth())
                    : "decades".equals(within) ? from.format(DMY) : from.format(DateTimeFormatter.ofPattern("dd/MM"));
            case "weeks" -> "decades".equals(within) ? "W" + key.substring(key.indexOf("-W") + 2) + " " + key.substring(0, 4)
                    : "W" + Integer.parseInt(key.substring(key.indexOf("-W") + 2));
            case "months" -> "decades".equals(within) ? monthName(from).substring(0, 3) + " " + from.getYear() : monthName(from).substring(0, 3);
            case "seasons" -> season();
            default -> key;
        };
    }

    public String typeLabel() {
        return switch (type) {
            case "days" -> "Day";
            case "weeks" -> "Week";
            case "months" -> "Month";
            case "seasons" -> "Season";
            case "years" -> "Year";
            default -> "Decade";
        };
    }

    /** Plural noun for counts and peers ("21 years", "22 winters"). */
    public String plural() {
        return "seasons".equals(type) ? season().toLowerCase(Locale.ROOT) + "s" : type;
    }

    public String href() {
        return "/" + type + "/" + key;
    }

    public long days() {
        return ChronoUnit.DAYS.between(from, to) + 1;
    }

    public boolean contains(LocalDate d) {
        return !d.isBefore(from) && !d.isAfter(to);
    }

    public TimeframePeriod previous() {
        return of(type, from.minusDays(1));
    }

    public TimeframePeriod next() {
        return of(type, to.plusDays(1));
    }

    /** What the tile deltas compare against: the same month or season last year, otherwise the previous period. */
    public TimeframePeriod comparison() {
        return "months".equals(type) || "seasons".equals(type) ? of(type, from.minusYears(1)) : previous();
    }

    /** Season name for seasons, empty otherwise: peers of a season are the same season in other years. */
    String season() {
        return "seasons".equals(type) ? key.substring(5) : "";
    }

    /** Every period of {@code childType} that overlaps this one, in order. */
    public List<TimeframePeriod> children(String childType) {
        List<TimeframePeriod> out = new ArrayList<>();
        TimeframePeriod p = of(childType, from);
        while (!p.from.isAfter(to)) {
            out.add(p);
            p = p.next();
        }
        return out;
    }

    private static String monthName(LocalDate d) {
        return d.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH);
    }
}
