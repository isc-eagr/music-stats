package library.dto;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Locale;

/**
 * One row of the Consistency ledger: how many days / weeks / months / years had plays,
 * out of the calendar periods since the first play.
 */
public record ConsistencyPeriodDTO(String key, String label, long played, long total) {

    public static List<ConsistencyPeriodDTO> of(long days, long weeks, long months, long years, LocalDate firstListened) {
        return of(days, weeks, months, years, firstListened, LocalDate.now());
    }

    /** List card More panel: nullable counts and the card's display-formatted first listened date. */
    public static List<ConsistencyPeriodDTO> forCard(Integer days, Integer weeks, Integer months, Integer years,
                                                     String firstListenedDate) {
        return of(days != null ? days : 0, weeks != null ? weeks : 0, months != null ? months : 0,
                years != null ? years : 0, library.util.DateFormatUtils.parseDisplayDate(firstListenedDate));
    }

    public static List<ConsistencyPeriodDTO> of(long days, long weeks, long months, long years,
                                                LocalDate firstListened, LocalDate today) {
        long totalDays = 0;
        long totalWeeks = 0;
        long totalMonths = 0;
        long totalYears = 0;
        if (firstListened != null) {
            totalDays = ChronoUnit.DAYS.between(firstListened, today) + 1;
            WeekFields weekFields = WeekFields.of(Locale.getDefault());
            // Approximate: 52 weeks per week-based year plus the week difference
            totalWeeks = (today.get(weekFields.weekBasedYear()) - firstListened.get(weekFields.weekBasedYear())) * 52L
                    + today.get(weekFields.weekOfWeekBasedYear()) - firstListened.get(weekFields.weekOfWeekBasedYear()) + 1;
            totalMonths = (today.getYear() - firstListened.getYear()) * 12L
                    + today.getMonthValue() - firstListened.getMonthValue() + 1;
            totalYears = today.getYear() - firstListened.getYear() + 1L;
        }
        return List.of(
                new ConsistencyPeriodDTO("days", "Days", days, totalDays),
                new ConsistencyPeriodDTO("weeks", "Weeks", weeks, totalWeeks),
                new ConsistencyPeriodDTO("months", "Months", months, totalMonths),
                new ConsistencyPeriodDTO("years", "Years", years, totalYears));
    }

    /** Share of the periods since the first play that had plays, 0-100. */
    public double share() {
        return total > 0 ? Math.min(100.0, played * 100.0 / total) : 0;
    }

    public String shareLabel() {
        if (total <= 0) {
            return "-";
        }
        double share = share();
        return share < 10 ? String.format(Locale.ROOT, "%.1f%%", share) : Math.round(share) + "%";
    }

    /** One-line form for the list card sort value, e.g. "19 / 21 years (90%)". */
    public String summary() {
        String unit = label.toLowerCase(Locale.ROOT);
        if (total <= 0) {
            return played + " " + unit;
        }
        return String.format(Locale.ROOT, "%,d / %,d %s (%s)", played, total, unit, shareLabel());
    }

    /** CSS width percentage for the coverage bar, always with a dot decimal separator. */
    public String barWidth() {
        return String.format(Locale.ROOT, "%.1f", share());
    }
}
