package library.util;

import java.time.LocalDate;

/**
 * Legacy plays come from the old robertlover account, which stopped scrobbling in late 2009.
 * Pages hide the Legacy figures where none can exist: the context (first play, release date, period) starts after 2009.
 */
public final class LegacyPlays {

    /** Last year the legacy account scrobbled. */
    public static final int LAST_YEAR = 2009;

    private LegacyPlays() {
    }

    /**
     * Whether the Legacy figures are worth showing. Always true when there are legacy plays (a data slip stays visible);
     * false when any of the known context start years is after {@link #LAST_YEAR}. Unknown (null) years are ignored.
     */
    public static boolean worthShowing(Number legacyPlays, Integer... contextStartYears) {
        if (legacyPlays != null && legacyPlays.longValue() > 0) {
            return true;
        }
        for (Integer year : contextStartYears == null ? new Integer[0] : contextStartYears) {
            if (year != null && year > LAST_YEAR) {
                return false;
            }
        }
        return true;
    }

    /**
     * Artists / Albums / Songs lists: false when the listened-date filter (yyyy-MM-dd or dd/MM/yyyy) starts after
     * {@link #LAST_YEAR}, as when the list is opened from a later timeframe or year; true without a usable date.
     */
    public static boolean worthShowingFrom(String listenedDateFrom) {
        String iso = DateFormatUtils.convertToIsoFormat(listenedDateFrom);
        if (iso == null || !iso.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return true;
        }
        return worthShowing(null, Integer.parseInt(iso.substring(0, 4)));
    }

    /** Same as {@link #worthShowing(Number, Integer...)} with dates (first play, release date, period start). */
    public static boolean worthShowing(Number legacyPlays, LocalDate... contextStarts) {
        Integer[] years = new Integer[contextStarts == null ? 0 : contextStarts.length];
        for (int i = 0; i < years.length; i++) {
            years[i] = contextStarts[i] != null ? contextStarts[i].getYear() : null;
        }
        return worthShowing(legacyPlays, years);
    }
}
