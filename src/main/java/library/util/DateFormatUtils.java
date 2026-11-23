package library.util;

/**
 * Utility class for date format conversions.
 * Consolidates duplicate convertDateFormat methods from multiple controllers.
 */
public final class DateFormatUtils {

    private static final String[] MONTH_NAMES = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    private DateFormatUtils() {
        // Prevent instantiation
    }
    
    /**
     * Converts date format from dd/MM/yyyy to yyyy-MM-dd for database queries.
     * Returns the original string if already in ISO format or if parsing fails.
     *
     * @param dateStr the date string to convert
     * @return ISO formatted date string (yyyy-MM-dd) or original if already valid
     */
    public static String convertToIsoFormat(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            // Check if it's already in yyyy-MM-dd format
            if (dateStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return dateStr;
            }
            // Try to parse dd/MM/yyyy format
            if (dateStr.matches("\\d{2}/\\d{2}/\\d{4}")) {
                String[] parts = dateStr.split("/");
                if (parts.length == 3) {
                    return parts[2] + "-" + parts[1] + "-" + parts[0];
                }
            }
        } catch (Exception e) {
            // If parsing fails, return original
        }
        return dateStr;
    }
    
    /**
     * Converts date format from dd/MM/yyyy to dd MMM yyyy for display.
     * Returns the original string if parsing fails.
     *
     * @param dateStr the date string in dd/MM/yyyy format
     * @return display formatted date string (dd MMM yyyy) or original if invalid
     */
    public static String convertToDisplayFormat(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            // Try to parse dd/MM/yyyy format
            if (dateStr.matches("\\d{2}/\\d{2}/\\d{4}")) {
                String[] parts = dateStr.split("/");
                if (parts.length == 3) {
                    int day = Integer.parseInt(parts[0]);
                    int month = Integer.parseInt(parts[1]);
                    int year = Integer.parseInt(parts[2]);
                    String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
                    if (month >= 1 && month <= 12) {
                        return String.format("%02d %s %d", day, months[month - 1], year);
                    }
                }
            }
        } catch (Exception e) {
            // If parsing fails, return original
        }
        return dateStr;
    }

    /**
     * Formats an ISO date (yyyy-MM-dd) as dd-MMM-yyyy for display.
     * Returns null for blank input and the original string if parsing fails.
     */
    public static String formatIsoDateForDisplay(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            String[] parts = dateStr.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                return String.format("%02d-%s-%d", day, MONTH_NAMES[month - 1], year);
            }
        } catch (Exception e) {
            // If parsing fails, return original
        }
        return dateStr;
    }

    /**
     * Formats a play timestamp ("yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd") as dd-MMM-yyyy.
     * Returns "-" for blank input and the original string if parsing fails.
     */
    public static String formatPlayDate(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
            return "-";
        }
        try {
            String datePart = dateTimeString.trim();
            if (datePart.contains(" ")) {
                datePart = datePart.split(" ")[0];
            }
            String[] parts = datePart.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                return String.format("%02d-%s-%d", day, MONTH_NAMES[month - 1], year);
            }
            return datePart;
        } catch (Exception e) {
            return dateTimeString;
        }
    }

    /**
     * Parse the SQLite timestamp variants stored in the database ("yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss",
     * ISO "T" separator). Falls back to the date part; returns null when nothing parses.
     */
    public static java.sql.Timestamp parseTimestamp(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        try {
            if (v.length() == 10 && v.matches("\\d{4}-\\d{2}-\\d{2}")) {
                v = v + " 00:00:00";
            } else if (v.contains("T") && v.matches("\\d{4}-\\d{2}-\\d{2}T.*")) {
                v = v.replace('T', ' ');
            }
            return java.sql.Timestamp.valueOf(v);
        } catch (Exception e) {
            try {
                if (v.length() >= 10) {
                    String datePart = v.substring(0, 10);
                    if (datePart.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        return java.sql.Timestamp.valueOf(datePart + " 00:00:00");
                    }
                }
            } catch (Exception ignore) {}
            return null;
        }
    }

    /**
     * Parse the date part (yyyy-MM-dd) of a database value; returns null when it is not a valid date.
     */
    public static java.sql.Date parseDate(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        if (v.contains("T")) v = v.replace('T', ' ');
        if (v.length() >= 10) v = v.substring(0, 10);
        if (v.matches("\\d{4}-\\d{2}-\\d{2}")) {
            try {
                return java.sql.Date.valueOf(v);
            } catch (Exception ignore) {}
        }
        return null;
    }

    /**
     * Human span between two display dates (dd-MMM-yyyy from {@link #formatPlayDate} or dd/MM/yyyy),
     * such as "21 years, 8 months, 26 days" (parts that are zero are left out). Returns null when either date
     * is missing or unparsable, or when both fall on the same day.
     */
    public static String spanLabel(String from, String to) {
        java.time.LocalDate start = parseDisplayDate(from);
        java.time.LocalDate end = parseDisplayDate(to);
        if (start == null || end == null || !end.isAfter(start)) {
            return null;
        }
        java.time.Period period = java.time.Period.between(start, end);
        java.util.List<String> parts = new java.util.ArrayList<>();
        addSpanPart(parts, period.getYears(), "year");
        addSpanPart(parts, period.getMonths(), "month");
        addSpanPart(parts, period.getDays(), "day");
        return String.join(", ", parts);
    }

    /**
     * How long ago a display date was ("Today", "3 weeks ago", "4 years ago") plus its recency level, the same
     * buckets as the catalog Table view: fresh within 1 year, aging 1-3 years, stale 3-5 years, old after that.
     */
    public record Recency(String ago, String level) {
    }

    /** {@link Recency} of a display date as of today; null when the date is missing or unparsable. */
    public static Recency recency(String displayDate) {
        return recency(displayDate, java.time.LocalDate.now());
    }

    static Recency recency(String displayDate, java.time.LocalDate today) {
        java.time.LocalDate date = parseDisplayDate(displayDate);
        if (date == null) {
            return null;
        }
        long days = java.time.temporal.ChronoUnit.DAYS.between(date, today);
        double years = days / 365.25;
        String level = years <= 1 ? "fresh" : years <= 3 ? "aging" : years <= 5 ? "stale" : "old";
        return new Recency(agoText(days), level);
    }

    private static String agoText(long days) {
        if (days < 0) return "Upcoming";
        if (days == 0) return "Today";
        if (days == 1) return "Yesterday";
        if (days < 7) return days + " days ago";
        if (days < 30) return plural(days / 7, "week") + " ago";
        if (days < 365) return plural(Math.max(1, (long) Math.floor(days / 30.44)), "month") + " ago";
        return plural((long) Math.floor(days / 365.25), "year") + " ago";
    }

    private static String plural(long amount, String unit) {
        return amount + " " + unit + (amount == 1 ? "" : "s");
    }

    private static void addSpanPart(java.util.List<String> parts, int amount, String unit) {
        if (amount > 0) {
            parts.add(amount + " " + unit + (amount == 1 ? "" : "s"));
        }
    }

    /**
     * Parses a display date (dd-MMM-yyyy or dd/MM/yyyy); returns null when blank or unparsable.
     */
    public static java.time.LocalDate parseDisplayDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        for (String pattern : new String[] {"dd-MMM-yyyy", "dd/MM/yyyy"}) {
            try {
                return java.time.LocalDate.parse(value.trim(),
                        java.time.format.DateTimeFormatter.ofPattern(pattern, java.util.Locale.ENGLISH));
            } catch (java.time.format.DateTimeParseException ignore) {
                // try the next display format
            }
        }
        return null;
    }
}
