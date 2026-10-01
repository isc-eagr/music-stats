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
}
