package library.util;

/**
 * Utility class for date format conversions.
 * Consolidates duplicate convertDateFormat methods from multiple controllers.
 */
public final class DateFormatUtils {
    
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
}
