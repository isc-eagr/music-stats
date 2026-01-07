package library.util;

/**
 * Utility class for formatting time durations.
 * Consolidates duplicate formatTime methods from multiple services.
 */
public final class TimeFormatUtils {
    
    private TimeFormatUtils() {
        // Prevent instantiation
    }
    
    /**
     * Formats time in seconds to a human-readable format.
     * Smart formatting for display: shows days+hours, hours+minutes, or just minutes
     * based on the magnitude.
     *
     * @param totalSeconds the total time in seconds
     * @return formatted string like "2d 5h", "3h 45m", or "20m"
     */
    public static String formatTime(long totalSeconds) {
        if (totalSeconds == 0) {
            return "0m";
        }
        
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        
        // Smart formatting for card display
        if (days > 0) {
            return String.format("%dd %dh", days, hours);
        } else if (hours > 0) {
            return String.format("%dh %dm", hours, minutes);
        } else {
            return String.format("%dm", minutes);
        }
    }
}
