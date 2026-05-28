package library.util;

import java.util.Locale;

public final class ChartAggregationUtils {

    private ChartAggregationUtils() {
    }

    public static String normalizeKeyPart(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    public static String pickPreferredDisplayValue(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank()) {
            return candidateValue;
        }

        int ignoreCase = candidateValue.compareToIgnoreCase(currentValue);
        if (ignoreCase < 0) {
            return candidateValue;
        }
        if (ignoreCase == 0 && candidateValue.compareTo(currentValue) < 0) {
            return candidateValue;
        }
        return currentValue;
    }

    public static String minDate(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank() || candidateValue.compareTo(currentValue) < 0) {
            return candidateValue;
        }
        return currentValue;
    }

    public static String maxDate(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank() || candidateValue.compareTo(currentValue) > 0) {
            return candidateValue;
        }
        return currentValue;
    }

    public static String safeLower(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }
}