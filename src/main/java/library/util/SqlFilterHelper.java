package library.util;

import java.util.List;

/**
 * Utility class for building SQL filter conditions dynamically.
 * Centralizes common filter patterns used across repositories and services.
 */
public final class SqlFilterHelper {
    
    private SqlFilterHelper() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Appends a filter condition for integer ID columns.
     * Supports modes: includes, excludes, isnull, isnotnull
     * 
     * @param sql The StringBuilder to append to
     * @param params The parameter list to add values to
     * @param columnName The SQL column name (e.g., "a.gender_id")
     * @param ids The list of IDs to filter by
     * @param mode The filter mode (includes, excludes, isnull, isnotnull)
     */
    public static void appendIdFilter(StringBuilder sql, List<Object> params,
                                       String columnName, List<Integer> ids, String mode) {
        if (mode == null) return;
        
        switch (mode) {
            case "includes":
                if (ids != null && !ids.isEmpty()) {
                    sql.append(" AND ").append(columnName).append(" IN (");
                    appendPlaceholders(sql, ids.size());
                    sql.append(") ");
                    params.addAll(ids);
                }
                break;
            case "excludes":
                if (ids != null && !ids.isEmpty()) {
                    sql.append(" AND (").append(columnName).append(" NOT IN (");
                    appendPlaceholders(sql, ids.size());
                    sql.append(") OR ").append(columnName).append(" IS NULL) ");
                    params.addAll(ids);
                }
                break;
            case "isnull":
                sql.append(" AND ").append(columnName).append(" IS NULL ");
                break;
            case "isnotnull":
                sql.append(" AND ").append(columnName).append(" IS NOT NULL ");
                break;
            default:
                // Unknown mode - ignore
                break;
        }
    }
    
    /**
     * Appends a filter condition for string columns.
     * Supports modes: includes, excludes, isnull, isnotnull
     * 
     * @param sql The StringBuilder to append to
     * @param params The parameter list to add values to
     * @param columnName The SQL column name (e.g., "a.country")
     * @param values The list of string values to filter by
     * @param mode The filter mode (includes, excludes, isnull, isnotnull)
     */
    public static void appendStringFilter(StringBuilder sql, List<Object> params,
                                          String columnName, List<String> values, String mode) {
        if (mode == null) return;
        
        switch (mode) {
            case "includes":
                if (values != null && !values.isEmpty()) {
                    sql.append(" AND ").append(columnName).append(" IN (");
                    appendPlaceholders(sql, values.size());
                    sql.append(") ");
                    params.addAll(values);
                }
                break;
            case "excludes":
                if (values != null && !values.isEmpty()) {
                    sql.append(" AND (").append(columnName).append(" NOT IN (");
                    appendPlaceholders(sql, values.size());
                    sql.append(") OR ").append(columnName).append(" IS NULL) ");
                    params.addAll(values);
                }
                break;
            case "isnull":
                sql.append(" AND ").append(columnName).append(" IS NULL ");
                break;
            case "isnotnull":
                sql.append(" AND ").append(columnName).append(" IS NOT NULL ");
                break;
            default:
                // Unknown mode - ignore
                break;
        }
    }
    
    /**
     * Appends a date filter condition with support for exact, gte, lte, and between modes.
     * 
     * @param sql The StringBuilder to append to
     * @param params The parameter list to add values to
     * @param dateExpression The SQL expression for the date (can be a column or subquery)
     * @param dateValue The date value for exact/gte/lte modes
     * @param dateFrom The start date for between mode
     * @param dateTo The end date for between mode
     * @param mode The filter mode (exact, gte, lte, between)
     */
    public static void appendDateFilter(StringBuilder sql, List<Object> params,
                                        String dateExpression, String dateValue,
                                        String dateFrom, String dateTo, String mode) {
        if (mode == null || mode.isEmpty()) return;
        
        switch (mode) {
            case "exact":
                if (dateValue != null && !dateValue.isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpression).append(") = ?");
                    params.add(dateValue);
                }
                break;
            case "gte":
                if (dateValue != null && !dateValue.isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpression).append(") >= ?");
                    params.add(dateValue);
                }
                break;
            case "lte":
                if (dateValue != null && !dateValue.isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpression).append(") <= ?");
                    params.add(dateValue);
                }
                break;
            case "between":
                if (dateFrom != null && !dateFrom.isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpression).append(") >= ?");
                    params.add(dateFrom);
                }
                if (dateTo != null && !dateTo.isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpression).append(") <= ?");
                    params.add(dateTo);
                }
                break;
            default:
                // Unknown mode - ignore
                break;
        }
    }
    
    /**
     * Appends a numeric range filter (min/max).
     * 
     * @param sql The StringBuilder to append to
     * @param params The parameter list to add values to
     * @param columnName The SQL column name
     * @param minValue The minimum value (inclusive)
     * @param maxValue The maximum value (inclusive)
     */
    public static void appendRangeFilter(StringBuilder sql, List<Object> params,
                                         String columnName, Number minValue, Number maxValue) {
        if (minValue != null) {
            sql.append(" AND ").append(columnName).append(" >= ?");
            params.add(minValue);
        }
        if (maxValue != null) {
            sql.append(" AND ").append(columnName).append(" <= ?");
            params.add(maxValue);
        }
    }
    
    /**
     * Appends a LIKE filter for text search with accent-insensitive comparison.
     * Uses SQL REPLACE chain to normalize accented characters in the database column.
     * 
     * @param sql The StringBuilder to append to
     * @param params The parameter list to add values to
     * @param columnName The SQL column name
     * @param searchValue The search value (will be prefixed with %)
     * @param matchMode How to match: "starts" (value%), "ends" (%value), "contains" (%value%)
     */
    public static void appendLikeFilter(StringBuilder sql, List<Object> params,
                                        String columnName, String searchValue, String matchMode) {
        if (searchValue == null || searchValue.isEmpty()) return;
        
        // Use accent-insensitive comparison: normalize both column and search value
        String normalizedColumn = StringNormalizer.sqlNormalizeColumn(columnName);
        String normalizedSearchValue = StringNormalizer.normalizeForSearch(searchValue);
        
        sql.append(" AND ").append(normalizedColumn).append(" LIKE ? ");
        
        switch (matchMode != null ? matchMode : "starts") {
            case "contains":
                params.add("%" + normalizedSearchValue + "%");
                break;
            case "ends":
                params.add("%" + normalizedSearchValue);
                break;
            case "starts":
            default:
                params.add(normalizedSearchValue + "%");
                break;
        }
    }
    
    /**
     * Appends a LIKE filter for text search (simple version without accent normalization).
     * Use this for cases where accent-insensitive search is not needed.
     * 
     * @param sql The StringBuilder to append to
     * @param params The parameter list to add values to
     * @param columnName The SQL column name
     * @param searchValue The search value (will be prefixed with %)
     * @param matchMode How to match: "starts" (value%), "ends" (%value), "contains" (%value%)
     */
    public static void appendLikeFilterSimple(StringBuilder sql, List<Object> params,
                                              String columnName, String searchValue, String matchMode) {
        if (searchValue == null || searchValue.isEmpty()) return;
        
        sql.append(" AND LOWER(").append(columnName).append(") LIKE LOWER(?) ");
        
        switch (matchMode != null ? matchMode : "starts") {
            case "contains":
                params.add("%" + searchValue + "%");
                break;
            case "ends":
                params.add("%" + searchValue);
                break;
            case "starts":
            default:
                params.add(searchValue + "%");
                break;
        }
    }
    
    /**
     * Helper to append comma-separated placeholders.
     */
    private static void appendPlaceholders(StringBuilder sql, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0) sql.append(",");
            sql.append("?");
        }
    }
}
