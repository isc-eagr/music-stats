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
     * Appends a many-to-many tag filter.
     * Supports modes: includes, excludes, isnull, isnotnull.
     */
    public static void appendTagFilter(StringBuilder sql, List<Object> params,
                                       String idColumn, String tagTableName, String entityIdColumn,
                                       List<Integer> tagIds, String mode) {
        if (mode == null) return;

        switch (mode) {
            case "includes":
                if (tagIds != null && !tagIds.isEmpty()) {
                    sql.append(" AND EXISTS (SELECT 1 FROM ")
                            .append(tagTableName)
                            .append(" tag_filter WHERE tag_filter.")
                            .append(entityIdColumn)
                            .append(" = ")
                            .append(idColumn)
                            .append(" AND tag_filter.tag_id IN (");
                    appendPlaceholders(sql, tagIds.size());
                    sql.append(")) ");
                    params.addAll(tagIds);
                }
                break;
            case "excludes":
                if (tagIds != null && !tagIds.isEmpty()) {
                    sql.append(" AND NOT EXISTS (SELECT 1 FROM ")
                            .append(tagTableName)
                            .append(" tag_filter WHERE tag_filter.")
                            .append(entityIdColumn)
                            .append(" = ")
                            .append(idColumn)
                            .append(" AND tag_filter.tag_id IN (");
                    appendPlaceholders(sql, tagIds.size());
                    sql.append(")) ");
                    params.addAll(tagIds);
                }
                break;
            case "isnull":
                sql.append(" AND NOT EXISTS (SELECT 1 FROM ")
                        .append(tagTableName)
                        .append(" tag_filter WHERE tag_filter.")
                        .append(entityIdColumn)
                        .append(" = ")
                        .append(idColumn)
                        .append(") ");
                break;
            case "isnotnull":
                sql.append(" AND EXISTS (SELECT 1 FROM ")
                        .append(tagTableName)
                        .append(" tag_filter WHERE tag_filter.")
                        .append(entityIdColumn)
                        .append(" = ")
                        .append(idColumn)
                        .append(") ");
                break;
            default:
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
            case "isnull":
                sql.append(" AND ").append(dateExpression).append(" IS NULL");
                break;
            case "isnotnull":
                sql.append(" AND ").append(dateExpression).append(" IS NOT NULL");
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
     * Appends an iTunes entity-ID filter using json_each() for efficient parameterized IN-list.
     * The caller pre-computes a JSON array of matching entity IDs (e.g. "[1,2,3]")
     * and passes the desired mode ("true" = include only matching, "false" = exclude matching).
     *
     * @param sql            The StringBuilder to append to
     * @param params         The parameter list to add values to
     * @param idColumn       The SQL column holding the entity PK (e.g. "a.id")
     * @param itunesIdsJson  JSON array string of entity IDs present in iTunes, or null if no filter
     * @param inItunesMode   "true" to include only those IDs, "false" to exclude them; null = no-op
     */
    public static void appendItunesIdFilter(StringBuilder sql, List<Object> params,
                                            String idColumn, String itunesIdsJson, String inItunesMode) {
        if (itunesIdsJson == null || inItunesMode == null || inItunesMode.isEmpty()) return;
        if ("true".equalsIgnoreCase(inItunesMode)) {
            sql.append(" AND ").append(idColumn).append(" IN (SELECT value FROM json_each(?)) ");
        } else {
            sql.append(" AND ").append(idColumn).append(" NOT IN (SELECT value FROM json_each(?)) ");
        }
        params.add(itunesIdsJson);
    }

    public static void appendChartStatsFilter(StringBuilder sql, List<Object> params,
                                              String chartEntryItemColumn, String outerItemIdExpression,
                                              String chartType, String periodType, String countAlias,
                                              Integer peak, Integer countMin,
                                              String dateFrom, String dateTo,
                                              String season) {
        if (peak == null && countMin == null && !hasValue(dateFrom) && !hasValue(dateTo) && !hasValue(season)) {
            return;
        }

        sql.append(" AND EXISTS (SELECT 1 FROM (");
        sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as ").append(countAlias).append(" ");
        sql.append("FROM ChartEntry ce ");
        sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
        sql.append("WHERE ").append(chartEntryItemColumn).append(" = ").append(outerItemIdExpression);
        sql.append(" AND c.chart_type = '").append(chartType).append("' AND c.period_type = '").append(periodType).append("'");
        appendChartDateOverlapFilter(sql, params, "c.period_start_date", "c.period_end_date", dateFrom, dateTo);
        appendChartSeasonFilter(sql, params, periodType, season);
        sql.append(") chart_stats WHERE chart_stats.").append(countAlias).append(" > 0");
        if (peak != null) {
            sql.append(" AND chart_stats.peak <= ?");
            params.add(peak);
        }
        if (countMin != null) {
            sql.append(" AND chart_stats.").append(countAlias).append(" >= ?");
            params.add(countMin);
        }
        sql.append(")");
    }

    public static void appendDateRangeBounds(StringBuilder sql, List<Object> params,
                                             String dateExpression, String dateFrom, String dateTo) {
        if (hasValue(dateFrom)) {
            sql.append(" AND DATE(").append(dateExpression).append(") >= DATE(?)");
            params.add(dateFrom);
        }
        if (hasValue(dateTo)) {
            sql.append(" AND DATE(").append(dateExpression).append(") <= DATE(?)");
            params.add(dateTo);
        }
    }

    public static boolean hasValue(String value) {
        return value != null && !value.isBlank();
    }

    private static void appendChartDateOverlapFilter(StringBuilder sql, List<Object> params,
                                                     String periodStartExpression, String periodEndExpression,
                                                     String dateFrom, String dateTo) {
        if (hasValue(dateFrom)) {
            sql.append(" AND DATE(COALESCE(")
                .append(periodEndExpression)
                .append(", ")
                .append(periodStartExpression)
                .append(")) >= DATE(?)");
            params.add(dateFrom);
        }
        if (hasValue(dateTo)) {
            sql.append(" AND DATE(").append(periodStartExpression).append(") <= DATE(?)");
            params.add(dateTo);
        }
    }

    private static void appendChartSeasonFilter(StringBuilder sql, List<Object> params,
                                                String periodType, String season) {
        if (!hasValue(season)) {
            return;
        }

        String seasonExpression = resolveSeasonExpression(periodType);
        if (seasonExpression == null) {
            return;
        }

        sql.append(" AND ").append(seasonExpression).append(" = ?");
        params.add(season);
    }

    private static String resolveSeasonExpression(String periodType) {
        return switch (periodType) {
            case "seasonal" -> "SUBSTR(c.period_key, 6)";
            case "weekly" -> "CASE " +
                    "WHEN c.period_start_date IS NULL THEN NULL " +
                    "WHEN CAST(strftime('%m', c.period_start_date) AS INTEGER) IN (12, 1, 2) THEN 'Winter' " +
                    "WHEN CAST(strftime('%m', c.period_start_date) AS INTEGER) BETWEEN 3 AND 5 THEN 'Spring' " +
                    "WHEN CAST(strftime('%m', c.period_start_date) AS INTEGER) BETWEEN 6 AND 8 THEN 'Summer' " +
                    "ELSE 'Fall' END";
            default -> null;
        };
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
