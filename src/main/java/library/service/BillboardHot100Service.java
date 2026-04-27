package library.service;

import library.dto.BillboardHot100OverviewRowDTO;
import library.util.BillboardHot100ImportSupport;
import library.util.StringNormalizer;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class BillboardHot100Service {

    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    public BillboardHot100Service(JdbcTemplate jdbcTemplate, DataSource dataSource) {
        this.jdbcTemplate = jdbcTemplate;
        this.dataSource = dataSource;
    }

    public List<BillboardHot100OverviewRowDTO> getOverviewRows() {
        return getOverviewRows(1, 250, "firstWeek", "desc", null);
    }

    public List<BillboardHot100OverviewRowDTO> getOverviewRows(int page, int size) {
        return getOverviewRows(page, size, "firstWeek", "desc", null);
    }

    public List<BillboardHot100OverviewRowDTO> getOverviewRows(int page, int size, String sort, String dir) {
        return getOverviewRows(page, size, sort, dir, null);
    }

    public List<BillboardHot100OverviewRowDTO> getOverviewRows(int page, int size, String sort, String dir, String query) {
        if (!tableExists()) {
            return Collections.emptyList();
        }

        int safePage = Math.max(1, page);
        int safeSize = Math.max(1, Math.min(size, 500));
        int offset = (safePage - 1) * safeSize;
        String orderBy = buildOrderBy(sort, dir);

        List<Object> params = new ArrayList<>();
        String searchClause = buildOverviewSearchClause(query, params);
        String sql = "SELECT * FROM (" + buildOverviewBaseSql() + ") overview" + searchClause + " ORDER BY " + orderBy + " LIMIT ? OFFSET ?";
        params.add(safeSize);
        params.add(offset);

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            BillboardHot100OverviewRowDTO row = new BillboardHot100OverviewRowDTO();
            row.setMatched(rs.getInt("matched") == 1);
            int songId = rs.getInt("song_id");
            if (!rs.wasNull()) {
                row.setSongId(songId);
            }
            int resolvedArtistId = rs.getInt("resolved_artist_id");
            if (!rs.wasNull()) {
                row.setResolvedArtistId(resolvedArtistId);
            }
            row.setSongTitle(rs.getString("song_title"));
            row.setArtistName(rs.getString("artist_name"));
            row.setFirstWeek(rs.getString("first_week"));
            row.setLastWeek(rs.getString("last_week"));
            row.setPeakWeek(rs.getString("peak_week"));
            row.setWeeksOnChart(rs.getInt("weeks_on_chart"));
            row.setPeakPosition(rs.getInt("peak_position"));
            row.setWeeksAtPeak(rs.getInt("weeks_at_peak"));
            row.setGenderClass(toGenderClass(rs.getString("gender_name")));
            return row;
        }, params.toArray());
    }

    public int countOverviewRows(String query) {
        if (!tableExists()) {
            return 0;
        }
        List<Object> params = new ArrayList<>();
        String sql = "SELECT COUNT(*) FROM (" + buildOverviewBaseSql() + ") overview" + buildOverviewSearchClause(query, params);
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, params.toArray());
        return count == null ? 0 : count;
    }

    private String buildOrderBy(String sort, String dir) {
        String safeDir = "asc".equalsIgnoreCase(dir) ? "ASC" : "DESC";
        return switch (sort) {
            case "weeks" -> "weeks_on_chart " + safeDir + ", peak_position ASC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
            case "peak" -> "peak_position " + safeDir + ", weeks_on_chart DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
            case "weeksAtPeak" -> "weeks_at_peak " + safeDir + ", peak_position ASC, weeks_on_chart DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
            case "peakWeek" -> "peak_week " + safeDir + ", peak_position ASC, weeks_on_chart DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
            case "lastWeek" -> "last_week " + safeDir + ", first_week DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
            case "song" -> "song_title COLLATE NOCASE " + safeDir + ", artist_name COLLATE NOCASE ASC, first_week DESC";
            case "artist" -> "artist_name COLLATE NOCASE " + safeDir + ", song_title COLLATE NOCASE ASC, first_week DESC";
            case "firstWeek" -> "first_week " + safeDir + ", last_week DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
            default -> "first_week DESC, last_week DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
        };
    }

    private String buildOverviewBaseSql() {
        return
            "WITH matched_groups AS ( " +
            "    SELECT e.song_id, " +
            "           MIN(e.chart_date) AS first_week, " +
            "           MAX(e.chart_date) AS last_week, " +
            "           COUNT(*) AS weeks_on_chart, " +
            "           MIN(e.position) AS peak_position " +
            "    FROM billboard_hot100_entry e " +
            "    WHERE e.song_id IS NOT NULL " +
            "    GROUP BY e.song_id " +
            "), " +
            "matched_peak AS ( " +
            "    SELECT e.song_id, COUNT(*) AS weeks_at_peak, MIN(e.chart_date) AS peak_week " +
            "    FROM billboard_hot100_entry e " +
            "    JOIN matched_groups mg ON mg.song_id = e.song_id AND e.position = mg.peak_position " +
            "    GROUP BY e.song_id " +
            "), " +
            "unmatched_groups AS ( " +
            "    SELECT e.artist_name, e.song_title, " +
            "           MIN(e.chart_date) AS first_week, " +
            "           MAX(e.chart_date) AS last_week, " +
            "           COUNT(*) AS weeks_on_chart, " +
            "           MIN(e.position) AS peak_position " +
            "    FROM billboard_hot100_entry e " +
            "    WHERE e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            "), " +
            "unmatched_peak AS ( " +
            "    SELECT e.artist_name, e.song_title, COUNT(*) AS weeks_at_peak, MIN(e.chart_date) AS peak_week " +
            "    FROM billboard_hot100_entry e " +
            "    JOIN unmatched_groups ug ON ug.artist_name = e.artist_name " +
            "                           AND ug.song_title = e.song_title " +
            "                           AND e.position = ug.peak_position " +
            "    WHERE e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            ") " +
            "SELECT 1 AS matched, mg.song_id, s.name AS song_title, a.name AS artist_name, " +
            "       mg.first_week, mg.last_week, mg.weeks_on_chart, mg.peak_position, " +
            "       COALESCE(mp.weeks_at_peak, 0) AS weeks_at_peak, mp.peak_week, " +
            "       a.id AS resolved_artist_id, LOWER(g.name) AS gender_name " +
            "FROM matched_groups mg " +
            "JOIN Song s ON s.id = mg.song_id " +
            "JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "LEFT JOIN matched_peak mp ON mp.song_id = mg.song_id " +
            "UNION ALL " +
            "SELECT 0 AS matched, NULL AS song_id, ug.song_title, ug.artist_name, " +
            "       ug.first_week, ug.last_week, ug.weeks_on_chart, ug.peak_position, " +
            "       COALESCE(up.weeks_at_peak, 0) AS weeks_at_peak, up.peak_week, " +
            "       NULL AS resolved_artist_id, NULL AS gender_name " +
            "FROM unmatched_groups ug " +
            "LEFT JOIN unmatched_peak up ON up.artist_name = ug.artist_name AND up.song_title = ug.song_title ";
    }

    private String buildOverviewSearchClause(String query, List<Object> params) {
        String normalizedQuery = StringNormalizer.normalizeForSearch(query);
        if (normalizedQuery == null || normalizedQuery.isBlank()) {
            return "";
        }
        String like = "%" + normalizedQuery + "%";
        params.add(like);
        params.add(like);
        return " WHERE " + StringNormalizer.sqlNormalizeColumn("song_title") + " LIKE ?" +
               "    OR " + StringNormalizer.sqlNormalizeColumn("artist_name") + " LIKE ?";
    }

    public Map<String, Object> getSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        if (!tableExists()) {
            summary.put("total", 0);
            summary.put("matched", 0);
            summary.put("unmatched", 0);
            summary.put("rowsImported", 0);
            summary.put("lastImportedChartDate", null);
            return summary;
        }

        String sql =
            "WITH matched_groups AS ( " +
            "    SELECT song_id FROM billboard_hot100_entry WHERE song_id IS NOT NULL GROUP BY song_id " +
            "), unmatched_groups AS ( " +
            "    SELECT artist_name, song_title FROM billboard_hot100_entry WHERE song_id IS NULL GROUP BY artist_name, song_title " +
            ") " +
            "SELECT (SELECT COUNT(*) FROM matched_groups) AS matched, " +
            "       (SELECT COUNT(*) FROM unmatched_groups) AS unmatched, " +
            "       (SELECT COUNT(*) FROM billboard_hot100_entry) AS rows_imported, " +
            "       (SELECT MAX(chart_date) FROM billboard_hot100_entry) AS last_imported_chart_date";

        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
            int matched = rs.getInt("matched");
            int unmatched = rs.getInt("unmatched");
            summary.put("total", matched + unmatched);
            summary.put("matched", matched);
            summary.put("unmatched", unmatched);
            summary.put("rowsImported", rs.getInt("rows_imported"));
            summary.put("lastImportedChartDate", rs.getString("last_imported_chart_date"));
            return summary;
        });
    }

    public Map<String, Object> importAllCharts() {
        try (Connection connection = dataSource.getConnection()) {
            BillboardHot100ImportSupport.ImportReport report = BillboardHot100ImportSupport.importAllCharts(connection);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("ok", true);
            result.put("mode", "full");
            result.put("chartsImported", report.chartCount());
            result.put("rowsImported", report.entryCount());
            result.put("preservedLinks", report.preservedLinks());
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to import Billboard Hot 100 data", e);
        }
    }

    public Map<String, Object> importIncrementalCharts() {
        try (Connection connection = dataSource.getConnection()) {
            BillboardHot100ImportSupport.ImportReport report = BillboardHot100ImportSupport.importNewCharts(connection);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("ok", true);
            result.put("mode", "incremental");
            result.put("chartsImported", report.chartCount());
            result.put("rowsImported", report.entryCount());
            result.put("preservedLinks", report.preservedLinks());
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to incrementally import Billboard Hot 100 data", e);
        }
    }

    public Map<String, Object> autoLinkExactMatches() {
        try (Connection connection = dataSource.getConnection()) {
            int unmatchedBefore = countUnmatchedGroups();
            int rowsLinked = BillboardHot100ImportSupport.autoLinkExactMatches(connection);
            int unmatchedAfter = countUnmatchedGroups();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("ok", true);
            result.put("rowsLinked", rowsLinked);
            result.put("groupsLinked", Math.max(0, unmatchedBefore - unmatchedAfter));
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to auto-link Billboard Hot 100 entries", e);
        }
    }

    public int normalizeCaseDifferences() {
        int updatedRows = normalizeLinkedRowsToLibraryCase();

        List<Map<String, Object>> variantGroups = jdbcTemplate.query(
            "SELECT LOWER(TRIM(artist_name)) AS artist_key, LOWER(TRIM(song_title)) AS song_key " +
            "FROM billboard_hot100_entry " +
            "WHERE song_id IS NULL " +
            "GROUP BY LOWER(TRIM(artist_name)), LOWER(TRIM(song_title)) " +
            "HAVING COUNT(DISTINCT artist_name || '||' || song_title) > 1",
            (rs, rowNum) -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("artistKey", rs.getString("artist_key"));
                row.put("songKey", rs.getString("song_key"));
                return row;
            }
        );

        for (Map<String, Object> group : variantGroups) {
            String artistKey = (String) group.get("artistKey");
            String songKey = (String) group.get("songKey");

            List<Map<String, Object>> canonicalChoices = jdbcTemplate.query(
                "SELECT artist_name, song_title, COUNT(*) AS usage_count " +
                "FROM billboard_hot100_entry " +
                "WHERE song_id IS NULL " +
                "  AND LOWER(TRIM(artist_name)) = ? " +
                "  AND LOWER(TRIM(song_title)) = ? " +
                "GROUP BY artist_name, song_title " +
                "ORDER BY usage_count DESC, artist_name COLLATE NOCASE ASC, artist_name ASC, song_title COLLATE NOCASE ASC, song_title ASC " +
                "LIMIT 1",
                (rs, rowNum) -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("artistName", rs.getString("artist_name"));
                    row.put("songTitle", rs.getString("song_title"));
                    return row;
                },
                artistKey,
                songKey
            );

            if (canonicalChoices.isEmpty()) {
                continue;
            }

            Map<String, Object> canonical = canonicalChoices.get(0);
            String canonicalArtist = (String) canonical.get("artistName");
            String canonicalSong = (String) canonical.get("songTitle");

            updatedRows += jdbcTemplate.update(
                "UPDATE billboard_hot100_entry SET artist_name = ?, song_title = ? " +
                "WHERE song_id IS NULL " +
                "  AND LOWER(TRIM(artist_name)) = ? " +
                "  AND LOWER(TRIM(song_title)) = ? " +
                "  AND (artist_name <> ? OR song_title <> ?)",
                canonicalArtist,
                canonicalSong,
                artistKey,
                songKey,
                canonicalArtist,
                canonicalSong
            );
        }

        return updatedRows;
    }

    public Map<String, Object> matchRawGroup(String rawArtist, String rawSong, Integer songId) {
        int updated = jdbcTemplate.update(
            "UPDATE billboard_hot100_entry SET song_id = ? " +
            "WHERE LOWER(TRIM(artist_name)) = LOWER(TRIM(?)) " +
            "  AND LOWER(TRIM(song_title)) = LOWER(TRIM(?))",
            songId, rawArtist, rawSong
        );

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("updatedRows", updated);
        return result;
    }

    public Map<String, Object> getStatsBySongId(Integer songId) {
        if (songId == null || !tableExists()) {
            return null;
        }

        String sql =
            "WITH stats AS ( " +
            "    SELECT COUNT(*) AS weeks_on_chart, MIN(position) AS peak_position " +
            "    FROM billboard_hot100_entry WHERE song_id = ? " +
            ") " +
            "SELECT weeks_on_chart, peak_position, " +
            "       (SELECT COUNT(*) FROM billboard_hot100_entry WHERE song_id = ? AND position = stats.peak_position) AS weeks_at_peak " +
            "FROM stats WHERE weeks_on_chart > 0";

        List<Map<String, Object>> rows = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("weeksOnChart", rs.getInt("weeks_on_chart"));
            result.put("peakPosition", rs.getInt("peak_position"));
            result.put("weeksAtPeak", rs.getInt("weeks_at_peak"));
            return result;
        }, songId, songId);

        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<String> getAvailableChartDates() {
        if (!tableExists()) {
            return Collections.emptyList();
        }
        return jdbcTemplate.queryForList(
            "SELECT DISTINCT chart_date FROM billboard_hot100_entry ORDER BY chart_date ASC",
            String.class
        );
    }

    public List<Map<String, Object>> getCountdownForDate(String chartDate) {
        if (chartDate == null || !tableExists()) {
            return Collections.emptyList();
        }

        String sql =
            "WITH prev_date AS ( " +
            "    SELECT MAX(chart_date) AS pd " +
            "    FROM billboard_hot100_entry " +
            "    WHERE chart_date < ? " +
            "), " +
            "entry_scope AS ( " +
            "    SELECT e.*, " +
            "           CASE " +
            "               WHEN e.song_id IS NOT NULL THEN 'song:' || e.song_id " +
            "               ELSE 'raw:' || LOWER(TRIM(e.artist_name)) || '||' || LOWER(TRIM(e.song_title)) " +
            "           END AS identity_key " +
            "    FROM billboard_hot100_entry e " +
            "), " +
            "song_stats AS ( " +
            "    SELECT es.identity_key, " +
            "           COUNT(DISTINCT es.chart_date) AS weeks_on_chart, " +
            "           MIN(es.position) AS peak_pos, " +
            "           MIN(es.chart_date) AS first_appearance " +
            "    FROM entry_scope es " +
            "    WHERE es.chart_date <= ? " +
            "    GROUP BY es.identity_key " +
            "), " +
            "peak_weeks AS ( " +
            "    SELECT es.identity_key, COUNT(DISTINCT es.chart_date) AS weeks_at_peak " +
            "    FROM entry_scope es " +
            "    JOIN song_stats ss ON ss.identity_key = es.identity_key AND es.position = ss.peak_pos " +
            "    WHERE es.chart_date <= ? " +
            "    GROUP BY es.identity_key " +
            "), " +
            "prev_pos AS ( " +
            "    SELECT es.identity_key, es.position AS prev_position " +
            "    FROM entry_scope es " +
            "    JOIN prev_date pd ON es.chart_date = pd.pd " +
            ") " +
            "SELECT es.position, COALESCE(s.name, es.song_title) AS song_title, COALESCE(a.name, es.artist_name) AS artist_name, " +
            "       es.song_id, a.id AS artist_id, LOWER(g.name) AS gender_name, " +
            "       ss.weeks_on_chart, ss.peak_pos, pw.weeks_at_peak, pp.prev_position, ss.first_appearance " +
            "FROM entry_scope es " +
            "LEFT JOIN Song s ON s.id = es.song_id " +
            "LEFT JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "LEFT JOIN song_stats ss ON ss.identity_key = es.identity_key " +
            "LEFT JOIN peak_weeks pw ON pw.identity_key = es.identity_key " +
            "LEFT JOIN prev_pos pp ON pp.identity_key = es.identity_key " +
            "WHERE es.chart_date = ? " +
            "ORDER BY es.position ASC";

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            int currentPosition = rs.getInt("position");
            row.put("position", currentPosition);
            row.put("songTitle", rs.getString("song_title"));
            row.put("artistName", rs.getString("artist_name"));

            int songId = rs.getInt("song_id");
            row.put("songId", rs.wasNull() ? null : songId);

            int artistId = rs.getInt("artist_id");
            row.put("artistId", rs.wasNull() ? null : artistId);

            row.put("weeks", null);
            row.put("peak", null);
            row.put("weeksAtPeak", null);
            row.put("movement", null);
            row.put("movementClass", null);

            String genderName = rs.getString("gender_name");
            row.put("genderClass", toGenderClass(genderName));

            int weeksOnChart = rs.getInt("weeks_on_chart");
            if (!rs.wasNull()) {
                row.put("weeks", weeksOnChart);
            }

            int peakPosition = rs.getInt("peak_pos");
            if (!rs.wasNull()) {
                row.put("peak", peakPosition);
            }

            int weeksAtPeak = rs.getInt("weeks_at_peak");
            if (!rs.wasNull()) {
                row.put("weeksAtPeak", weeksAtPeak);
            }

            int prevPosition = rs.getInt("prev_position");
            boolean hasPrev = !rs.wasNull();
            String firstAppearance = rs.getString("first_appearance");
            if (hasPrev) {
                int delta = prevPosition - currentPosition;
                if (delta > 0) {
                    row.put("movement", "+" + delta);
                    row.put("movementClass", "mvmt-up");
                } else if (delta < 0) {
                    row.put("movement", String.valueOf(delta));
                    row.put("movementClass", "mvmt-down");
                } else {
                    row.put("movement", "=");
                    row.put("movementClass", "mvmt-same");
                }
            } else if (chartDate.equals(firstAppearance)) {
                row.put("movement", "NEW");
                row.put("movementClass", "mvmt-new");
            } else {
                row.put("movement", "RE");
                row.put("movementClass", "mvmt-re");
            }

            return row;
        }, chartDate, chartDate, chartDate, chartDate);
    }

    public String findClosestChartDate(String date) {
        if (date == null || date.isBlank() || !tableExists()) {
            return null;
        }
        try {
            return jdbcTemplate.queryForObject(
                "SELECT DISTINCT chart_date FROM billboard_hot100_entry " +
                "WHERE chart_date <= ? " +
                "ORDER BY chart_date DESC LIMIT 1",
                String.class,
                date
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public String getPrevChartDate(String date) {
        if (date == null || date.isBlank() || !tableExists()) {
            return null;
        }
        try {
            return jdbcTemplate.queryForObject(
                "SELECT DISTINCT chart_date FROM billboard_hot100_entry " +
                "WHERE chart_date < ? " +
                "ORDER BY chart_date DESC LIMIT 1",
                String.class,
                date
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public String getNextChartDate(String date) {
        if (date == null || date.isBlank() || !tableExists()) {
            return null;
        }
        try {
            return jdbcTemplate.queryForObject(
                "SELECT DISTINCT chart_date FROM billboard_hot100_entry " +
                "WHERE chart_date > ? " +
                "ORDER BY chart_date ASC LIMIT 1",
                String.class,
                date
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public BillboardHot100ImportSupport.NameIssueReport getNameIssueReport(int sampleLimit) {
        if (!tableExists()) {
            return new BillboardHot100ImportSupport.NameIssueReport(0, 0, List.of());
        }

        try (Connection connection = dataSource.getConnection()) {
            return BillboardHot100ImportSupport.analyzePotentialNameIssues(connection, sampleLimit);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to analyze Billboard Hot 100 name issues", e);
        }
    }

    private int countUnmatchedGroups() {
        if (!tableExists()) {
            return 0;
        }
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM (SELECT artist_name, song_title FROM billboard_hot100_entry WHERE song_id IS NULL GROUP BY artist_name, song_title)",
            Integer.class
        );
        return count == null ? 0 : count;
    }

    private int normalizeLinkedRowsToLibraryCase() {
        List<Map<String, Object>> linkedRows = jdbcTemplate.query(
            "SELECT e.id, s.name AS song_name, a.name AS artist_name " +
            "FROM billboard_hot100_entry e " +
            "JOIN Song s ON s.id = e.song_id " +
            "JOIN Artist a ON a.id = s.artist_id " +
            "WHERE e.song_id IS NOT NULL " +
            "  AND (e.artist_name <> a.name OR e.song_title <> s.name)",
            (rs, rowNum) -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("artistName", rs.getString("artist_name"));
                row.put("songTitle", rs.getString("song_name"));
                return row;
            }
        );

        int updatedRows = 0;
        for (Map<String, Object> row : linkedRows) {
            updatedRows += jdbcTemplate.update(
                "UPDATE billboard_hot100_entry SET artist_name = ?, song_title = ? WHERE id = ?",
                row.get("artistName"),
                row.get("songTitle"),
                row.get("id")
            );
        }
        return updatedRows;
    }

    private boolean tableExists() {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND lower(name) = 'billboard_hot100_entry'",
            Integer.class
        );
        return count != null && count > 0;
    }

    private String toGenderClass(String genderName) {
        if (genderName == null) {
            return null;
        }
        if (genderName.contains("female")) {
            return "gender-female";
        }
        if (genderName.contains("male")) {
            return "gender-male";
        }
        return null;
    }
}