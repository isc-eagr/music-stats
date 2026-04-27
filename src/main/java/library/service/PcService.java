package library.service;

import library.dto.PcOverviewRowDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class PcService {

    private final JdbcTemplate jdbcTemplate;

    public PcService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String, Object> getSummary() {
        String sql =
            "WITH matched_groups AS ( " +
            "    SELECT song_id " +
            "    FROM pc_countdown_entry " +
            "    WHERE is_close_call = 0 AND song_id IS NOT NULL " +
            "    GROUP BY song_id " +
            "), unmatched_groups AS ( " +
            "    SELECT artist_name, song_title " +
            "    FROM pc_countdown_entry " +
            "    WHERE is_close_call = 0 AND song_id IS NULL " +
            "    GROUP BY artist_name, song_title " +
            ") " +
            "SELECT (SELECT COUNT(*) FROM matched_groups) AS matched, " +
            "       (SELECT COUNT(*) FROM unmatched_groups) AS unmatched";
        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
            int matched = rs.getInt("matched");
            int unmatched = rs.getInt("unmatched");
            return Map.of(
                "total", matched + unmatched,
                "matched", matched,
                "unmatched", unmatched
            );
        });
    }

    public Map<String, Object> getPcStatsBySongId(Integer songId) {
        if (songId == null) return null;
        String sql =
            "WITH cs AS ( " +
            "    SELECT MIN(ce.position) AS peak_position, COUNT(DISTINCT ce.chart_date) AS actual_days " +
            "    FROM pc_countdown_entry ce " +
            "    WHERE ce.song_id = ? AND ce.is_close_call = 0 " +
            ") " +
            "SELECT cs.actual_days AS days_on_countdown, cs.peak_position, cs.actual_days, " +
            "    (SELECT COUNT(DISTINCT ce2.chart_date) FROM pc_countdown_entry ce2 " +
            "     WHERE ce2.song_id = ? AND ce2.position = cs.peak_position AND ce2.is_close_call = 0 " +
            "    ) AS days_at_peak " +
            "FROM cs WHERE cs.actual_days > 0";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("daysOnCountdown", rs.getInt("days_on_countdown"));
                int peak = rs.getInt("peak_position");
                if (!rs.wasNull()) m.put("peakPosition", peak);
                int actualDays = rs.getInt("actual_days");
                if (!rs.wasNull()) m.put("actualDays", actualDays);
                int daysAtPeak = rs.getInt("days_at_peak");
                if (!rs.wasNull()) m.put("daysAtPeak", daysAtPeak);
                return m;
            }, songId, songId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<PcOverviewRowDTO> getOverviewRows() {
        return jdbcTemplate.query(buildOverviewRowsSql(false), this::mapOverviewRow);
    }

    public List<PcOverviewRowDTO> getOverviewRows(int page, int size) {
        int safePage = Math.max(1, page);
        int safeSize = Math.max(1, Math.min(size, 500));
        int offset = (safePage - 1) * safeSize;

        return jdbcTemplate.query(buildOverviewRowsSql(true), this::mapOverviewRow, safeSize, offset);
    }

    private String buildOverviewRowsSql(boolean paged) {
        String sql =
            "SELECT * FROM ( " +
            "WITH matched_groups AS ( " +
            "    SELECT e.song_id, " +
            "           MIN(e.chart_date) AS first_week, " +
            "           MAX(e.chart_date) AS last_week, " +
            "           COUNT(DISTINCT e.chart_date) AS days_on_countdown, " +
            "           MIN(e.position) AS peak_position, " +
            "           COUNT(DISTINCT LOWER(TRIM(e.artist_name)) || '||' || LOWER(TRIM(e.song_title))) AS raw_variant_count " +
            "    FROM pc_countdown_entry e " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NOT NULL " +
            "    GROUP BY e.song_id " +
            "), matched_peak AS ( " +
            "    SELECT e.song_id, COUNT(DISTINCT e.chart_date) AS days_at_peak, MIN(e.chart_date) AS peak_week " +
            "    FROM pc_countdown_entry e " +
            "    JOIN matched_groups mg ON mg.song_id = e.song_id AND e.position = mg.peak_position " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NOT NULL " +
            "    GROUP BY e.song_id " +
            "), unmatched_groups AS ( " +
            "    SELECT e.artist_name, e.song_title, " +
            "           MIN(e.chart_date) AS first_week, " +
            "           MAX(e.chart_date) AS last_week, " +
            "           COUNT(DISTINCT e.chart_date) AS days_on_countdown, " +
            "           MIN(e.position) AS peak_position, " +
            "           1 AS raw_variant_count " +
            "    FROM pc_countdown_entry e " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            "), unmatched_peak AS ( " +
            "    SELECT e.artist_name, e.song_title, COUNT(DISTINCT e.chart_date) AS days_at_peak, MIN(e.chart_date) AS peak_week " +
            "    FROM pc_countdown_entry e " +
            "    JOIN unmatched_groups ug ON ug.artist_name = e.artist_name AND ug.song_title = e.song_title AND e.position = ug.peak_position " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            ") " +
            "SELECT 1 AS matched, mg.song_id, s.name AS song_title, a.name AS artist_name, " +
            "       mg.first_week, mg.last_week, mg.days_on_countdown, mg.peak_position, " +
            "       COALESCE(mp.days_at_peak, 0) AS days_at_peak, mp.peak_week, mg.raw_variant_count, " +
            "       a.id AS resolved_artist_id, LOWER(g.name) AS gender_name " +
            "FROM matched_groups mg " +
            "JOIN Song s ON s.id = mg.song_id " +
            "JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "LEFT JOIN matched_peak mp ON mp.song_id = mg.song_id " +
            "UNION ALL " +
            "SELECT 0 AS matched, NULL AS song_id, ug.song_title, ug.artist_name, " +
            "       ug.first_week, ug.last_week, ug.days_on_countdown, ug.peak_position, " +
            "       COALESCE(up.days_at_peak, 0) AS days_at_peak, up.peak_week, ug.raw_variant_count, " +
            "       NULL AS resolved_artist_id, NULL AS gender_name " +
            "FROM unmatched_groups ug " +
            "LEFT JOIN unmatched_peak up ON up.artist_name = ug.artist_name AND up.song_title = ug.song_title " +
            "ORDER BY days_on_countdown DESC, peak_position ASC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC " +
            ")";

        if (paged) {
            sql += " LIMIT ? OFFSET ?";
        }
        return sql;
    }

    private PcOverviewRowDTO mapOverviewRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        PcOverviewRowDTO row = new PcOverviewRowDTO();
        row.setMatched(rs.getInt("matched") == 1);
        int songId = rs.getInt("song_id");
        if (!rs.wasNull()) row.setSongId(songId);
        int artistId = rs.getInt("resolved_artist_id");
        if (!rs.wasNull()) row.setResolvedArtistId(artistId);
        row.setSongTitle(rs.getString("song_title"));
        row.setArtistName(rs.getString("artist_name"));
        row.setFirstWeek(rs.getString("first_week"));
        row.setLastWeek(rs.getString("last_week"));
        row.setPeakWeek(rs.getString("peak_week"));
        row.setDaysOnCountdown(rs.getInt("days_on_countdown"));
        row.setPeakPosition(rs.getInt("peak_position"));
        row.setDaysAtPeak(rs.getInt("days_at_peak"));
        row.setRawVariantCount(rs.getInt("raw_variant_count"));
        String genderName = rs.getString("gender_name");
        if (genderName != null) {
            if (genderName.contains("female")) {
                row.setGenderClass("gender-female");
            } else if (genderName.contains("male")) {
                row.setGenderClass("gender-male");
            }
        }
        return row;
    }

    public Map<String, Object> matchRawGroup(String rawArtist, String rawSong, Integer songId) {
        Map<String, Object> lib = jdbcTemplate.queryForMap(
            "SELECT s.name AS song_name, a.name AS artist_name " +
            "FROM Song s JOIN Artist a ON a.id = s.artist_id WHERE s.id = ?", songId);
        String canonArtist = (String) lib.get("artist_name");
        String canonSong = (String) lib.get("song_name");

        int updatedEntries = jdbcTemplate.update(
            "UPDATE pc_countdown_entry SET artist_name = ?, song_title = ?, song_id = ? " +
            "WHERE LOWER(TRIM(artist_name)) = LOWER(TRIM(?)) " +
            "  AND LOWER(TRIM(song_title)) = LOWER(TRIM(?))",
            canonArtist, canonSong, songId, rawArtist, rawSong
        );

        return Map.of(
            "ok", true,
            "updatedEntries", updatedEntries,
            "canonArtist", canonArtist,
            "canonSong", canonSong
        );
    }

    public int mergeEntries(String sourceArtist, String sourceSong, String targetArtist, String targetSong) {
        Integer sourceSongId = jdbcTemplate.query(
            "SELECT song_id FROM pc_countdown_entry " +
            "WHERE artist_name = ? AND song_title = ? AND song_id IS NOT NULL LIMIT 1",
            rs -> rs.next() ? rs.getInt("song_id") : null,
            sourceArtist,
            sourceSong
        );

        int updated;
        if (sourceSongId != null) {
            updated = jdbcTemplate.update(
                "UPDATE pc_countdown_entry SET artist_name = ?, song_title = ?, song_id = ? " +
                "WHERE artist_name = ? AND song_title = ?",
                sourceArtist, sourceSong, sourceSongId, targetArtist, targetSong
            );
        } else {
            updated = jdbcTemplate.update(
                "UPDATE pc_countdown_entry SET artist_name = ?, song_title = ? " +
                "WHERE artist_name = ? AND song_title = ?",
                sourceArtist, sourceSong, targetArtist, targetSong
            );
            jdbcTemplate.update(
                "UPDATE pc_countdown_entry " +
                "SET song_id = (SELECT song_id FROM pc_countdown_entry " +
                "               WHERE artist_name = ? AND song_title = ? AND song_id IS NOT NULL LIMIT 1) " +
                "WHERE artist_name = ? AND song_title = ? AND song_id IS NULL",
                sourceArtist, sourceSong, sourceArtist, sourceSong
            );
        }
        return updated;
    }

    public int normalizeCaseDifferences() {
        int updatedRows = normalizeLinkedRowsToLibraryCase();

        List<Map<String, Object>> variantGroups = jdbcTemplate.query(
            "SELECT LOWER(TRIM(artist_name)) AS artist_key, LOWER(TRIM(song_title)) AS song_key " +
            "FROM pc_countdown_entry " +
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
                "FROM pc_countdown_entry " +
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
                "UPDATE pc_countdown_entry SET artist_name = ?, song_title = ? " +
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

    private int normalizeLinkedRowsToLibraryCase() {
        List<Map<String, Object>> linkedRows = jdbcTemplate.query(
            "SELECT e.id, s.name AS song_name, a.name AS artist_name " +
            "FROM pc_countdown_entry e " +
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
                "UPDATE pc_countdown_entry SET artist_name = ?, song_title = ? WHERE id = ?",
                row.get("artistName"),
                row.get("songTitle"),
                row.get("id")
            );
        }
        return updatedRows;
    }

    public List<Map<String, Object>> searchSongs(String q, int limit) {
        String like = "%" + q.toLowerCase() + "%";
        String sql =
            "SELECT s.id, s.name AS title, a.name AS artist_name " +
            "FROM Song s " +
            "JOIN Artist a ON a.id = s.artist_id " +
            "WHERE LOWER(s.name) LIKE ? OR LOWER(a.name) LIKE ? " +
            "ORDER BY s.name COLLATE NOCASE ASC " +
            "LIMIT ?";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("title", rs.getString("title"));
            row.put("artistName", rs.getString("artist_name"));
            return row;
        }, like, like, limit);
    }

    /**
     * Auto-match unlinked rows to songs in the library
     * using exact case-insensitive artist + title comparison.
     * Returns number of rows linked.
     */
    public int autoLinkExactMatches() {
        String sql =
            "UPDATE pc_countdown_entry " +
            "SET song_id = ( " +
            "        SELECT s.id FROM Song s " +
            "        JOIN Artist a ON a.id = s.artist_id " +
            "        WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(pc_countdown_entry.artist_name)) " +
            "          AND LOWER(TRIM(s.name)) = LOWER(TRIM(pc_countdown_entry.song_title)) " +
            "        LIMIT 1 " +
            "    ), " +
            "    artist_name = COALESCE(( " +
            "        SELECT a.name FROM Song s " +
            "        JOIN Artist a ON a.id = s.artist_id " +
            "        WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(pc_countdown_entry.artist_name)) " +
            "          AND LOWER(TRIM(s.name)) = LOWER(TRIM(pc_countdown_entry.song_title)) " +
            "        LIMIT 1 " +
            "    ), artist_name), " +
            "    song_title = COALESCE(( " +
            "        SELECT s.name FROM Song s " +
            "        JOIN Artist a ON a.id = s.artist_id " +
            "        WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(pc_countdown_entry.artist_name)) " +
            "          AND LOWER(TRIM(s.name)) = LOWER(TRIM(pc_countdown_entry.song_title)) " +
            "        LIMIT 1 " +
            "    ), song_title) " +
            "WHERE song_id IS NULL " +
            "  AND 1 = ( " +
            "    SELECT COUNT(*) FROM Song s " +
            "    JOIN Artist a ON a.id = s.artist_id " +
            "    WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(pc_countdown_entry.artist_name)) " +
            "      AND LOWER(TRIM(s.name)) = LOWER(TRIM(pc_countdown_entry.song_title)) " +
            "  )";
        return jdbcTemplate.update(sql);
    }

    public List<String> getAvailableChartDates() {
        return jdbcTemplate.queryForList(
            "SELECT DISTINCT chart_date FROM pc_countdown_entry ORDER BY chart_date ASC",
            String.class
        );
    }

    public List<Map<String, Object>> getCountdownForDate(String chartDate) {
        String sql =
            "WITH prev_date AS ( " +
            "    SELECT MAX(chart_date) AS pd " +
            "    FROM pc_countdown_entry " +
            "    WHERE chart_date < ? AND is_close_call = 0 " +
            "), " +
            "entry_scope AS ( " +
            "    SELECT ce.*, " +
            "           CASE " +
            "               WHEN ce.song_id IS NOT NULL THEN 'song:' || ce.song_id " +
            "               ELSE 'raw:' || LOWER(TRIM(ce.artist_name)) || '||' || LOWER(TRIM(ce.song_title)) " +
            "           END AS identity_key " +
            "    FROM pc_countdown_entry ce " +
            "), " +
            "song_stats AS ( " +
            "    SELECT es.identity_key, " +
            "           COUNT(DISTINCT es.chart_date) AS days_on_chart, " +
            "           MIN(es.position) AS peak_pos, " +
            "           MIN(es.chart_date) AS first_appearance " +
            "    FROM entry_scope es " +
            "    WHERE es.chart_date <= ? " +
            "      AND es.is_close_call = 0 " +
            "    GROUP BY es.identity_key " +
            "), " +
            "peak_days AS ( " +
            "    SELECT es.identity_key, COUNT(DISTINCT es.chart_date) AS dap " +
            "    FROM entry_scope es " +
            "    JOIN song_stats ss ON ss.identity_key = es.identity_key AND es.position = ss.peak_pos " +
            "    WHERE es.chart_date <= ? AND es.is_close_call = 0 " +
            "    GROUP BY es.identity_key " +
            "), " +
            "prev_pos AS ( " +
            "    SELECT es.identity_key, es.position AS pp " +
            "    FROM entry_scope es " +
            "    JOIN prev_date pd ON es.chart_date = pd.pd " +
            "    WHERE es.is_close_call = 0 " +
            ") " +
            "SELECT es.position, es.artist_name, es.song_title, es.is_close_call, es.song_id, " +
            "       a.id AS artist_id, LOWER(g.name) AS gender_name, " +
            "       ss.days_on_chart, ss.peak_pos, pd.dap AS days_at_peak, pp.pp AS prev_position, ss.first_appearance " +
            "FROM entry_scope es " +
            "LEFT JOIN Song s ON s.id = es.song_id " +
            "LEFT JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "LEFT JOIN song_stats ss ON ss.identity_key = es.identity_key " +
            "LEFT JOIN peak_days pd ON pd.identity_key = es.identity_key " +
            "LEFT JOIN prev_pos pp ON pp.identity_key = es.identity_key " +
            "WHERE es.chart_date = ? " +
            "ORDER BY es.is_close_call ASC, es.position DESC";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            int currentPos = rs.getInt("position");
            row.put("position", currentPos);
            row.put("artistName", rs.getString("artist_name"));
            row.put("songTitle", rs.getString("song_title"));
            boolean isClosecall = rs.getInt("is_close_call") == 1;
            row.put("isClosecall", isClosecall);
            int songId = rs.getInt("song_id");
            boolean hasIdentity = !rs.wasNull();
            row.put("songId", hasIdentity ? songId : null);
            int artistId = rs.getInt("artist_id");
            row.put("artistId", rs.wasNull() ? null : artistId);
            String gn = rs.getString("gender_name");
            String genderClass = null;
            if (gn != null) {
                if (gn.contains("female")) genderClass = "gender-female";
                else if (gn.contains("male")) genderClass = "gender-male";
            }
            row.put("genderClass", genderClass);
            row.put("days", null);
            row.put("peak", null);
            row.put("daysAtPeak", null);
            row.put("movement", null);
            row.put("movementClass", null);

            if (!isClosecall) {
                int daysOnChart = rs.getInt("days_on_chart");
                if (!rs.wasNull()) row.put("days", daysOnChart);
                int peakPos = rs.getInt("peak_pos");
                if (!rs.wasNull()) row.put("peak", peakPos);
                int dap = rs.getInt("days_at_peak");
                if (!rs.wasNull()) row.put("daysAtPeak", dap);

                int prevPosition = rs.getInt("prev_position");
                boolean hasPrev = !rs.wasNull();
                String firstApp = rs.getString("first_appearance");
                if (hasPrev) {
                    int delta = prevPosition - currentPos;
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
                } else if (chartDate.equals(firstApp)) {
                    row.put("movement", "NEW");
                    row.put("movementClass", "mvmt-new");
                } else {
                    row.put("movement", "RE");
                    row.put("movementClass", "mvmt-re");
                }
            }
            return row;
        }, chartDate, chartDate, chartDate, chartDate);
    }

    public String findClosestChartDate(String date) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT DISTINCT chart_date FROM pc_countdown_entry " +
                "WHERE chart_date <= ? " +
                "ORDER BY chart_date DESC LIMIT 1",
                String.class, date);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public String getPrevChartDate(String date) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT DISTINCT chart_date FROM pc_countdown_entry " +
                "WHERE chart_date < ? " +
                "ORDER BY chart_date DESC LIMIT 1",
                String.class, date);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public String getNextChartDate(String date) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT DISTINCT chart_date FROM pc_countdown_entry " +
                "WHERE chart_date > ? " +
                "ORDER BY chart_date ASC LIMIT 1",
                String.class, date);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }
}
