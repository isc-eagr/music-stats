package library.service;

import library.entity.PcDebut;
import library.repository.PcDebutRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class PcService {

    private final PcDebutRepository pcDebutRepository;
    private final JdbcTemplate jdbcTemplate;

    public PcService(PcDebutRepository pcDebutRepository, JdbcTemplate jdbcTemplate) {
        this.pcDebutRepository = pcDebutRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<PcDebut> getAllDebuts() {
        String sql =
            "WITH chart_stats AS ( " +
            "    SELECT " +
            "        debut_id, " +
            "        MIN(chart_date)            AS debut_date, " +
            "        MAX(chart_date)            AS last_appearance_date, " +
            "        MIN(position)              AS peak_position, " +
            "        COUNT(DISTINCT chart_date) AS actual_days " +
            "    FROM pc_countdown_entry " +
            "    WHERE debut_id IS NOT NULL AND is_close_call = 0 " +
            "    GROUP BY debut_id " +
            "), " +
            "peak_days AS ( " +
            "    SELECT ce.debut_id, COUNT(*) AS days_at_peak " +
            "    FROM pc_countdown_entry ce " +
            "    JOIN chart_stats cs ON cs.debut_id = ce.debut_id AND ce.position = cs.peak_position " +
            "    WHERE ce.is_close_call = 0 " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "debut_pos AS ( " +
            "    SELECT ce.debut_id, MIN(ce.position) AS debut_position " +
            "    FROM pc_countdown_entry ce " +
            "    JOIN chart_stats cs ON cs.debut_id = ce.debut_id AND ce.chart_date = cs.debut_date " +
            "    WHERE ce.is_close_call = 0 " +
            "    GROUP BY ce.debut_id " +
            ") " +
            "SELECT t.id, t.days_on_countdown, t.song_title, t.artist_name, t.song_id, t.retired, " +
            "       cs.debut_date, dp.debut_position, " +
            "       cs.peak_position, pd.days_at_peak, cs.last_appearance_date, cs.actual_days, " +
            "       a.id AS resolved_artist_id, " +
            "       LOWER(g.name) AS gender_name " +
            "FROM pc_debut t " +
            "LEFT JOIN chart_stats cs ON cs.debut_id = t.id " +
            "LEFT JOIN peak_days pd    ON pd.debut_id  = t.id " +
            "LEFT JOIN debut_pos dp    ON dp.debut_id  = t.id " +
            "LEFT JOIN Song s ON s.id = t.song_id " +
            "LEFT JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "WHERE (t.days_on_countdown > 0 OR cs.debut_date IS NOT NULL) " +
            "ORDER BY cs.debut_date ASC, dp.debut_position ASC";

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PcDebut d = new PcDebut();
            d.setId(rs.getInt("id"));
            d.setDaysOnCountdown(rs.getInt("days_on_countdown"));
            d.setDebutDate(rs.getString("debut_date"));
            int debutPos = rs.getInt("debut_position");
            if (!rs.wasNull()) d.setDebutPosition(debutPos);
            d.setSongTitle(rs.getString("song_title"));
            d.setArtistName(rs.getString("artist_name"));
            int songId = rs.getInt("song_id");
            if (!rs.wasNull()) d.setSongId(songId);
            d.setRetired(rs.getInt("retired") == 1);
            int peak = rs.getInt("peak_position");
            if (!rs.wasNull()) d.setPeakPosition(peak);
            int dap = rs.getInt("days_at_peak");
            if (!rs.wasNull()) d.setDaysAtPeak(dap);
            d.setLastAppearanceDate(rs.getString("last_appearance_date"));
            int actualDays = rs.getInt("actual_days");
            if (!rs.wasNull()) d.setActualDays(actualDays);
            int artistId = rs.getInt("resolved_artist_id");
            if (!rs.wasNull()) d.setResolvedArtistId(artistId);
            String genderName = rs.getString("gender_name");
            if (genderName != null) {
                if (genderName.contains("female")) {
                    d.setGenderClass("gender-female");
                } else if (genderName.contains("male")) {
                    d.setGenderClass("gender-male");
                }
            }
            return d;
        });
    }

    public Map<String, Object> getSummary() {
        Integer total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pc_debut", Integer.class);
        Integer matched = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pc_debut WHERE song_id IS NOT NULL", Integer.class);
        return Map.of(
                "total", total != null ? total : 0,
                "matched", matched != null ? matched : 0,
                "unmatched", (total != null ? total : 0) - (matched != null ? matched : 0)
        );
    }

    public Map<String, Object> getPcStatsBySongId(Integer songId) {
        if (songId == null) return null;
        String sql =
            "WITH cs AS ( " +
            "    SELECT MIN(position) AS peak_position, COUNT(DISTINCT chart_date) AS actual_days " +
            "    FROM pc_countdown_entry ce " +
            "    JOIN pc_debut t ON ce.debut_id = t.id AND t.song_id = ? " +
            "    WHERE ce.is_close_call = 0 " +
            ") " +
            "SELECT t.days_on_countdown, cs.peak_position, cs.actual_days, " +
            "    (SELECT COUNT(*) FROM pc_countdown_entry ce2 " +
            "     JOIN pc_debut t2 ON ce2.debut_id = t2.id AND t2.song_id = ? " +
            "     WHERE ce2.position = cs.peak_position AND ce2.is_close_call = 0 " +
            "    ) AS days_at_peak " +
            "FROM pc_debut t, cs " +
            "WHERE t.song_id = ? " +
            "LIMIT 1";
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
            }, songId, songId, songId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            try {
                Integer days = jdbcTemplate.queryForObject(
                    "SELECT days_on_countdown FROM pc_debut WHERE song_id = ? LIMIT 1",
                    Integer.class, songId);
                if (days == null) return null;
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("daysOnCountdown", days);
                return m;
            } catch (org.springframework.dao.EmptyResultDataAccessException e2) {
                return null;
            }
        }
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

    public Map<String, Object> matchSong(Integer pcId, Integer songId) {
        Map<String, Object> lib = jdbcTemplate.queryForMap(
            "SELECT s.name AS song_name, a.name AS artist_name " +
            "FROM Song s JOIN Artist a ON a.id = s.artist_id WHERE s.id = ?", songId);
        String canonArtist = (String) lib.get("artist_name");
        String canonSong   = (String) lib.get("song_name");

        jdbcTemplate.update(
            "UPDATE pc_countdown_entry SET artist_name = ?, song_title = ? WHERE debut_id = ?",
            canonArtist, canonSong, pcId);

        jdbcTemplate.update(
            "UPDATE pc_debut SET song_id = ?, artist_name = ?, song_title = ? WHERE id = ?",
            songId, canonArtist, canonSong, pcId);

        return Map.of("ok", true, "canonArtist", canonArtist, "canonSong", canonSong);
    }

    public void unmatchSong(Integer pcId) {
        jdbcTemplate.update("UPDATE pc_debut SET song_id = NULL WHERE id = ?", pcId);
    }

    /**
     * Merge target pc_debut INTO source: reassign all countdown entries, delete target,
     * then recalculate days_on_countdown for the source.
     */
    public void mergeDebuts(int sourceId, int targetId) {
        Map<String, Object> src = jdbcTemplate.queryForMap(
            "SELECT artist_name, song_title FROM pc_debut WHERE id = ?", sourceId);
        String srcArtist = (String) src.get("artist_name");
        String srcTitle  = (String) src.get("song_title");

        // Reassign all countdown entries from target to source, normalizing names
        jdbcTemplate.update(
            "UPDATE pc_countdown_entry SET debut_id = ?, artist_name = ?, song_title = ? " +
            "WHERE debut_id = ?",
            sourceId, srcArtist, srcTitle, targetId);

        // Delete the merged (target) debut
        jdbcTemplate.update("DELETE FROM pc_debut WHERE id = ?", targetId);

        // Recalculate days_on_countdown for the source (excluding close calls)
        jdbcTemplate.update(
            "UPDATE pc_debut SET days_on_countdown = " +
            "(SELECT COUNT(DISTINCT chart_date) FROM pc_countdown_entry " +
            " WHERE debut_id = ? AND is_close_call = 0) " +
            "WHERE id = ?",
            sourceId, sourceId);
    }

    /**
     * Auto-match unlinked pc_debut rows to songs in the library
     * using exact case-insensitive artist + title comparison.
     * Returns number of debuts linked.
     */
    public int autoLinkExactMatches() {
        String sql =
            "UPDATE pc_debut " +
            "SET song_id = ( " +
            "    SELECT s.id FROM Song s " +
            "    JOIN Artist a ON a.id = s.artist_id " +
            "    WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(pc_debut.artist_name)) " +
            "      AND LOWER(TRIM(s.name)) = LOWER(TRIM(pc_debut.song_title)) " +
            "    LIMIT 1 " +
            ") " +
            "WHERE song_id IS NULL " +
            "  AND EXISTS ( " +
            "    SELECT 1 FROM Song s " +
            "    JOIN Artist a ON a.id = s.artist_id " +
            "    WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(pc_debut.artist_name)) " +
            "      AND LOWER(TRIM(s.name)) = LOWER(TRIM(pc_debut.song_title)) " +
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
            "song_stats AS ( " +
            "    SELECT ce.debut_id, " +
            "           COUNT(*) AS days_on_chart, " +
            "           MIN(ce.position) AS peak_pos, " +
            "           MIN(ce.chart_date) AS first_appearance " +
            "    FROM pc_countdown_entry ce " +
            "    WHERE ce.debut_id IS NOT NULL " +
            "      AND ce.chart_date <= ? " +
            "      AND ce.is_close_call = 0 " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "peak_days AS ( " +
            "    SELECT ce.debut_id, COUNT(*) AS dap " +
            "    FROM pc_countdown_entry ce " +
            "    JOIN song_stats ss ON ss.debut_id = ce.debut_id AND ce.position = ss.peak_pos " +
            "    WHERE ce.chart_date <= ? AND ce.is_close_call = 0 " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "prev_pos AS ( " +
            "    SELECT ce.debut_id, ce.position AS pp " +
            "    FROM pc_countdown_entry ce " +
            "    JOIN prev_date pd ON ce.chart_date = pd.pd " +
            "    WHERE ce.debut_id IS NOT NULL AND ce.is_close_call = 0 " +
            ") " +
            "SELECT ce.position, ce.artist_name, ce.song_title, ce.is_close_call, " +
            "       t.id AS debut_id, t.song_id, t.retired, " +
            "       a.id AS artist_id, LOWER(g.name) AS gender_name, " +
            "       ss.days_on_chart, ss.peak_pos, pd.dap AS days_at_peak, " +
            "       pp.pp AS prev_position, ss.first_appearance " +
            "FROM pc_countdown_entry ce " +
            "LEFT JOIN pc_debut t ON t.id = ce.debut_id " +
            "LEFT JOIN Song s ON s.id = t.song_id " +
            "LEFT JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "LEFT JOIN song_stats ss ON ss.debut_id = ce.debut_id " +
            "LEFT JOIN peak_days pd ON pd.debut_id = ce.debut_id " +
            "LEFT JOIN prev_pos pp ON pp.debut_id = ce.debut_id " +
            "WHERE ce.chart_date = ? " +
            "ORDER BY ce.is_close_call ASC, ce.position DESC";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            int currentPos = rs.getInt("position");
            row.put("position", currentPos);
            row.put("artistName", rs.getString("artist_name"));
            row.put("songTitle", rs.getString("song_title"));
            boolean isClosecall = rs.getInt("is_close_call") == 1;
            row.put("isClosecall", isClosecall);
            int debutId = rs.getInt("debut_id");
            boolean hasDebut = !rs.wasNull();
            row.put("debutId", hasDebut ? debutId : null);
            int songId = rs.getInt("song_id");
            row.put("songId", rs.wasNull() ? null : songId);
            row.put("retired", rs.getInt("retired") == 1);
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

            if (hasDebut && !isClosecall) {
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
