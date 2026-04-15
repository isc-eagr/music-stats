package library.service;

import library.dto.TrlChartEntryGroupDTO;
import library.entity.TrlDebut;
import library.repository.TrlDebutRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class TrlService {

    private final TrlDebutRepository trlDebutRepository;
    private final JdbcTemplate jdbcTemplate;

    public TrlService(TrlDebutRepository trlDebutRepository, JdbcTemplate jdbcTemplate) {
        this.trlDebutRepository = trlDebutRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<TrlDebut> getAllDebuts() {
        String sql =
            "WITH chart_stats AS ( " +
            "    SELECT " +
            "        debut_id, " +
            "        MIN(chart_date)            AS debut_date, " +
            "        MAX(chart_date)            AS last_appearance_date, " +
            "        MIN(position)              AS peak_position, " +
            "        COUNT(DISTINCT chart_date) AS actual_days " +
            "    FROM trl_chart_entry " +
            "    WHERE debut_id IS NOT NULL " +
            "    GROUP BY debut_id " +
            "), " +
            "peak_days AS ( " +
            "    SELECT ce.debut_id, COUNT(*) AS days_at_peak " +
            "    FROM trl_chart_entry ce " +
            "    JOIN chart_stats cs ON cs.debut_id = ce.debut_id AND ce.position = cs.peak_position " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "debut_pos AS ( " +
            "    SELECT ce.debut_id, MIN(ce.position) AS debut_position " +
            "    FROM trl_chart_entry ce " +
            "    JOIN chart_stats cs ON cs.debut_id = ce.debut_id AND ce.chart_date = cs.debut_date " +
            "    GROUP BY ce.debut_id " +
            ") " +
            "SELECT t.id, t.days_on_countdown, t.song_title, t.artist_name, t.song_id, t.retired, " +
            "       cs.debut_date, dp.debut_position, " +
            "       cs.peak_position, pd.days_at_peak, cs.last_appearance_date, cs.actual_days, " +
            "       a.id AS resolved_artist_id, " +
            "       LOWER(g.name) AS gender_name " +
            "FROM trl_debut t " +
            "LEFT JOIN chart_stats cs ON cs.debut_id = t.id " +
            "LEFT JOIN peak_days pd    ON pd.debut_id  = t.id " +
            "LEFT JOIN debut_pos dp    ON dp.debut_id  = t.id " +
            "LEFT JOIN Song s ON s.id = t.song_id " +
            "LEFT JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "ORDER BY cs.debut_date ASC, dp.debut_position ASC";

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            TrlDebut d = new TrlDebut();
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
                "SELECT COUNT(*) FROM trl_debut", Integer.class);
        Integer matched = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM trl_debut WHERE song_id IS NOT NULL", Integer.class);
        return Map.of(
                "total", total != null ? total : 0,
                "matched", matched != null ? matched : 0,
                "unmatched", (total != null ? total : 0) - (matched != null ? matched : 0)
        );
    }

    public Integer getDaysOnTrlBySongId(Integer songId) {
        if (songId == null) return null;
        try {
            return jdbcTemplate.queryForObject(
                "SELECT days_on_countdown FROM trl_debut WHERE song_id = ? LIMIT 1",
                Integer.class, songId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public Map<String, Object> getTrlStatsBySongId(Integer songId) {
        if (songId == null) return null;
        String sql =
            "WITH cs AS ( " +
            "    SELECT MIN(position) AS peak_position, COUNT(DISTINCT chart_date) AS actual_days " +
            "    FROM trl_chart_entry ce " +
            "    JOIN trl_debut t ON ce.debut_id = t.id AND t.song_id = ? " +
            ") " +
            "SELECT t.days_on_countdown, cs.peak_position, cs.actual_days, " +
            "    (SELECT COUNT(*) FROM trl_chart_entry ce2 " +
            "     JOIN trl_debut t2 ON ce2.debut_id = t2.id AND t2.song_id = ? " +
            "     WHERE ce2.position = cs.peak_position " +
            "    ) AS days_at_peak " +
            "FROM trl_debut t, cs " +
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
            // No chart data yet — fall back to just the stored days
            try {
                Integer days = jdbcTemplate.queryForObject(
                    "SELECT days_on_countdown FROM trl_debut WHERE song_id = ? LIMIT 1",
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

    /** Search songs by title/artist for the match modal. */
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

    /** Link a TRL debut entry to a song in the library and normalize names. */
    public Map<String, Object> matchSong(Integer trlId, Integer songId) {
        // Fetch canonical names from the library
        Map<String, Object> lib = jdbcTemplate.queryForMap(
            "SELECT s.name AS song_name, a.name AS artist_name " +
            "FROM Song s JOIN Artist a ON a.id = s.artist_id WHERE s.id = ?", songId);
        String canonArtist = (String) lib.get("artist_name");
        String canonSong   = (String) lib.get("song_name");

        // Update chart entries linked to this debut to use canonical names
        jdbcTemplate.update(
            "UPDATE trl_chart_entry SET artist_name = ?, song_title = ? WHERE debut_id = ?",
            canonArtist, canonSong, trlId);

        // Update debut: canonical names + song_id
        jdbcTemplate.update(
            "UPDATE trl_debut SET song_id = ?, artist_name = ?, song_title = ? WHERE id = ?",
            songId, canonArtist, canonSong, trlId);

        return Map.of("ok", true, "canonArtist", canonArtist, "canonSong", canonSong);
    }

    /** Retroactively normalize all already-linked trl_debut rows and their chart entries. */
    public int retroactiveNormalizeLinked() {
        String sql =
            "SELECT t.id, s.name AS canon_song, a.name AS canon_artist " +
            "FROM trl_debut t " +
            "JOIN Song s ON s.id = t.song_id " +
            "JOIN Artist a ON a.id = s.artist_id " +
            "WHERE t.song_id IS NOT NULL";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        int updated = 0;
        for (Map<String, Object> row : rows) {
            String canonArtist = (String) row.get("canon_artist");
            String canonSong   = (String) row.get("canon_song");
            Integer id = ((Number) row.get("id")).intValue();

            jdbcTemplate.update(
                "UPDATE trl_chart_entry SET artist_name = ?, song_title = ? WHERE debut_id = ?",
                canonArtist, canonSong, id);

            jdbcTemplate.update(
                "UPDATE trl_debut SET artist_name = ?, song_title = ? WHERE id = ?",
                canonArtist, canonSong, id);

            updated++;
        }
        return updated;
    }

    /** Remove the song link from a TRL debut entry. */
    public void unmatchSong(Integer trlId) {
        jdbcTemplate.update("UPDATE trl_debut SET song_id = NULL WHERE id = ?", trlId);
    }

    /** Get all distinct (artist, song) combos from trl_chart_entry with counts. */
    public List<TrlChartEntryGroupDTO> getDistinctChartEntries() {
        String sql =
            "SELECT artist_name, song_title, COUNT(*) AS appearances " +
            "FROM trl_chart_entry " +
            "GROUP BY artist_name, song_title " +
            "ORDER BY artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
        return jdbcTemplate.query(sql, (rs, rowNum) ->
            new TrlChartEntryGroupDTO(
                rs.getString("artist_name"),
                rs.getString("song_title"),
                rs.getInt("appearances")
            )
        );
    }

    /** Search distinct chart entries for the chart-link modal. */
    public List<TrlChartEntryGroupDTO> searchChartEntries(String q, int limit) {
        String like = "%" + q.toLowerCase() + "%";
        String sql =
            "SELECT artist_name, song_title, COUNT(*) AS appearances " +
            "FROM trl_chart_entry " +
            "WHERE LOWER(artist_name) LIKE ? OR LOWER(song_title) LIKE ? " +
            "GROUP BY artist_name, song_title " +
            "ORDER BY appearances DESC " +
            "LIMIT ?";
        return jdbcTemplate.query(sql, (rs, rowNum) ->
            new TrlChartEntryGroupDTO(
                rs.getString("artist_name"),
                rs.getString("song_title"),
                rs.getInt("appearances")
            ),
            like, like, limit
        );
    }

    /** Merge chart entries: update all rows matching target to use source values. */
    public int mergeChartEntries(String sourceArtist, String sourceSong,
                                 String targetArtist, String targetSong) {
        // Rename target entries to source names
        int updated = jdbcTemplate.update(
            "UPDATE trl_chart_entry SET artist_name = ?, song_title = ? " +
            "WHERE artist_name = ? AND song_title = ?",
            sourceArtist, sourceSong, targetArtist, targetSong
        );
        // Propagate debut_id from source to merged entries that lost their FK link
        jdbcTemplate.update(
            "UPDATE trl_chart_entry " +
            "SET debut_id = (SELECT debut_id FROM trl_chart_entry " +
            "                WHERE artist_name = ? AND song_title = ? AND debut_id IS NOT NULL LIMIT 1) " +
            "WHERE artist_name = ? AND song_title = ? AND debut_id IS NULL",
            sourceArtist, sourceSong, sourceArtist, sourceSong
        );
        return updated;
    }

    /** Link a TRL debut to a specific chart entry combo by setting debut_id on those rows. */
    public void linkChart(Integer trlId, String chartArtist, String chartSong) {
        jdbcTemplate.update(
            "UPDATE trl_chart_entry SET debut_id = ? " +
            "WHERE LOWER(TRIM(artist_name)) = LOWER(TRIM(?)) AND LOWER(TRIM(song_title)) = LOWER(TRIM(?))",
            trlId, chartArtist, chartSong
        );
    }

    /** Remove the chart link from a TRL debut by clearing debut_id on its chart entries. */
    public void unlinkChart(Integer trlId) {
        jdbcTemplate.update(
            "UPDATE trl_chart_entry SET debut_id = NULL WHERE debut_id = ?",
            trlId
        );
    }

    /**
     * Auto-link all trl_debut rows that have an exact case-insensitive match in trl_chart_entry
     * AND whose chart_artist_name is still NULL (not yet explicitly linked).
     * Returns the number of debuts linked.
     */
    public int autoLinkExactMatches() {
        String sql =
            "UPDATE trl_chart_entry " +
            "SET debut_id = ( " +
            "    SELECT t.id FROM trl_debut t " +
            "    WHERE LOWER(TRIM(t.artist_name)) = LOWER(TRIM(trl_chart_entry.artist_name)) " +
            "      AND LOWER(TRIM(t.song_title))  = LOWER(TRIM(trl_chart_entry.song_title)) " +
            "    LIMIT 1 " +
            ") " +
            "WHERE debut_id IS NULL " +
            "  AND EXISTS ( " +
            "    SELECT 1 FROM trl_debut t " +
            "    WHERE LOWER(TRIM(t.artist_name)) = LOWER(TRIM(trl_chart_entry.artist_name)) " +
            "      AND LOWER(TRIM(t.song_title))  = LOWER(TRIM(trl_chart_entry.song_title)) " +
            ")";
        return jdbcTemplate.update(sql);
    }

    /** Get all distinct chart_dates available for the recap date picker. */
    public List<String> getAvailableChartDates() {
        return jdbcTemplate.queryForList(
            "SELECT DISTINCT chart_date FROM trl_chart_entry ORDER BY chart_date ASC",
            String.class
        );
    }

    /** Get the countdown entries for a specific chart date. */
    public List<Map<String, Object>> getCountdownForDate(String chartDate) {
        String sql =
            "WITH prev_date AS ( " +
            "    SELECT MAX(chart_date) AS pd " +
            "    FROM trl_chart_entry " +
            "    WHERE chart_date < ? " +
            "), " +
            "song_stats AS ( " +
            "    SELECT ce.debut_id, " +
            "           COUNT(*) AS days_on_chart, " +
            "           MIN(ce.position) AS peak_pos, " +
            "           MIN(ce.chart_date) AS first_appearance " +
            "    FROM trl_chart_entry ce " +
            "    WHERE ce.debut_id IS NOT NULL " +
            "      AND ce.chart_date <= ? " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "peak_days AS ( " +
            "    SELECT ce.debut_id, COUNT(*) AS dap " +
            "    FROM trl_chart_entry ce " +
            "    JOIN song_stats ss ON ss.debut_id = ce.debut_id AND ce.position = ss.peak_pos " +
            "    WHERE ce.chart_date <= ? " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "prev_pos AS ( " +
            "    SELECT ce.debut_id, ce.position AS pp " +
            "    FROM trl_chart_entry ce " +
            "    JOIN prev_date pd ON ce.chart_date = pd.pd " +
            "    WHERE ce.debut_id IS NOT NULL " +
            ") " +
            "SELECT ce.position, ce.artist_name, ce.song_title, " +
            "       t.id AS debut_id, t.song_id, t.retired, " +
            "       a.id AS artist_id, LOWER(g.name) AS gender_name, " +
            "       ss.days_on_chart, ss.peak_pos, pd.dap AS days_at_peak, " +
            "       pp.pp AS prev_position, ss.first_appearance " +
            "FROM trl_chart_entry ce " +
            "LEFT JOIN trl_debut t ON t.id = ce.debut_id " +
            "LEFT JOIN Song s ON s.id = t.song_id " +
            "LEFT JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "LEFT JOIN song_stats ss ON ss.debut_id = ce.debut_id " +
            "LEFT JOIN peak_days pd ON pd.debut_id = ce.debut_id " +
            "LEFT JOIN prev_pos pp ON pp.debut_id = ce.debut_id " +
            "WHERE ce.chart_date = ? " +
            "ORDER BY ce.position DESC";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            int currentPos = rs.getInt("position");
            row.put("position", currentPos);
            row.put("artistName", rs.getString("artist_name"));
            row.put("songTitle", rs.getString("song_title"));
            int debutId = rs.getInt("debut_id");
            boolean hasDebut = !rs.wasNull();
            row.put("debutId", hasDebut ? debutId : null);
            int songId = rs.getInt("song_id");
            if (!rs.wasNull()) {
                row.put("songId", songId);
            } else {
                row.put("songId", null);
            }
            row.put("retired", rs.getInt("retired") == 1);
            int artistId = rs.getInt("artist_id");
            if (!rs.wasNull()) {
                row.put("artistId", artistId);
            } else {
                row.put("artistId", null);
            }
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
            if (hasDebut) {
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

    /**
     * Find the closest available chart date on or before the given date.
     * Returns null if no chart date exists before/on the given date.
     */
    public String findClosestChartDate(String date) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT chart_date FROM trl_chart_entry " +
                "WHERE chart_date <= ? " +
                "ORDER BY chart_date DESC LIMIT 1",
                String.class, date);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    /** Get the previous available chart date strictly before the given date. */
    public String getPrevChartDate(String date) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT chart_date FROM trl_chart_entry " +
                "WHERE chart_date < ? " +
                "ORDER BY chart_date DESC LIMIT 1",
                String.class, date);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    /** Get the next available chart date strictly after the given date. */
    public String getNextChartDate(String date) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT chart_date FROM trl_chart_entry " +
                "WHERE chart_date > ? " +
                "ORDER BY chart_date ASC LIMIT 1",
                String.class, date);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }
}
