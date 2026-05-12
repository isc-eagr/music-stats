package library.service;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.TrlChartEntryGroupDTO;
import library.entity.TrlDebut;
import library.repository.TrlDebutRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
            "    SELECT ce.debut_id, COUNT(*) AS days_at_peak, MIN(ce.chart_date) AS peak_date " +
            "    FROM trl_chart_entry ce " +
            "    JOIN chart_stats cs ON cs.debut_id = ce.debut_id AND ce.position = cs.peak_position " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "debut_pos AS ( " +
            "    SELECT ce.debut_id, MIN(ce.position) AS debut_position " +
            "    FROM trl_chart_entry ce " +
            "    JOIN chart_stats cs ON cs.debut_id = ce.debut_id AND ce.chart_date = cs.debut_date " +
            "    GROUP BY ce.debut_id " +
            "), " +
            "tier_stats AS ( " +
            "    SELECT debut_id, " +
            "           COUNT(DISTINCT CASE WHEN position <= 1  THEN chart_date END) AS days_at_top1, " +
            "           COUNT(DISTINCT CASE WHEN position <= 5  THEN chart_date END) AS days_at_top5, " +
            "           COUNT(DISTINCT CASE WHEN position <= 10 THEN chart_date END) AS days_at_top10 " +
            "    FROM trl_chart_entry " +
            "    WHERE debut_id IS NOT NULL " +
            "    GROUP BY debut_id " +
            ") " +
            "SELECT t.id, t.days_on_countdown, t.song_title, t.artist_name, t.song_id, t.retired, " +
            "       cs.debut_date, dp.debut_position, " +
            "       cs.peak_position, pd.days_at_peak, pd.peak_date, cs.last_appearance_date, cs.actual_days, " +
            "       a.id AS resolved_artist_id, " +
            "       LOWER(g.name) AS gender_name, " +
            "       COALESCE(ts.days_at_top1, 0)  AS days_at_top1, " +
            "       COALESCE(ts.days_at_top5, 0)  AS days_at_top5, " +
            "       COALESCE(ts.days_at_top10, 0) AS days_at_top10 " +
            "FROM trl_debut t " +
            "LEFT JOIN chart_stats cs ON cs.debut_id = t.id " +
            "LEFT JOIN peak_days pd    ON pd.debut_id  = t.id " +
            "LEFT JOIN debut_pos dp    ON dp.debut_id  = t.id " +
            "LEFT JOIN tier_stats ts   ON ts.debut_id  = t.id " +
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
            d.setPeakDate(rs.getString("peak_date"));
            d.setLastAppearanceDate(rs.getString("last_appearance_date"));
            int actualDays = rs.getInt("actual_days");
            if (!rs.wasNull()) d.setActualDays(actualDays);
            int artistId = rs.getInt("resolved_artist_id");
            if (!rs.wasNull()) d.setResolvedArtistId(artistId);
            // Map gender name to CSS class: female->pink (genderId 1), male->blue (genderId 2)
            // IMPORTANT: genderId 1 = female, genderId 2 = male (not intuitive but matches original schema)
            String genderName = rs.getString("gender_name");
            if (genderName != null) {
                if (genderName.contains("female")) {
                    d.setGenderClass("gender-female");
                } else if (genderName.contains("male")) {
                    d.setGenderClass("gender-male");
                }
            }
            d.setDaysAtTop1(rs.getInt("days_at_top1"));
            d.setDaysAtTop5(rs.getInt("days_at_top5"));
            d.setDaysAtTop10(rs.getInt("days_at_top10"));
            return d;
        });
    }

    public List<ChartAlbumOverviewRowDTO> getAlbumOverviewRows() {
        return getAlbumOverviewRows(getAllDebuts());
    }

    public List<ChartAlbumOverviewRowDTO> getAlbumOverviewRows(List<TrlDebut> debuts) {
        Map<Integer, AlbumSongInfo> albumSongInfoBySongId = getAlbumSongInfoBySongId(debuts);
        Map<Integer, AlbumOverviewAccumulator> grouped = new LinkedHashMap<>();

        for (TrlDebut debut : debuts) {
            if (debut.getSongId() == null) {
                continue;
            }
            AlbumSongInfo albumSongInfo = albumSongInfoBySongId.get(debut.getSongId());
            if (albumSongInfo == null) {
                continue;
            }

            AlbumOverviewAccumulator accumulator = grouped.computeIfAbsent(
                albumSongInfo.albumId,
                ignored -> new AlbumOverviewAccumulator(albumSongInfo.albumId, albumSongInfo.albumName, debut.getResolvedArtistId(), debut.getArtistName(), debut.getGenderClass())
            );
            accumulator.accept(debut);
        }

        List<ChartAlbumOverviewRowDTO> rows = new ArrayList<>();
        for (AlbumOverviewAccumulator accumulator : grouped.values()) {
            rows.add(accumulator.toRow());
        }
        rows.sort(Comparator
            .comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan).reversed()
            .thenComparing(row -> row.getHighestPeak() == null ? Integer.MAX_VALUE : row.getHighestPeak())
            .thenComparing(row -> safeLower(row.getArtistName()))
            .thenComparing(row -> safeLower(row.getAlbumName())));
        return rows;
    }

    public List<ChartArtistOverviewRowDTO> getArtistOverviewRows() {
        return getArtistOverviewRows(getAllDebuts());
    }

    public List<ChartArtistOverviewRowDTO> getArtistOverviewRows(List<TrlDebut> debuts) {
        Map<String, ArtistOverviewAccumulator> grouped = new LinkedHashMap<>();

        for (TrlDebut debut : debuts) {
            String key;
            boolean matched = debut.getResolvedArtistId() != null;
            if (matched) {
                key = "artist:" + debut.getResolvedArtistId();
            } else {
                key = "raw:" + normalizeKeyPart(debut.getArtistName());
            }

            ArtistOverviewAccumulator accumulator = grouped.computeIfAbsent(
                key,
                ignored -> new ArtistOverviewAccumulator(matched, debut.getResolvedArtistId(), debut.getArtistName(), debut.getGenderClass())
            );
            accumulator.accept(debut);
        }

        List<ChartArtistOverviewRowDTO> rows = new ArrayList<>();
        for (ArtistOverviewAccumulator accumulator : grouped.values()) {
            rows.add(accumulator.toRow());
        }
        rows.sort(Comparator
            .comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan).reversed()
            .thenComparing(row -> row.getHighestPeak() == null ? Integer.MAX_VALUE : row.getHighestPeak())
            .thenComparing(row -> safeLower(row.getArtistName())));
        return rows;
    }

    public List<Map<String, Object>> getChartRunForDebut(int debutId) {
        return jdbcTemplate.queryForList(
            "SELECT d.chart_date, " +
            "       CASE WHEN ce.debut_id IS NOT NULL THEN 1 ELSE 0 END AS on_chart, " +
            "       ce.position " +
            "FROM (SELECT DISTINCT chart_date FROM trl_chart_entry) d " +
            "LEFT JOIN trl_chart_entry ce ON ce.chart_date = d.chart_date AND ce.debut_id = ? " +
            "ORDER BY d.chart_date ASC",
            debutId);
    }

    public List<Map<String, Object>> getChartRunByNames(String artistName, String songTitle) {
        return jdbcTemplate.queryForList(
            "SELECT d.chart_date, " +
            "       CASE WHEN ce.artist_name IS NOT NULL THEN 1 ELSE 0 END AS on_chart, " +
            "       ce.position " +
            "FROM (SELECT DISTINCT chart_date FROM trl_chart_entry) d " +
            "LEFT JOIN trl_chart_entry ce ON ce.chart_date = d.chart_date " +
            "    AND LOWER(TRIM(ce.artist_name)) = LOWER(TRIM(?)) " +
            "    AND LOWER(TRIM(ce.song_title)) = LOWER(TRIM(?)) " +
            "ORDER BY d.chart_date ASC",
            artistName, songTitle);
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
            "    SELECT MIN(position) AS peak_position, COUNT(DISTINCT chart_date) AS actual_days, " +
            "           MIN(ce.chart_date) AS debut_date " +
            "    FROM trl_chart_entry ce " +
            "    JOIN trl_debut t ON ce.debut_id = t.id AND t.song_id = ? " +
            ") " +
            "SELECT t.days_on_countdown, cs.peak_position, cs.actual_days, cs.debut_date, " +
            "    (SELECT COUNT(*) FROM trl_chart_entry ce2 " +
            "     JOIN trl_debut t2 ON ce2.debut_id = t2.id AND t2.song_id = ? " +
            "     WHERE ce2.position = cs.peak_position " +
            "    ) AS days_at_peak, " +
            "    (SELECT MIN(ce3.chart_date) FROM trl_chart_entry ce3 " +
            "     JOIN trl_debut t3 ON ce3.debut_id = t3.id AND t3.song_id = ? " +
            "     WHERE ce3.position = cs.peak_position " +
            "    ) AS peak_date " +
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
                String debutDate = rs.getString("debut_date");
                if (debutDate != null) m.put("debutDate", debutDate);
                String peakDate = rs.getString("peak_date");
                if (peakDate != null) m.put("peakDate", peakDate);
                return m;
            }, songId, songId, songId, songId);
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
    public List<Map<String, Object>> searchSongs(String q) {
        String like = "%" + q.toLowerCase() + "%";
        String sql =
            "SELECT s.id, s.name AS title, a.name AS artist_name " +
            "FROM Song s " +
            "JOIN Artist a ON a.id = s.artist_id " +
            "WHERE LOWER(s.name) LIKE ? OR LOWER(a.name) LIKE ? " +
            "ORDER BY s.name COLLATE NOCASE ASC";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("title", rs.getString("title"));
            row.put("artistName", rs.getString("artist_name"));
            return row;
        }, like, like);
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
    public List<TrlChartEntryGroupDTO> searchChartEntries(String q) {
        String like = "%" + q.toLowerCase() + "%";
        String sql =
            "SELECT artist_name, song_title, COUNT(*) AS appearances " +
            "FROM trl_chart_entry " +
            "WHERE LOWER(artist_name) LIKE ? OR LOWER(song_title) LIKE ? " +
            "GROUP BY artist_name, song_title " +
            "ORDER BY appearances DESC";
        return jdbcTemplate.query(sql, (rs, rowNum) ->
            new TrlChartEntryGroupDTO(
                rs.getString("artist_name"),
                rs.getString("song_title"),
                rs.getInt("appearances")
            ),
            like, like
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

    public List<Map<String, Object>> getFallOffsForDate(String chartDate) {
        return getFallOffsForDate(chartDate, null);
    }

    public List<Map<String, Object>> getFallOffsForDate(String chartDate, List<Map<String, Object>> currentEntries) {
        String prevDate = getPrevChartDate(chartDate);
        if (prevDate == null) {
            return List.of();
        }

        List<Map<String, Object>> previousEntries = getCountdownForDate(prevDate);
        List<Map<String, Object>> effectiveCurrentEntries = currentEntries != null ? currentEntries : getCountdownForDate(chartDate);
        Set<String> currentKeys = effectiveCurrentEntries.stream()
            .map(this::buildRecapIdentityKey)
            .collect(Collectors.toSet());

        return previousEntries.stream()
            .filter(entry -> !currentKeys.contains(buildRecapIdentityKey(entry)))
            .toList();
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

    private Map<Integer, AlbumSongInfo> getAlbumSongInfoBySongId(List<TrlDebut> debuts) {
        Set<Integer> songIds = new HashSet<>();
        for (TrlDebut debut : debuts) {
            if (debut.getSongId() != null) {
                songIds.add(debut.getSongId());
            }
        }
        if (songIds.isEmpty()) {
            return Map.of();
        }

        List<Integer> orderedSongIds = new ArrayList<>(songIds);
        String placeholders = String.join(",", java.util.Collections.nCopies(orderedSongIds.size(), "?"));
        String sql =
            "SELECT s.id AS song_id, al.id AS album_id, al.name AS album_name " +
            "FROM Song s " +
            "JOIN Album al ON al.id = s.album_id " +
            "WHERE s.id IN (" + placeholders + ")";

        Map<Integer, AlbumSongInfo> result = new HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            result.put(
                rs.getInt("song_id"),
                new AlbumSongInfo(
                    rs.getInt("album_id"),
                    rs.getString("album_name")
                )
            );
        }, orderedSongIds.toArray());
        return result;
    }

    private String buildRecapIdentityKey(Map<String, Object> entry) {
        Object debutId = entry.get("debutId");
        if (debutId != null) {
            return "debut:" + debutId;
        }
        Object songId = entry.get("songId");
        if (songId != null) {
            return "song:" + songId;
        }
        return "raw:" + normalizeIdentityPart(entry.get("artistName")) + "||" + normalizeIdentityPart(entry.get("songTitle"));
    }

    private String normalizeIdentityPart(Object value) {
        return value == null ? "" : value.toString().trim().toLowerCase(Locale.ROOT);
    }

    private int getChartSpan(TrlDebut debut) {
        if (debut.getActualDays() != null && debut.getActualDays() > 0) {
            return debut.getActualDays();
        }
        return debut.getDaysOnCountdown() != null ? debut.getDaysOnCountdown() : 0;
    }

    private Integer getBestPeak(TrlDebut debut) {
        if (debut.getPeakPosition() != null) {
            return debut.getPeakPosition();
        }
        return debut.getDebutPosition();
    }

    private String normalizeKeyPart(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private String pickPreferredDisplayValue(String currentValue, String candidateValue) {
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

    private String minDate(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank() || candidateValue.compareTo(currentValue) < 0) {
            return candidateValue;
        }
        return currentValue;
    }

    private String maxDate(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank() || candidateValue.compareTo(currentValue) > 0) {
            return candidateValue;
        }
        return currentValue;
    }

    private String safeLower(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }

    private final class AlbumOverviewAccumulator {
        private final ChartAlbumOverviewRowDTO row;
        private final Set<Integer> songIds = new HashSet<>();
        private final Set<Integer> numberOneSongIds = new HashSet<>();

        private AlbumOverviewAccumulator(Integer albumId, String albumName, Integer resolvedArtistId, String artistName, String genderClass) {
            row = new ChartAlbumOverviewRowDTO();
            row.setAlbumId(albumId);
            row.setAlbumName(albumName);
            row.setResolvedArtistId(resolvedArtistId);
            row.setArtistName(artistName);
            row.setGenderClass(genderClass);
        }

        private void accept(TrlDebut debut) {
            if (debut.getSongId() != null && songIds.add(debut.getSongId())) {
                row.setChartedSongsCount(songIds.size());
            }
            row.setTotalChartSpan(row.getTotalChartSpan() + getChartSpan(debut));

            Integer bestPeak = getBestPeak(debut);
            if (bestPeak != null && (row.getHighestPeak() == null || bestPeak < row.getHighestPeak())) {
                row.setHighestPeak(bestPeak);
            }

            if (bestPeak != null && bestPeak == 1 && debut.getSongId() != null && numberOneSongIds.add(debut.getSongId())) {
                row.setNumberOneSongsCount(numberOneSongIds.size());
            }
            row.setTotalSpanAtNumberOne(row.getTotalSpanAtNumberOne() + debut.getDaysAtTop1());
            row.setFirstDebutDate(minDate(row.getFirstDebutDate(), debut.getDebutDate()));
            row.setLastAppearanceDate(maxDate(row.getLastAppearanceDate(), debut.getLastAppearanceDate()));
        }

        private ChartAlbumOverviewRowDTO toRow() {
            return row;
        }
    }

    private final class ArtistOverviewAccumulator {
        private final ChartArtistOverviewRowDTO row;
        private final Set<String> songKeys = new HashSet<>();
        private final Set<String> numberOneSongKeys = new HashSet<>();

        private ArtistOverviewAccumulator(boolean matched, Integer resolvedArtistId, String artistName, String genderClass) {
            row = new ChartArtistOverviewRowDTO();
            row.setMatched(matched);
            row.setResolvedArtistId(resolvedArtistId);
            row.setArtistName(artistName);
            row.setGenderClass(genderClass);
        }

        private void accept(TrlDebut debut) {
            row.setArtistName(pickPreferredDisplayValue(row.getArtistName(), debut.getArtistName()));
            if (row.getResolvedArtistId() == null) {
                row.setResolvedArtistId(debut.getResolvedArtistId());
            }
            if (row.getGenderClass() == null) {
                row.setGenderClass(debut.getGenderClass());
            }

            String songKey = debut.getSongId() != null
                ? "song:" + debut.getSongId()
                : "raw:" + normalizeKeyPart(debut.getSongTitle());
            if (songKeys.add(songKey)) {
                row.setChartedSongsCount(songKeys.size());
            }
            row.setTotalChartSpan(row.getTotalChartSpan() + getChartSpan(debut));

            Integer bestPeak = getBestPeak(debut);
            if (bestPeak != null && (row.getHighestPeak() == null || bestPeak < row.getHighestPeak())) {
                row.setHighestPeak(bestPeak);
            }

            if (bestPeak != null && bestPeak == 1 && numberOneSongKeys.add(songKey)) {
                row.setNumberOneSongsCount(numberOneSongKeys.size());
            }
            row.setTotalSpanAtNumberOne(row.getTotalSpanAtNumberOne() + debut.getDaysAtTop1());
            row.setFirstDebutDate(minDate(row.getFirstDebutDate(), debut.getDebutDate()));
            row.setLastAppearanceDate(maxDate(row.getLastAppearanceDate(), debut.getLastAppearanceDate()));
        }

        private ChartArtistOverviewRowDTO toRow() {
            return row;
        }
    }

    public List<Map<String, Object>> getChartRunBySongId(int songId) {
        try {
            Integer debutId = jdbcTemplate.queryForObject(
                "SELECT id FROM trl_debut WHERE song_id = ? LIMIT 1",
                Integer.class, songId);
            if (debutId == null) return java.util.Collections.emptyList();
            return getChartRunForDebut(debutId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return java.util.Collections.emptyList();
        }
    }

    public List<Map<String, Object>> getChartedSongsByAlbumId(int albumId) {
        String sql =
            "WITH stats AS ( " +
            "    SELECT t.song_id, s.name AS song_name, s.track_number, t.days_on_countdown, " +
            "           MIN(ce.position) AS peak_position, COUNT(DISTINCT ce.chart_date) AS actual_days, " +
            "           MIN(ce.chart_date) AS debut_date, " +
            "           (SELECT MIN(ce2.chart_date) FROM trl_chart_entry ce2 JOIN trl_debut t2 ON ce2.debut_id = t2.id WHERE t2.song_id = t.song_id AND ce2.position = MIN(ce.position)) AS peak_date " +
            "    FROM trl_debut t " +
            "    JOIN song s ON t.song_id = s.id " +
            "    LEFT JOIN trl_chart_entry ce ON ce.debut_id = t.id " +
            "    WHERE s.album_id = ? " +
            "    GROUP BY t.song_id, s.name, s.track_number, t.days_on_countdown " +
            ") " +
            "SELECT song_id, song_name, track_number, days_on_countdown, peak_position, actual_days, debut_date, peak_date, " +
            "       (SELECT COUNT(*) FROM trl_chart_entry ce2 " +
            "        JOIN trl_debut t2 ON ce2.debut_id = t2.id " +
            "        WHERE t2.song_id = stats.song_id AND ce2.position = stats.peak_position) AS days_at_peak " +
            "FROM stats WHERE days_on_countdown > 0 " +
            "ORDER BY CASE WHEN peak_position IS NULL THEN 999999 ELSE peak_position END ASC, days_at_peak DESC, track_number ASC";
        return jdbcTemplate.queryForList(sql, albumId);
    }

    public List<Map<String, Object>> getChartedSongsByArtistId(int artistId) {
        String sql =
            "WITH stats AS ( " +
            "    SELECT t.song_id, s.name AS song_name, t.days_on_countdown, " +
            "           MIN(ce.position) AS peak_position, COUNT(DISTINCT ce.chart_date) AS actual_days, " +
            "           MIN(ce.chart_date) AS debut_date, " +
            "           (SELECT MIN(ce2.chart_date) FROM trl_chart_entry ce2 JOIN trl_debut t2 ON ce2.debut_id = t2.id WHERE t2.song_id = t.song_id AND ce2.position = MIN(ce.position)) AS peak_date " +
            "    FROM trl_debut t " +
            "    JOIN song s ON t.song_id = s.id " +
            "    LEFT JOIN trl_chart_entry ce ON ce.debut_id = t.id " +
            "    WHERE s.artist_id = ? " +
            "    GROUP BY t.song_id, s.name, t.days_on_countdown " +
            ") " +
            "SELECT song_id, song_name, days_on_countdown, peak_position, actual_days, debut_date, peak_date, " +
            "       (SELECT COUNT(*) FROM trl_chart_entry ce2 " +
            "        JOIN trl_debut t2 ON ce2.debut_id = t2.id " +
            "        WHERE t2.song_id = stats.song_id AND ce2.position = stats.peak_position) AS days_at_peak " +
            "FROM stats WHERE days_on_countdown > 0 " +
            "ORDER BY CASE WHEN peak_position IS NULL THEN 999999 ELSE peak_position END ASC, days_at_peak DESC, song_name ASC";
        return jdbcTemplate.queryForList(sql, artistId);
    }

    private record AlbumSongInfo(Integer albumId, String albumName) {
    }
}
