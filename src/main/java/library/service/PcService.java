package library.service;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.PcOverviewRowDTO;
import library.util.ChartAggregationUtils;
import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class PcService {

    private static final String LEGACY_TABLE = "pc_countdown_entry";
    private static final String CURRENT_TABLE = "vatos_cuntdown_entry";

    private final JdbcTemplate jdbcTemplate;

    public PcService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String, Object> getPcStatsBySongId(Integer songId) {
        if (songId == null) return null;
        String sql =
            "WITH song_entries AS ( " +
            "    SELECT ce.chart_date, ce.position " +
            "    FROM vatos_cuntdown_entry ce " +
            "    WHERE ce.song_id = ? AND ce.is_close_call = 0 " +
            "), cs AS ( " +
            "    SELECT MIN(se.position) AS peak_position, " +
            "           COUNT(DISTINCT se.chart_date) AS actual_days, " +
            "           MIN(se.chart_date) AS debut_date " +
            "    FROM song_entries se " +
            ") " +
            "SELECT cs.actual_days AS days_on_countdown, cs.peak_position, cs.actual_days, cs.debut_date, " +
            "       COUNT(DISTINCT CASE WHEN se.position = cs.peak_position THEN se.chart_date END) AS days_at_peak, " +
            "       MIN(CASE WHEN se.position = cs.peak_position THEN se.chart_date END) AS peak_date " +
            "FROM cs " +
            "LEFT JOIN song_entries se ON 1 = 1 " +
            "WHERE cs.actual_days > 0 " +
            "GROUP BY cs.actual_days, cs.peak_position, cs.debut_date";
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
            }, songId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<PcOverviewRowDTO> getOverviewRows() {
        return jdbcTemplate.query(buildOverviewRowsSql(false), this::mapOverviewRow);
    }

    public List<ChartAlbumOverviewRowDTO> getAlbumOverviewRows() {
        return getAlbumOverviewRows(getOverviewRows());
    }

    public List<ChartAlbumOverviewRowDTO> getAlbumOverviewRows(List<PcOverviewRowDTO> entries) {
        Map<Integer, AlbumSongInfo> albumSongInfoBySongId = getAlbumSongInfoBySongId(entries);
        Map<Integer, AlbumOverviewAccumulator> grouped = new LinkedHashMap<>();

        for (PcOverviewRowDTO entry : entries) {
            if (entry.getSongId() == null) {
                continue;
            }
            AlbumSongInfo albumSongInfo = albumSongInfoBySongId.get(entry.getSongId());
            if (albumSongInfo == null) {
                continue;
            }

            AlbumOverviewAccumulator accumulator = grouped.computeIfAbsent(
                albumSongInfo.albumId,
                ignored -> new AlbumOverviewAccumulator(albumSongInfo.albumId, albumSongInfo.albumName, entry.getResolvedArtistId(), entry.getArtistName(), entry.getGenderClass())
            );
            accumulator.accept(entry);
        }

        List<ChartAlbumOverviewRowDTO> rows = new ArrayList<>();
        for (AlbumOverviewAccumulator accumulator : grouped.values()) {
            rows.add(accumulator.toRow());
        }
        rows.sort(Comparator
            .comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan).reversed()
            .thenComparing(row -> row.getHighestPeak() == null ? Integer.MAX_VALUE : row.getHighestPeak())
            .thenComparing(row -> ChartAggregationUtils.safeLower(row.getArtistName()))
            .thenComparing(row -> ChartAggregationUtils.safeLower(row.getAlbumName())));
        return rows;
    }

    public List<ChartArtistOverviewRowDTO> getArtistOverviewRows() {
        return getArtistOverviewRows(getOverviewRows(), false);
    }

    public List<ChartArtistOverviewRowDTO> getArtistOverviewRows(List<PcOverviewRowDTO> entries) {
        return getArtistOverviewRows(entries, false);
    }

    public List<ChartArtistOverviewRowDTO> getArtistOverviewRows(boolean includeFeatured) {
        return getArtistOverviewRows(getOverviewRows(), includeFeatured);
    }

    public List<ChartArtistOverviewRowDTO> getArtistOverviewRows(List<PcOverviewRowDTO> entries, boolean includeFeatured) {
        Map<String, ArtistOverviewAccumulator> grouped = new LinkedHashMap<>();

        for (PcOverviewRowDTO entry : entries) {
            String key;
            boolean matched = entry.getResolvedArtistId() != null;
            if (matched) {
                key = "artist:" + entry.getResolvedArtistId();
            } else {
                key = "raw:" + ChartAggregationUtils.normalizeKeyPart(entry.getArtistName());
            }

            ArtistOverviewAccumulator accumulator = grouped.computeIfAbsent(
                key,
                ignored -> new ArtistOverviewAccumulator(matched, entry.getResolvedArtistId(), entry.getArtistName(), entry.getGenderClass())
            );
            accumulator.accept(entry);
        }

        if (includeFeatured) {
            Map<Integer, List<FeaturedArtistRef>> featuredArtistRefsBySongId = getFeaturedArtistRefsBySongId(entries.stream()
                .map(PcOverviewRowDTO::getSongId)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .toList());
            for (PcOverviewRowDTO entry : entries) {
                if (entry.getSongId() == null) {
                    continue;
                }
                List<FeaturedArtistRef> featuredArtistRefs = featuredArtistRefsBySongId.get(entry.getSongId());
                if (featuredArtistRefs == null || featuredArtistRefs.isEmpty()) {
                    continue;
                }
                for (FeaturedArtistRef featuredArtistRef : featuredArtistRefs) {
                    if (entry.getResolvedArtistId() != null && entry.getResolvedArtistId().equals(featuredArtistRef.artistId())) {
                        continue;
                    }
                    PcOverviewRowDTO featuredEntry = copyOverviewRowForFeaturedArtist(entry, featuredArtistRef);
                    ArtistOverviewAccumulator accumulator = grouped.computeIfAbsent(
                        "artist:" + featuredArtistRef.artistId(),
                        ignored -> new ArtistOverviewAccumulator(true, featuredArtistRef.artistId(), featuredArtistRef.artistName(), featuredArtistRef.genderClass())
                    );
                    accumulator.acceptFeatured(featuredEntry);
                }
            }
        }

        List<ChartArtistOverviewRowDTO> rows = new ArrayList<>();
        for (ArtistOverviewAccumulator accumulator : grouped.values()) {
            rows.add(accumulator.toRow());
        }
        rows.sort(Comparator
            .comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan).reversed()
            .thenComparing(row -> row.getHighestPeak() == null ? Integer.MAX_VALUE : row.getHighestPeak())
            .thenComparing(row -> ChartAggregationUtils.safeLower(row.getArtistName())));
        return rows;
    }

    public List<Map<String, Object>> getChartRunBySongId(int songId) {
        return jdbcTemplate.queryForList(
            "SELECT d.chart_date, " +
            "       CASE WHEN ce.song_id IS NOT NULL THEN 1 ELSE 0 END AS on_chart, " +
            "       ce.position " +
            "FROM (SELECT DISTINCT chart_date FROM vatos_cuntdown_entry WHERE is_close_call = 0) d " +
            "LEFT JOIN vatos_cuntdown_entry ce ON ce.chart_date = d.chart_date " +
            "    AND ce.song_id = ? AND ce.is_close_call = 0 " +
            "ORDER BY d.chart_date ASC",
            songId);
    }

    public List<Map<String, Object>> getChartRunByNames(String artistName, String songTitle) {
        return jdbcTemplate.queryForList(
            "SELECT d.chart_date, " +
            "       CASE WHEN ce.artist_name IS NOT NULL THEN 1 ELSE 0 END AS on_chart, " +
            "       ce.position " +
            "FROM (SELECT DISTINCT chart_date FROM vatos_cuntdown_entry WHERE is_close_call = 0) d " +
            "LEFT JOIN vatos_cuntdown_entry ce ON ce.chart_date = d.chart_date " +
            "    AND LOWER(ce.artist_name) = LOWER(?) AND LOWER(ce.song_title) = LOWER(?) " +
            "    AND ce.song_id IS NULL AND ce.is_close_call = 0 " +
            "ORDER BY d.chart_date ASC",
            artistName, songTitle);
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
            "    FROM vatos_cuntdown_entry e " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NOT NULL " +
            "    GROUP BY e.song_id " +
            "), matched_debut AS ( " +
            "    SELECT e.song_id, MIN(e.position) AS debut_position " +
            "    FROM vatos_cuntdown_entry e " +
            "    JOIN matched_groups mg ON mg.song_id = e.song_id AND e.chart_date = mg.first_week " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NOT NULL " +
            "    GROUP BY e.song_id " +
            "), matched_peak AS ( " +
            "    SELECT e.song_id, COUNT(DISTINCT e.chart_date) AS days_at_peak, MIN(e.chart_date) AS peak_week " +
            "    FROM vatos_cuntdown_entry e " +
            "    JOIN matched_groups mg ON mg.song_id = e.song_id AND e.position = mg.peak_position " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NOT NULL " +
            "    GROUP BY e.song_id " +
            "), matched_tiers AS ( " +
            "    SELECT e.song_id, " +
            "           COUNT(DISTINCT CASE WHEN e.position <= 1  THEN e.chart_date END) AS days_at_top1, " +
            "           COUNT(DISTINCT CASE WHEN e.position <= 5  THEN e.chart_date END) AS days_at_top5, " +
            "           COUNT(DISTINCT CASE WHEN e.position <= 10 THEN e.chart_date END) AS days_at_top10 " +
            "    FROM vatos_cuntdown_entry e " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NOT NULL " +
            "    GROUP BY e.song_id " +
            "), unmatched_groups AS ( " +
            "    SELECT e.artist_name, e.song_title, " +
            "           MIN(e.chart_date) AS first_week, " +
            "           MAX(e.chart_date) AS last_week, " +
            "           COUNT(DISTINCT e.chart_date) AS days_on_countdown, " +
            "           MIN(e.position) AS peak_position, " +
            "           1 AS raw_variant_count " +
            "    FROM vatos_cuntdown_entry e " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            "), unmatched_debut AS ( " +
            "    SELECT e.artist_name, e.song_title, MIN(e.position) AS debut_position " +
            "    FROM vatos_cuntdown_entry e " +
            "    JOIN unmatched_groups ug ON ug.artist_name = e.artist_name AND ug.song_title = e.song_title AND e.chart_date = ug.first_week " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            "), unmatched_peak AS ( " +
            "    SELECT e.artist_name, e.song_title, COUNT(DISTINCT e.chart_date) AS days_at_peak, MIN(e.chart_date) AS peak_week " +
            "    FROM vatos_cuntdown_entry e " +
            "    JOIN unmatched_groups ug ON ug.artist_name = e.artist_name AND ug.song_title = e.song_title AND e.position = ug.peak_position " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            "), unmatched_tiers AS ( " +
            "    SELECT e.artist_name, e.song_title, " +
            "           COUNT(DISTINCT CASE WHEN e.position <= 1  THEN e.chart_date END) AS days_at_top1, " +
            "           COUNT(DISTINCT CASE WHEN e.position <= 5  THEN e.chart_date END) AS days_at_top5, " +
            "           COUNT(DISTINCT CASE WHEN e.position <= 10 THEN e.chart_date END) AS days_at_top10 " +
            "    FROM vatos_cuntdown_entry e " +
            "    WHERE e.is_close_call = 0 AND e.song_id IS NULL " +
            "    GROUP BY e.artist_name, e.song_title " +
            ") " +
            "SELECT 1 AS matched, mg.song_id, s.name AS song_title, a.name AS artist_name, " +
            "       mg.first_week, mg.last_week, md.debut_position, mg.days_on_countdown, mg.peak_position, " +
            "       COALESCE(mp.days_at_peak, 0) AS days_at_peak, mp.peak_week, mg.raw_variant_count, " +
            "       a.id AS resolved_artist_id, LOWER(g.name) AS gender_name, " +
            "       COALESCE(mt.days_at_top1, 0) AS days_at_top1, " +
            "       COALESCE(mt.days_at_top5, 0) AS days_at_top5, " +
            "       COALESCE(mt.days_at_top10, 0) AS days_at_top10 " +
            "FROM matched_groups mg " +
            "JOIN Song s ON s.id = mg.song_id " +
            "JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id " +
            "LEFT JOIN matched_debut md ON md.song_id = mg.song_id " +
            "LEFT JOIN matched_peak mp ON mp.song_id = mg.song_id " +
            "LEFT JOIN matched_tiers mt ON mt.song_id = mg.song_id " +
            "UNION ALL " +
            "SELECT 0 AS matched, NULL AS song_id, ug.song_title, ug.artist_name, " +
            "       ug.first_week, ug.last_week, ud.debut_position, ug.days_on_countdown, ug.peak_position, " +
            "       COALESCE(up.days_at_peak, 0) AS days_at_peak, up.peak_week, ug.raw_variant_count, " +
            "       NULL AS resolved_artist_id, NULL AS gender_name, " +
            "       COALESCE(ut.days_at_top1, 0) AS days_at_top1, " +
            "       COALESCE(ut.days_at_top5, 0) AS days_at_top5, " +
            "       COALESCE(ut.days_at_top10, 0) AS days_at_top10 " +
            "FROM unmatched_groups ug " +
            "LEFT JOIN unmatched_debut ud ON ud.artist_name = ug.artist_name AND ud.song_title = ug.song_title " +
            "LEFT JOIN unmatched_peak up ON up.artist_name = ug.artist_name AND up.song_title = ug.song_title " +
            "LEFT JOIN unmatched_tiers ut ON ut.artist_name = ug.artist_name AND ut.song_title = ug.song_title " +
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
        int debutPosition = rs.getInt("debut_position");
        if (!rs.wasNull()) row.setDebutPosition(debutPosition);
        row.setDaysOnCountdown(rs.getInt("days_on_countdown"));
        row.setPeakPosition(rs.getInt("peak_position"));
        row.setDaysAtPeak(rs.getInt("days_at_peak"));
        row.setRawVariantCount(rs.getInt("raw_variant_count"));
        // Map gender name to CSS class: female->pink (genderId 1), male->blue (genderId 2)
        // IMPORTANT: genderId 1 = female, genderId 2 = male (not intuitive but matches original schema)
        String genderName = rs.getString("gender_name");
        if (genderName != null) {
            if (genderName.contains("female")) {
                row.setGenderClass("gender-female");
            } else if (genderName.contains("male")) {
                row.setGenderClass("gender-male");
            }
        }
        row.setDaysAtTop1(rs.getInt("days_at_top1"));
        row.setDaysAtTop5(rs.getInt("days_at_top5"));
        row.setDaysAtTop10(rs.getInt("days_at_top10"));
        return row;
    }

    private Map<Integer, AlbumSongInfo> getAlbumSongInfoBySongId(List<PcOverviewRowDTO> entries) {
        Set<Integer> songIds = new HashSet<>();
        for (PcOverviewRowDTO entry : entries) {
            if (entry.getSongId() != null) {
                songIds.add(entry.getSongId());
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
        List<AlbumSongInfoRow> rows = jdbcTemplate.query(
            sql,
            (rs, rowNum) -> new AlbumSongInfoRow(
                rs.getInt("song_id"),
                rs.getInt("album_id"),
                rs.getString("album_name")
            ),
            orderedSongIds.toArray()
        );
        for (AlbumSongInfoRow row : rows) {
            result.put(row.songId(), new AlbumSongInfo(row.albumId(), row.albumName()));
        }
        return result;
    }

    private Map<Integer, List<FeaturedArtistRef>> getFeaturedArtistRefsBySongId(List<Integer> songIds) {
        if (songIds == null || songIds.isEmpty()) {
            return Map.of();
        }
        String placeholders = songIds.stream().map(ignored -> "?").collect(Collectors.joining(","));
        String sql = """
            SELECT sfa.song_id,
                   a.id AS artist_id,
                   a.name AS artist_name,
                   LOWER(g.name) AS gender_name
            FROM SongFeaturedArtist sfa
            INNER JOIN Artist a ON a.id = sfa.artist_id
            LEFT JOIN Gender g ON g.id = a.gender_id
            WHERE sfa.song_id IN (%s)
            ORDER BY sfa.song_id ASC, a.name COLLATE NOCASE ASC
            """.formatted(placeholders);

        Map<Integer, List<FeaturedArtistRef>> refsBySongId = new LinkedHashMap<>();
        jdbcTemplate.query(sql, (rs) -> {
            FeaturedArtistRef ref = new FeaturedArtistRef(
                rs.getInt("artist_id"),
                rs.getString("artist_name"),
                mapGenderClass(rs.getString("gender_name"))
            );
            refsBySongId.computeIfAbsent(rs.getInt("song_id"), ignored -> new ArrayList<>()).add(ref);
        }, songIds.toArray());
        return refsBySongId;
    }

    private PcOverviewRowDTO copyOverviewRowForFeaturedArtist(PcOverviewRowDTO source, FeaturedArtistRef featuredArtistRef) {
        PcOverviewRowDTO target = new PcOverviewRowDTO();
        target.setMatched(true);
        target.setSongId(source.getSongId());
        target.setResolvedArtistId(featuredArtistRef.artistId());
        target.setSongTitle(source.getSongTitle());
        target.setArtistName(featuredArtistRef.artistName());
        target.setFirstWeek(source.getFirstWeek());
        target.setLastWeek(source.getLastWeek());
        target.setPeakWeek(source.getPeakWeek());
        target.setDebutPosition(source.getDebutPosition());
        target.setDaysOnCountdown(source.getDaysOnCountdown());
        target.setPeakPosition(source.getPeakPosition());
        target.setDaysAtPeak(source.getDaysAtPeak());
        target.setRawVariantCount(source.getRawVariantCount());
        target.setGenderClass(featuredArtistRef.genderClass());
        target.setDaysAtTop1(source.getDaysAtTop1());
        target.setDaysAtTop5(source.getDaysAtTop5());
        target.setDaysAtTop10(source.getDaysAtTop10());
        return target;
    }

    private String mapGenderClass(String genderName) {
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

    private String formatNumberOneSongLabel(String title, int daysAtTop1) {
        if (title == null || title.isBlank()) {
            return title;
        }
        if (daysAtTop1 <= 0) {
            return title;
        }
        return title + " (" + daysAtTop1 + (daysAtTop1 == 1 ? " day" : " days") + ")";
    }

    private record FeaturedArtistRef(Integer artistId, String artistName, String genderClass) {
    }

    private final class AlbumOverviewAccumulator {
        private final ChartAlbumOverviewRowDTO row;
        private final Set<Integer> songIds = new HashSet<>();
        private final Set<Integer> numberOneSongIds = new HashSet<>();
        private final Map<String, String> numberOneSongTitles = new LinkedHashMap<>();

        private AlbumOverviewAccumulator(Integer albumId, String albumName, Integer resolvedArtistId, String artistName, String genderClass) {
            row = new ChartAlbumOverviewRowDTO();
            row.setAlbumId(albumId);
            row.setAlbumName(albumName);
            row.setResolvedArtistId(resolvedArtistId);
            row.setArtistName(artistName);
            row.setGenderClass(genderClass);
        }

        private void accept(PcOverviewRowDTO entry) {
            if (entry.getSongId() != null && songIds.add(entry.getSongId())) {
                row.setChartedSongsCount(songIds.size());
            }
            row.setTotalChartSpan(row.getTotalChartSpan() + entry.getDaysOnCountdown());

            int bestPeak = entry.getPeakPosition();
            if (row.getHighestPeak() == null || bestPeak < row.getHighestPeak()) {
                row.setHighestPeak(bestPeak);
            }

            if (bestPeak == 1 && entry.getSongId() != null && numberOneSongIds.add(entry.getSongId())) {
                row.setNumberOneSongsCount(numberOneSongIds.size());
                numberOneSongTitles.putIfAbsent("song:" + entry.getSongId(), entry.getSongTitle());
            }
            row.setTotalSpanAtNumberOne(row.getTotalSpanAtNumberOne() + entry.getDaysAtTop1());
            row.setFirstDebutDate(ChartAggregationUtils.minDate(row.getFirstDebutDate(), entry.getFirstWeek()));
            row.setLastAppearanceDate(ChartAggregationUtils.maxDate(row.getLastAppearanceDate(), entry.getLastWeek()));
        }

        private ChartAlbumOverviewRowDTO toRow() {
            row.setNumberOneSongTitles(new ArrayList<>(numberOneSongTitles.values()));
            return row;
        }
    }

    private final class ArtistOverviewAccumulator {
        private final ChartArtistOverviewRowDTO row;
        private final Set<String> songKeys = new HashSet<>();
        private final Set<String> numberOneSongKeys = new HashSet<>();
        private final Map<String, String> numberOneSongTitles = new LinkedHashMap<>();

        private ArtistOverviewAccumulator(boolean matched, Integer resolvedArtistId, String artistName, String genderClass) {
            row = new ChartArtistOverviewRowDTO();
            row.setMatched(matched);
            row.setResolvedArtistId(resolvedArtistId);
            row.setArtistName(artistName);
            row.setGenderClass(genderClass);
        }

        private void accept(PcOverviewRowDTO entry) {
            accept(entry, true);
        }

        private void acceptFeatured(PcOverviewRowDTO entry) {
            accept(entry, false);
        }

        private void accept(PcOverviewRowDTO entry, boolean includeTimelineMetrics) {
            row.setArtistName(ChartAggregationUtils.pickPreferredDisplayValue(row.getArtistName(), entry.getArtistName()));
            if (row.getResolvedArtistId() == null) {
                row.setResolvedArtistId(entry.getResolvedArtistId());
            }
            if (row.getGenderClass() == null) {
                row.setGenderClass(entry.getGenderClass());
            }

            String songKey = entry.getSongId() != null
                ? "song:" + entry.getSongId()
                : "raw:" + ChartAggregationUtils.normalizeKeyPart(entry.getSongTitle());
            if (songKeys.add(songKey)) {
                row.setChartedSongsCount(songKeys.size());
            }
            row.setTotalChartSpan(row.getTotalChartSpan() + entry.getDaysOnCountdown());

            int bestPeak = entry.getPeakPosition();
            if (includeTimelineMetrics && (row.getHighestPeak() == null || bestPeak < row.getHighestPeak())) {
                row.setHighestPeak(bestPeak);
            }

            if (bestPeak == 1 && numberOneSongKeys.add(songKey)) {
                row.setNumberOneSongsCount(numberOneSongKeys.size());
                numberOneSongTitles.putIfAbsent(songKey, formatNumberOneSongLabel(entry.getSongTitle(), entry.getDaysAtTop1()));
            }
            row.setTotalSpanAtNumberOne(row.getTotalSpanAtNumberOne() + entry.getDaysAtTop1());
            if (includeTimelineMetrics) {
                row.setFirstDebutDate(ChartAggregationUtils.minDate(row.getFirstDebutDate(), entry.getFirstWeek()));
                row.setLastAppearanceDate(ChartAggregationUtils.maxDate(row.getLastAppearanceDate(), entry.getLastWeek()));
            }
        }

        private ChartArtistOverviewRowDTO toRow() {
            row.setNumberOneSongTitles(new ArrayList<>(numberOneSongTitles.values()));
            return row;
        }
    }

    private record AlbumSongInfo(Integer albumId, String albumName) {
    }

    private record AlbumSongInfoRow(Integer songId, Integer albumId, String albumName) {
    }

    public Map<String, Object> matchRawGroup(String rawArtist, String rawSong, Integer songId) {
        Map<String, Object> lib = jdbcTemplate.queryForMap(
            "SELECT s.name AS song_name, a.name AS artist_name " +
            "FROM Song s JOIN Artist a ON a.id = s.artist_id WHERE s.id = ?", songId);
        String canonArtist = (String) lib.get("artist_name");
        String canonSong = (String) lib.get("song_name");

        int updatedEntries = jdbcTemplate.update(
            "UPDATE vatos_cuntdown_entry SET artist_name = ?, song_title = ?, song_id = ? " +
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
            "SELECT song_id FROM vatos_cuntdown_entry " +
            "WHERE artist_name = ? AND song_title = ? AND song_id IS NOT NULL LIMIT 1",
            rs -> rs.next() ? rs.getInt("song_id") : null,
            sourceArtist,
            sourceSong
        );

        int updated;
        if (sourceSongId != null) {
            updated = jdbcTemplate.update(
                "UPDATE vatos_cuntdown_entry SET artist_name = ?, song_title = ?, song_id = ? " +
                "WHERE artist_name = ? AND song_title = ?",
                sourceArtist, sourceSong, sourceSongId, targetArtist, targetSong
            );
        } else {
            updated = jdbcTemplate.update(
                "UPDATE vatos_cuntdown_entry SET artist_name = ?, song_title = ? " +
                "WHERE artist_name = ? AND song_title = ?",
                sourceArtist, sourceSong, targetArtist, targetSong
            );
            jdbcTemplate.update(
                "UPDATE vatos_cuntdown_entry " +
                "SET song_id = (SELECT song_id FROM vatos_cuntdown_entry " +
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
            "FROM vatos_cuntdown_entry " +
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
                "FROM vatos_cuntdown_entry " +
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
                "UPDATE vatos_cuntdown_entry SET artist_name = ?, song_title = ? " +
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
            "FROM vatos_cuntdown_entry e " +
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
                "UPDATE vatos_cuntdown_entry SET artist_name = ?, song_title = ? WHERE id = ?",
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
            "UPDATE vatos_cuntdown_entry " +
            "SET song_id = ( " +
            "        SELECT s.id FROM Song s " +
            "        JOIN Artist a ON a.id = s.artist_id " +
            "        WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(vatos_cuntdown_entry.artist_name)) " +
            "          AND LOWER(TRIM(s.name)) = LOWER(TRIM(vatos_cuntdown_entry.song_title)) " +
            "        LIMIT 1 " +
            "    ), " +
            "    artist_name = COALESCE(( " +
            "        SELECT a.name FROM Song s " +
            "        JOIN Artist a ON a.id = s.artist_id " +
            "        WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(vatos_cuntdown_entry.artist_name)) " +
            "          AND LOWER(TRIM(s.name)) = LOWER(TRIM(vatos_cuntdown_entry.song_title)) " +
            "        LIMIT 1 " +
            "    ), artist_name), " +
            "    song_title = COALESCE(( " +
            "        SELECT s.name FROM Song s " +
            "        JOIN Artist a ON a.id = s.artist_id " +
            "        WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(vatos_cuntdown_entry.artist_name)) " +
            "          AND LOWER(TRIM(s.name)) = LOWER(TRIM(vatos_cuntdown_entry.song_title)) " +
            "        LIMIT 1 " +
            "    ), song_title) " +
            "WHERE song_id IS NULL " +
            "  AND 1 = ( " +
            "    SELECT COUNT(*) FROM Song s " +
            "    JOIN Artist a ON a.id = s.artist_id " +
            "    WHERE LOWER(TRIM(a.name)) = LOWER(TRIM(vatos_cuntdown_entry.artist_name)) " +
            "      AND LOWER(TRIM(s.name)) = LOWER(TRIM(vatos_cuntdown_entry.song_title)) " +
            "  )";
        return jdbcTemplate.update(sql);
    }

    public List<String> getAvailableChartDates() {
        return jdbcTemplate.queryForList(
            "SELECT DISTINCT chart_date FROM vatos_cuntdown_entry ORDER BY chart_date ASC",
            String.class
        );
    }

    public List<Map<String, Object>> getCountdownForDate(String chartDate) {
        String sql =
            "WITH prev_date AS ( " +
            "    SELECT MAX(chart_date) AS pd " +
            "    FROM vatos_cuntdown_entry " +
            "    WHERE chart_date < ? AND is_close_call = 0 " +
            "), " +
            "entry_scope AS ( " +
            "    SELECT ce.*, " +
            "           CASE " +
            "               WHEN ce.song_id IS NOT NULL THEN 'song:' || ce.song_id " +
            "               ELSE 'raw:' || LOWER(TRIM(ce.artist_name)) || '||' || LOWER(TRIM(ce.song_title)) " +
            "           END AS identity_key " +
            "    FROM vatos_cuntdown_entry ce " +
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
            "ORDER BY es.is_close_call ASC, es.position ASC";
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

    public List<Map<String, Object>> getFallOffsForDate(String chartDate) {
        return getFallOffsForDate(chartDate, null);
    }

    public List<Map<String, Object>> getFallOffsForDate(String chartDate, List<Map<String, Object>> currentEntries) {
        String prevDate = getPrevChartDate(chartDate);
        if (prevDate == null) {
            return List.of();
        }

        List<Map<String, Object>> previousEntries = getCountdownForDate(prevDate).stream()
            .filter(entry -> !Boolean.TRUE.equals(entry.get("isClosecall")))
            .toList();
        List<Map<String, Object>> effectiveCurrentEntries = (currentEntries != null ? currentEntries : getCountdownForDate(chartDate)).stream()
            .filter(entry -> !Boolean.TRUE.equals(entry.get("isClosecall")))
            .toList();
        Set<String> currentKeys = effectiveCurrentEntries.stream()
            .map(this::buildRecapIdentityKey)
            .collect(Collectors.toSet());

        return previousEntries.stream()
            .filter(entry -> !currentKeys.contains(buildRecapIdentityKey(entry)))
            .toList();
    }

    public String findClosestChartDate(String date) {
        try {
            return jdbcTemplate.queryForObject(
                "SELECT DISTINCT chart_date FROM vatos_cuntdown_entry " +
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
                "SELECT DISTINCT chart_date FROM vatos_cuntdown_entry " +
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
                "SELECT DISTINCT chart_date FROM vatos_cuntdown_entry " +
                "WHERE chart_date > ? " +
                "ORDER BY chart_date ASC LIMIT 1",
                String.class, date);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    private String buildRecapIdentityKey(Map<String, Object> entry) {
        Object songId = entry.get("songId");
        if (songId != null) {
            return "song:" + songId;
        }
        return "raw:" + normalizeIdentityPart(entry.get("artistName")) + "||" + normalizeIdentityPart(entry.get("songTitle"));
    }

    public List<Map<String, Object>> getChartedSongsByAlbumId(int albumId) {
        String sql =
            "WITH stats AS ( " +
            "    SELECT s.id AS song_id, s.name AS song_name, s.track_number, " +
            "           MIN(ce.position) AS peak_position, COUNT(DISTINCT ce.chart_date) AS actual_days, " +
            "           MIN(ce.chart_date) AS debut_date, " +
            "           (SELECT MIN(ce2.chart_date) FROM vatos_cuntdown_entry ce2 WHERE ce2.song_id = s.id AND ce2.position = MIN(ce.position) AND ce2.is_close_call = 0) AS peak_date " +
            "    FROM vatos_cuntdown_entry ce " +
            "    JOIN song s ON ce.song_id = s.id " +
            "    WHERE s.album_id = ? AND ce.is_close_call = 0 " +
            "    GROUP BY s.id, s.name, s.track_number " +
            ") " +
            "SELECT song_id, song_name, track_number, actual_days AS days_on_countdown, peak_position, debut_date, peak_date, " +
            "    (SELECT COUNT(DISTINCT ce2.chart_date) FROM vatos_cuntdown_entry ce2 " +
            "     WHERE ce2.song_id = stats.song_id AND ce2.position = stats.peak_position AND ce2.is_close_call = 0 " +
            "    ) AS days_at_peak " +
            "FROM stats WHERE actual_days > 0 " +
            "ORDER BY peak_position ASC, days_at_peak DESC, track_number ASC";
        return jdbcTemplate.queryForList(sql, albumId);
    }

    public List<Map<String, Object>> getChartedSongsByArtistId(int artistId) {
        String sql =
            "WITH stats AS ( " +
            "    SELECT s.id AS song_id, s.name AS song_name, " +
            "           MIN(ce.position) AS peak_position, COUNT(DISTINCT ce.chart_date) AS actual_days, " +
            "           MIN(ce.chart_date) AS debut_date, " +
            "           (SELECT MIN(ce2.chart_date) FROM vatos_cuntdown_entry ce2 WHERE ce2.song_id = s.id AND ce2.position = MIN(ce.position) AND ce2.is_close_call = 0) AS peak_date " +
            "    FROM vatos_cuntdown_entry ce " +
            "    JOIN song s ON ce.song_id = s.id " +
            "    WHERE s.artist_id = ? AND ce.is_close_call = 0 " +
            "    GROUP BY s.id, s.name " +
            ") " +
            "SELECT song_id, song_name, actual_days AS days_on_countdown, peak_position, debut_date, peak_date, " +
            "    (SELECT COUNT(DISTINCT ce2.chart_date) FROM vatos_cuntdown_entry ce2 " +
            "     WHERE ce2.song_id = stats.song_id AND ce2.position = stats.peak_position AND ce2.is_close_call = 0 " +
            "    ) AS days_at_peak " +
            "FROM stats WHERE actual_days > 0 " +
            "ORDER BY peak_position ASC, days_at_peak DESC, song_name ASC";
        return jdbcTemplate.queryForList(sql, artistId);
    }

    private String normalizeIdentityPart(Object value) {
        return ChartAggregationUtils.normalizeKeyPart(value == null ? null : value.toString());
    }
}
