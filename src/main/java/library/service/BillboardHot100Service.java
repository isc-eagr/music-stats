package library.service;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.BillboardHot100OverviewRowDTO;
import library.util.BillboardHot100ImportSupport;
import library.util.StringNormalizer;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    public List<Map<String, Object>> getChartRunBySongId(int songId) {
        return jdbcTemplate.queryForList(
            "SELECT d.chart_date, " +
            "       CASE WHEN ce.song_id IS NOT NULL THEN 1 ELSE 0 END AS on_chart, " +
            "       ce.position " +
            "FROM (SELECT DISTINCT chart_date FROM billboard_hot100_entry) d " +
            "LEFT JOIN billboard_hot100_entry ce ON ce.chart_date = d.chart_date AND ce.song_id = ? " +
            "ORDER BY d.chart_date ASC",
            songId);
    }

    public List<Map<String, Object>> getChartRunByNames(String artistName, String songTitle) {
        return jdbcTemplate.queryForList(
            "SELECT d.chart_date, " +
            "       CASE WHEN ce.artist_name IS NOT NULL THEN 1 ELSE 0 END AS on_chart, " +
            "       ce.position " +
            "FROM (SELECT DISTINCT chart_date FROM billboard_hot100_entry) d " +
            "LEFT JOIN billboard_hot100_entry ce ON ce.chart_date = d.chart_date " +
            "    AND LOWER(ce.artist_name) = LOWER(?) AND LOWER(ce.song_title) = LOWER(?) " +
            "    AND ce.song_id IS NULL " +
            "ORDER BY d.chart_date ASC",
            artistName, songTitle);
    }

    public List<BillboardHot100OverviewRowDTO> getOverviewRows(int page, int size) {
        return getOverviewRows(page, size, "firstWeek", "desc", null);
    }

    public List<BillboardHot100OverviewRowDTO> getOverviewRows(int page, int size, String sort, String dir) {
        return getOverviewRows(page, size, sort, dir, null);
    }

    public List<BillboardHot100OverviewRowDTO> getOverviewRows(int page, int size, String sort, String dir, String query) {
        ensureDebutSnapshot();
        if (!debutTableExists()) {
            return Collections.emptyList();
        }

        int safePage = Math.max(1, page);
        int safeSize = Math.max(1, Math.min(size, 500));
        int offset = (safePage - 1) * safeSize;
        String orderBy = buildOrderBy(sort, dir);

        List<Object> params = new ArrayList<>();
        String searchClause = buildOverviewSearchClause(query, params);
        String sql = "SELECT matched, song_id, resolved_artist_id, song_title, artist_name, debut_week, last_week, peak_week, " +
                 "debut_position, weeks_on_chart, peak_position, weeks_at_peak, gender_name, weeks_at_top1, weeks_at_top5, weeks_at_top10, " +
                 "weeks_at_top20, weeks_at_top50, weeks_at_top100 " +
                 "FROM billboard_hot100_debut overview" + searchClause + " ORDER BY " + orderBy + " LIMIT ? OFFSET ?";
        params.add(safeSize);
        params.add(offset);

        return jdbcTemplate.query(sql, this::mapOverviewRow, params.toArray());
    }

    public int countAlbumOverviewRows(String query) {
        return getAllAlbumOverviewRows(query).size();
    }

    public List<ChartAlbumOverviewRowDTO> getAlbumOverviewRows(int page, int size, String sort, String dir, String query) {
        List<ChartAlbumOverviewRowDTO> rows = getAllAlbumOverviewRows(query);
        rows.sort(buildAlbumOverviewComparator(sort, dir));
        return paginate(rows, page, size);
    }

    public int countArtistOverviewRows(String query) {
        return getAllArtistOverviewRows(query).size();
    }

    public List<ChartArtistOverviewRowDTO> getArtistOverviewRows(int page, int size, String sort, String dir, String query) {
        List<ChartArtistOverviewRowDTO> rows = getAllArtistOverviewRows(query);
        rows.sort(buildArtistOverviewComparator(sort, dir));
        return paginate(rows, page, size);
    }

    public int countOverviewRows(String query) {
        ensureDebutSnapshot();
        if (!debutTableExists()) {
            return 0;
        }
        List<Object> params = new ArrayList<>();
        String sql = "SELECT COUNT(*) FROM billboard_hot100_debut overview" + buildOverviewSearchClause(query, params);
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
            case "firstWeek" -> "debut_week " + safeDir + ", last_week DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
            default -> "debut_week DESC, last_week DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
        };
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

    private List<BillboardHot100OverviewRowDTO> getAllOverviewRows(String query) {
        ensureDebutSnapshot();
        if (!debutTableExists()) {
            return Collections.emptyList();
        }

        List<Object> params = new ArrayList<>();
        String searchClause = buildOverviewSearchClause(query, params);
        String sql = "SELECT matched, song_id, resolved_artist_id, song_title, artist_name, debut_week, last_week, peak_week, " +
            "debut_position, weeks_on_chart, peak_position, weeks_at_peak, gender_name, weeks_at_top1, weeks_at_top5, weeks_at_top10, " +
            "weeks_at_top20, weeks_at_top50, weeks_at_top100 " +
            "FROM billboard_hot100_debut overview" + searchClause +
            " ORDER BY debut_week DESC, last_week DESC, artist_name COLLATE NOCASE ASC, song_title COLLATE NOCASE ASC";
        return jdbcTemplate.query(sql, this::mapOverviewRow, params.toArray());
    }

    private BillboardHot100OverviewRowDTO mapOverviewRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
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
        row.setFirstWeek(rs.getString("debut_week"));
        int debutPosition = rs.getInt("debut_position");
        if (!rs.wasNull()) {
            row.setDebutPosition(debutPosition);
        }
        row.setLastWeek(rs.getString("last_week"));
        row.setPeakWeek(rs.getString("peak_week"));
        row.setWeeksOnChart(rs.getInt("weeks_on_chart"));
        row.setPeakPosition(rs.getInt("peak_position"));
        row.setWeeksAtPeak(rs.getInt("weeks_at_peak"));
        row.setGenderClass(toGenderClass(rs.getString("gender_name")));
        row.setWeeksAtTop1(rs.getInt("weeks_at_top1"));
        row.setWeeksAtTop5(rs.getInt("weeks_at_top5"));
        row.setWeeksAtTop10(rs.getInt("weeks_at_top10"));
        row.setWeeksAtTop20(rs.getInt("weeks_at_top20"));
        row.setWeeksAtTop50(rs.getInt("weeks_at_top50"));
        row.setWeeksAtTop100(rs.getInt("weeks_at_top100"));
        return row;
    }

    private List<ChartAlbumOverviewRowDTO> getAllAlbumOverviewRows(String query) {
        List<BillboardHot100OverviewRowDTO> songRows = getAllOverviewRows(query);
        Map<Integer, AlbumSongInfo> albumSongInfoBySongId = getAlbumSongInfoBySongId(songRows);
        Map<Integer, AlbumOverviewAccumulator> grouped = new LinkedHashMap<>();

        for (BillboardHot100OverviewRowDTO row : songRows) {
            if (row.getSongId() == null) {
                continue;
            }
            AlbumSongInfo albumSongInfo = albumSongInfoBySongId.get(row.getSongId());
            if (albumSongInfo == null) {
                continue;
            }
            AlbumOverviewAccumulator accumulator = grouped.computeIfAbsent(
                albumSongInfo.albumId,
                ignored -> new AlbumOverviewAccumulator(albumSongInfo.albumId, albumSongInfo.albumName, row.getResolvedArtistId(), row.getArtistName(), row.getGenderClass())
            );
            accumulator.accept(row);
        }

        List<ChartAlbumOverviewRowDTO> rows = new ArrayList<>();
        for (AlbumOverviewAccumulator accumulator : grouped.values()) {
            rows.add(accumulator.toRow());
        }
        return rows;
    }

    private List<ChartArtistOverviewRowDTO> getAllArtistOverviewRows(String query) {
        List<BillboardHot100OverviewRowDTO> songRows = getAllOverviewRows(query);
        Map<String, ArtistOverviewAccumulator> grouped = new LinkedHashMap<>();

        for (BillboardHot100OverviewRowDTO row : songRows) {
            boolean matched = row.getResolvedArtistId() != null;
            String key = matched
                ? "artist:" + row.getResolvedArtistId()
                : "raw:" + normalizeKeyPart(row.getArtistName());
            ArtistOverviewAccumulator accumulator = grouped.computeIfAbsent(
                key,
                ignored -> new ArtistOverviewAccumulator(matched, row.getResolvedArtistId(), row.getArtistName(), row.getGenderClass())
            );
            accumulator.accept(row);
        }

        List<ChartArtistOverviewRowDTO> rows = new ArrayList<>();
        for (ArtistOverviewAccumulator accumulator : grouped.values()) {
            rows.add(accumulator.toRow());
        }
        return rows;
    }

    private Comparator<ChartAlbumOverviewRowDTO> buildAlbumOverviewComparator(String sort, String dir) {
        Comparator<ChartAlbumOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(row -> safeLower(row.getArtistName()));
            case "album" -> Comparator.comparing(row -> safeLower(row.getAlbumName()));
            case "songs" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparingInt(row -> row.getHighestPeak() == null ? Integer.MAX_VALUE : row.getHighestPeak());
            case "numberOnes" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getNumberOneSongsCount);
            case "weeksAtNumberOne" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalSpanAtNumberOne);
            case "firstWeek" -> Comparator.comparing(row -> safeLower(row.getFirstDebutDate()));
            case "lastWeek" -> Comparator.comparing(row -> safeLower(row.getLastAppearanceDate()));
            case "weeks" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan);
            default -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan);
        };
        if (!"asc".equalsIgnoreCase(dir)) {
            comparator = comparator.reversed();
        }
        return comparator.thenComparing(row -> safeLower(row.getArtistName()))
            .thenComparing(row -> safeLower(row.getAlbumName()));
    }

    private Comparator<ChartArtistOverviewRowDTO> buildArtistOverviewComparator(String sort, String dir) {
        Comparator<ChartArtistOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(row -> safeLower(row.getArtistName()));
            case "songs" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparingInt(row -> row.getHighestPeak() == null ? Integer.MAX_VALUE : row.getHighestPeak());
            case "numberOnes" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getNumberOneSongsCount);
            case "weeksAtNumberOne" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalSpanAtNumberOne);
            case "firstWeek" -> Comparator.comparing(row -> safeLower(row.getFirstDebutDate()));
            case "lastWeek" -> Comparator.comparing(row -> safeLower(row.getLastAppearanceDate()));
            case "weeks" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan);
            default -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan);
        };
        if (!"asc".equalsIgnoreCase(dir)) {
            comparator = comparator.reversed();
        }
        return comparator.thenComparing(row -> safeLower(row.getArtistName()));
    }

    private <T> List<T> paginate(List<T> rows, int page, int size) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }
        int safeSize = Math.max(1, Math.min(size, 500));
        int totalPages = Math.max(1, (int) Math.ceil(rows.size() / (double) safeSize));
        int safePage = Math.max(1, Math.min(page, totalPages));
        int fromIndex = Math.min((safePage - 1) * safeSize, rows.size());
        int toIndex = Math.min(fromIndex + safeSize, rows.size());
        return rows.subList(fromIndex, toIndex);
    }

    private Map<Integer, AlbumSongInfo> getAlbumSongInfoBySongId(List<BillboardHot100OverviewRowDTO> rows) {
        Set<Integer> songIds = new HashSet<>();
        for (BillboardHot100OverviewRowDTO row : rows) {
            if (row.getSongId() != null) {
                songIds.add(row.getSongId());
            }
        }
        if (songIds.isEmpty()) {
            return Map.of();
        }

        List<Integer> orderedSongIds = new ArrayList<>(songIds);
        String placeholders = String.join(",", Collections.nCopies(orderedSongIds.size(), "?"));
        String sql =
            "SELECT s.id AS song_id, al.id AS album_id, al.name AS album_name " +
            "FROM Song s " +
            "JOIN Album al ON al.id = s.album_id " +
            "WHERE s.id IN (" + placeholders + ")";
        Map<Integer, AlbumSongInfo> result = new HashMap<>();
        List<AlbumSongInfoRow> albumRows = jdbcTemplate.query(
            sql,
            (rs, rowNum) -> new AlbumSongInfoRow(
                rs.getInt("song_id"),
                rs.getInt("album_id"),
                rs.getString("album_name")
            ),
            orderedSongIds.toArray()
        );
        for (AlbumSongInfoRow row : albumRows) {
            result.put(row.songId(), new AlbumSongInfo(row.albumId(), row.albumName()));
        }
        return result;
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

    public Map<String, Object> importAllCharts() {
        try (Connection connection = dataSource.getConnection()) {
            BillboardHot100ImportSupport.ImportReport report = BillboardHot100ImportSupport.importAllCharts(connection);
            BillboardHot100ImportSupport.rebuildDebutTable(connection);
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
            BillboardHot100ImportSupport.rebuildDebutTable(connection);
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
            BillboardHot100ImportSupport.rebuildDebutTable(connection);
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

        rebuildDebutTable();

        return updatedRows;
    }

    public Map<String, Object> matchRawGroup(String rawArtist, String rawSong, Integer songId) {
        int updated = jdbcTemplate.update(
            "UPDATE billboard_hot100_entry SET song_id = ? " +
            "WHERE LOWER(TRIM(artist_name)) = LOWER(TRIM(?)) " +
            "  AND LOWER(TRIM(song_title)) = LOWER(TRIM(?))",
            songId, rawArtist, rawSong
        );

        rebuildDebutTable();

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

    public List<Map<String, Object>> getFallOffsForDate(String chartDate) {
        return getFallOffsForDate(chartDate, null);
    }

    public List<Map<String, Object>> getFallOffsForDate(String chartDate, List<Map<String, Object>> currentEntries) {
        if (chartDate == null || !tableExists()) {
            return List.of();
        }

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

    private String buildRecapIdentityKey(Map<String, Object> entry) {
        Object songId = entry.get("songId");
        if (songId != null) {
            return "song:" + songId;
        }
        return "raw:" + normalizeIdentityPart(entry.get("artistName")) + "||" + normalizeIdentityPart(entry.get("songTitle"));
    }

    private String normalizeIdentityPart(Object value) {
        return value == null ? "" : value.toString().trim().toLowerCase(Locale.ROOT);
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

    private boolean debutTableExists() {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND lower(name) = 'billboard_hot100_debut'",
            Integer.class
        );
        return count != null && count > 0;
    }

    private void ensureDebutSnapshot() {
        if (!tableExists()) {
            return;
        }

        if (!debutTableExists()) {
            rebuildDebutTable();
            return;
        }

        Integer entryCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM billboard_hot100_entry",
            Integer.class
        );
        Integer debutCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM billboard_hot100_debut",
            Integer.class
        );
        if ((entryCount != null && entryCount > 0) && (debutCount == null || debutCount == 0)) {
            rebuildDebutTable();
        }
    }

    private void rebuildDebutTable() {
        try (Connection connection = dataSource.getConnection()) {
            BillboardHot100ImportSupport.rebuildDebutTable(connection);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to rebuild Billboard Hot 100 debut rows", e);
        }
    }

    /**
     * Map gender name to CSS gender class for styling.
     * IMPORTANT: This application's gender_id mapping is:
     *   - genderId 1 = FEMALE artists (pink/magenta color)
     *   - genderId 2 = MALE artists (blue color)
     * This is NOT intuitive order but matches the original schema.
     */
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

        private void accept(BillboardHot100OverviewRowDTO songRow) {
            if (songRow.getSongId() != null && songIds.add(songRow.getSongId())) {
                row.setChartedSongsCount(songIds.size());
            }
            row.setTotalChartSpan(row.getTotalChartSpan() + songRow.getWeeksOnChart());
            if (row.getHighestPeak() == null || songRow.getPeakPosition() < row.getHighestPeak()) {
                row.setHighestPeak(songRow.getPeakPosition());
            }
            if (songRow.getPeakPosition() == 1 && songRow.getSongId() != null && numberOneSongIds.add(songRow.getSongId())) {
                row.setNumberOneSongsCount(numberOneSongIds.size());
                numberOneSongTitles.putIfAbsent("song:" + songRow.getSongId(), songRow.getSongTitle());
            }
            row.setTotalSpanAtNumberOne(row.getTotalSpanAtNumberOne() + songRow.getWeeksAtTop1());
            row.setFirstDebutDate(minDate(row.getFirstDebutDate(), songRow.getFirstWeek()));
            row.setLastAppearanceDate(maxDate(row.getLastAppearanceDate(), songRow.getLastWeek()));
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

        private void accept(BillboardHot100OverviewRowDTO songRow) {
            row.setArtistName(pickPreferredDisplayValue(row.getArtistName(), songRow.getArtistName()));
            if (row.getResolvedArtistId() == null) {
                row.setResolvedArtistId(songRow.getResolvedArtistId());
            }
            if (row.getGenderClass() == null) {
                row.setGenderClass(songRow.getGenderClass());
            }

            String songKey = songRow.getSongId() != null
                ? "song:" + songRow.getSongId()
                : "raw:" + normalizeKeyPart(songRow.getSongTitle());
            if (songKeys.add(songKey)) {
                row.setChartedSongsCount(songKeys.size());
            }
            row.setTotalChartSpan(row.getTotalChartSpan() + songRow.getWeeksOnChart());
            if (row.getHighestPeak() == null || songRow.getPeakPosition() < row.getHighestPeak()) {
                row.setHighestPeak(songRow.getPeakPosition());
            }
            if (songRow.getPeakPosition() == 1 && numberOneSongKeys.add(songKey)) {
                row.setNumberOneSongsCount(numberOneSongKeys.size());
                numberOneSongTitles.putIfAbsent(songKey, songRow.getSongTitle());
            }
            row.setTotalSpanAtNumberOne(row.getTotalSpanAtNumberOne() + songRow.getWeeksAtTop1());
            row.setFirstDebutDate(minDate(row.getFirstDebutDate(), songRow.getFirstWeek()));
            row.setLastAppearanceDate(maxDate(row.getLastAppearanceDate(), songRow.getLastWeek()));
        }

        private ChartArtistOverviewRowDTO toRow() {
            row.setNumberOneSongTitles(new ArrayList<>(numberOneSongTitles.values()));
            return row;
        }
    }

    public List<Map<String, Object>> getChartedSongsByAlbumId(int albumId) {
        if (!tableExists()) return Collections.emptyList();
        String sql =
            "WITH stats AS ( " +
            "    SELECT s.id AS song_id, s.name AS song_name, s.track_number, " +
            "           COUNT(e.id) AS weeks_on_chart, MIN(e.position) AS peak_position, " +
            "           MIN(e.chart_date) AS debut_date, " +
            "           (SELECT MIN(e2.chart_date) FROM billboard_hot100_entry e2 WHERE e2.song_id = s.id AND e2.position = MIN(e.position)) AS peak_date " +
            "    FROM billboard_hot100_entry e " +
            "    JOIN song s ON e.song_id = s.id " +
            "    WHERE s.album_id = ? " +
            "    GROUP BY s.id, s.name, s.track_number " +
            ") " +
            "SELECT song_id, song_name, track_number, weeks_on_chart, peak_position, debut_date, peak_date, " +
            "       (SELECT COUNT(*) FROM billboard_hot100_entry e2 WHERE e2.song_id = stats.song_id AND e2.position = stats.peak_position) AS weeks_at_peak " +
            "FROM stats WHERE weeks_on_chart > 0 " +
            "ORDER BY peak_position ASC, weeks_on_chart DESC, track_number ASC";
        return jdbcTemplate.queryForList(sql, albumId);
    }

    public List<Map<String, Object>> getChartedSongsByArtistId(int artistId) {
        if (!tableExists()) return Collections.emptyList();
        String sql =
            "WITH stats AS ( " +
            "    SELECT s.id AS song_id, s.name AS song_name, " +
            "           COUNT(e.id) AS weeks_on_chart, MIN(e.position) AS peak_position, " +
            "           MIN(e.chart_date) AS debut_date, " +
            "           (SELECT MIN(e2.chart_date) FROM billboard_hot100_entry e2 WHERE e2.song_id = s.id AND e2.position = MIN(e.position)) AS peak_date " +
            "    FROM billboard_hot100_entry e " +
            "    JOIN song s ON e.song_id = s.id " +
            "    WHERE s.artist_id = ? " +
            "    GROUP BY s.id, s.name " +
            ") " +
            "SELECT song_id, song_name, weeks_on_chart, peak_position, debut_date, peak_date, " +
            "       (SELECT COUNT(*) FROM billboard_hot100_entry e2 WHERE e2.song_id = stats.song_id AND e2.position = stats.peak_position) AS weeks_at_peak " +
            "FROM stats WHERE weeks_on_chart > 0 " +
            "ORDER BY peak_position ASC, weeks_on_chart DESC, song_name ASC";
        return jdbcTemplate.queryForList(sql, artistId);
    }

    private record AlbumSongInfo(Integer albumId, String albumName) {
    }

    private record AlbumSongInfoRow(Integer songId, Integer albumId, String albumName) {
    }
}