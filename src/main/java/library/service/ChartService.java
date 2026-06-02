package library.service;

import library.dto.*;
import library.entity.Chart;
import library.entity.ChartEntry;
import library.repository.ChartEntryRepository;
import library.repository.ChartRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class ChartService {
    
    private static final int TOP_SONGS_COUNT = 20;
    private static final int TOP_ALBUMS_COUNT = 10;
    
    private final ChartRepository chartRepository;
    private final ChartEntryRepository chartEntryRepository;
    private final JdbcTemplate jdbcTemplate;
    private final ItunesService itunesService;
    private final AppConfigService appConfigService;
    private final SongLinkService songLinkService;
    
    // Progress tracking for bulk generation
    private final ConcurrentHashMap<String, ChartGenerationProgressDTO> generationProgress = new ConcurrentHashMap<>();
    
    public ChartService(ChartRepository chartRepository, ChartEntryRepository chartEntryRepository, JdbcTemplate jdbcTemplate, ItunesService itunesService,
                        AppConfigService appConfigService, SongLinkService songLinkService) {
        this.chartRepository = chartRepository;
        this.chartEntryRepository = chartEntryRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.itunesService = itunesService;
        this.appConfigService = appConfigService;
        this.songLinkService = songLinkService;
    }
    
    /**
     * Get set of period keys that already have charts for a given type.
     */
    public Set<String> getExistingChartPeriodKeys(String chartType) {
        return chartRepository.findAllPeriodKeysByChartType(chartType);
    }
    
    /**
     * Check if a chart exists for a given type and period.
     */
    public boolean chartExists(String chartType, String periodKey) {
        return chartRepository.existsByChartTypeAndPeriodKey(chartType, periodKey);
    }
    
    /**
     * Get the latest chart for a given type.
     */
    public Optional<Chart> getLatestChart(String chartType) {
        List<Chart> charts = chartRepository.findLatestByChartType(chartType);
        return charts.isEmpty() ? Optional.empty() : Optional.of(charts.get(0));
    }
    
    /**
     * Get the latest weekly chart for a given type.
     */
    public Optional<Chart> getLatestWeeklyChart(String chartType) {
        return chartRepository.findLatestWeeklyChart(chartType);
    }

    /**
     * Get a weekly chart by type and period key.
     */
    public Optional<Chart> getChart(String chartType, String periodKey) {
        return chartRepository.findByChartTypeAndPeriodKey(chartType, periodKey);
    }

    /**
     * Get the previous weekly chart for navigation.
     */
    public Optional<Chart> getPreviousChart(String chartType, String periodKey) {
        return chartRepository.findPreviousChart(chartType, periodKey);
    }

    /**
     * Get the next weekly chart for navigation.
     */
    public Optional<Chart> getNextChart(String chartType, String periodKey) {
        return chartRepository.findNextChart(chartType, periodKey);
    }

    /**
     * Format a weekly period key into its display date range.
     */
    public String formatPeriodKey(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        return formatDateRange(dateRange[0].toString(), dateRange[1].toString());
    }

    /**
     * Returns true only after the weekly period has fully passed.
     */
    public boolean isWeekComplete(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        return dateRange[1].isBefore(LocalDate.now());
    }

    /**
     * Get the next weekly period key after the provided one.
     */
    public String getNextWeekPeriodKey(String periodKey) {
        LocalDate nextWeekStart = parsePeriodKeyToDateRange(periodKey)[0].plusWeeks(1);
        return jdbcTemplate.queryForObject("SELECT strftime('%Y-W%W', ?)", String.class, nextWeekStart.toString());
    }

    /**
     * Get weekly song chart with full statistics for display.
     */
    public List<ChartEntryDTO> getWeeklyChartWithStats(String periodKey) {
        Optional<Chart> chartOpt = chartRepository.findByChartTypeAndPeriodKey("song", periodKey);
        if (chartOpt.isEmpty()) {
            return Collections.emptyList();
        }

        Chart chart = chartOpt.get();
        List<Object[]> rawEntries = chartEntryRepository.findEntriesWithSongDetailsRaw(chart.getId());

        Optional<Chart> prevChartOpt = chartRepository.findPreviousChart("song", periodKey);
        Map<Integer, Integer> lastWeekPositions = new HashMap<>();
        Map<Integer, Integer> lastWeekPlayCounts = new HashMap<>();

        if (prevChartOpt.isPresent()) {
            List<Object[]> prevEntries = chartEntryRepository.findEntriesWithSongDetailsRaw(prevChartOpt.get().getId());
            for (Object[] row : prevEntries) {
                Integer songId = (Integer) row[3];
                Integer position = (Integer) row[2];
                Integer playCount = (Integer) row[5];
                lastWeekPositions.put(songId, position);
                lastWeekPlayCounts.put(songId, playCount);
            }
        }

        List<ChartEntryDTO> result = new ArrayList<>();
        for (int currentChartIndex = 0; currentChartIndex < rawEntries.size(); currentChartIndex++) {
            Object[] row = rawEntries.get(currentChartIndex);

            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition((Integer) row[2]);
            dto.setSongId((Integer) row[3]);
            dto.setAlbumId(row[4] != null ? (Integer) row[4] : null);
            dto.setPlayCount((Integer) row[5]);
            dto.setSongName((String) row[6]);
            dto.setArtistName((String) row[7]);
            dto.setHasImage(((Number) row[8]).intValue() == 1);
            dto.setArtistId((Integer) row[9]);
            dto.setAlbumName((String) row[10]);
            dto.setGenderId(row[11] != null ? ((Number) row[11]).intValue() : null);
            dto.setAlbumHasImage(((Number) row[12]).intValue() == 1);
            dto.setGenreName(row[13] != null ? (String) row[13] : null);

            Integer songId = dto.getSongId();
            if (lastWeekPositions.containsKey(songId)) {
                dto.setLastWeekPosition(lastWeekPositions.get(songId));
                dto.setLastWeekPlayCount(lastWeekPlayCounts.get(songId));
            } else {
                boolean wasOnPreviousChart = checkIfSongWasOnPreviousChart(songId, periodKey, prevChartOpt.map(Chart::getPeriodKey).orElse(null));
                if (wasOnPreviousChart) {
                    dto.setLastWeekPosition(-1);
                }
            }

            calculateChartRunStats(dto, songId, periodKey, currentChartIndex);
            result.add(dto);
        }

        if (appConfigService.isCombineLinkedSongsEnabled()) {
            applySongPlayBreakdowns(result, periodKey);
        }

        return result;
    }

    /**
     * Generate both weekly song and album charts for a period.
     */
    @Transactional
    public void generateWeeklyCharts(String periodKey) {
        generateWeeklySongChart(periodKey);
        generateWeeklyAlbumChart(periodKey);
    }

    /**
     * Generate or replace the weekly song chart for a period.
     */
    @Transactional
    public void generateWeeklySongChart(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        Chart chart = chartRepository.findByChartTypeAndPeriodKey("song", periodKey)
            .orElseGet(() -> chartRepository.save(new Chart("song", periodKey, dateRange[0], dateRange[1])));

        chart.setPeriodStartDate(dateRange[0].toString());
        chart.setPeriodEndDate(dateRange[1].toString());
        chart.setPeriodType("weekly");
        chart.setIsFinalized(true);
        chart = chartRepository.save(chart);
        Integer chartId = chart.getId();

        chartEntryRepository.deleteByChartId(chartId);

        List<ChartEntry> entries = getWeeklySongChartPreview(periodKey).stream()
            .filter(entry -> entry.getSongId() != null)
            .map(entry -> ChartEntry.forSong(chartId, entry.getPosition(), entry.getSongId(), entry.getPlayCount()))
            .toList();

        if (!entries.isEmpty()) {
            chartEntryRepository.saveAll(entries);
        }
    }

    /**
     * Generate or replace the weekly album chart for a period.
     */
    @Transactional
    public void generateWeeklyAlbumChart(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        Chart chart = chartRepository.findByChartTypeAndPeriodKey("album", periodKey)
            .orElseGet(() -> chartRepository.save(new Chart("album", periodKey, dateRange[0], dateRange[1])));

        chart.setPeriodStartDate(dateRange[0].toString());
        chart.setPeriodEndDate(dateRange[1].toString());
        chart.setPeriodType("weekly");
        chart.setIsFinalized(true);
        chart = chartRepository.save(chart);
        Integer chartId = chart.getId();

        chartEntryRepository.deleteByChartId(chartId);

        List<ChartEntry> entries = getWeeklyAlbumChartPreview(periodKey).stream()
            .filter(entry -> entry.getAlbumId() != null)
            .map(entry -> ChartEntry.forAlbum(chartId, entry.getPosition(), entry.getAlbumId(), entry.getPlayCount()))
            .toList();

        if (!entries.isEmpty()) {
            chartEntryRepository.saveAll(entries);
        }
    }

    public List<ChartEntryDTO> getWeeklyChartFallOffs(String periodKey, List<ChartEntryDTO> currentEntries) {
        Optional<Chart> prevChartOpt = chartRepository.findPreviousChart("song", periodKey);
        if (prevChartOpt.isEmpty()) {
            return Collections.emptyList();
        }

        Set<Integer> currentSongIds = currentEntries.stream()
            .map(ChartEntryDTO::getSongId)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        return getWeeklyChartWithStats(prevChartOpt.get().getPeriodKey()).stream()
            .filter(entry -> entry.getSongId() != null)
            .filter(entry -> !currentSongIds.contains(entry.getSongId()))
            .toList();
    }

    public List<ChartEntryDTO> getWeeklyAlbumChartFallOffs(String periodKey, List<ChartEntryDTO> currentEntries) {
        Optional<Chart> prevChartOpt = chartRepository.findPreviousChart("album", periodKey);
        if (prevChartOpt.isEmpty()) {
            return Collections.emptyList();
        }

        Set<Integer> currentAlbumIds = currentEntries.stream()
            .map(ChartEntryDTO::getAlbumId)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        return getWeeklyAlbumChartWithStats(prevChartOpt.get().getPeriodKey()).stream()
            .filter(entry -> entry.getAlbumId() != null)
            .filter(entry -> !currentAlbumIds.contains(entry.getAlbumId()))
            .toList();
    }
    
    /**
     * Get weekly album chart with full statistics for display.
     */
    public List<ChartEntryDTO> getWeeklyAlbumChartWithStats(String periodKey) {
        Optional<Chart> chartOpt = chartRepository.findByChartTypeAndPeriodKey("album", periodKey);
        if (chartOpt.isEmpty()) {
            return Collections.emptyList();
        }
        
        Chart chart = chartOpt.get();
        
        // Get chart entries with album details
        List<Object[]> rawEntries = chartEntryRepository.findEntriesWithAlbumDetailsRaw(chart.getId());
        
        // Get previous chart for last week comparison
        Optional<Chart> prevChartOpt = chartRepository.findPreviousChart("album", periodKey);
        Map<Integer, Integer> lastWeekPositions = new HashMap<>();
        Map<Integer, Integer> lastWeekPlayCounts = new HashMap<>();
        
        if (prevChartOpt.isPresent()) {
            List<Object[]> prevEntries = chartEntryRepository.findEntriesWithAlbumDetailsRaw(prevChartOpt.get().getId());
            for (Object[] row : prevEntries) {
                Integer albumId = (Integer) row[4];
                Integer position = (Integer) row[2];
                Integer playCount = (Integer) row[5];
                lastWeekPositions.put(albumId, position);
                lastWeekPlayCounts.put(albumId, playCount);
            }
        }
        
        List<ChartEntryDTO> result = new ArrayList<>();
        
        for (Object[] row : rawEntries) {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition((Integer) row[2]);
            dto.setAlbumId((Integer) row[4]);
            dto.setPlayCount((Integer) row[5]);
            dto.setAlbumName((String) row[6]);
            dto.setArtistName((String) row[7]);
            dto.setHasImage(((Number) row[8]).intValue() == 1);
            dto.setArtistId((Integer) row[9]);
            dto.setGenderId(row[10] != null ? ((Number) row[10]).intValue() : null);
            dto.setGenreName(row[11] != null ? (String) row[11] : null);
            
            Integer albumId = dto.getAlbumId();
            
            // Last week position
            if (lastWeekPositions.containsKey(albumId)) {
                dto.setLastWeekPosition(lastWeekPositions.get(albumId));
                dto.setLastWeekPlayCount(lastWeekPlayCounts.get(albumId));
            } else {
                // Check if this is a re-entry
                boolean wasOnPreviousChart = checkIfAlbumWasOnPreviousChart(albumId, periodKey, prevChartOpt.map(Chart::getPeriodKey).orElse(null));
                if (wasOnPreviousChart) {
                    dto.setLastWeekPosition(-1); // -1 indicates re-entry
                }
            }
            
            // Calculate peak, times at peak, and weeks on chart for albums
            calculateAlbumChartRunStats(dto, albumId, periodKey);
            
            result.add(dto);
        }

        return result;
    }

    /**
     * Get a PREVIEW of the weekly song chart for an in-progress week (chart not yet generated).
     * This shows what the chart would look like based on plays so far.
     * Only shows basic info: artist, album, song, plays - no chart history stats.
     */
    public List<ChartEntryDTO> getWeeklySongChartPreview(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate startDate = dateRange[0];
        LocalDate endDate = dateRange[1];

        if (appConfigService.isCombineLinkedSongsEnabled()) {
            List<ChartEntryDTO> preview = getCombinedWeeklySongChartPreview(startDate, endDate);
            applySongPlayBreakdowns(preview, startDate, endDate);
            return preview;
        }

        // Query top 20 songs for this period with song/artist/album details
        String sql = """
            SELECT 
                s.id as song_id,
                s.album_id,
                s.name as song_name,
                ar.id as artist_id,
                ar.name as artist_name,
                al.name as album_name,
                g.id as gender_id,
                COUNT(*) as play_count,
                MAX(CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                MAX(CASE WHEN al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id) THEN 1 ELSE 0 END) as album_has_image,
                (SELECT gn.name FROM Genre gn WHERE gn.id = COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id)) as genre_name
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            WHERE DATE(p.play_date) >= ? AND DATE(p.play_date) <= ?
              AND p.song_id IS NOT NULL
            GROUP BY s.id
            ORDER BY play_count DESC, MAX(p.play_date) ASC
            LIMIT ?
            """;

        List<ChartEntryDTO> result = new ArrayList<>();
        int[] position = {1};

        jdbcTemplate.query(sql, rs -> {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition(position[0]++);
            dto.setSongId(rs.getInt("song_id"));
            dto.setAlbumId(rs.getObject("album_id") != null ? rs.getInt("album_id") : null);
            dto.setSongName(rs.getString("song_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setAlbumName(rs.getString("album_name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            dto.setAlbumHasImage(rs.getInt("album_has_image") == 1);
            dto.setGenreName(rs.getString("genre_name"));
            result.add(dto);
        }, startDate.toString(), endDate.toString(), TOP_SONGS_COUNT);

        return result;
    }

    private List<ChartEntryDTO> getCombinedWeeklySongChartPreview(LocalDate startDate, LocalDate endDate) {
        String sql = """
            WITH play_rows AS (
                SELECT
                    COALESCE(slgm.group_id, -s.id) as entity_key,
                    s.id as song_id,
                    s.album_id,
                    s.name as song_name,
                    ar.id as artist_id,
                    ar.name as artist_name,
                    al.name as album_name,
                    ar.gender_id as gender_id,
                    p.play_date,
                    CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END as has_image,
                    CASE WHEN al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id) THEN 1 ELSE 0 END as album_has_image,
                    (SELECT gn.name FROM Genre gn WHERE gn.id = COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id)) as genre_name
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN song_link_group_member slgm ON slgm.song_id = s.id
                WHERE DATE(p.play_date) >= ? AND DATE(p.play_date) <= ?
                  AND p.song_id IS NOT NULL
            ),
            group_stats AS (
                SELECT entity_key, COUNT(*) as play_count, MAX(play_date) as last_play
                FROM play_rows
                GROUP BY entity_key
            ),
            candidates AS (
                SELECT pr.*,
                       gs.play_count,
                       gs.last_play,
                       ROW_NUMBER() OVER (
                           PARTITION BY pr.entity_key
                           ORDER BY
                               CASE WHEN lower(pr.song_name) GLOB '*remix*'
                                      OR lower(pr.song_name) GLOB '*demo*'
                                      OR lower(pr.song_name) GLOB '*alternate*'
                                      OR lower(pr.song_name) GLOB '*version*'
                                      OR lower(pr.song_name) GLOB '*live*'
                                      OR lower(pr.song_name) GLOB '*acoustic*'
                                      OR lower(pr.song_name) GLOB '*remaster*'
                                      OR lower(pr.song_name) GLOB '*edit*'
                                    THEN 1 ELSE 0 END,
                               LENGTH(pr.song_name),
                               pr.song_id
                       ) as representative_rank
                FROM play_rows pr
                INNER JOIN group_stats gs ON gs.entity_key = pr.entity_key
            )
            SELECT song_id, album_id, song_name, artist_id, artist_name, album_name, gender_id,
                   play_count, has_image, album_has_image, genre_name
            FROM candidates
            WHERE representative_rank = 1
            ORDER BY play_count DESC, last_play ASC
            LIMIT ?
            """;

        List<ChartEntryDTO> result = new ArrayList<>();
        int[] position = {1};
        jdbcTemplate.query(sql, rs -> {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition(position[0]++);
            dto.setSongId(rs.getInt("song_id"));
            dto.setAlbumId(rs.getObject("album_id") != null ? rs.getInt("album_id") : null);
            dto.setSongName(rs.getString("song_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setAlbumName(rs.getString("album_name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            dto.setAlbumHasImage(rs.getInt("album_has_image") == 1);
            dto.setGenreName(rs.getString("genre_name"));
            result.add(dto);
        }, startDate.toString(), endDate.toString(), TOP_SONGS_COUNT);

        return result;
    }

    private void applySongPlayBreakdowns(List<ChartEntryDTO> entries, String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        applySongPlayBreakdowns(entries, dateRange[0], dateRange[1]);
    }

    private void applySongPlayBreakdowns(List<ChartEntryDTO> entries, LocalDate startDate, LocalDate endDate) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        for (ChartEntryDTO entry : entries) {
            if (entry == null || entry.getSongId() == null) {
                continue;
            }
            List<String> playBreakdownItems = getSongPlayBreakdownItems(entry.getSongId(), entry.getArtistName(), startDate, endDate);
            entry.setPlayBreakdownItems(playBreakdownItems);
            entry.setPlayBreakdown(playBreakdownItems.isEmpty() ? null : String.join("\n", playBreakdownItems));
        }
    }

    private List<String> getSongPlayBreakdownItems(Integer songId, String representativeArtistName, LocalDate startDate, LocalDate endDate) {
        List<Integer> linkedSongIds = songLinkService.getLinkedSongIds(songId);
        if (linkedSongIds == null || linkedSongIds.size() <= 1) {
            return List.of();
        }

        String sql = """
            SELECT
                s.id as song_id,
                s.name as song_name,
                ar.name as artist_name,
                al.name as album_name,
                COUNT(*) as play_count
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE p.song_id IN (%s)
              AND DATE(p.play_date) >= ?
              AND DATE(p.play_date) <= ?
            GROUP BY s.id, s.name, ar.name, al.name
            ORDER BY play_count DESC, lower(s.name) ASC, s.id ASC
            """.formatted(String.join(",", linkedSongIds.stream().map(id -> "?").toList()));

        List<Object> params = new ArrayList<>(linkedSongIds);
        params.add(startDate.toString());
        params.add(endDate.toString());

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        if (rows.size() <= 1) {
            return List.of();
        }

        long distinctArtists = rows.stream()
                .map(row -> (String) row.get("artist_name"))
                .filter(Objects::nonNull)
                .distinct()
                .count();
        boolean includeArtist = distinctArtists > 1;

        return rows.stream()
                .map(row -> formatSongPlayBreakdownLine(row, representativeArtistName, includeArtist))
            .toList();
    }

    private String formatSongPlayBreakdownLine(Map<String, Object> row, String representativeArtistName, boolean includeArtist) {
        String songName = row.get("song_name") != null ? row.get("song_name").toString() : "Unknown song";
        String artistName = row.get("artist_name") != null ? row.get("artist_name").toString() : null;
        String albumName = row.get("album_name") != null ? row.get("album_name").toString() : null;
        Number playCount = (Number) row.get("play_count");
        StringBuilder label = new StringBuilder();
        if (includeArtist && artistName != null && !artistName.isBlank()) {
            label.append(artistName).append(" - ");
        } else if (representativeArtistName != null && artistName != null && !artistName.equalsIgnoreCase(representativeArtistName)) {
            label.append(artistName).append(" - ");
        }
        label.append(songName);
        if (albumName != null && !albumName.isBlank()) {
            label.append(" (").append(albumName).append(")");
        }
        label.append(": ")
                .append(playCount != null ? String.format(Locale.US, "%,d", playCount.intValue()) : "0")
                .append(" plays");
        return label.toString();
    }

    private List<String> buildLinkedSongWeekBreakdownItems(Integer songId) {
        if (!appConfigService.isCombineLinkedSongsEnabled() || songId == null) {
            return List.of();
        }

        List<Integer> linkedSongIds = songLinkService.getLinkedSongIds(songId);
        return buildLinkedSongWeekBreakdownItems(linkedSongIds);
    }

    private List<String> buildLinkedSongWeekBreakdownItems(List<Integer> linkedSongIds) {
        if (!appConfigService.isCombineLinkedSongsEnabled()) {
            return List.of();
        }

        if (linkedSongIds == null || linkedSongIds.size() <= 1) {
            return List.of();
        }

        String placeholders = String.join(",", linkedSongIds.stream().map(id -> "?").toList());
        String sql = """
            WITH chart_weeks AS (
                SELECT DISTINCT c.period_key, c.period_start_date, c.period_end_date
                FROM ChartEntry ce
                INNER JOIN Chart c ON ce.chart_id = c.id
                WHERE c.chart_type = 'song'
                  AND c.period_type = 'weekly'
                  AND ce.song_id IN (%s)
            )
            SELECT p.song_id,
                   s.name as song_name,
                   ar.name as artist_name,
                   al.name as album_name,
                   COUNT(DISTINCT cw.period_key) as contributed_weeks
            FROM chart_weeks cw
            INNER JOIN Play p ON DATE(p.play_date) >= cw.period_start_date
                              AND DATE(p.play_date) <= cw.period_end_date
            INNER JOIN Song s ON s.id = p.song_id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE p.song_id IN (%s)
            GROUP BY p.song_id, s.name, ar.name, al.name
            ORDER BY contributed_weeks DESC, lower(s.name) ASC, p.song_id ASC
            """.formatted(placeholders, placeholders);

        List<Object> params = new ArrayList<>();
        params.addAll(linkedSongIds);
        params.addAll(linkedSongIds);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, params.toArray());
        if (rows.size() <= 1) {
            return List.of();
        }

        long distinctArtists = rows.stream()
                .map(row -> (String) row.get("artist_name"))
                .filter(Objects::nonNull)
                .distinct()
                .count();
        boolean includeArtist = distinctArtists > 1;

        return rows.stream()
                .map(row -> formatLinkedSongWeekBreakdownLine(row, includeArtist))
                .filter(item -> item != null && !item.isBlank())
                .toList();
    }

    private CombinedSongChartHistoryMetrics getCombinedSongChartHistoryMetrics(List<Integer> linkedSongIds) {
        if (linkedSongIds == null || linkedSongIds.isEmpty()) {
            return new CombinedSongChartHistoryMetrics(0, 0, 0, null, null, null, null);
        }

        String placeholders = String.join(",", linkedSongIds.stream().map(id -> "?").toList());
        String sql = """
            SELECT c.period_key,
                   c.period_start_date,
                   c.period_end_date,
                   MIN(ce.position) as best_position
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE c.chart_type = 'song'
              AND c.period_type = 'weekly'
              AND ce.song_id IN (%s)
            GROUP BY c.period_key, c.period_start_date, c.period_end_date
            ORDER BY c.period_start_date ASC
            """.formatted(placeholders);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, linkedSongIds.toArray());
        if (rows.isEmpty()) {
            return new CombinedSongChartHistoryMetrics(0, 0, 0, null, null, null, null);
        }

        int peakPosition = rows.stream()
                .map(row -> ((Number) row.get("best_position")).intValue())
                .min(Integer::compareTo)
                .orElse(0);
        int weeksAtPeak = (int) rows.stream()
                .filter(row -> ((Number) row.get("best_position")).intValue() == peakPosition)
                .count();

        Map<String, Object> debutRow = rows.get(0);
        Map<String, Object> peakRow = rows.stream()
                .filter(row -> ((Number) row.get("best_position")).intValue() == peakPosition)
                .findFirst()
                .orElse(debutRow);

        return new CombinedSongChartHistoryMetrics(
                peakPosition,
                weeksAtPeak,
                rows.size(),
                formatPeakDate((String) peakRow.get("period_end_date")),
                (String) peakRow.get("period_key"),
                formatPeakDate((String) debutRow.get("period_end_date")),
                (String) debutRow.get("period_key")
        );
    }

    private ChartHistoryDTO copyChartHistory(ChartHistoryDTO source) {
        ChartHistoryDTO target = new ChartHistoryDTO();
        target.setId(source.getId());
        target.setName(source.getName());
        target.setArtistName(source.getArtistName());
        target.setPeakPosition(source.getPeakPosition());
        target.setWeeksAtPeak(source.getWeeksAtPeak());
        target.setTotalWeeks(source.getTotalWeeks());
        target.setChartType(source.getChartType());
        target.setReleaseDate(source.getReleaseDate());
        target.setPeakDate(source.getPeakDate());
        target.setDebutDate(source.getDebutDate());
        target.setPeakWeek(source.getPeakWeek());
        target.setDebutWeek(source.getDebutWeek());
        target.setHasImage(source.isHasImage());
        target.setAlbumId(source.getAlbumId());
        target.setFeaturedOn(source.isFeaturedOn());
        target.setFromGroup(source.isFromGroup());
        target.setSourceArtistId(source.getSourceArtistId());
        target.setSourceArtistName(source.getSourceArtistName());
        target.setInItunes(source.getInItunes());
        target.setTotalWeekBreakdownItems(source.getTotalWeekBreakdownItems());
        return target;
    }

    private record CombinedSongChartHistoryMetrics(
            int peakPosition,
            int weeksAtPeak,
            int totalWeeks,
            String peakDate,
            String peakWeek,
            String debutDate,
            String debutWeek
    ) {
    }

    private String formatLinkedSongWeekBreakdownLine(Map<String, Object> row, boolean includeArtist) {
        String songName = row.get("song_name") != null ? row.get("song_name").toString() : "Unknown song";
        String artistName = row.get("artist_name") != null ? row.get("artist_name").toString() : null;
        String albumName = row.get("album_name") != null ? row.get("album_name").toString() : null;
        Number contributedWeeks = (Number) row.get("contributed_weeks");

        StringBuilder label = new StringBuilder();
        if (includeArtist && artistName != null && !artistName.isBlank()) {
            label.append(artistName).append(" - ");
        }
        label.append(songName);
        if (albumName != null && !albumName.isBlank()) {
            label.append(" (").append(albumName).append(")");
        }
        label.append(": ")
                .append(contributedWeeks != null ? String.format(Locale.US, "%,d", contributedWeeks.intValue()) : "0")
                .append(" weeks");
        return label.toString();
    }

    /**
     * Get a PREVIEW of the weekly album chart for an in-progress week (chart not yet generated).
     * This shows what the chart would look like based on plays so far.
     * Only shows basic info: artist, album, plays - no chart history stats.
     */
    public List<ChartEntryDTO> getWeeklyAlbumChartPreview(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate startDate = dateRange[0];
        LocalDate endDate = dateRange[1];

        // Query top 10 albums for this period with album/artist details
        String sql = """
            SELECT 
                al.id as album_id,
                al.name as album_name,
                ar.id as artist_id,
                ar.name as artist_name,
                g.id as gender_id,
                COUNT(*) as play_count,
                MAX(CASE WHEN al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id) THEN 1 ELSE 0 END) as has_image,
                (SELECT gn.name FROM Genre gn WHERE gn.id = COALESCE(al.override_genre_id, ar.genre_id)) as genre_name
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            INNER JOIN Album al ON s.album_id = al.id
            INNER JOIN Artist ar ON al.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            WHERE DATE(p.play_date) >= ? AND DATE(p.play_date) <= ?
              AND p.song_id IS NOT NULL
              AND s.album_id IS NOT NULL
            GROUP BY al.id
            ORDER BY play_count DESC, MAX(p.play_date) ASC
            LIMIT ?
            """;

        List<ChartEntryDTO> result = new ArrayList<>();
        int[] position = {1};

        jdbcTemplate.query(sql, rs -> {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition(position[0]++);
            dto.setAlbumId(rs.getInt("album_id"));
            dto.setAlbumName(rs.getString("album_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            dto.setGenreName(rs.getString("genre_name"));
            result.add(dto);
        }, startDate.toString(), endDate.toString(), TOP_ALBUMS_COUNT);

        return result;
    }
    
    /**
     * Check if an album was on any chart before the previous chart (for re-entry detection).
     */
    private boolean checkIfAlbumWasOnPreviousChart(Integer albumId, String currentPeriodKey, String previousPeriodKey) {
        String sql = """
            SELECT COUNT(*) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ?
              AND c.chart_type = 'album'
              AND c.period_type = 'weekly'
              AND c.period_start_date < (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'album' AND period_type = 'weekly')
              AND c.period_key != COALESCE(?, '')
            """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, albumId, currentPeriodKey, previousPeriodKey);
        return count != null && count > 0;
    }
    
    /**
     * Calculate peak position, times at peak, and weeks on chart for an album.
     */
    private void calculateAlbumChartRunStats(ChartEntryDTO dto, Integer albumId, String currentPeriodKey) {
        String sql = """
            SELECT ce.position, c.period_key
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ?
              AND c.chart_type = 'album'
              AND c.period_type = 'weekly'
              AND c.period_start_date <= (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'album' AND period_type = 'weekly')
            ORDER BY c.period_start_date ASC
            """;
        
        List<Map<String, Object>> history = jdbcTemplate.queryForList(sql, albumId, currentPeriodKey);
        
        if (history.isEmpty()) {
            dto.setPeakPosition(dto.getPosition());
            dto.setTimesAtPeak(1);
            dto.setWeeksOnChart(1);
            return;
        }
        
        int peak = Integer.MAX_VALUE;
        int timesAtPeak = 0;
        int weeksOnChart = history.size();
        
        for (Map<String, Object> row : history) {
            int position = ((Number) row.get("position")).intValue();
            if (position < peak) {
                peak = position;
                timesAtPeak = 1;
            } else if (position == peak) {
                timesAtPeak++;
            }
        }
        
        dto.setPeakPosition(peak);
        dto.setTimesAtPeak(timesAtPeak);
        dto.setWeeksOnChart(weeksOnChart);
    }
    
    /**
     * Check if a song was on any chart before the previous chart (for re-entry detection).
     * Only looks at charts BEFORE the current one (by period_start_date) and excludes
     * the immediate previous chart.
     */
    private boolean checkIfSongWasOnPreviousChart(Integer songId, String currentPeriodKey, String previousPeriodKey) {
        String sql = """
            SELECT COUNT(*) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ?
              AND c.chart_type = 'song'
              AND c.period_type = 'weekly'
              AND c.period_start_date < (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'song' AND period_type = 'weekly')
              AND c.period_key != COALESCE(?, '')
            """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId, currentPeriodKey, previousPeriodKey);
        return count != null && count > 0;
    }
    
    /**
     * Calculate peak position, times at peak, and weeks on chart for a song.
     */
    private void calculateChartRunStats(ChartEntryDTO dto, Integer songId, String currentPeriodKey, int currentChartIndex) {
        String sql = """
            SELECT ce.position, c.period_key
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ?
              AND c.chart_type = 'song'
              AND c.period_type = 'weekly'
              AND c.period_start_date <= (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'song' AND period_type = 'weekly')
            ORDER BY c.period_start_date ASC
            """;
        
        List<Map<String, Object>> history = jdbcTemplate.queryForList(sql, songId, currentPeriodKey);
        
        if (history.isEmpty()) {
            dto.setPeakPosition(dto.getPosition());
            dto.setTimesAtPeak(1);
            dto.setWeeksOnChart(1);
            return;
        }
        
        int peak = Integer.MAX_VALUE;
        int timesAtPeak = 0;
        int weeksOnChart = history.size();
        
        for (Map<String, Object> row : history) {
            int position = ((Number) row.get("position")).intValue();
            if (position < peak) {
                peak = position;
                timesAtPeak = 1;
            } else if (position == peak) {
                timesAtPeak++;
            }
        }
        
        dto.setPeakPosition(peak);
        dto.setTimesAtPeak(timesAtPeak);
        dto.setWeeksOnChart(weeksOnChart);
    }
    
    /**
     * Get chart run history for a song (for the expandable detail view).
     */
    public ChartRunDTO getSongChartRun(Integer songId, String currentPeriodKey) {
        // Get song and artist names
        String nameSql = "SELECT s.name, a.name FROM Song s INNER JOIN Artist a ON s.artist_id = a.id WHERE s.id = ?";
        Map<String, Object> songInfo = jdbcTemplate.queryForMap(nameSql, songId);
        
        ChartRunDTO dto = new ChartRunDTO();
        dto.setSongId(songId);
        dto.setSongName((String) songInfo.get("name"));
        dto.setArtistName((String) songInfo.get("name")); // Second column in query
        
        // Get artist name properly
        List<Map<String, Object>> nameRows = jdbcTemplate.queryForList(
            "SELECT s.name as song_name, a.name as artist_name FROM Song s INNER JOIN Artist a ON s.artist_id = a.id WHERE s.id = ?", 
            songId);
        if (!nameRows.isEmpty()) {
            dto.setSongName((String) nameRows.get(0).get("song_name"));
            dto.setArtistName((String) nameRows.get(0).get("artist_name"));
        }
        
        String historySql = """
            SELECT c.period_key, ce.position, c.period_start_date, c.period_end_date
            FROM Chart c
            LEFT JOIN ChartEntry ce ON c.id = ce.chart_id AND ce.song_id = ?
            WHERE c.chart_type = 'song'
            AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            """;
        
        List<Map<String, Object>> history = jdbcTemplate.queryForList(historySql, songId);
        
        // Find the first week this song appeared and the current week
        int firstAppearanceIndex = -1;
        int currentIndex = -1;
        for (int i = 0; i < history.size(); i++) {
            Map<String, Object> row = history.get(i);
            if (row.get("position") != null && firstAppearanceIndex == -1) {
                firstAppearanceIndex = i;
            }
            if (currentPeriodKey.equals(row.get("period_key"))) {
                currentIndex = i;
            }
        }
        
        // Only include weeks from first appearance to current
        List<ChartRunDTO.ChartRunWeek> weeks = new ArrayList<>();
        int weeksAtTop1 = 0, weeksAtTop5 = 0, weeksAtTop10 = 0, weeksAtTop20 = 0;
        int peakPosition = Integer.MAX_VALUE;
        int totalWeeksOnChart = 0;
        
        if (firstAppearanceIndex >= 0 && currentIndex >= 0) {
            for (int i = firstAppearanceIndex; i <= currentIndex; i++) {
                Map<String, Object> row = history.get(i);
                String periodKey = (String) row.get("period_key");
                Integer position = row.get("position") != null ? ((Number) row.get("position")).intValue() : null;
                String startDate = (String) row.get("period_start_date");
                String endDate = (String) row.get("period_end_date");
                String dateRange = formatDateRange(startDate, endDate);
                
                boolean isCurrent = periodKey.equals(currentPeriodKey);
                weeks.add(new ChartRunDTO.ChartRunWeek(periodKey, position, isCurrent, dateRange));
                
                if (position != null) {
                    totalWeeksOnChart++;
                    if (position <= 1) weeksAtTop1++;
                    if (position <= 5) weeksAtTop5++;
                    if (position <= 10) weeksAtTop10++;
                    if (position <= 20) weeksAtTop20++;
                    if (position < peakPosition) peakPosition = position;
                }
            }
        }
        
        dto.setWeeks(weeks);
        dto.setWeeksAtTop1(weeksAtTop1);
        dto.setWeeksAtTop5(weeksAtTop5);
        dto.setWeeksAtTop10(weeksAtTop10);
        dto.setWeeksAtTop20(weeksAtTop20);
        dto.setTotalWeeksOnChart(totalWeeksOnChart);
        dto.setPeakPosition(peakPosition == Integer.MAX_VALUE ? null : peakPosition);
        
        return dto;
    }
    
    /**
     * Get all weeks that have plays but no chart generated yet.
     * Only returns weeks that have completely passed (not the current ongoing week).
     * Excludes W00 (days before first Monday of year) since those are covered by the last week of the previous year.
     */
    public List<String> getWeeksWithoutCharts() {
        String sql = """
            SELECT DISTINCT strftime('%Y-W%W', p.play_date) as period_key
            FROM Play p
            WHERE p.play_date IS NOT NULL
              AND p.song_id IS NOT NULL
              AND strftime('%Y-W%W', p.play_date) NOT IN (
                  SELECT period_key FROM Chart WHERE chart_type = 'song'
              )
            ORDER BY period_key ASC
            """;
        List<String> allWeeks = jdbcTemplate.queryForList(sql, String.class);
        
        // Filter out:
        // 1. Weeks that haven't completed yet
        // 2. W00 entries (days before first Monday are covered by the last week of previous year)
        return allWeeks.stream()
            .filter(week -> !week.endsWith("-W00"))
            .filter(this::isWeekComplete)
            .toList();
    }
    
    /**
     * Generate all missing charts asynchronously.
     * Returns a session ID for tracking progress.
     */
    public String startBulkGeneration() {
        String sessionId = UUID.randomUUID().toString();
        List<String> missingWeeks = getWeeksWithoutCharts();
        
        ChartGenerationProgressDTO progress = new ChartGenerationProgressDTO(
            missingWeeks.size(), 0, null, false
        );
        generationProgress.put(sessionId, progress);
        
        // Start generation in a separate thread
        Thread generationThread = new Thread(() -> {
            try {
                for (int i = 0; i < missingWeeks.size(); i++) {
                    String week = missingWeeks.get(i);
                    progress.setCurrentWeek(week);
                    progress.setCompletedWeeks(i);
                    
                    try {
                        // Generate both song and album charts
                        generateWeeklyCharts(week);
                    } catch (Exception e) {
                        // Log but continue with other weeks
                        System.err.println("Error generating chart for " + week + ": " + e.getMessage());
                    }
                    
                    progress.setCompletedWeeks(i + 1);
                }
                progress.setComplete(true);
                progress.setCurrentWeek(null);
            } catch (Exception e) {
                progress.setError(e.getMessage());
                progress.setComplete(true);
            }
        });
        generationThread.start();
        
        return sessionId;
    }
    
    /**
     * Get progress of bulk generation.
     */
    public ChartGenerationProgressDTO getGenerationProgress(String sessionId) {
        return generationProgress.getOrDefault(sessionId, 
            ChartGenerationProgressDTO.error("Session not found"));
    }
    
    /**
     * Clean up completed generation session.
     */
    public void cleanupGenerationSession(String sessionId) {
        generationProgress.remove(sessionId);
    }
    
    /**
     * Get all weeks that have charts (for regeneration).
     */
    public List<String> getWeeksWithCharts() {
        String sql = """
            SELECT DISTINCT period_key
            FROM Chart
            WHERE chart_type = 'song' AND period_type = 'weekly'
            ORDER BY period_key ASC
            """;
        return jdbcTemplate.queryForList(sql, String.class);
    }

    @Transactional
    public void regenerateAffectedWeeklySongChartsForLinkedSongs() {
        regenerateWeeklySongCharts(songLinkService.getAffectedWeeklyPeriodKeysForLinkedSongs());
    }

    @Transactional
    public void regenerateAffectedWeeklySongChartsForSongIds(Set<Integer> songIds) {
        regenerateWeeklySongCharts(songLinkService.getAffectedWeeklyPeriodKeysForSongIds(songIds));
    }

    private void regenerateWeeklySongCharts(List<String> periodKeys) {
        if (periodKeys == null || periodKeys.isEmpty()) {
            return;
        }
        for (String periodKey : periodKeys) {
            if (periodKey != null && !periodKey.endsWith("-W00") && chartRepository.existsByChartTypeAndPeriodKey("song", periodKey)) {
                generateWeeklySongChart(periodKey);
            }
        }
    }

    /**
     * Delete all weekly chart data for a specific period.
     * This includes both song and album charts and their entries.
     */
    @Transactional
    public void deleteWeeklyCharts(String periodKey) {
        // Delete chart entries first (foreign key constraint)
        jdbcTemplate.update("""
            DELETE FROM ChartEntry 
            WHERE chart_id IN (
                SELECT id FROM Chart 
                WHERE period_key = ? AND (chart_type = 'song' OR chart_type = 'album') AND period_type = 'weekly'
            )
            """, periodKey);

        // Delete the charts
        jdbcTemplate.update("""
            DELETE FROM Chart 
            WHERE period_key = ? AND (chart_type = 'song' OR chart_type = 'album') AND period_type = 'weekly'
            """, periodKey);
    }

    /**
     * Delete ALL weekly charts from the database.
     * This includes both song and album charts and their entries.
     */
    @Transactional
    public void deleteAllWeeklyCharts() {
        // Delete chart entries first (foreign key constraint)
        jdbcTemplate.update("""
            DELETE FROM ChartEntry 
            WHERE chart_id IN (
                SELECT id FROM Chart 
                WHERE (chart_type = 'song' OR chart_type = 'album') AND period_type = 'weekly'
            )
            """);

        // Delete the charts
        jdbcTemplate.update("""
            DELETE FROM Chart 
            WHERE (chart_type = 'song' OR chart_type = 'album') AND period_type = 'weekly'
            """);
    }

    /**
     * Get all weeks that have play data (for regeneration).
     * Excludes W00 (days before first Monday are covered by the last week of previous year).
     * Only includes weeks that have completely passed.
     */
    public List<String> getAllWeeksWithPlayData() {
        String sql = """
            SELECT DISTINCT strftime('%Y-W%W', p.play_date) as period_key
            FROM Play p
            WHERE p.play_date IS NOT NULL
              AND p.song_id IS NOT NULL
            ORDER BY period_key ASC
            """;
        List<String> allWeeks = jdbcTemplate.queryForList(sql, String.class);
        
        // Filter out:
        // 1. Weeks that haven't completed yet
        // 2. W00 entries (days before first Monday are covered by the last week of previous year)
        return allWeeks.stream()
            .filter(week -> !week.endsWith("-W00"))
            .filter(this::isWeekComplete)
            .toList();
    }

    /**
     * Regenerate all weekly charts asynchronously.
     * Deletes ALL existing weekly charts first, then regenerates from scratch using play data.
     * This ensures any buggy charts (like W00) are cleaned up.
     * Returns a session ID for tracking progress.
     */
    public String startBulkRegeneration() {
        String sessionId = UUID.randomUUID().toString();
        
        // Get all weeks from play data (properly filtered, excluding W00)
        List<String> weeksToGenerate = getAllWeeksWithPlayData();

        ChartGenerationProgressDTO progress = new ChartGenerationProgressDTO(
            weeksToGenerate.size(), 0, "Deleting existing charts...", false
        );
        generationProgress.put(sessionId, progress);

        // Start regeneration in a separate thread
        Thread generationThread = new Thread(() -> {
            try {
                // First, delete ALL weekly charts (including any buggy ones like W00)
                deleteAllWeeklyCharts();
                
                // Now generate fresh charts from play data
                for (int i = 0; i < weeksToGenerate.size(); i++) {
                    String week = weeksToGenerate.get(i);
                    progress.setCurrentWeek(week);
                    progress.setCompletedWeeks(i);

                    try {
                        // Generate both song and album charts
                        generateWeeklySongChart(week);
                        generateWeeklyAlbumChart(week);
                    } catch (Exception e) {
                        // Log but continue with other weeks
                        System.err.println("Error generating chart for " + week + ": " + e.getMessage());
                    }

                    progress.setCompletedWeeks(i + 1);
                }
                progress.setComplete(true);
                progress.setCurrentWeek(null);
            } catch (Exception e) {
                progress.setError(e.getMessage());
                progress.setComplete(true);
            }
        });
        generationThread.start();

        return sessionId;
    }

    /**
     * Parse a period key (e.g., "2024-W48") to a date range.
     * Uses SQLite's %W week numbering convention to match how weeks are grouped in the database:
     * Parse period key to date range.
     *
     * Uses SQLite's %W week numbering convention:
     * - Week 00: Days before the first Monday of the year
     * - Week 01: Starts from the first Monday of the year
     * - Week N: Starts from first Monday + (N-1)*7 days
     */
    private LocalDate[] parsePeriodKeyToDateRange(String periodKey) {
        // Format: YYYY-WXX where XX is week number (00-53)
        String[] parts = periodKey.split("-W");
        int year = Integer.parseInt(parts[0]);
        int week = Integer.parseInt(parts[1]);

        // Find the first Monday of the year (matches SQLite's %W week 01)
        LocalDate jan1 = LocalDate.of(year, 1, 1);
        LocalDate firstMonday = jan1.with(java.time.temporal.TemporalAdjusters.nextOrSame(DayOfWeek.MONDAY));

        LocalDate startOfWeek;
        LocalDate endOfWeek;

        if (week == 0) {
            // Week 00: Jan 1 to the day before the first Monday
            startOfWeek = jan1;
            endOfWeek = firstMonday.minusDays(1);
            // If first Monday IS Jan 1, week 00 is empty
            if (endOfWeek.isBefore(startOfWeek)) {
                // Return week 01 instead for safety
                startOfWeek = firstMonday;
                endOfWeek = startOfWeek.plusDays(6);
            }
        } else {
            // Week N: first Monday + (N-1)*7 days, for 7 days
            startOfWeek = firstMonday.plusWeeks(week - 1);
            endOfWeek = startOfWeek.plusDays(6);
        }

        return new LocalDate[]{startOfWeek, endOfWeek};
    }
    
    /**
     * Get the #1 song's image for display.
     */
    public byte[] getNumberOneSongImage(String periodKey) {
        Optional<Chart> chartOpt = chartRepository.findByChartTypeAndPeriodKey("song", periodKey);
        if (chartOpt.isEmpty()) {
            return null;
        }
        
        String sql = """
            SELECT COALESCE(
                al.image,
                (SELECT ai.image FROM AlbumImage ai WHERE ai.album_id = al.id ORDER BY ai.display_order ASC LIMIT 1),
                s.single_cover,
                (SELECT si.image FROM SongImage si WHERE si.song_id = s.id ORDER BY si.display_order ASC LIMIT 1)
            ) as image
            FROM ChartEntry ce
            INNER JOIN Song s ON ce.song_id = s.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE ce.chart_id = ? AND ce.position = 1
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("image"), chartOpt.get().getId());
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get chart history for an artist (all their songs and albums that have charted).
     * Returns list sorted by peak position, then by weeks at peak (descending).
     */
    public List<ChartHistoryDTO> getArtistChartHistory(Integer artistId) {
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        // Get song chart history for this artist (weekly charts only)
        String songSql = """
            SELECT s.id, s.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.artist_id = ? AND c.chart_type = 'song' AND c.period_type = 'weekly'
            GROUP BY s.id, s.name
            """;
        
        List<Map<String, Object>> songRows = jdbcTemplate.queryForList(songSql, artistId);
        for (Map<String, Object> row : songRows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            
            // Count weeks at peak
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDate(songId, "song");
            String peakDate = getFirstPeakDate(songId, peakPosition, "song");
            String debutDate = getDebutDate(songId, "song");
            String peakWeek = getFirstPeakWeek(songId, peakPosition, "song");
            String debutWeek = getDebutWeek(songId, "song");
            
            result.add(new ChartHistoryDTO(
                songId,
                (String) row.get("name"),
                null, // artist name not needed for artist's own songs
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "song",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            ));
        }
        
        // Get album chart history for this artist (weekly charts only)
        String albumSql = """
            SELECT al.id, al.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            WHERE al.artist_id = ? AND c.chart_type = 'album' AND c.period_type = 'weekly'
            GROUP BY al.id, al.name
            """;
        
        List<Map<String, Object>> albumRows = jdbcTemplate.queryForList(albumSql, artistId);
        for (Map<String, Object> row : albumRows) {
            Integer albumId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            
            // Count weeks at peak
            Integer weeksAtPeak = countWeeksAtPosition(albumId, peakPosition, "album");
            String releaseDate = getReleaseDate(albumId, "album");
            String peakDate = getFirstPeakDate(albumId, peakPosition, "album");
            String debutDate = getDebutDate(albumId, "album");
            String peakWeek = getFirstPeakWeek(albumId, peakPosition, "album");
            String debutWeek = getDebutWeek(albumId, "album");
            
            result.add(new ChartHistoryDTO(
                albumId,
                (String) row.get("name"),
                null,
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "album",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            ));
        }
        
        // Sort by peak position (ascending), then by weeks at peak (descending)
        result.sort((a, b) -> {
            int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
            if (peakCompare != 0) return peakCompare;
            return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
        });
        
        return result;
    }
    
    /**
     * Get song chart history for an artist (just songs).
     */
    public List<ChartHistoryDTO> getArtistSongChartHistory(Integer artistId) {
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        String songSql = """
            SELECT s.id, s.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks,
                   MAX(CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                   s.album_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.artist_id = ? AND c.chart_type = 'song' AND c.period_type = 'weekly'
            GROUP BY s.id, s.name, s.album_id
            """;
        
        List<Map<String, Object>> songRows = jdbcTemplate.queryForList(songSql, artistId);
        for (Map<String, Object> row : songRows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDate(songId, "song");
            String peakDate = getFirstPeakDate(songId, peakPosition, "song");
            String debutDate = getDebutDate(songId, "song");
            String peakWeek = getFirstPeakWeek(songId, peakPosition, "song");
            String debutWeek = getDebutWeek(songId, "song");
            boolean hasImage = ((Number) row.get("has_image")).intValue() == 1;
            Integer albumId = row.get("album_id") != null ? ((Number) row.get("album_id")).intValue() : null;
            
            ChartHistoryDTO dto = new ChartHistoryDTO(
                songId,
                (String) row.get("name"),
                null,
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "song",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            dto.setHasImage(hasImage);
            dto.setAlbumId(albumId);
            dto.setTotalWeekBreakdownItems(buildLinkedSongWeekBreakdownItems(songId));
            applyItunesPresence(dto);
            result.add(dto);
        }
        
        result.sort((a, b) -> {
            int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
            if (peakCompare != 0) return peakCompare;
            return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
        });
        
        return result;
    }
    
    /**
     * Get album chart history for an artist (just albums).
     */
    public List<ChartHistoryDTO> getArtistAlbumChartHistory(Integer artistId) {
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        String albumSql = """
            SELECT al.id, al.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            WHERE al.artist_id = ? AND c.chart_type = 'album' AND c.period_type = 'weekly'
            GROUP BY al.id, al.name
            """;
        
        List<Map<String, Object>> albumRows = jdbcTemplate.queryForList(albumSql, artistId);
        for (Map<String, Object> row : albumRows) {
            Integer albumId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(albumId, peakPosition, "album");
            String releaseDate = getReleaseDate(albumId, "album");
            String peakDate = getFirstPeakDate(albumId, peakPosition, "album");
            String debutDate = getDebutDate(albumId, "album");
            String peakWeek = getFirstPeakWeek(albumId, peakPosition, "album");
            String debutWeek = getDebutWeek(albumId, "album");
            
            ChartHistoryDTO dto = new ChartHistoryDTO(
                albumId,
                (String) row.get("name"),
                null,
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "album",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            applyItunesPresence(dto);
            result.add(dto);
        }
        
        result.sort((a, b) -> {
            int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
            if (peakCompare != 0) return peakCompare;
            return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
        });
        
        return result;
    }
    
    /**
     * Get song chart history for songs on an album (songs that have charted).
     */
    public List<ChartHistoryDTO> getAlbumSongChartHistory(Integer albumId) {
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        String songSql = """
            SELECT s.id, s.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.album_id = ? AND c.chart_type = 'song' AND c.period_type = 'weekly'
            GROUP BY s.id, s.name
            """;
        
        List<Map<String, Object>> songRows = jdbcTemplate.queryForList(songSql, albumId);
        for (Map<String, Object> row : songRows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDate(songId, "song");
            String peakDate = getFirstPeakDate(songId, peakPosition, "song");
            String debutDate = getDebutDate(songId, "song");
            String peakWeek = getFirstPeakWeek(songId, peakPosition, "song");
            String debutWeek = getDebutWeek(songId, "song");
            
            ChartHistoryDTO dto = new ChartHistoryDTO(
                songId,
                (String) row.get("name"),
                null,
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "song",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            applyItunesPresence(dto);
            result.add(dto);
        }
        
        result.sort((a, b) -> {
            int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
            if (peakCompare != 0) return peakCompare;
            return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
        });
        
        return result;
    }
    
    /**
     * Get chart history for an album (the album's own chart entries).
     */
    public List<ChartHistoryDTO> getAlbumChartHistory(Integer albumId) {
        String sql = """
            SELECT al.id, al.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            WHERE al.id = ? AND c.chart_type = 'album' AND c.period_type = 'weekly'
            GROUP BY al.id, al.name
            """;
        
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, albumId);
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        for (Map<String, Object> row : rows) {
            Integer id = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(id, peakPosition, "album");
            String releaseDate = getReleaseDate(id, "album");
            String peakDate = getFirstPeakDate(id, peakPosition, "album");
            String debutDate = getDebutDate(id, "album");
            String peakWeek = getFirstPeakWeek(id, peakPosition, "album");
            String debutWeek = getDebutWeek(id, "album");
            
            ChartHistoryDTO dto = new ChartHistoryDTO(
                id,
                (String) row.get("name"),
                null,
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "album",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            applyItunesPresence(dto);
            result.add(dto);
        }
        
        return result;
    }
    
    /**
     * Get chart history for a song (the song's own chart entries).
     */
    public List<ChartHistoryDTO> getSongChartHistory(Integer songId) {
        String sql = """
            SELECT s.id, s.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks,
                   MAX(CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                   s.album_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.id = ? AND c.chart_type = 'song' AND c.period_type = 'weekly'
            GROUP BY s.id, s.name, s.album_id
            """;
        
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, songId);
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        for (Map<String, Object> row : rows) {
            Integer id = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(id, peakPosition, "song");
            String releaseDate = getReleaseDate(id, "song");
            String peakDate = getFirstPeakDate(id, peakPosition, "song");
            String debutDate = getDebutDate(id, "song");
            String peakWeek = getFirstPeakWeek(id, peakPosition, "song");
            String debutWeek = getDebutWeek(id, "song");
            boolean hasImage = ((Number) row.get("has_image")).intValue() == 1;
            Integer albumId = row.get("album_id") != null ? ((Number) row.get("album_id")).intValue() : null;
            
            ChartHistoryDTO dto = new ChartHistoryDTO(
                id,
                (String) row.get("name"),
                null,
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "song",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            dto.setHasImage(hasImage);
            dto.setAlbumId(albumId);
            dto.setTotalWeekBreakdownItems(buildLinkedSongWeekBreakdownItems(songId));
            applyItunesPresence(dto);
            result.add(dto);
        }
        
        return result;
    }
    
    /**
     * Count how many weeks an item (song or album) spent at a specific position (weekly charts only).
     * @param itemId The song or album ID
     * @param position The chart position to count
     * @param chartType "song" or "album"
     */
    private Integer countWeeksAtPosition(Integer itemId, Integer position, String chartType) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String sql = String.format("""
            SELECT COUNT(*) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND ce.position = ? AND c.chart_type = ? AND c.period_type = 'weekly'
            """, idColumn);
        return jdbcTemplate.queryForObject(sql, Integer.class, itemId, position, chartType);
    }
    
    /**
     * Get the first date an item reached its peak position (weekly charts only).
     * @param itemId The song or album ID
     * @param position The peak position
     * @param chartType "song" or "album"
     */
    private String getFirstPeakDate(Integer itemId, Integer position, String chartType) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String sql = String.format("""
            SELECT c.period_end_date FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND ce.position = ? AND c.chart_type = ? AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """, idColumn);
        String dateStr = jdbcTemplate.queryForObject(sql, String.class, itemId, position, chartType);
        return formatPeakDate(dateStr);
    }

    /**
     * Get the period key for when an item first reached its peak position (weekly charts only).
     * @param itemId The song or album ID
     * @param position The peak position
     * @param chartType "song" or "album"
     */
    private String getFirstPeakWeek(Integer itemId, Integer position, String chartType) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String sql = String.format("""
            SELECT c.period_key FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND ce.position = ? AND c.chart_type = ? AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """, idColumn);
        try {
            return jdbcTemplate.queryForObject(sql, String.class, itemId, position, chartType);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get the first date an item appeared on the chart (debut date, weekly charts only).
     * Uses period_end_date to show the week ending date.
     * @param itemId The song or album ID
     * @param chartType "song" or "album"
     */
    private String getDebutDate(Integer itemId, String chartType) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String sql = String.format("""
            SELECT c.period_end_date FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND c.chart_type = ? AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """, idColumn);
        String dateStr = jdbcTemplate.queryForObject(sql, String.class, itemId, chartType);
        return formatPeakDate(dateStr);
    }

    /**
     * Get the period key for when an item first appeared on the chart (debut week, weekly charts only).
     * @param itemId The song or album ID
     * @param chartType "song" or "album"
     */
    private String getDebutWeek(Integer itemId, String chartType) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String sql = String.format("""
            SELECT c.period_key FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND c.chart_type = ? AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """, idColumn);
        try {
            return jdbcTemplate.queryForObject(sql, String.class, itemId, chartType);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get the release date for a song or album (formatted).
     * @param itemId The song or album ID
     * @param itemType "song" or "album"
     */
    private String getReleaseDate(Integer itemId, String itemType) {
        String sql;
        if ("song".equals(itemType)) {
            // For songs, fall back to album release date if song's release date is null
            sql = "SELECT COALESCE(s.release_date, a.release_date) FROM Song s LEFT JOIN Album a ON s.album_id = a.id WHERE s.id = ?";
        } else {
            sql = "SELECT release_date FROM Album WHERE id = ?";
        }
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, itemId);
            return formatPeakDate(dateStr);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Format a date string (yyyy-MM-dd) to a display format (dd-MMM-yyyy).
     */
    private String formatPeakDate(String dateStr) {
        if (dateStr == null) return null;
        try {
            LocalDate date = LocalDate.parse(dateStr);
            return date.format(java.time.format.DateTimeFormatter.ofPattern("dd-MMM-yyyy"));
        } catch (Exception e) {
            return dateStr;
        }
    }
    
    /**
     * Format a date range for display in chart run tooltips.
     * Converts "2025-11-06" to "Nov 6" and shows "Nov 6 - Nov 12, 2025"
     */
    private String formatDateRange(String startDate, String endDate) {
        if (startDate == null || endDate == null) return null;
        try {
            LocalDate start = LocalDate.parse(startDate);
            LocalDate end = LocalDate.parse(endDate);
            java.time.format.DateTimeFormatter monthDay = java.time.format.DateTimeFormatter.ofPattern("MMM d");
            java.time.format.DateTimeFormatter monthDayYear = java.time.format.DateTimeFormatter.ofPattern("MMM d, yyyy");
            
            if (start.getYear() == end.getYear()) {
                return start.format(monthDay) + " - " + end.format(monthDayYear);
            } else {
                return start.format(monthDayYear) + " - " + end.format(monthDayYear);
            }
        } catch (Exception e) {
            return startDate + " - " + endDate;
        }
    }
    
    // ==================== Seasonal/Yearly Chart Methods ====================
    
    public static final int SEASONAL_YEARLY_SONGS_COUNT = 30;
    public static final int SEASONAL_ALBUMS_COUNT = 5;
    public static final int YEARLY_ALBUMS_COUNT = 10;
    public static final int SEASONAL_EXTRA_SONGS_COUNT = 5;  // Extra songs for playlist, not in main chart
    
    /**
     * Get a seasonal chart by chart type (song/album) and period key.
     */
    public Optional<Chart> getSeasonalChart(String chartType, String periodKey) {
        return chartRepository.findByChartTypeAndPeriodTypeAndPeriodKey(chartType, "seasonal", periodKey);
    }
    
    /**
     * Get a yearly chart by chart type (song/album) and period key.
     */
    public Optional<Chart> getYearlyChart(String chartType, String periodKey) {
        return chartRepository.findByChartTypeAndPeriodTypeAndPeriodKey(chartType, "yearly", periodKey);
    }
    
    /**
     * Get set of period keys that have finalized charts for a given period type.
     */
    public Set<String> getFinalizedChartPeriodKeys(String periodType) {
        return chartRepository.findFinalizedPeriodKeysByPeriodType(periodType);
    }
    
    /**
     * Get set of period keys that have any charts (draft or finalized) for a given period type.
     */
    public Set<String> getAllChartPeriodKeys(String periodType) {
        return chartRepository.findAllPeriodKeysByPeriodType(periodType);
    }
    
    /**
     * Check if a finalized chart exists for a given period type and period key.
     */
    public boolean hasFinalizedChart(String periodType, String periodKey) {
        return chartRepository.existsFinalizedChart(periodType, periodKey);
    }
    
    /**
     * Check if any chart (draft or finalized) exists for a given period type, chart type, and period key.
     */
    public boolean hasChart(String periodType, String chartType, String periodKey) {
        return chartRepository.existsByChartTypeAndPeriodTypeAndPeriodKey(chartType, periodType, periodKey);
    }
    
    /**
     * Get a seasonal or yearly chart by period type, chart type, and period key.
     */
    public Optional<Chart> getSeasonalYearlyChart(String periodType, String chartType, String periodKey) {
        return chartRepository.findByPeriodTypeAndPeriodKeyAndChartType(periodType, periodKey, chartType);
    }
    
    /**
     * Get the latest finalized chart for a period type.
     */
    public Optional<Chart> getLatestSeasonalYearlyChart(String periodType, String chartType) {
        List<Chart> charts = chartRepository.findLatestByPeriodTypeAndChartType(periodType, chartType);
        return charts.isEmpty() ? Optional.empty() : Optional.of(charts.get(0));
    }
    
    /**
     * Get the current season's period key (e.g., "2024-Winter").
     */
    public String getCurrentSeasonPeriodKey() {
        LocalDate now = LocalDate.now();
        int month = now.getMonthValue();
        int year = now.getYear();
        
        if (month == 12) {
            // December belongs to next year's Winter
            return (year + 1) + "-Winter";
        } else if (month <= 2) {
            return year + "-Winter";
        } else if (month <= 5) {
            return year + "-Spring";
        } else if (month <= 8) {
            return year + "-Summer";
        } else {
            return year + "-Fall";
        }
    }
    
    /**
     * Check if a season has completely passed.
     * Season definitions: Winter (Dec-Feb), Spring (Mar-May), Summer (Jun-Aug), Fall (Sep-Nov)
     * Note: Winter 2024 = Dec 2023 + Jan-Feb 2024
     */
    public boolean isSeasonComplete(String periodKey) {
        LocalDate[] dateRange = parseSeasonPeriodKeyToDateRange(periodKey);
        if (dateRange == null) return false;
        LocalDate endDate = dateRange[1];
        return LocalDate.now().isAfter(endDate);
    }
    
    /**
     * Check if a year is complete enough for chart finalization.
     * Allows finalization from December 20th of the year onwards.
     */
    public boolean isYearComplete(String periodKey) {
        try {
            int year = Integer.parseInt(periodKey);
            LocalDate finalizationDate = LocalDate.of(year, 12, 20);
            return !LocalDate.now().isBefore(finalizationDate);
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    /**
     * Parse a season period key (e.g., "2024-Winter") to date range.
     * Returns [startDate, endDate] or null if invalid.
     */
    public LocalDate[] parseSeasonPeriodKeyToDateRange(String periodKey) {
        try {
            String[] parts = periodKey.split("-");
            if (parts.length != 2) return null;
            
            int year = Integer.parseInt(parts[0]);
            String season = parts[1];
            
            return switch (season) {
                case "Winter" -> new LocalDate[]{
                    LocalDate.of(year - 1, 12, 1),
                    LocalDate.of(year, 2, 28).withDayOfMonth(LocalDate.of(year, 2, 1).lengthOfMonth())
                };
                case "Spring" -> new LocalDate[]{
                    LocalDate.of(year, 3, 1),
                    LocalDate.of(year, 5, 31)
                };
                case "Summer" -> new LocalDate[]{
                    LocalDate.of(year, 6, 1),
                    LocalDate.of(year, 8, 31)
                };
                case "Fall" -> new LocalDate[]{
                    LocalDate.of(year, 9, 1),
                    LocalDate.of(year, 11, 30)
                };
                default -> null;
            };
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Parse a yearly period key (e.g., "2024") to date range.
     */
    public LocalDate[] parseYearPeriodKeyToDateRange(String periodKey) {
        try {
            int year = Integer.parseInt(periodKey);
            return new LocalDate[]{
                LocalDate.of(year, 1, 1),
                LocalDate.of(year, 12, 31)
            };
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Create or get existing draft charts for a seasonal/yearly period.
     * Creates both song and album chart records if they don't exist.
     */
    @Transactional
    public Map<String, Chart> createOrGetDraftCharts(String periodType, String periodKey) {
        Map<String, Chart> charts = new HashMap<>();
        
        // Parse date range based on period type
        LocalDate[] dateRange = "seasonal".equals(periodType) 
            ? parseSeasonPeriodKeyToDateRange(periodKey)
            : parseYearPeriodKeyToDateRange(periodKey);
        
        if (dateRange == null) {
            throw new IllegalArgumentException("Invalid period key: " + periodKey);
        }
        
        // Get or create song chart
        Optional<Chart> existingSongChart = chartRepository.findByChartTypeAndPeriodTypeAndPeriodKey("song", periodType, periodKey);
        Chart songChart = existingSongChart.orElseGet(() -> {
            Chart chart = new Chart("song", periodKey, dateRange[0], dateRange[1], periodType);
            return chartRepository.save(chart);
        });
        charts.put("song", songChart);
        
        // Get or create album chart
        Optional<Chart> existingAlbumChart = chartRepository.findByChartTypeAndPeriodTypeAndPeriodKey("album", periodType, periodKey);
        Chart albumChart = existingAlbumChart.orElseGet(() -> {
            Chart chart = new Chart("album", periodKey, dateRange[0], dateRange[1], periodType);
            return chartRepository.save(chart);
        });
        charts.put("album", albumChart);
        
        return charts;
    }
    
    /**
     * Save chart entries for a seasonal/yearly chart.
     * Validates no duplicate song/album IDs within the same chart.
     * @param chartId The chart ID
     * @param entries List of maps with {position: int, itemId: int} where itemId is song_id or album_id
     * @param chartType "song" or "album"
     */
    @Transactional
    public void saveChartEntries(Integer chartId, List<Map<String, Integer>> entries, String chartType) {
        // Verify chart exists and is not finalized
        Chart chart = chartRepository.findById(chartId)
            .orElseThrow(() -> new IllegalArgumentException("Chart not found: " + chartId));
        
        if (Boolean.TRUE.equals(chart.getIsFinalized())) {
            throw new IllegalArgumentException("Cannot modify a finalized chart");
        }
        
        // Check for duplicates
        Set<Integer> itemIds = new HashSet<>();
        for (Map<String, Integer> entry : entries) {
            Integer itemId = entry.get("itemId");
            if (itemId != null && !itemIds.add(itemId)) {
                throw new IllegalArgumentException("Duplicate " + chartType + " ID: " + itemId);
            }
        }
        
        // Delete existing entries for this chart
        chartEntryRepository.deleteByChartId(chartId);
        
        // Create new entries
        List<ChartEntry> newEntries = new ArrayList<>();
        for (Map<String, Integer> entry : entries) {
            Integer position = entry.get("position");
            Integer itemId = entry.get("itemId");
            
            if (position != null && itemId != null) {
                ChartEntry chartEntry = "song".equals(chartType)
                    ? ChartEntry.forSong(chartId, position, itemId, 0)  // 0 play_count for manual charts
                    : ChartEntry.forAlbum(chartId, position, itemId, 0);
                newEntries.add(chartEntry);
            }
        }
        
        if (!newEntries.isEmpty()) {
            chartEntryRepository.saveAll(newEntries);
        }
    }
    
    /**
     * Finalize a seasonal/yearly chart.
     * Validates that the period has passed and all positions are filled.
     */
    @Transactional
    public void finalizeChart(Integer songChartId, Integer albumChartId, String periodType) {
        Chart songChart = chartRepository.findById(songChartId)
            .orElseThrow(() -> new IllegalArgumentException("Song chart not found"));
        Chart albumChart = chartRepository.findById(albumChartId)
            .orElseThrow(() -> new IllegalArgumentException("Album chart not found"));
        
        String periodKey = songChart.getPeriodKey();
        
        // Verify period has passed
        boolean periodComplete = "seasonal".equals(periodType) 
            ? isSeasonComplete(periodKey) 
            : isYearComplete(periodKey);
        
        if (!periodComplete) {
            throw new IllegalArgumentException("Cannot finalize chart: period has not ended yet");
        }
        
        // Verify all positions are filled
        long songEntryCount = chartEntryRepository.countByChartId(songChartId);
        long albumEntryCount = chartEntryRepository.countByChartId(albumChartId);
        
        int requiredAlbums = "seasonal".equals(periodType) ? SEASONAL_ALBUMS_COUNT : YEARLY_ALBUMS_COUNT;
        
        if (songEntryCount < SEASONAL_YEARLY_SONGS_COUNT) {
            throw new IllegalArgumentException(
                "Cannot finalize: song chart needs " + SEASONAL_YEARLY_SONGS_COUNT + 
                " entries but only has " + songEntryCount);
        }
        
        if (albumEntryCount < requiredAlbums) {
            throw new IllegalArgumentException(
                "Cannot finalize: album chart needs " + requiredAlbums + 
                " entries but only has " + albumEntryCount);
        }
        
        // Finalize both charts
        songChart.setIsFinalized(true);
        albumChart.setIsFinalized(true);
        chartRepository.save(songChart);
        chartRepository.save(albumChart);
    }
    
    /**
     * Finalize a single chart (song or album) independently.
     * Validates that the period has passed and the chart has all required entries.
     */
    @Transactional
    public void finalizeSingleChart(Integer chartId, String periodType) {
        Chart chart = chartRepository.findById(chartId)
            .orElseThrow(() -> new IllegalArgumentException("Chart not found"));
        
        String periodKey = chart.getPeriodKey();
        
        boolean periodComplete = "seasonal".equals(periodType)
            ? isSeasonComplete(periodKey)
            : isYearComplete(periodKey);
        
        if (!periodComplete) {
            throw new IllegalArgumentException("Cannot save chart: period has not ended yet");
        }
        
        long entryCount = chartEntryRepository.countByChartId(chartId);
        
        if ("song".equals(chart.getChartType())) {
            if (entryCount < SEASONAL_YEARLY_SONGS_COUNT) {
                throw new IllegalArgumentException(
                    "Cannot save: song chart needs " + SEASONAL_YEARLY_SONGS_COUNT +
                    " entries but only has " + entryCount);
            }
        } else {
            int required = "seasonal".equals(periodType) ? SEASONAL_ALBUMS_COUNT : YEARLY_ALBUMS_COUNT;
            if (entryCount < required) {
                throw new IllegalArgumentException(
                    "Cannot save: album chart needs " + required +
                    " entries but only has " + entryCount);
            }
        }
        
        chart.setIsFinalized(true);
        chartRepository.save(chart);
    }

    /**
     * Revert a chart to draft status, allowing entries to be edited again.
     */
    @Transactional
    public void unfinalizeChart(Integer chartId) {
        Chart chart = chartRepository.findById(chartId)
            .orElseThrow(() -> new IllegalArgumentException("Chart not found"));
        chart.setIsFinalized(false);
        chartRepository.save(chart);
    }
    
    /**
     * Get the #1 song name with artist for a finalized seasonal or yearly chart.
     * Returns format "Artist - Song" or null if no chart exists or chart is not finalized.
     */
    public String getNumberOneSongName(String periodType, String periodKey) {
        String sql = """
            SELECT ar.name || ' - ' || s.name as display_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = 'song' AND c.period_key = ? 
                  AND c.is_finalized = 1 AND ce.position = 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, String.class, periodType, periodKey);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get the gender ID for the #1 song artist for a finalized seasonal or yearly chart.
     */
    public Integer getNumberOneSongGenderId(String periodType, String periodKey) {
        String sql = """
            SELECT ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = 'song' AND c.period_key = ? 
                  AND c.is_finalized = 1 AND ce.position = 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, periodType, periodKey);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get the #1 album name with artist for a finalized seasonal or yearly chart.
     * Returns format "Artist - Album" or null if no chart exists or chart is not finalized.
     */
    public String getNumberOneAlbumName(String periodType, String periodKey) {
        String sql = """
            SELECT ar.name || ' - ' || a.name as display_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album a ON ce.album_id = a.id
            INNER JOIN Artist ar ON a.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = 'album' AND c.period_key = ? 
                  AND c.is_finalized = 1 AND ce.position = 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, String.class, periodType, periodKey);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get the gender ID for the #1 album artist for a finalized seasonal or yearly chart.
     */
    public Integer getNumberOneAlbumGenderId(String periodType, String periodKey) {
        String sql = """
            SELECT ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album a ON ce.album_id = a.id
            INNER JOIN Artist ar ON a.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = 'album' AND c.period_key = ? 
                  AND c.is_finalized = 1 AND ce.position = 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, periodType, periodKey);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get #1 song and album with artist names for a finalized weekly chart.
     * Returns format "Artist - Song/Album".
     */
    public Map<String, String> getWeeklyChartTopEntries(String periodKey) {
        Map<String, String> result = new HashMap<>();
        
        String songSql = """
            SELECT ar.name || ' - ' || s.name as display_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE c.chart_type = 'song' AND c.period_key = ? AND ce.position = 1
            """;
        try {
            result.put("song", jdbcTemplate.queryForObject(songSql, String.class, periodKey));
        } catch (Exception e) {
            result.put("song", null);
        }
        
        String albumSql = """
            SELECT ar.name || ' - ' || a.name as display_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album a ON ce.album_id = a.id
            INNER JOIN Artist ar ON a.artist_id = ar.id
            WHERE c.chart_type = 'album' AND c.period_key = ? AND ce.position = 1
            """;
        try {
            result.put("album", jdbcTemplate.queryForObject(albumSql, String.class, periodKey));
        } catch (Exception e) {
            result.put("album", null);
        }
        
        return result;
    }
    
    /**
     * Get gender IDs for the #1 song and album artists in a weekly chart.
     * Returns map with "song" and "album" keys containing gender IDs (may be null).
     */
    public Map<String, Integer> getWeeklyChartTopGenderIds(String periodKey) {
        Map<String, Integer> result = new HashMap<>();
        
        String songSql = """
            SELECT ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE c.chart_type = 'song' AND c.period_key = ? AND ce.position = 1
            """;
        try {
            result.put("song", jdbcTemplate.queryForObject(songSql, Integer.class, periodKey));
        } catch (Exception e) {
            result.put("song", null);
        }
        
        String albumSql = """
            SELECT ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album a ON ce.album_id = a.id
            INNER JOIN Artist ar ON a.artist_id = ar.id
            WHERE c.chart_type = 'album' AND c.period_key = ? AND ce.position = 1
            """;
        try {
            result.put("album", jdbcTemplate.queryForObject(albumSql, Integer.class, periodKey));
        } catch (Exception e) {
            result.put("album", null);
        }
        
        return result;
    }
    
    /**
     * BATCH: Get #1 entries for multiple periods at once (weekly charts).
     * Eliminates N+1 queries when displaying timeframe lists.
     * Returns a map of periodKey -> ChartTopEntryDTO with song/album names and gender IDs.
     */
    public Map<String, ChartTopEntryDTO> getWeeklyChartTopEntriesBatch(Set<String> periodKeys) {
        if (periodKeys == null || periodKeys.isEmpty()) {
            return new HashMap<>();
        }
        
        Map<String, ChartTopEntryDTO> result = new HashMap<>();
        for (String pk : periodKeys) {
            result.put(pk, new ChartTopEntryDTO(pk));
        }
        
        String placeholders = String.join(",", periodKeys.stream().map(pk -> "?").toList());
        
        // Get #1 songs for all periods in one query
        String songSql = """
            SELECT c.period_key, ar.name || ' - ' || s.name as display_name, ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE c.chart_type = 'song' AND c.period_key IN (%s) AND ce.position = 1
            """.formatted(placeholders);
        
        jdbcTemplate.query(songSql, (rs) -> {
            String periodKey = rs.getString("period_key");
            ChartTopEntryDTO dto = result.get(periodKey);
            if (dto != null) {
                dto.setNumberOneSongName(rs.getString("display_name"));
                dto.setNumberOneSongGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            }
        }, periodKeys.toArray());
        
        // Get #1 albums for all periods in one query
        String albumSql = """
            SELECT c.period_key, ar.name || ' - ' || a.name as display_name, ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album a ON ce.album_id = a.id
            INNER JOIN Artist ar ON a.artist_id = ar.id
            WHERE c.chart_type = 'album' AND c.period_key IN (%s) AND ce.position = 1
            """.formatted(placeholders);
        
        jdbcTemplate.query(albumSql, (rs) -> {
            String periodKey = rs.getString("period_key");
            ChartTopEntryDTO dto = result.get(periodKey);
            if (dto != null) {
                dto.setNumberOneAlbumName(rs.getString("display_name"));
                dto.setNumberOneAlbumGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            }
        }, periodKeys.toArray());
        
        return result;
    }
    
    /**
     * BATCH: Get #1 entries for multiple finalized seasonal/yearly charts at once.
     * Eliminates N+1 queries when displaying timeframe lists.
     * Returns a map of periodKey -> ChartTopEntryDTO with song/album names and gender IDs.
     */
    public Map<String, ChartTopEntryDTO> getFinalizedChartTopEntriesBatch(String periodType, Set<String> periodKeys) {
        if (periodKeys == null || periodKeys.isEmpty()) {
            return new HashMap<>();
        }
        
        Map<String, ChartTopEntryDTO> result = new HashMap<>();
        for (String pk : periodKeys) {
            result.put(pk, new ChartTopEntryDTO(pk));
        }
        
        String placeholders = String.join(",", periodKeys.stream().map(pk -> "?").toList());
        
        // Get #1 songs for all finalized periods in one query
        String songSql = """
            SELECT c.period_key, ar.name || ' - ' || s.name as display_name, ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = 'song' AND c.period_key IN (%s) 
                  AND c.is_finalized = 1 AND ce.position = 1
            """.formatted(placeholders);
        
        Object[] songParams = new Object[periodKeys.size() + 1];
        songParams[0] = periodType;
        int i = 1;
        for (String pk : periodKeys) {
            songParams[i++] = pk;
        }
        
        jdbcTemplate.query(songSql, (rs) -> {
            String periodKey = rs.getString("period_key");
            ChartTopEntryDTO dto = result.get(periodKey);
            if (dto != null) {
                dto.setNumberOneSongName(rs.getString("display_name"));
                dto.setNumberOneSongGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            }
        }, songParams);
        
        // Get #1 albums for all finalized periods in one query
        String albumSql = """
            SELECT c.period_key, ar.name || ' - ' || a.name as display_name, ar.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album a ON ce.album_id = a.id
            INNER JOIN Artist ar ON a.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = 'album' AND c.period_key IN (%s) 
                  AND c.is_finalized = 1 AND ce.position = 1
            """.formatted(placeholders);
        
        Object[] albumParams = new Object[periodKeys.size() + 1];
        albumParams[0] = periodType;
        i = 1;
        for (String pk : periodKeys) {
            albumParams[i++] = pk;
        }
        
        jdbcTemplate.query(albumSql, (rs) -> {
            String periodKey = rs.getString("period_key");
            ChartTopEntryDTO dto = result.get(periodKey);
            if (dto != null) {
                dto.setNumberOneAlbumName(rs.getString("display_name"));
                dto.setNumberOneAlbumGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            }
        }, albumParams);
        
        return result;
    }
    
    /**
     * Get ALL chart entries for a seasonal/yearly chart for the edit page.
     * Returns all entries regardless of position (for drafts that may have more than 30).
     * Does not include play count - use for draft editing only.
     */
    public List<ChartEntryDTO> getAllChartEntriesForEdit(String periodType, String chartType, String periodKey) {
        String itemTable = "song".equals(chartType) ? "Song" : "Album";
        String itemIdCol = "song".equals(chartType) ? "song_id" : "album_id";

        // For songs, get both single_cover and album image separately for hover pattern
        String imageColumns = "song".equals(chartType)
            ? "CASE WHEN item.single_cover IS NOT NULL THEN 1 ELSE 0 END as has_single_cover, " +
              "CASE WHEN EXISTS(SELECT 1 FROM Album al WHERE al.id = item.album_id AND al.image IS NOT NULL) THEN 1 ELSE 0 END as album_has_image"
            : "CASE WHEN item.image IS NOT NULL THEN 1 ELSE 0 END as has_image, 0 as album_has_image";

        String genreSubquery = "song".equals(chartType)
            ? "(SELECT g.name FROM Genre g WHERE g.id = COALESCE(item.override_genre_id, (SELECT al2.override_genre_id FROM Album al2 WHERE al2.id = item.album_id), ar.genre_id)) as genre_name"
            : "(SELECT g.name FROM Genre g WHERE g.id = COALESCE(item.override_genre_id, ar.genre_id)) as genre_name";

        String sql = String.format("""
            SELECT ce.position, ce.%s as item_id, 
                   item.name as item_name,
                   ar.id as artist_id, ar.name as artist_name,
                   ar.gender_id as gender_id,
                   %s,
                   %s,
                   %s
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN %s item ON ce.%s = item.id
            INNER JOIN Artist ar ON item.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = ? AND c.period_key = ?
            ORDER BY ce.position ASC
            """,
            itemIdCol,
            imageColumns,
            "song".equals(chartType) ? "item.album_id, (SELECT name FROM Album WHERE id = item.album_id) as album_name" : "NULL as album_id, NULL as album_name",
            genreSubquery,
            itemTable,
            itemIdCol
        );
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition(rs.getInt("position"));
            
            if ("song".equals(chartType)) {
                dto.setSongId(rs.getInt("item_id"));
                dto.setSongName(rs.getString("item_name"));
                dto.setAlbumId(rs.getObject("album_id") != null ? rs.getInt("album_id") : null);
                dto.setAlbumName(rs.getString("album_name"));
                dto.setHasImage(rs.getInt("has_single_cover") == 1);
                dto.setAlbumHasImage(rs.getInt("album_has_image") == 1);
            } else {
                dto.setAlbumId(rs.getInt("item_id"));
                dto.setAlbumName(rs.getString("item_name"));
                dto.setHasImage(rs.getInt("has_image") == 1);
            }
            
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setPlayCount(0); // No play count for draft editing
            dto.setGenreName(rs.getString("genre_name"));
            
            return dto;
        }, periodType, chartType, periodKey);
    }
    
    /**
     * Get chart entries for a seasonal/yearly chart with song/album details.
     * Returns only main entries (position 1-30), not extras.
     * Includes play count for the period if chart is finalized.
     */
    public List<ChartEntryDTO> getSeasonalYearlyChartEntries(String periodType, String chartType, String periodKey) {
        String itemTable = "song".equals(chartType) ? "Song" : "Album";
        String itemIdCol = "song".equals(chartType) ? "song_id" : "album_id";

        // For songs, get both single_cover and album image separately for hover pattern
        String imageColumns = "song".equals(chartType)
            ? "CASE WHEN item.single_cover IS NOT NULL THEN 1 ELSE 0 END as has_single_cover, " +
              "CASE WHEN EXISTS(SELECT 1 FROM Album al WHERE al.id = item.album_id AND al.image IS NOT NULL) THEN 1 ELSE 0 END as album_has_image"
            : "CASE WHEN item.image IS NOT NULL THEN 1 ELSE 0 END as has_image, 0 as album_has_image";

        // Subquery to count plays within the period date range
        String playCountSubquery = "song".equals(chartType)
            ? "(SELECT COUNT(*) FROM Play sc WHERE sc.song_id = item.id AND DATE(sc.play_date) >= c.period_start_date AND DATE(sc.play_date) <= c.period_end_date)"
            : "(SELECT COUNT(*) FROM Play sc INNER JOIN Song s ON sc.song_id = s.id WHERE s.album_id = item.id AND DATE(sc.play_date) >= c.period_start_date AND DATE(sc.play_date) <= c.period_end_date)";

        String genreSubquery2 = "song".equals(chartType)
            ? "(SELECT g.name FROM Genre g WHERE g.id = COALESCE(item.override_genre_id, (SELECT al2.override_genre_id FROM Album al2 WHERE al2.id = item.album_id), ar.genre_id)) as genre_name"
            : "(SELECT g.name FROM Genre g WHERE g.id = COALESCE(item.override_genre_id, ar.genre_id)) as genre_name";
        
        String sql = String.format("""
            SELECT ce.position, ce.%s as item_id, 
                   item.name as item_name,
                   ar.id as artist_id, ar.name as artist_name,
                   ar.gender_id as gender_id,
                   %s,
                   %s,
                   %s,
                   %s as play_count
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN %s item ON ce.%s = item.id
            INNER JOIN Artist ar ON item.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = ? AND c.period_key = ?
              AND ce.position <= %d
            ORDER BY ce.position ASC
            """,
            itemIdCol,
            imageColumns,
            "song".equals(chartType) ? "item.album_id, (SELECT name FROM Album WHERE id = item.album_id) as album_name" : "NULL as album_id, NULL as album_name",
            genreSubquery2,
            playCountSubquery,
            itemTable,
            itemIdCol,
            SEASONAL_YEARLY_SONGS_COUNT
        );
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition(rs.getInt("position"));
            
            if ("song".equals(chartType)) {
                dto.setSongId(rs.getInt("item_id"));
                dto.setSongName(rs.getString("item_name"));
                dto.setAlbumId(rs.getObject("album_id") != null ? rs.getInt("album_id") : null);
                dto.setAlbumName(rs.getString("album_name"));
                dto.setHasImage(rs.getInt("has_single_cover") == 1);
                dto.setAlbumHasImage(rs.getInt("album_has_image") == 1);
            } else {
                dto.setAlbumId(rs.getInt("item_id"));
                dto.setAlbumName(rs.getString("item_name"));
                dto.setHasImage(rs.getInt("has_image") == 1);
            }
            
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setGenreName(rs.getString("genre_name"));
            
            return dto;
        }, periodType, chartType, periodKey);
    }
    
    /**
     * Get extra song entries for a seasonal chart (positions 31-35).
     * These are bonus songs added to the playlist but not counted in chart stats.
     */
    public List<ChartEntryDTO> getSeasonalExtraSongEntries(String periodKey) {
        String sql = """
            SELECT ce.position, ce.song_id as item_id, 
                   item.name as item_name,
                   ar.id as artist_id, ar.name as artist_name,
                   ar.gender_id as gender_id,
                   CASE WHEN item.single_cover IS NOT NULL THEN 1 ELSE 0 END as has_single_cover,
                   CASE WHEN EXISTS(SELECT 1 FROM Album al WHERE al.id = item.album_id AND al.image IS NOT NULL) THEN 1 ELSE 0 END as album_has_image,
                   item.album_id, (SELECT name FROM Album WHERE id = item.album_id) as album_name,
                   (SELECT g.name FROM Genre g WHERE g.id = COALESCE(item.override_genre_id, (SELECT al2.override_genre_id FROM Album al2 WHERE al2.id = item.album_id), ar.genre_id)) as genre_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song item ON ce.song_id = item.id
            INNER JOIN Artist ar ON item.artist_id = ar.id
            WHERE c.period_type = 'seasonal' AND c.chart_type = 'song' AND c.period_key = ?
              AND ce.position > ?
            ORDER BY ce.position ASC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition(rs.getInt("position"));
            dto.setSongId(rs.getInt("item_id"));
            dto.setSongName(rs.getString("item_name"));
            dto.setAlbumId(rs.getObject("album_id") != null ? rs.getInt("album_id") : null);
            dto.setAlbumName(rs.getString("album_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setHasImage(rs.getInt("has_single_cover") == 1);
            dto.setAlbumHasImage(rs.getInt("album_has_image") == 1);
            dto.setGenreName(rs.getString("genre_name"));

            return dto;
        }, periodKey, SEASONAL_YEARLY_SONGS_COUNT);
    }
    
    /**
     * Get chart history for an item (song or album) on a specific period type (seasonal/yearly).
     * For detail page display - returns list of maps with position, periodKey, and displayName.
     * @param itemId The song or album ID
     * @param chartType "song" or "album"
     * @param periodType "seasonal" or "yearly"
     */
    public List<Map<String, Object>> getChartHistoryForItem(Integer itemId, String chartType, String periodType) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        // Exclude extra songs (position > 30) from chart history
        String sql = String.format("""
            SELECT ce.position, c.period_key
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND c.period_type = ? AND c.chart_type = ? AND c.is_finalized = 1
              AND ce.position <= %d
            ORDER BY c.period_start_date DESC
            """, idColumn, SEASONAL_YEARLY_SONGS_COUNT);
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            int position = rs.getInt("position");
            String periodKey = rs.getString("period_key");
            entry.put("position", position);
            entry.put("periodKey", periodKey);
            entry.put("displayName", "#" + position + " in " + 
                ("seasonal".equals(periodType) ? formatSeasonPeriodKey(periodKey) : periodKey));
            return entry;
        }, itemId, periodType, chartType);
    }

    /**
     * Get past finalized chart appearances for a specific song or album, excluding the given period key.
     * Used in the chart editor to show whether an item has previously charted.
     * Includes gender_id for color coding in the UI.
     *
     * @param itemId          Song or album ID
     * @param chartType       "song" or "album"
     * @param periodType      "seasonal" or "yearly"
     * @param excludePeriodKey The current period being edited (excluded from results)
     */
    public List<Map<String, Object>> getPastChartAppearancesForItem(Integer itemId, String chartType, String periodType, String excludePeriodKey) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String itemTable = "song".equals(chartType) ? "Song" : "Album";
        int maxPosition = "seasonal".equals(periodType) && "song".equals(chartType)
            ? SEASONAL_YEARLY_SONGS_COUNT + SEASONAL_EXTRA_SONGS_COUNT
            : SEASONAL_YEARLY_SONGS_COUNT;
        String genderJoin = "song".equals(chartType) 
            ? "LEFT JOIN Artist a ON s.artist_id = a.id"
            : "LEFT JOIN Artist a ON al.artist_id = a.id";
        String itemAlias = "song".equals(chartType) ? "s" : "al";
        
        String sql = String.format("""
            SELECT ce.position, c.period_key, a.gender_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN %s %s ON ce.%s = %s.id
            %s
            WHERE ce.%s = ? AND c.period_type = ? AND c.chart_type = ? AND c.is_finalized = 1
              AND ce.position <= %d
              AND c.period_key != ?
            ORDER BY c.period_start_date DESC
            """, itemTable, itemAlias, idColumn, itemAlias, genderJoin, idColumn, maxPosition);

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            int position = rs.getInt("position");
            String pk = rs.getString("period_key");
            Integer genderId = rs.getObject("gender_id", Integer.class);
            entry.put("position", position);
            entry.put("periodKey", pk);
            entry.put("genderId", genderId);
            entry.put("displayName", "seasonal".equals(periodType) ? formatSeasonPeriodKey(pk) : pk);
            return entry;
        }, itemId, periodType, chartType, excludePeriodKey != null ? excludePeriodKey : "");
    }

    /**
     * Get chart history for all songs/albums by an artist on a specific period type.
     * Returns list of maps with item info (song/album ID and name) and chart position.
     * @param artistId The artist ID
     * @param chartType "song" or "album"
     * @param periodType "seasonal" or "yearly"
     */
    public List<Map<String, Object>> getArtistChartHistoryByPeriodType(Integer artistId, String chartType, String periodType) {
        String itemTable = "song".equals(chartType) ? "Song" : "Album";
        String itemAlias = "song".equals(chartType) ? "s" : "a";
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String itemIdKey = "song".equals(chartType) ? "songId" : "albumId";
        String itemNameKey = "song".equals(chartType) ? "songName" : "albumName";
        
        // Exclude extra songs (position > 30) from chart history
        String sql = String.format("""
            SELECT ce.position, c.period_key, %s.id as item_id, %s.name as item_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN %s %s ON ce.%s = %s.id
            WHERE %s.artist_id = ? AND c.period_type = ? AND c.chart_type = ? AND c.is_finalized = 1
              AND ce.position <= %d
            ORDER BY c.period_key DESC, ce.position ASC
            """, itemAlias, itemAlias, itemTable, itemAlias, idColumn, itemAlias, itemAlias, SEASONAL_YEARLY_SONGS_COUNT);
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("position", rs.getInt("position"));
            String periodKey = rs.getString("period_key");
            entry.put("periodKey", periodKey);
            entry.put("displayName", "seasonal".equals(periodType) ? formatSeasonPeriodKey(periodKey) : periodKey);
            entry.put(itemIdKey, rs.getInt("item_id"));
            entry.put(itemNameKey, rs.getString("item_name"));
            applyItunesPresence(entry, chartType);
            return entry;
        }, artistId, periodType, chartType);
    }

    /**
     * Get chart history for songs in an album on a specific period type.
     * @param albumId The album ID
     * @param periodType "seasonal" or "yearly"
     */
    public List<Map<String, Object>> getAlbumSongsChartHistoryByPeriodType(Integer albumId, String periodType) {
        // Exclude extra songs (position > 30) from chart history
        String sql = String.format("""
            SELECT ce.position, c.period_key, s.id as song_id, s.name as song_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.album_id = ? AND c.period_type = ? AND c.chart_type = 'song' AND c.is_finalized = 1
              AND ce.position <= %d
            ORDER BY c.period_key DESC, ce.position ASC
            """, SEASONAL_YEARLY_SONGS_COUNT);
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("position", rs.getInt("position"));
            String periodKey = rs.getString("period_key");
            entry.put("periodKey", periodKey);
            entry.put("displayName", "seasonal".equals(periodType) ? formatSeasonPeriodKey(periodKey) : periodKey);
            entry.put("songId", rs.getInt("song_id"));
            entry.put("songName", rs.getString("song_name"));
            applyItunesPresence(entry, "song");
            return entry;
        }, albumId, periodType);
    }
    
    /**
     * Format season period key for display (e.g., "2024-Winter" -> "Winter 2024")
     */
    public String formatSeasonPeriodKey(String periodKey) {
        String[] parts = periodKey.split("-");
        if (parts.length == 2) {
            return parts[1] + " " + parts[0];
        }
        return periodKey;
    }
    
    /**
     * Get all weeks that have charts (for weekly overview page).
     * Returns list of maps with periodKey, displayName, hasChart, #1 info, etc.
     */
    public List<Map<String, Object>> getAllWeeksWithCharts() {
        // Get all weeks that have song charts
        String sql = """
            SELECT c.period_key, c.period_start_date, c.period_end_date, c.is_finalized,
                   (SELECT COUNT(*) FROM ChartEntry ce WHERE ce.chart_id = c.id) as entry_count
            FROM Chart c
            WHERE c.chart_type = 'song' AND c.period_type IS NULL
            ORDER BY c.period_start_date DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            String periodKey = rs.getString("period_key");
            Map<String, Object> map = new HashMap<>();
            map.put("periodKey", periodKey);
            map.put("displayName", formatWeekPeriodKey(periodKey));
            map.put("periodStartDate", rs.getString("period_start_date"));
            map.put("periodEndDate", rs.getString("period_end_date"));
            map.put("entryCount", rs.getInt("entry_count"));
            
            // Get #1 song and album names
            Map<String, String> topEntries = getWeeklyChartTopEntries(periodKey);
            map.put("numberOneSongName", topEntries.get("song"));
            map.put("numberOneAlbumName", topEntries.get("album"));
            
            return map;
        });
    }
    
    /**
     * Format a week period key (e.g., "2024-W45") to display name (e.g., "Week 45, 2024").
     */
    public String formatWeekPeriodKey(String periodKey) {
        if (periodKey == null) return null;
        // Format: "2024-W45" -> "Week 45, 2024"
        String[] parts = periodKey.split("-W");
        if (parts.length == 2) {
            return "Week " + Integer.parseInt(parts[1]) + ", " + parts[0];
        }
        return periodKey;
    }
    
    /**
     * Get the previous season period key (for navigation).
     * Season order: Winter(1), Spring(2), Summer(3), Fall(4)
     */
    public String getPreviousSeasonPeriodKey(String periodKey) {
        if (periodKey == null) return null;
        String[] parts = periodKey.split("-");
        if (parts.length != 2) return null;
        
        int year = Integer.parseInt(parts[0]);
        String season = parts[1];
        
        return switch (season) {
            case "Winter" -> (year - 1) + "-Fall";
            case "Spring" -> year + "-Winter";
            case "Summer" -> year + "-Spring";
            case "Fall" -> year + "-Summer";
            default -> null;
        };
    }
    
    /**
     * Get the next season period key (for navigation).
     */
    public String getNextSeasonPeriodKey(String periodKey) {
        if (periodKey == null) return null;
        String[] parts = periodKey.split("-");
        if (parts.length != 2) return null;
        
        int year = Integer.parseInt(parts[0]);
        String season = parts[1];
        
        return switch (season) {
            case "Winter" -> year + "-Spring";
            case "Spring" -> year + "-Summer";
            case "Summer" -> year + "-Fall";
            case "Fall" -> (year + 1) + "-Winter";
            default -> null;
        };
    }
    
    /**
     * Check if a season has play data OR an existing chart entry.
     * Used for prev/next navigation on the seasonal chart editor.
     */
    public boolean hasSeasonData(String periodKey) {
        LocalDate[] dateRange = parseSeasonPeriodKeyToDateRange(periodKey);
        if (dateRange == null) return false;
        
        String playSql = "SELECT COUNT(*) FROM Play WHERE play_date >= ? AND play_date <= ?";
        Integer playCount = jdbcTemplate.queryForObject(playSql, Integer.class, 
            dateRange[0].toString(), dateRange[1].toString());
        if (playCount != null && playCount > 0) return true;

        // Also consider seasons that have chart records (so navigation works for pre-play charts)
        String chartSql = "SELECT COUNT(*) FROM Chart WHERE period_type = 'seasonal' AND period_key = ?";
        Integer chartCount = jdbcTemplate.queryForObject(chartSql, Integer.class, periodKey);
        return chartCount != null && chartCount > 0;
    }
    
    /**
     * Get the previous year period key (for navigation).
     */
    public String getPreviousYearPeriodKey(String periodKey) {
        if (periodKey == null) return null;
        try {
            int year = Integer.parseInt(periodKey);
            return String.valueOf(year - 1);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Get the next year period key (for navigation).
     */
    public String getNextYearPeriodKey(String periodKey) {
        if (periodKey == null) return null;
        try {
            int year = Integer.parseInt(periodKey);
            return String.valueOf(year + 1);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Check if a year has play data OR an existing chart entry.
     * Used for prev/next navigation on the yearly chart editor.
     */
    public boolean hasYearData(String periodKey) {
        if (periodKey == null) return false;
        String playSql = "SELECT COUNT(*) FROM Play WHERE strftime('%Y', play_date) = ?";
        Integer playCount = jdbcTemplate.queryForObject(playSql, Integer.class, periodKey);
        if (playCount != null && playCount > 0) return true;

        // Also consider years that have chart records (so navigation works for pre-play charts)
        String chartSql = "SELECT COUNT(*) FROM Chart WHERE period_type = 'yearly' AND period_key = ?";
        Integer chartCount = jdbcTemplate.queryForObject(chartSql, Integer.class, periodKey);
        return chartCount != null && chartCount > 0;
    }
    
    /**
     * Get all seasons that have play data OR chart entries (for chart list page dropdown).
     */
    public List<Map<String, Object>> getAllSeasonsWithData() {
        String sql = """
            SELECT DISTINCT period_key, sort_order FROM (
                SELECT
                    CASE
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) = 12
                            THEN (CAST(strftime('%Y', play_date) AS INTEGER) + 1) || '-Winter'
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (1, 2)
                            THEN strftime('%Y', play_date) || '-Winter'
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (3, 4, 5)
                            THEN strftime('%Y', play_date) || '-Spring'
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (6, 7, 8)
                            THEN strftime('%Y', play_date) || '-Summer'
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (9, 10, 11)
                            THEN strftime('%Y', play_date) || '-Fall'
                    END as period_key,
                    CASE
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) = 12
                            THEN (CAST(strftime('%Y', play_date) AS INTEGER) + 1) * 10 + 1
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (1, 2)
                            THEN CAST(strftime('%Y', play_date) AS INTEGER) * 10 + 1
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (3, 4, 5)
                            THEN CAST(strftime('%Y', play_date) AS INTEGER) * 10 + 2
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (6, 7, 8)
                            THEN CAST(strftime('%Y', play_date) AS INTEGER) * 10 + 3
                        WHEN CAST(strftime('%m', play_date) AS INTEGER) IN (9, 10, 11)
                            THEN CAST(strftime('%Y', play_date) AS INTEGER) * 10 + 4
                    END as sort_order
                FROM Play
                WHERE play_date IS NOT NULL
                UNION
                SELECT
                    c.period_key,
                    CAST(SUBSTR(c.period_key, 1, 4) AS INTEGER) * 10 +
                        CASE SUBSTR(c.period_key, 6)
                            WHEN 'Winter' THEN 1
                            WHEN 'Spring' THEN 2
                            WHEN 'Summer' THEN 3
                            WHEN 'Fall'   THEN 4
                            ELSE 0
                        END as sort_order
                FROM Chart c
                WHERE c.period_type = 'seasonal'
            ) combined
            ORDER BY sort_order DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            String periodKey = rs.getString("period_key");
            Map<String, Object> map = new HashMap<>();
            map.put("periodKey", periodKey);
            map.put("displayName", formatSeasonPeriodKey(periodKey));
            map.put("isComplete", isSeasonComplete(periodKey));
            return map;
        });
    }
    
    /**
     * Get all years that have play data OR chart entries (for chart list page dropdown).
     */
    public List<Map<String, Object>> getAllYearsWithData() {
        String sql = """
            SELECT DISTINCT period_key FROM (
                SELECT DISTINCT strftime('%Y', play_date) as period_key
                FROM Play
                WHERE play_date IS NOT NULL
                UNION
                SELECT DISTINCT period_key
                FROM Chart
                WHERE period_type = 'yearly'
            ) combined
            ORDER BY period_key DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            String periodKey = rs.getString("period_key");
            Map<String, Object> map = new HashMap<>();
            map.put("periodKey", periodKey);
            map.put("displayName", periodKey);
            map.put("isComplete", isYearComplete(periodKey));
            return map;
        });
    }
    
    public List<ChartSongOverviewRowDTO> getChartOverviewSongRows(String periodType) {
        return buildChartOverviewSongRows(periodType);
    }

    public List<ChartAlbumOverviewRowDTO> getChartOverviewAlbumRows(String periodType) {
        return buildChartOverviewAlbumRows(periodType);
    }

    public List<ChartAlbumOverviewRowDTO> getChartOverviewAlbumRows(List<ChartSongOverviewRowDTO> songRows) {
        return buildChartOverviewAlbumRows(songRows);
    }

    public List<ChartArtistOverviewRowDTO> getChartOverviewArtistRows(String periodType) {
        return getChartOverviewArtistRows(periodType, false);
    }

    public List<ChartArtistOverviewRowDTO> getChartOverviewArtistRows(String periodType, boolean includeFeatured) {
        List<ChartSongOverviewRowDTO> songRows = buildChartOverviewSongRows(periodType);
        List<ChartAlbumOverviewRowDTO> albumRows = buildChartOverviewAlbumRows(periodType);
        return buildChartOverviewArtistRows(periodType, songRows, albumRows, includeFeatured);
    }

    public List<ChartArtistOverviewRowDTO> getChartOverviewArtistRows(String periodType,
                                                                       List<ChartSongOverviewRowDTO> songRows) {
        return getChartOverviewArtistRows(periodType, songRows, List.of(), false);
    }

    public List<ChartArtistOverviewRowDTO> getChartOverviewArtistRows(String periodType,
                                                                       List<ChartSongOverviewRowDTO> songRows,
                                                                       List<ChartAlbumOverviewRowDTO> albumRows) {
        return getChartOverviewArtistRows(periodType, songRows, albumRows, false);
    }

    public List<ChartArtistOverviewRowDTO> getChartOverviewArtistRows(String periodType,
                                                                       List<ChartSongOverviewRowDTO> songRows,
                                                                       List<ChartAlbumOverviewRowDTO> albumRows,
                                                                       boolean includeFeatured) {
        return buildChartOverviewArtistRows(periodType, songRows, albumRows, includeFeatured);
    }

    public List<ChartArtistOverviewRowDTO> getChartOverviewArtistRows(List<ChartSongOverviewRowDTO> songRows) {
        return buildChartOverviewArtistRows(null, songRows, List.of(), false);
    }

    public List<ChartArtistOverviewRowDTO> getChartOverviewArtistRows(List<ChartSongOverviewRowDTO> songRows, List<ChartAlbumOverviewRowDTO> albumRows) {
        return buildChartOverviewArtistRows(null, songRows, albumRows, false);
    }

    private List<ChartSongOverviewRowDTO> buildChartOverviewSongRows(String periodType) {
        String sql = """
            SELECT ce.song_id,
                   s.name as song_name,
                   s.album_id,
                   al.name as album_name,
                   ar.id as artist_id,
                   ar.name as artist_name,
                   ar.gender_id,
                   CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END as has_image,
                   CASE WHEN al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id) THEN 1 ELSE 0 END as album_has_image,
                   CASE WHEN ar.image IS NOT NULL THEN 1 ELSE 0 END as artist_has_image,
                   ce.position,
                   c.period_key,
                   c.period_start_date
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE c.chart_type = 'song'
              AND c.period_type = ?
              AND c.is_finalized = 1
            """ + getChartOverviewPositionClause(periodType) + """
            ORDER BY c.period_start_date ASC, ce.position ASC, ce.id ASC
            """;

        List<ChartOverviewAppearanceRow> appearances = jdbcTemplate.query(sql, (rs, rowNum) -> new ChartOverviewAppearanceRow(
            rs.getInt("song_id"),
            rs.getString("song_name"),
            rs.getObject("album_id") != null ? rs.getInt("album_id") : null,
            rs.getString("album_name"),
            rs.getInt("artist_id"),
            rs.getString("artist_name"),
            rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null,
            rs.getInt("has_image") == 1,
            rs.getInt("album_has_image") == 1,
            rs.getInt("artist_has_image") == 1,
            rs.getInt("position"),
            rs.getString("period_key"),
            rs.getString("period_start_date")
        ), periodType);

        Map<Integer, ChartOverviewSongAccumulator> accumulators = new LinkedHashMap<>();
        for (ChartOverviewAppearanceRow appearance : appearances) {
            accumulators.computeIfAbsent(appearance.songId(), ignored -> new ChartOverviewSongAccumulator(appearance))
                .addAppearance(appearance);
        }

        List<ChartSongOverviewRowDTO> rows = new ArrayList<>();
        for (ChartOverviewSongAccumulator accumulator : accumulators.values()) {
            rows.add(accumulator.toDto(periodType));
        }

        rows.sort(Comparator.comparingInt(ChartSongOverviewRowDTO::getPeakPosition)
            .thenComparing(ChartSongOverviewRowDTO::getTotalChartSpan, Comparator.reverseOrder())
            .thenComparing(ChartSongOverviewRowDTO::getSpanAtPeak, Comparator.reverseOrder())
            .thenComparing(ChartSongOverviewRowDTO::getFirstAppearanceSortValue, Comparator.nullsLast(String::compareTo))
            .thenComparing(ChartSongOverviewRowDTO::getSongTitle, String.CASE_INSENSITIVE_ORDER));
        return rows;
    }

    private List<ChartAlbumOverviewRowDTO> buildChartOverviewAlbumRows(String periodType) {
        String sql = """
            SELECT ce.album_id,
                   al.name as album_name,
                   ar.id as artist_id,
                   ar.name as artist_name,
                   ar.gender_id,
                   CASE WHEN al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id) THEN 1 ELSE 0 END as has_image,
                   CASE WHEN ar.image IS NOT NULL THEN 1 ELSE 0 END as artist_has_image,
                   ce.position,
                   c.period_key,
                   c.period_start_date
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            INNER JOIN Artist ar ON al.artist_id = ar.id
            WHERE c.chart_type = 'album'
              AND c.period_type = ?
              AND c.is_finalized = 1
            """ + getChartOverviewAlbumPositionClause(periodType) + """
            ORDER BY c.period_start_date ASC, ce.position ASC, ce.id ASC
            """;

        List<ChartOverviewAlbumAppearanceRow> appearances = jdbcTemplate.query(sql, (rs, rowNum) -> new ChartOverviewAlbumAppearanceRow(
            rs.getInt("album_id"),
            rs.getString("album_name"),
            rs.getInt("artist_id"),
            rs.getString("artist_name"),
            rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null,
            rs.getInt("has_image") == 1,
            rs.getInt("artist_has_image") == 1,
            rs.getInt("position"),
            rs.getString("period_key"),
            rs.getString("period_start_date")
        ), periodType);

        Map<Integer, ChartOverviewAlbumChartAccumulator> accumulators = new LinkedHashMap<>();
        for (ChartOverviewAlbumAppearanceRow appearance : appearances) {
            accumulators.computeIfAbsent(appearance.albumId(), ignored -> new ChartOverviewAlbumChartAccumulator(appearance))
                .addAppearance(appearance);
        }

        List<ChartAlbumOverviewRowDTO> rows = new ArrayList<>();
        for (ChartOverviewAlbumChartAccumulator accumulator : accumulators.values()) {
            rows.add(accumulator.toDto(periodType));
        }

        rows.sort(Comparator.comparing(ChartAlbumOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo))
            .thenComparing(ChartAlbumOverviewRowDTO::getTotalChartSpan, Comparator.reverseOrder())
            .thenComparing(ChartAlbumOverviewRowDTO::getSpanAtPeak, Comparator.reverseOrder())
            .thenComparing(ChartAlbumOverviewRowDTO::getFirstDebutSortValue, Comparator.nullsLast(String::compareTo))
            .thenComparing(ChartAlbumOverviewRowDTO::getAlbumName, String.CASE_INSENSITIVE_ORDER));
        return rows;
    }

    private List<ChartAlbumOverviewRowDTO> buildChartOverviewAlbumRows(List<ChartSongOverviewRowDTO> songRows) {
        Map<Integer, ChartOverviewAlbumAccumulator> accumulators = new LinkedHashMap<>();

        for (ChartSongOverviewRowDTO row : songRows) {
            if (row.getAlbumId() == null) {
                continue;
            }
            accumulators.computeIfAbsent(row.getAlbumId(), ignored -> new ChartOverviewAlbumAccumulator(row))
                .addSong(row);
        }

        List<ChartAlbumOverviewRowDTO> rows = new ArrayList<>();
        for (ChartOverviewAlbumAccumulator accumulator : accumulators.values()) {
            rows.add(accumulator.toDto());
        }

        rows.sort(Comparator.comparing(ChartAlbumOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo))
            .thenComparing(ChartAlbumOverviewRowDTO::getTotalChartSpan, Comparator.reverseOrder())
            .thenComparing(ChartAlbumOverviewRowDTO::getNumberOneSongsCount, Comparator.reverseOrder())
            .thenComparing(ChartAlbumOverviewRowDTO::getFirstDebutSortValue, Comparator.nullsLast(String::compareTo))
            .thenComparing(ChartAlbumOverviewRowDTO::getAlbumName, String.CASE_INSENSITIVE_ORDER));
        return rows;
    }

    private List<ChartArtistOverviewRowDTO> buildChartOverviewArtistRows(String periodType,
                                                                         List<ChartSongOverviewRowDTO> songRows,
                                                                         List<ChartAlbumOverviewRowDTO> albumRows,
                                                                         boolean includeFeatured) {
        Map<Integer, ChartOverviewArtistAccumulator> accumulators = new LinkedHashMap<>();

        for (ChartSongOverviewRowDTO row : songRows) {
            accumulators.computeIfAbsent(row.getArtistId(), ignored -> new ChartOverviewArtistAccumulator(periodType, row))
                .addSong(row);
        }

        if (includeFeatured) {
            Map<Integer, List<FeaturedArtistOverviewRef>> featuredArtistRefsBySongId = getFeaturedArtistOverviewRefsBySongId(songRows);
            for (ChartSongOverviewRowDTO row : songRows) {
                if (row.getSongId() == null) {
                    continue;
                }
                List<FeaturedArtistOverviewRef> featuredArtistRefs = featuredArtistRefsBySongId.get(row.getSongId());
                if (featuredArtistRefs == null || featuredArtistRefs.isEmpty()) {
                    continue;
                }
                for (FeaturedArtistOverviewRef featuredArtistRef : featuredArtistRefs) {
                    if (Objects.equals(featuredArtistRef.artistId(), row.getArtistId())) {
                        continue;
                    }
                    ChartSongOverviewRowDTO featuredRow = copySongOverviewRowForArtist(row, featuredArtistRef);
                    accumulators.computeIfAbsent(featuredArtistRef.artistId(), ignored -> new ChartOverviewArtistAccumulator(periodType, featuredRow))
                        .addFeaturedSongMetrics(featuredRow);
                }
            }
        }

        for (ChartAlbumOverviewRowDTO row : albumRows) {
            accumulators.computeIfAbsent(row.getResolvedArtistId(), ignored -> new ChartOverviewArtistAccumulator(periodType, row))
                .addAlbum(row);
        }

        List<ChartArtistOverviewRowDTO> rows = new ArrayList<>();
        for (ChartOverviewArtistAccumulator accumulator : accumulators.values()) {
            rows.add(accumulator.toDto());
        }

        rows.sort(Comparator.comparing(ChartArtistOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo))
            .thenComparing(ChartArtistOverviewRowDTO::getTotalChartSpan, Comparator.reverseOrder())
            .thenComparing(ChartArtistOverviewRowDTO::getAlbumTotalChartSpan, Comparator.reverseOrder())
            .thenComparing(ChartArtistOverviewRowDTO::getNumberOneSongsCount, Comparator.reverseOrder())
            .thenComparing(ChartArtistOverviewRowDTO::getNumberOneAlbumsCount, Comparator.reverseOrder())
            .thenComparing(ChartArtistOverviewRowDTO::getFirstDebutSortValue, Comparator.nullsLast(String::compareTo))
            .thenComparing(ChartArtistOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER));
        return rows;
    }

    private Map<Integer, List<FeaturedArtistOverviewRef>> getFeaturedArtistOverviewRefsBySongId(List<ChartSongOverviewRowDTO> songRows) {
        List<Integer> songIds = songRows.stream()
            .map(ChartSongOverviewRowDTO::getSongId)
            .filter(Objects::nonNull)
            .distinct()
            .toList();
        if (songIds.isEmpty()) {
            return Map.of();
        }

        String placeholders = songIds.stream()
            .map(ignored -> "?")
            .collect(Collectors.joining(","));
        String sql = """
            SELECT sfa.song_id,
                   a.id AS artist_id,
                   a.name AS artist_name,
                   a.gender_id,
                   CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END AS has_image
            FROM SongFeaturedArtist sfa
            INNER JOIN Artist a ON a.id = sfa.artist_id
            WHERE sfa.song_id IN (%s)
            ORDER BY sfa.song_id ASC, a.name COLLATE NOCASE ASC
            """.formatted(placeholders);

        Map<Integer, List<FeaturedArtistOverviewRef>> refsBySongId = new LinkedHashMap<>();
        jdbcTemplate.query(sql, (rs) -> {
            Integer songId = rs.getInt("song_id");
            FeaturedArtistOverviewRef ref = new FeaturedArtistOverviewRef(
                rs.getInt("artist_id"),
                rs.getString("artist_name"),
                resolveGenderClass(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null),
                rs.getInt("has_image") == 1
            );
            refsBySongId.computeIfAbsent(songId, ignored -> new ArrayList<>()).add(ref);
        }, songIds.toArray());
        return refsBySongId;
    }

    private ChartSongOverviewRowDTO copySongOverviewRowForArtist(ChartSongOverviewRowDTO source, FeaturedArtistOverviewRef featuredArtistRef) {
        ChartSongOverviewRowDTO target = new ChartSongOverviewRowDTO();
        target.setSongId(source.getSongId());
        target.setAlbumId(source.getAlbumId());
        target.setArtistId(featuredArtistRef.artistId());
        target.setSongTitle(source.getSongTitle());
        target.setAlbumName(source.getAlbumName());
        target.setArtistName(featuredArtistRef.artistName());
        target.setHasImage(source.isHasImage());
        target.setAlbumHasImage(source.isAlbumHasImage());
        target.setArtistHasImage(featuredArtistRef.hasImage());
        target.setFirstAppearanceLabel(source.getFirstAppearanceLabel());
        target.setFirstAppearanceKey(source.getFirstAppearanceKey());
        target.setFirstAppearanceSortValue(source.getFirstAppearanceSortValue());
        target.setLastAppearanceLabel(source.getLastAppearanceLabel());
        target.setLastAppearanceKey(source.getLastAppearanceKey());
        target.setLastAppearanceSortValue(source.getLastAppearanceSortValue());
        target.setPeakAppearanceLabel(source.getPeakAppearanceLabel());
        target.setPeakAppearanceKey(source.getPeakAppearanceKey());
        target.setPeakAppearanceSortValue(source.getPeakAppearanceSortValue());
        target.setDebutPosition(source.getDebutPosition());
        target.setTotalChartSpan(source.getTotalChartSpan());
        target.setPeakPosition(source.getPeakPosition());
        target.setSpanAtPeak(source.getSpanAtPeak());
        target.setSpanAtTop1(source.getSpanAtTop1());
        target.setSpanAtTop5(source.getSpanAtTop5());
        target.setSpanAtTop10(source.getSpanAtTop10());
        target.setSpanAtTopThresholds(source.getSpanAtTopThresholds());
        target.setGenderClass(featuredArtistRef.genderClass());
        target.setLinkedSongTitles(source.getLinkedSongTitles());
        return target;
    }

    private static String formatArtistOverviewNumberOneSongLabel(String periodType, ChartSongOverviewRowDTO row) {
        if (row == null || row.getSongTitle() == null || row.getSongTitle().isBlank()) {
            return null;
        }

        String qualifier = null;
        if ("weekly".equals(periodType) && row.getSpanAtTop1() > 0) {
            qualifier = row.getSpanAtTop1() == 1 ? "1 week" : row.getSpanAtTop1() + " weeks";
        } else if ("yearly".equals(periodType) && row.getPeakPosition() == 1
                && row.getPeakAppearanceLabel() != null && !row.getPeakAppearanceLabel().isBlank()) {
            qualifier = row.getPeakAppearanceLabel();
        }

        return qualifier == null ? row.getSongTitle() : row.getSongTitle() + " (" + qualifier + ")";
    }

    private static String formatArtistOverviewNumberOneAlbumLabel(String periodType, ChartAlbumOverviewRowDTO row) {
        if (row == null || row.getAlbumName() == null || row.getAlbumName().isBlank()) {
            return null;
        }

        String qualifier = null;
        if ("weekly".equals(periodType) && row.getHighestPeak() != null && row.getHighestPeak() == 1 && row.getSpanAtPeak() > 0) {
            qualifier = row.getSpanAtPeak() == 1 ? "1 week" : row.getSpanAtPeak() + " weeks";
        } else if ("yearly".equals(periodType) && row.getHighestPeak() != null && row.getHighestPeak() == 1
                && row.getPeakAppearanceDate() != null && !row.getPeakAppearanceDate().isBlank()) {
            qualifier = row.getPeakAppearanceDate();
        }

        return qualifier == null ? row.getAlbumName() : row.getAlbumName() + " (" + qualifier + ")";
    }

    public String formatOverviewPeriodLabel(String periodType, String periodKey) {
        if (periodKey == null) {
            return null;
        }
        return switch (periodType) {
            case "weekly" -> formatPeriodKey(periodKey);
            case "seasonal" -> formatSeasonPeriodKey(periodKey);
            case "yearly" -> periodKey;
            default -> periodKey;
        };
    }

    private String getChartOverviewPositionClause(String periodType) {
        if ("seasonal".equals(periodType)) {
            return " AND ce.position <= 35\n";  // Include main 30 + 5 extras
        }
        if ("yearly".equals(periodType)) {
            return " AND ce.position <= " + SEASONAL_YEARLY_SONGS_COUNT + "\n";
        }
        return "\n";
    }

    private String getChartOverviewAlbumPositionClause(String periodType) {
        if ("seasonal".equals(periodType)) {
            return " AND ce.position <= " + SEASONAL_ALBUMS_COUNT + "\n";
        }
        if ("yearly".equals(periodType)) {
            return " AND ce.position <= " + YEARLY_ALBUMS_COUNT + "\n";
        }
        return " AND ce.position <= " + TOP_ALBUMS_COUNT + "\n";
    }

    private int getMaxSongOverviewThreshold(String periodType) {
        if ("seasonal".equals(periodType)) {
            return 35;
        }
        if ("yearly".equals(periodType)) {
            return SEASONAL_YEARLY_SONGS_COUNT;
        }
        return TOP_SONGS_COUNT;
    }

    private int getMaxAlbumOverviewThreshold(String periodType) {
        if ("seasonal".equals(periodType)) {
            return SEASONAL_ALBUMS_COUNT;
        }
        if ("yearly".equals(periodType)) {
            return YEARLY_ALBUMS_COUNT;
        }
        return TOP_ALBUMS_COUNT;
    }

    /**
     * Resolves genderId to CSS gender class for styling.
     * IMPORTANT: This application's gender_id mapping is:
     *   - genderId 1 = FEMALE artists (pink/magenta color)
     *   - genderId 2 = MALE artists (blue color)
     * This is NOT intuitive order but matches the original schema.
     * All gender-aware styling cascades from this method.
     */
    private String resolveGenderClass(Integer genderId) {
        if (genderId == null) {
            return null;
        }
        if (genderId == 1) {
            return "gender-female";
        }
        if (genderId == 2) {
            return "gender-male";
        }
        return null;
    }

    private List<String> buildLinkedSongTooltipItems(Integer songId) {
        if (!appConfigService.isCombineLinkedSongsEnabled() || songId == null) {
            return List.of();
        }

        List<LinkedSongDTO> linkedSongs = songLinkService.getLinkedSongs(songId);
        if (linkedSongs == null || linkedSongs.size() <= 1) {
            return List.of();
        }

        long distinctArtists = linkedSongs.stream()
                .map(LinkedSongDTO::getArtistName)
                .filter(Objects::nonNull)
                .distinct()
                .count();
        boolean includeArtist = distinctArtists > 1;

        return linkedSongs.stream()
                .map(linkedSong -> formatLinkedSongTooltipItem(linkedSong, includeArtist))
                .filter(item -> item != null && !item.isBlank())
                .toList();
    }

    private String formatLinkedSongTooltipItem(LinkedSongDTO linkedSong, boolean includeArtist) {
        if (linkedSong == null || linkedSong.getName() == null || linkedSong.getName().isBlank()) {
            return null;
        }

        StringBuilder label = new StringBuilder();
        if (includeArtist && linkedSong.getArtistName() != null && !linkedSong.getArtistName().isBlank()) {
            label.append(linkedSong.getArtistName()).append(" - ");
        }
        label.append(linkedSong.getName());
        if (linkedSong.getAlbumName() != null && !linkedSong.getAlbumName().isBlank()) {
            label.append(" (").append(linkedSong.getAlbumName()).append(")");
        }
        return label.toString();
    }

    private record ChartOverviewAppearanceRow(
        Integer songId,
        String songTitle,
        Integer albumId,
        String albumName,
        Integer artistId,
        String artistName,
        Integer genderId,
        boolean hasImage,
        boolean albumHasImage,
        boolean artistHasImage,
        int position,
        String periodKey,
        String periodStartDate
    ) {
    }

    private record ChartOverviewAlbumAppearanceRow(
        Integer albumId,
        String albumName,
        Integer artistId,
        String artistName,
        Integer genderId,
        boolean hasImage,
        boolean artistHasImage,
        int position,
        String periodKey,
        String periodStartDate
    ) {
    }

    private record FeaturedArtistOverviewRef(
        Integer artistId,
        String artistName,
        String genderClass,
        boolean hasImage
    ) {
    }

    private final class ChartOverviewSongAccumulator {

        private final Integer songId;
        private final Integer albumId;
        private final Integer artistId;
        private final String songTitle;
        private final String albumName;
        private final String artistName;
        private final Integer genderId;
        private final boolean hasImage;
        private final boolean albumHasImage;
        private final boolean artistHasImage;
        private int totalChartSpan;
        private int peakPosition;
        private int spanAtPeak;
        private int spanAtTop1;
        private int spanAtTop5;
        private int spanAtTop10;
        private final List<Integer> appearancePositions = new ArrayList<>();
        private Integer debutPosition;
        private String firstAppearanceKey;
        private String firstAppearanceSortValue;
        private String lastAppearanceKey;
        private String lastAppearanceSortValue;
        private String peakAppearanceKey;
        private String peakAppearanceSortValue;

        private ChartOverviewSongAccumulator(ChartOverviewAppearanceRow appearance) {
            this.songId = appearance.songId();
            this.albumId = appearance.albumId();
            this.artistId = appearance.artistId();
            this.songTitle = appearance.songTitle();
            this.albumName = appearance.albumName();
            this.artistName = appearance.artistName();
            this.genderId = appearance.genderId();
            this.hasImage = appearance.hasImage();
            this.albumHasImage = appearance.albumHasImage();
            this.artistHasImage = appearance.artistHasImage();
            this.peakPosition = Integer.MAX_VALUE;
        }

        private void addAppearance(ChartOverviewAppearanceRow appearance) {
            totalChartSpan++;
            appearancePositions.add(appearance.position());

            if (firstAppearanceSortValue == null || appearance.periodStartDate().compareTo(firstAppearanceSortValue) < 0) {
                firstAppearanceKey = appearance.periodKey();
                firstAppearanceSortValue = appearance.periodStartDate();
                debutPosition = appearance.position();
            }

            if (lastAppearanceSortValue == null || appearance.periodStartDate().compareTo(lastAppearanceSortValue) > 0) {
                lastAppearanceKey = appearance.periodKey();
                lastAppearanceSortValue = appearance.periodStartDate();
            }

            if (appearance.position() < peakPosition) {
                peakPosition = appearance.position();
                spanAtPeak = 1;
                peakAppearanceKey = appearance.periodKey();
                peakAppearanceSortValue = appearance.periodStartDate();
            } else if (appearance.position() == peakPosition) {
                spanAtPeak++;
            }

            if (appearance.position() == 1) {
                spanAtTop1++;
            }
            if (appearance.position() <= 5) {
                spanAtTop5++;
            }
            if (appearance.position() <= 10) {
                spanAtTop10++;
            }
        }

        private ChartSongOverviewRowDTO toDto(String periodType) {
            int maxThreshold = getMaxSongOverviewThreshold(periodType);
            int[] spanAtTopThresholds = new int[maxThreshold + 1];
            for (Integer position : appearancePositions) {
                if (position == null) {
                    continue;
                }
                int safePosition = Math.max(1, Math.min(position, maxThreshold));
                for (int threshold = safePosition; threshold <= maxThreshold; threshold++) {
                    spanAtTopThresholds[threshold]++;
                }
            }

            ChartSongOverviewRowDTO dto = new ChartSongOverviewRowDTO();
            dto.setSongId(songId);
            dto.setAlbumId(albumId);
            dto.setArtistId(artistId);
            dto.setSongTitle(songTitle);
            dto.setAlbumName(albumName);
            dto.setArtistName(artistName);
            dto.setHasImage(hasImage);
            dto.setAlbumHasImage(albumHasImage);
            dto.setArtistHasImage(artistHasImage);
            dto.setFirstAppearanceKey(firstAppearanceKey);
            dto.setFirstAppearanceLabel(formatOverviewPeriodLabel(periodType, firstAppearanceKey));
            dto.setFirstAppearanceSortValue(firstAppearanceSortValue);
            dto.setLastAppearanceKey(lastAppearanceKey);
            dto.setLastAppearanceLabel(formatOverviewPeriodLabel(periodType, lastAppearanceKey));
            dto.setLastAppearanceSortValue(lastAppearanceSortValue);
            dto.setPeakAppearanceKey(peakAppearanceKey);
            dto.setPeakAppearanceLabel(formatOverviewPeriodLabel(periodType, peakAppearanceKey));
            dto.setPeakAppearanceSortValue(peakAppearanceSortValue);
            dto.setDebutPosition(debutPosition);
            dto.setTotalChartSpan(totalChartSpan);
            dto.setPeakPosition(peakPosition == Integer.MAX_VALUE ? 0 : peakPosition);
            dto.setSpanAtPeak(spanAtPeak);
            dto.setSpanAtTop1(spanAtTop1);
            dto.setSpanAtTop5(spanAtTop5);
            dto.setSpanAtTop10(spanAtTop10);
            dto.setSpanAtTopThresholds(spanAtTopThresholds);
            dto.setGenderClass(resolveGenderClass(genderId));
            dto.setLinkedSongTitles(buildLinkedSongTooltipItems(songId));
            return dto;
        }
    }

    private static final class ChartOverviewAlbumAccumulator {

        private final Integer albumId;
        private final Integer resolvedArtistId;
        private final String albumName;
        private final String artistName;
        private final String genderClass;
        private final boolean hasImage;
        private final boolean artistHasImage;
        private int chartedSongsCount;
        private int totalChartSpan;
        private Integer highestPeak;
        private int numberOneSongsCount;
        private int totalSpanAtNumberOne;
        private String firstDebutDate;
        private String firstDebutKey;
        private String firstDebutSortValue;
        private String lastAppearanceDate;
        private String lastAppearanceKey;
        private String lastAppearanceSortValue;

        private ChartOverviewAlbumAccumulator(ChartSongOverviewRowDTO row) {
            this.albumId = row.getAlbumId();
            this.resolvedArtistId = row.getArtistId();
            this.albumName = row.getAlbumName();
            this.artistName = row.getArtistName();
            this.genderClass = row.getGenderClass();
            this.hasImage = row.isAlbumHasImage();
            this.artistHasImage = row.isArtistHasImage();
        }

        private void addSong(ChartSongOverviewRowDTO row) {
            chartedSongsCount++;
            totalChartSpan += row.getTotalChartSpan();
            totalSpanAtNumberOne += row.getSpanAtTop1();

            if (highestPeak == null || row.getPeakPosition() < highestPeak) {
                highestPeak = row.getPeakPosition();
            }
            if (row.getSpanAtTop1() > 0) {
                numberOneSongsCount++;
            }

            if (firstDebutSortValue == null || compareSortValues(row.getFirstAppearanceSortValue(), firstDebutSortValue) < 0) {
                firstDebutDate = row.getFirstAppearanceLabel();
                firstDebutKey = row.getFirstAppearanceKey();
                firstDebutSortValue = row.getFirstAppearanceSortValue();
            }

            if (lastAppearanceSortValue == null || compareSortValues(row.getLastAppearanceSortValue(), lastAppearanceSortValue) > 0) {
                lastAppearanceDate = row.getLastAppearanceLabel();
                lastAppearanceKey = row.getLastAppearanceKey();
                lastAppearanceSortValue = row.getLastAppearanceSortValue();
            }
        }

        private ChartAlbumOverviewRowDTO toDto() {
            ChartAlbumOverviewRowDTO dto = new ChartAlbumOverviewRowDTO();
            dto.setAlbumId(albumId);
            dto.setResolvedArtistId(resolvedArtistId);
            dto.setAlbumName(albumName);
            dto.setArtistName(artistName);
            dto.setHasImage(hasImage);
            dto.setArtistHasImage(artistHasImage);
            dto.setChartedSongsCount(chartedSongsCount);
            dto.setTotalChartSpan(totalChartSpan);
            dto.setHighestPeak(highestPeak);
            dto.setNumberOneSongsCount(numberOneSongsCount);
            dto.setTotalSpanAtNumberOne(totalSpanAtNumberOne);
            dto.setFirstDebutDate(firstDebutDate);
            dto.setFirstDebutKey(firstDebutKey);
            dto.setFirstDebutSortValue(firstDebutSortValue);
            dto.setLastAppearanceDate(lastAppearanceDate);
            dto.setLastAppearanceKey(lastAppearanceKey);
            dto.setLastAppearanceSortValue(lastAppearanceSortValue);
            dto.setGenderClass(genderClass);
            return dto;
        }
    }

    private final class ChartOverviewAlbumChartAccumulator {

        private final Integer albumId;
        private final Integer resolvedArtistId;
        private final String albumName;
        private final String artistName;
        private final Integer genderId;
        private final boolean hasImage;
        private final boolean artistHasImage;
        private int totalChartSpan;
        private Integer highestPeak;
        private int spanAtPeak;
        private Integer debutPosition;
        private String firstDebutKey;
        private String firstDebutSortValue;
        private String lastAppearanceKey;
        private String lastAppearanceSortValue;
        private String peakAppearanceKey;
        private String peakAppearanceSortValue;
        private final List<Integer> appearancePositions = new ArrayList<>();

        private ChartOverviewAlbumChartAccumulator(ChartOverviewAlbumAppearanceRow appearance) {
            this.albumId = appearance.albumId();
            this.resolvedArtistId = appearance.artistId();
            this.albumName = appearance.albumName();
            this.artistName = appearance.artistName();
            this.genderId = appearance.genderId();
            this.hasImage = appearance.hasImage();
            this.artistHasImage = appearance.artistHasImage();
        }

        private void addAppearance(ChartOverviewAlbumAppearanceRow appearance) {
            totalChartSpan++;
            appearancePositions.add(appearance.position());

            if (firstDebutSortValue == null || appearance.periodStartDate().compareTo(firstDebutSortValue) < 0) {
                firstDebutKey = appearance.periodKey();
                firstDebutSortValue = appearance.periodStartDate();
                debutPosition = appearance.position();
            }

            if (lastAppearanceSortValue == null || appearance.periodStartDate().compareTo(lastAppearanceSortValue) > 0) {
                lastAppearanceKey = appearance.periodKey();
                lastAppearanceSortValue = appearance.periodStartDate();
            }

            if (highestPeak == null || appearance.position() < highestPeak) {
                highestPeak = appearance.position();
                spanAtPeak = 1;
                peakAppearanceKey = appearance.periodKey();
                peakAppearanceSortValue = appearance.periodStartDate();
            } else if (appearance.position() == highestPeak) {
                spanAtPeak++;
            }
        }

        private ChartAlbumOverviewRowDTO toDto(String periodType) {
            int maxThreshold = getMaxAlbumOverviewThreshold(periodType);
            int[] spanAtTopThresholds = new int[maxThreshold + 1];
            for (Integer position : appearancePositions) {
                if (position == null) {
                    continue;
                }
                int safePosition = Math.max(1, Math.min(position, maxThreshold));
                for (int threshold = safePosition; threshold <= maxThreshold; threshold++) {
                    spanAtTopThresholds[threshold]++;
                }
            }

            ChartAlbumOverviewRowDTO dto = new ChartAlbumOverviewRowDTO();
            dto.setAlbumId(albumId);
            dto.setResolvedArtistId(resolvedArtistId);
            dto.setAlbumName(albumName);
            dto.setArtistName(artistName);
            dto.setHasImage(hasImage);
            dto.setArtistHasImage(artistHasImage);
            dto.setTotalChartSpan(totalChartSpan);
            dto.setHighestPeak(highestPeak);
            dto.setSpanAtPeak(spanAtPeak);
            dto.setFirstDebutKey(firstDebutKey);
            dto.setFirstDebutDate(formatOverviewPeriodLabel(periodType, firstDebutKey));
            dto.setFirstDebutSortValue(firstDebutSortValue);
            dto.setDebutPosition(debutPosition);
            dto.setPeakAppearanceKey(peakAppearanceKey);
            dto.setPeakAppearanceDate(formatOverviewPeriodLabel(periodType, peakAppearanceKey));
            dto.setPeakAppearanceSortValue(peakAppearanceSortValue);
            dto.setLastAppearanceKey(lastAppearanceKey);
            dto.setLastAppearanceDate(formatOverviewPeriodLabel(periodType, lastAppearanceKey));
            dto.setLastAppearanceSortValue(lastAppearanceSortValue);
            dto.setSpanAtTopThresholds(spanAtTopThresholds);
            dto.setGenderClass(resolveGenderClass(genderId));
            return dto;
        }
    }

    private static final class ChartOverviewArtistAccumulator {

        private final String periodType;
        private final Integer resolvedArtistId;
        private final String artistName;
        private final String genderClass;
        private boolean hasImage;
        private int chartedSongsCount;
        private int totalChartSpan;
        private Integer highestPeak;
        private int numberOneSongsCount;
        private int totalSpanAtNumberOne;
        private String firstDebutDate;
        private String firstDebutKey;
        private String firstDebutSortValue;
        private String lastAppearanceDate;
        private String lastAppearanceKey;
        private String lastAppearanceSortValue;
        private int chartedAlbumsCount;
        private int albumTotalChartSpan;
        private Integer albumHighestPeak;
        private int numberOneAlbumsCount;
        private int albumTotalSpanAtNumberOne;
        private final Map<String, String> numberOneSongTitles = new LinkedHashMap<>();
        private final Map<String, String> numberOneAlbumTitles = new LinkedHashMap<>();
        private int[] topSongCounts = new int[0];
        private int[] topSongWeeks = new int[0];
        private int[] topAlbumCounts = new int[0];
        private int[] topAlbumWeeks = new int[0];

        private ChartOverviewArtistAccumulator(String periodType, ChartSongOverviewRowDTO row) {
            this.periodType = periodType;
            this.resolvedArtistId = row.getArtistId();
            this.artistName = row.getArtistName();
            this.genderClass = row.getGenderClass();
            this.hasImage = row.isArtistHasImage();
        }

        private ChartOverviewArtistAccumulator(String periodType, ChartAlbumOverviewRowDTO row) {
            this.periodType = periodType;
            this.resolvedArtistId = row.getResolvedArtistId();
            this.artistName = row.getArtistName();
            this.genderClass = row.getGenderClass();
            this.hasImage = row.isArtistHasImage();
        }

        private void addSong(ChartSongOverviewRowDTO row) {
            addSongMetrics(row, true);
        }

        private void addFeaturedSongMetrics(ChartSongOverviewRowDTO row) {
            addSongMetrics(row, false);
        }

        private void addSongMetrics(ChartSongOverviewRowDTO row, boolean includeTimelineMetrics) {
            hasImage = hasImage || row.isArtistHasImage();
            chartedSongsCount++;
            totalChartSpan += row.getTotalChartSpan();
            totalSpanAtNumberOne += row.getSpanAtTop1();

            if (includeTimelineMetrics && (highestPeak == null || row.getPeakPosition() < highestPeak)) {
                highestPeak = row.getPeakPosition();
            }
            if (row.getSpanAtTop1() > 0) {
                numberOneSongsCount++;
                String songKey = row.getSongId() != null ? "song:" + row.getSongId() : "song:" + row.getSongTitle();
                String label = formatArtistOverviewNumberOneSongLabel(periodType, row);
                numberOneSongTitles.putIfAbsent(songKey, label != null ? label : row.getSongTitle());
            }

            if (includeTimelineMetrics && (firstDebutSortValue == null || compareSortValues(row.getFirstAppearanceSortValue(), firstDebutSortValue) < 0)) {
                firstDebutDate = row.getFirstAppearanceLabel();
                firstDebutKey = row.getFirstAppearanceKey();
                firstDebutSortValue = row.getFirstAppearanceSortValue();
            }

            if (includeTimelineMetrics && (lastAppearanceSortValue == null || compareSortValues(row.getLastAppearanceSortValue(), lastAppearanceSortValue) > 0)) {
                lastAppearanceDate = row.getLastAppearanceLabel();
                lastAppearanceKey = row.getLastAppearanceKey();
                lastAppearanceSortValue = row.getLastAppearanceSortValue();
            }

            int[] thresholdSpans = row.getSpanAtTopThresholds();
            if (thresholdSpans != null && thresholdSpans.length > 0) {
                topSongCounts = ensureCapacity(topSongCounts, thresholdSpans.length);
                topSongWeeks = ensureCapacity(topSongWeeks, thresholdSpans.length);
                int peak = Math.max(1, row.getPeakPosition());
                for (int threshold = peak; threshold < thresholdSpans.length; threshold++) {
                    topSongCounts[threshold]++;
                    topSongWeeks[threshold] += thresholdSpans[threshold];
                }
            }
        }

        private void addAlbum(ChartAlbumOverviewRowDTO row) {
            hasImage = hasImage || row.isArtistHasImage();
            chartedAlbumsCount++;
            albumTotalChartSpan += row.getTotalChartSpan();
            albumTotalSpanAtNumberOne += row.getHighestPeak() != null && row.getHighestPeak() == 1 ? row.getSpanAtPeak() : 0;

            if (albumHighestPeak == null || (row.getHighestPeak() != null && row.getHighestPeak() < albumHighestPeak)) {
                albumHighestPeak = row.getHighestPeak();
            }
            if (row.getHighestPeak() != null && row.getHighestPeak() == 1) {
                numberOneAlbumsCount++;
                String albumKey = row.getAlbumId() != null ? "album:" + row.getAlbumId() : "album:" + row.getAlbumName();
                String label = formatArtistOverviewNumberOneAlbumLabel(periodType, row);
                numberOneAlbumTitles.putIfAbsent(albumKey, label != null ? label : row.getAlbumName());
            }

            int[] thresholdSpans = row.getSpanAtTopThresholds();
            if (thresholdSpans != null && thresholdSpans.length > 0 && row.getHighestPeak() != null) {
                topAlbumCounts = ensureCapacity(topAlbumCounts, thresholdSpans.length);
                topAlbumWeeks = ensureCapacity(topAlbumWeeks, thresholdSpans.length);
                int peak = Math.max(1, row.getHighestPeak());
                for (int threshold = peak; threshold < thresholdSpans.length; threshold++) {
                    topAlbumCounts[threshold]++;
                    topAlbumWeeks[threshold] += thresholdSpans[threshold];
                }
            }
        }

        private int[] ensureCapacity(int[] source, int requiredLength) {
            if (source.length >= requiredLength) {
                return source;
            }
            return Arrays.copyOf(source, requiredLength);
        }

        private ChartArtistOverviewRowDTO toDto() {
            ChartArtistOverviewRowDTO dto = new ChartArtistOverviewRowDTO();
            dto.setMatched(true);
            dto.setResolvedArtistId(resolvedArtistId);
            dto.setArtistName(artistName);
            dto.setHasImage(hasImage);
            dto.setChartedSongsCount(chartedSongsCount);
            dto.setTotalChartSpan(totalChartSpan);
            dto.setHighestPeak(highestPeak);
            dto.setNumberOneSongsCount(numberOneSongsCount);
            dto.setTotalSpanAtNumberOne(totalSpanAtNumberOne);
            dto.setFirstDebutDate(firstDebutDate);
            dto.setFirstDebutKey(firstDebutKey);
            dto.setFirstDebutSortValue(firstDebutSortValue);
            dto.setLastAppearanceDate(lastAppearanceDate);
            dto.setLastAppearanceKey(lastAppearanceKey);
            dto.setLastAppearanceSortValue(lastAppearanceSortValue);
            dto.setChartedAlbumsCount(chartedAlbumsCount);
            dto.setAlbumTotalChartSpan(albumTotalChartSpan);
            dto.setAlbumHighestPeak(albumHighestPeak);
            dto.setNumberOneAlbumsCount(numberOneAlbumsCount);
            dto.setAlbumTotalSpanAtNumberOne(albumTotalSpanAtNumberOne);
            dto.setTopSongCounts(topSongCounts);
            dto.setTopSongWeeks(topSongWeeks);
            dto.setTopAlbumCounts(topAlbumCounts);
            dto.setTopAlbumWeeks(topAlbumWeeks);
            dto.setNumberOneSongTitles(new ArrayList<>(numberOneSongTitles.values()));
            dto.setNumberOneAlbumTitles(new ArrayList<>(numberOneAlbumTitles.values()));
            dto.setGenderClass(genderClass);
            return dto;
        }
    }

    private static int compareSortValues(String left, String right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return left.compareTo(right);
    }
    
    /**
     * Get chart run history for an album (for the expandable detail view or detail page).
     * If currentPeriodKey is null, uses the latest weekly chart as the end point.
     */
    public AlbumChartRunDTO getAlbumChartRun(Integer albumId, String currentPeriodKey) {
        // Get album and artist names
        List<Map<String, Object>> nameRows = jdbcTemplate.queryForList(
            "SELECT al.name as album_name, ar.name as artist_name FROM Album al INNER JOIN Artist ar ON al.artist_id = ar.id WHERE al.id = ?", 
            albumId);
        
        AlbumChartRunDTO dto = new AlbumChartRunDTO();
        dto.setAlbumId(albumId);
        if (!nameRows.isEmpty()) {
            dto.setAlbumName((String) nameRows.get(0).get("album_name"));
            dto.setArtistName((String) nameRows.get(0).get("artist_name"));
        }
        
        // If no currentPeriodKey provided, use the latest weekly chart
        if (currentPeriodKey == null) {
            Optional<Chart> latestChart = getLatestWeeklyChart("album");
            if (latestChart.isPresent()) {
                currentPeriodKey = latestChart.get().getPeriodKey();
            } else {
                // No charts exist
                dto.setWeeks(new ArrayList<>());
                dto.setWeeksAtTop1(0);
                dto.setWeeksAtTop5(0);
                dto.setWeeksAtTop10(0);
                dto.setTotalWeeksOnChart(0);
                return dto;
            }
        }
        
        String historySql = """
            SELECT c.period_key, ce.position, c.period_start_date, c.period_end_date
            FROM Chart c
            LEFT JOIN ChartEntry ce ON c.id = ce.chart_id AND ce.album_id = ?
            WHERE c.chart_type = 'album'
            AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            """;
        
        List<Map<String, Object>> history = jdbcTemplate.queryForList(historySql, albumId);
        
        // Find the first week this album appeared and the current week
        int firstAppearanceIndex = -1;
        int currentIndex = -1;
        for (int i = 0; i < history.size(); i++) {
            Map<String, Object> row = history.get(i);
            if (row.get("position") != null && firstAppearanceIndex == -1) {
                firstAppearanceIndex = i;
            }
            if (currentPeriodKey.equals(row.get("period_key"))) {
                currentIndex = i;
            }
        }
        
        // Only include weeks from first appearance to current
        List<AlbumChartRunDTO.ChartRunWeek> weeks = new ArrayList<>();
        int weeksAtTop1 = 0, weeksAtTop5 = 0, weeksAtTop10 = 0;
        int peakPosition = Integer.MAX_VALUE;
        int totalWeeksOnChart = 0;
        
        if (firstAppearanceIndex >= 0 && currentIndex >= 0) {
            for (int i = firstAppearanceIndex; i <= currentIndex; i++) {
                Map<String, Object> row = history.get(i);
                String periodKey = (String) row.get("period_key");
                Integer position = row.get("position") != null ? ((Number) row.get("position")).intValue() : null;
                String startDate = (String) row.get("period_start_date");
                String endDate = (String) row.get("period_end_date");
                String dateRange = formatDateRange(startDate, endDate);
                
                boolean isCurrent = periodKey.equals(currentPeriodKey);
                weeks.add(new AlbumChartRunDTO.ChartRunWeek(periodKey, position, isCurrent, dateRange));
                
                if (position != null) {
                    totalWeeksOnChart++;
                    if (position <= 1) weeksAtTop1++;
                    if (position <= 5) weeksAtTop5++;
                    if (position <= 10) weeksAtTop10++;
                    if (position < peakPosition) peakPosition = position;
                }
            }
        }
        
        dto.setWeeks(weeks);
        dto.setWeeksAtTop1(weeksAtTop1);
        dto.setWeeksAtTop5(weeksAtTop5);
        dto.setWeeksAtTop10(weeksAtTop10);
        dto.setTotalWeeksOnChart(totalWeeksOnChart);
        dto.setPeakPosition(peakPosition == Integer.MAX_VALUE ? null : peakPosition);
        
        return dto;
    }
    
    /**
     * Get chart run history for a song without specifying a period key.
     * Uses the latest weekly chart as the end point.
     */
    public ChartRunDTO getSongChartRunAllTime(Integer songId) {
        // Get the latest weekly chart for songs
        Optional<Chart> latestChart = getLatestWeeklyChart("song");
        if (latestChart.isPresent()) {
            return getSongChartRun(songId, latestChart.get().getPeriodKey());
        }
        
        // No charts exist - return empty DTO
        ChartRunDTO dto = new ChartRunDTO();
        dto.setSongId(songId);
        dto.setWeeks(new ArrayList<>());
        dto.setWeeksAtTop1(0);
        dto.setWeeksAtTop5(0);
        dto.setWeeksAtTop10(0);
        dto.setWeeksAtTop20(0);
        dto.setTotalWeeksOnChart(0);
        return dto;
    }
    
    // =====================================
    // AGGREGATED CHART HISTORY METHODS
    // For including group artists' chart history
    // =====================================
    
    /**
     * Get aggregated weekly song chart history for an artist including all groups
     */
    public List<ChartHistoryDTO> getAggregatedArtistSongChartHistory(Integer artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        
        String sql = """
            SELECT s.id, s.name, ar.id as artist_id, ar.name as artist_name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks,
                   MAX(CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                   s.album_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE s.artist_id IN (%s) AND c.chart_type = 'song' AND c.period_type = 'weekly'
            GROUP BY s.id, s.name, ar.id, ar.name, s.album_id
            """.formatted(placeholders);
        
        List<ChartHistoryDTO> result = new ArrayList<>();
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, allArtistIds.toArray());
        
        for (Map<String, Object> row : rows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDate(songId, "song");
            String peakDate = getFirstPeakDate(songId, peakPosition, "song");
            String debutDate = getDebutDate(songId, "song");
            String peakWeek = getFirstPeakWeek(songId, peakPosition, "song");
            String debutWeek = getDebutWeek(songId, "song");
            boolean hasImage = ((Number) row.get("has_image")).intValue() == 1;
            Integer albumId = row.get("album_id") != null ? ((Number) row.get("album_id")).intValue() : null;
            Integer songArtistId = ((Number) row.get("artist_id")).intValue();

            ChartHistoryDTO dto = new ChartHistoryDTO(
                songId,
                (String) row.get("name"),
                (String) row.get("artist_name"),
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "song",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            dto.setHasImage(hasImage);
            dto.setAlbumId(albumId);
            applyItunesPresence(dto);

            // Mark as from group if artist is different from requested artist
            if (!songArtistId.equals(artistId)) {
                dto.setFromGroup(true);
                dto.setSourceArtistId(songArtistId);
                dto.setSourceArtistName((String) row.get("artist_name"));
            }
            result.add(dto);
        }
        
        result.sort((a, b) -> {
            int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
            if (peakCompare != 0) return peakCompare;
            return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
        });
        
        return result;
    }

    public List<ChartHistoryDTO> combineLinkedSongChartHistory(List<ChartHistoryDTO> rows) {
        if (!appConfigService.isCombineLinkedSongsEnabled() || rows == null || rows.isEmpty()) {
            return rows;
        }

        List<Integer> songIds = rows.stream()
                .map(ChartHistoryDTO::getId)
                .filter(Objects::nonNull)
                .toList();
        if (songIds.isEmpty()) {
            return rows;
        }

        Map<Integer, Integer> groupIds = songLinkService.getGroupIdsForSongs(songIds);
        Map<String, List<ChartHistoryDTO>> grouped = new LinkedHashMap<>();
        for (ChartHistoryDTO row : rows) {
            Integer groupId = groupIds.get(row.getId());
            String key = groupId != null ? "g:" + groupId : "s:" + row.getId();
            grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row);
        }

        List<ChartHistoryDTO> combined = new ArrayList<>();
        for (List<ChartHistoryDTO> group : grouped.values()) {
            if (group.size() == 1) {
                ChartHistoryDTO single = group.get(0);
                single.setTotalWeekBreakdownItems(buildLinkedSongWeekBreakdownItems(single.getId()));
                combined.add(single);
                continue;
            }

                ChartHistoryDTO representative = songLinkService.chooseRepresentativeSong(group, ChartHistoryDTO::getId, ChartHistoryDTO::getName);
                if (representative == null) {
                representative = group.get(0);
                }

            List<Integer> linkedSongIds = group.stream()
                    .map(ChartHistoryDTO::getId)
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList();

            CombinedSongChartHistoryMetrics metrics = getCombinedSongChartHistoryMetrics(linkedSongIds);

            ChartHistoryDTO dto = copyChartHistory(representative);
            dto.setHasImage(group.stream().anyMatch(ChartHistoryDTO::isHasImage));
            dto.setPeakPosition(metrics.peakPosition());
            dto.setWeeksAtPeak(metrics.weeksAtPeak());
            dto.setTotalWeeks(metrics.totalWeeks());
            dto.setPeakDate(metrics.peakDate());
            dto.setPeakWeek(metrics.peakWeek());
            dto.setDebutDate(metrics.debutDate());
            dto.setDebutWeek(metrics.debutWeek());
            dto.setTotalWeekBreakdownItems(buildLinkedSongWeekBreakdownItems(linkedSongIds));

            dto.setFeaturedOn(group.stream().allMatch(ChartHistoryDTO::isFeaturedOn));
            if (dto.isFeaturedOn()) {
                dto.setSourceArtistId(representative.getSourceArtistId());
                dto.setSourceArtistName(representative.getSourceArtistName());
            } else {
                dto.setSourceArtistId(null);
                dto.setSourceArtistName(null);
            }

            dto.setFromGroup(group.stream().allMatch(ChartHistoryDTO::isFromGroup));
            if (dto.isFromGroup()) {
                dto.setSourceArtistId(representative.getSourceArtistId());
                dto.setSourceArtistName(representative.getSourceArtistName());
            } else if (!dto.isFeaturedOn()) {
                dto.setSourceArtistId(null);
                dto.setSourceArtistName(null);
            }

            combined.add(dto);
        }

        combined.sort((a, b) -> {
            int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
            if (peakCompare != 0) {
                return peakCompare;
            }
            int weeksAtPeakCompare = b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
            if (weeksAtPeakCompare != 0) {
                return weeksAtPeakCompare;
            }
            return Integer.compare(b.getTotalWeeks() != null ? b.getTotalWeeks() : 0,
                    a.getTotalWeeks() != null ? a.getTotalWeeks() : 0);
        });
        return combined;
    }
    
    /**
     * Get aggregated weekly album chart history for an artist including all groups
     */
    public List<ChartHistoryDTO> getAggregatedArtistAlbumChartHistory(Integer artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        
        String sql = """
            SELECT al.id, al.name, ar.id as artist_id, ar.name as artist_name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            INNER JOIN Artist ar ON al.artist_id = ar.id
            WHERE al.artist_id IN (%s) AND c.chart_type = 'album' AND c.period_type = 'weekly'
            GROUP BY al.id, al.name, ar.id, ar.name
            """.formatted(placeholders);
        
        List<ChartHistoryDTO> result = new ArrayList<>();
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, allArtistIds.toArray());
        
        for (Map<String, Object> row : rows) {
            Integer albumId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(albumId, peakPosition, "album");
            String releaseDate = getReleaseDate(albumId, "album");
            String peakDate = getFirstPeakDate(albumId, peakPosition, "album");
            String debutDate = getDebutDate(albumId, "album");
            String peakWeek = getFirstPeakWeek(albumId, peakPosition, "album");
            String debutWeek = getDebutWeek(albumId, "album");
            Integer albumArtistId = ((Number) row.get("artist_id")).intValue();

            ChartHistoryDTO dto = new ChartHistoryDTO(
                albumId,
                (String) row.get("name"),
                (String) row.get("artist_name"),
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "album",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            applyItunesPresence(dto);

            // Mark as from group if artist is different from requested artist
            if (!albumArtistId.equals(artistId)) {
                dto.setFromGroup(true);
                dto.setSourceArtistId(albumArtistId);
                dto.setSourceArtistName((String) row.get("artist_name"));
            }
            result.add(dto);
        }
        
        result.sort((a, b) -> {
            int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
            if (peakCompare != 0) return peakCompare;
            return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
        });
        
        return result;
    }
    
    /**
     * Get aggregated seasonal/yearly chart history for an artist including all groups
     */
    public List<Map<String, Object>> getAggregatedArtistChartHistoryByPeriodType(Integer artistId, List<Integer> groupIds, String chartType, String periodType) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String itemTable = "song".equals(chartType) ? "Song" : "Album";
        String itemAlias = "song".equals(chartType) ? "s" : "a";
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String itemIdKey = "song".equals(chartType) ? "songId" : "albumId";
        String itemNameKey = "song".equals(chartType) ? "songName" : "albumName";
        
        String sql = String.format("""
            SELECT ce.position, c.period_key, %s.id as item_id, %s.name as item_name, ar.id as artist_id, ar.name as artist_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN %s %s ON ce.%s = %s.id
            INNER JOIN Artist ar ON %s.artist_id = ar.id
            WHERE %s.artist_id IN (%s) AND c.period_type = ? AND c.chart_type = ? AND c.is_finalized = 1
              AND ce.position <= %d
            ORDER BY c.period_key DESC, ce.position ASC
            """, itemAlias, itemAlias, itemTable, itemAlias, idColumn, itemAlias, itemAlias, itemAlias, placeholders, SEASONAL_YEARLY_SONGS_COUNT);
        
        List<Object> params = new ArrayList<>(allArtistIds);
        params.add(periodType);
        params.add(chartType);
        
        String finalPeriodType = periodType;
        Integer mainArtistId = artistId;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("position", rs.getInt("position"));
            String periodKey = rs.getString("period_key");
            entry.put("periodKey", periodKey);
            entry.put("displayName", "seasonal".equals(finalPeriodType) ? formatSeasonPeriodKey(periodKey) : periodKey);
            entry.put(itemIdKey, rs.getInt("item_id"));
            entry.put(itemNameKey, rs.getString("item_name"));
            entry.put("artistName", rs.getString("artist_name"));
            applyItunesPresence(entry, chartType);

            // Track if from group
            Integer itemArtistId = rs.getInt("artist_id");
            if (!itemArtistId.equals(mainArtistId)) {
                entry.put("fromGroup", true);
                entry.put("sourceArtistId", itemArtistId);
                entry.put("sourceArtistName", rs.getString("artist_name"));
            }
            return entry;
        }, params.toArray());
    }

    /**
     * Get compact weekly chart statistics for a song (total weeks, peak position, weeks at peak).
     * Used for detail page chips.
     */
    public WeeklyChartStatsDTO getSongWeeklyChartStats(Integer songId) {
        String sql = """
            SELECT ce.position
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ?
              AND c.chart_type = 'song'
              AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            """;

        List<Integer> positions = jdbcTemplate.queryForList(sql, Integer.class, songId);

        if (positions.isEmpty()) {
            return new WeeklyChartStatsDTO(0, null, 0);
        }

        int totalWeeks = positions.size();
        int peakPosition = Integer.MAX_VALUE;
        int weeksAtPeak = 0;

        for (Integer position : positions) {
            if (position < peakPosition) {
                peakPosition = position;
                weeksAtPeak = 1;
            } else if (position.equals(peakPosition)) {
                weeksAtPeak++;
            }
        }

        return new WeeklyChartStatsDTO(totalWeeks, peakPosition, weeksAtPeak);
    }

    /**
     * Get compact weekly chart statistics for an album (total weeks, peak position, weeks at peak).
     * Used for detail page chips.
     */
    public WeeklyChartStatsDTO getAlbumWeeklyChartStats(Integer albumId) {
        String sql = """
            SELECT ce.position
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ?
              AND c.chart_type = 'album'
              AND c.period_type = 'weekly'
            ORDER BY c.period_start_date ASC
            """;

        List<Integer> positions = jdbcTemplate.queryForList(sql, Integer.class, albumId);

        if (positions.isEmpty()) {
            return new WeeklyChartStatsDTO(0, null, 0);
        }

        int totalWeeks = positions.size();
        int peakPosition = Integer.MAX_VALUE;
        int weeksAtPeak = 0;

        for (Integer position : positions) {
            if (position < peakPosition) {
                peakPosition = position;
                weeksAtPeak = 1;
            } else if (position.equals(peakPosition)) {
                weeksAtPeak++;
            }
        }

        return new WeeklyChartStatsDTO(totalWeeks, peakPosition, weeksAtPeak);
    }

    /**
     * Get featured song weekly chart history for an artist (songs where this artist is featured).
     */
    public List<ChartHistoryDTO> getFeaturedArtistSongChartHistory(Integer artistId) {
        String sql = """
            SELECT s.id, s.name, ar.id as primary_artist_id, ar.name as primary_artist_name, 
                   MIN(ce.position) as peak_position, COUNT(*) as total_weeks,
                   MAX(CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                   s.album_id
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            INNER JOIN SongFeaturedArtist sfa ON sfa.song_id = s.id
            WHERE sfa.artist_id = ? AND c.chart_type = 'song' AND c.period_type = 'weekly'
            GROUP BY s.id, s.name, ar.id, ar.name, s.album_id
            """;

        List<ChartHistoryDTO> result = new ArrayList<>();
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, artistId);

        for (Map<String, Object> row : rows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDate(songId, "song");
            String peakDate = getFirstPeakDate(songId, peakPosition, "song");
            String debutDate = getDebutDate(songId, "song");
            String peakWeek = getFirstPeakWeek(songId, peakPosition, "song");
            String debutWeek = getDebutWeek(songId, "song");
            boolean hasImage = ((Number) row.get("has_image")).intValue() == 1;
            Integer albumId = row.get("album_id") != null ? ((Number) row.get("album_id")).intValue() : null;

            ChartHistoryDTO dto = new ChartHistoryDTO(
                songId,
                (String) row.get("name"),
                (String) row.get("primary_artist_name"),
                peakPosition,
                weeksAtPeak,
                totalWeeks,
                "song",
                releaseDate,
                peakDate,
                debutDate,
                peakWeek,
                debutWeek
            );
            dto.setHasImage(hasImage);
            dto.setAlbumId(albumId);
            dto.setFeaturedOn(true);
            dto.setSourceArtistId(((Number) row.get("primary_artist_id")).intValue());
            dto.setSourceArtistName((String) row.get("primary_artist_name"));
            dto.setTotalWeekBreakdownItems(buildLinkedSongWeekBreakdownItems(songId));
            applyItunesPresence(dto);
            result.add(dto);
        }

        return result;
    }

    /**
     * Get featured song seasonal/yearly chart history for an artist
     */
    public List<Map<String, Object>> getFeaturedArtistChartHistoryByPeriodType(Integer artistId, String periodType) {
        String sql = String.format("""
            SELECT ce.position, c.period_key, s.id as song_id, s.name as song_name, 
                   ar.id as primary_artist_id, ar.name as primary_artist_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            INNER JOIN SongFeaturedArtist sfa ON sfa.song_id = s.id
            WHERE sfa.artist_id = ? AND c.period_type = ? AND c.chart_type = 'song' AND c.is_finalized = 1
              AND ce.position <= %d
            ORDER BY c.period_key DESC, ce.position ASC
            """, SEASONAL_YEARLY_SONGS_COUNT);

        String finalPeriodType = periodType;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("position", rs.getInt("position"));
            String periodKey = rs.getString("period_key");
            entry.put("periodKey", periodKey);
            entry.put("displayName", "seasonal".equals(finalPeriodType) ? formatSeasonPeriodKey(periodKey) : periodKey);
            entry.put("songId", rs.getInt("song_id"));
            entry.put("songName", rs.getString("song_name"));
            entry.put("featuredOn", true);
            entry.put("sourceArtistId", rs.getInt("primary_artist_id"));
            entry.put("sourceArtistName", rs.getString("primary_artist_name"));
            applyItunesPresence(entry, "song");
            return entry;
        }, artistId, periodType);
    }

    private void applyItunesPresence(ChartHistoryDTO dto) {
        if (dto == null || dto.getId() == null) {
            return;
        }

        if ("song".equals(dto.getChartType())) {
            dto.setInItunes(resolveSongItunesPresence(dto.getId()));
            return;
        }

        dto.setInItunes(resolveAlbumItunesPresence(dto.getId()));
    }

    private void applyItunesPresence(Map<String, Object> entry, String chartType) {
        if (entry == null) {
            return;
        }

        String idKey = "song".equals(chartType) ? "songId" : "albumId";
        Object rawId = entry.get(idKey);
        if (!(rawId instanceof Number itemId)) {
            return;
        }

        if ("song".equals(chartType)) {
            entry.put("inItunes", resolveSongItunesPresence(itemId.intValue()));
            return;
        }

        entry.put("inItunes", resolveAlbumItunesPresence(itemId.intValue()));
    }

    private Boolean resolveSongItunesPresence(Integer songId) {
        Map<String, Object> songRow = jdbcTemplate.queryForMap(
            "SELECT ar.name AS artist_name, a.name AS album_name, s.name AS song_name " +
                "FROM Song s " +
                "INNER JOIN Artist ar ON s.artist_id = ar.id " +
                "LEFT JOIN Album a ON s.album_id = a.id " +
                "WHERE s.id = ?",
            songId
        );
        return itunesService.songExistsInItunes(
            (String) songRow.get("artist_name"),
            (String) songRow.get("album_name"),
            (String) songRow.get("song_name")
        );
    }

    private Boolean resolveAlbumItunesPresence(Integer albumId) {
        Map<String, Object> albumRow = jdbcTemplate.queryForMap(
            "SELECT ar.name AS artist_name, a.name AS album_name " +
                "FROM Album a " +
                "INNER JOIN Artist ar ON a.artist_id = ar.id " +
                "WHERE a.id = ?",
            albumId
        );
        return itunesService.albumExistsInItunes(
            (String) albumRow.get("artist_name"),
            (String) albumRow.get("album_name")
        );
    }
    
    /**
     * Get the count of distinct songs that reached #1 on the weekly songs chart for an artist.
     */
    public Integer getNumberOneSongsCount(Integer artistId) {
        String sql = """
            SELECT COUNT(DISTINCT s.id) as count
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.artist_id = ? 
              AND c.chart_type = 'song' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            """;
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get the total number of weeks an artist had songs at #1 on the weekly songs chart.
     */
    public Integer getNumberOneWeeksCount(Integer artistId) {
        String sql = """
            SELECT COUNT(*) as count
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.artist_id = ? 
              AND c.chart_type = 'song' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            """;
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get the list of songs that reached #1 on the weekly songs chart for an artist.
     * Returns song names sorted by the first week they reached #1 (most recent first).
     */
    public List<String> getNumberOneSongNames(Integer artistId) {
        String sql = """
            SELECT DISTINCT s.name, MIN(c.period_key) as first_number_one_week
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.artist_id = ? 
              AND c.chart_type = 'song' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            GROUP BY s.id, s.name
            ORDER BY first_number_one_week DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("name"), artistId);
    }
    
    /**
     * Get the count of distinct albums that reached #1 on the weekly albums chart for an artist.
     */
    public Integer getNumberOneAlbumsCount(Integer artistId) {
        String sql = """
            SELECT COUNT(DISTINCT al.id) as count
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            WHERE al.artist_id = ? 
              AND c.chart_type = 'album' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            """;
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get the total number of weeks an artist had albums at #1 on the weekly albums chart.
     */
    public Integer getNumberOneAlbumWeeksCount(Integer artistId) {
        String sql = """
            SELECT COUNT(*) as count
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            WHERE al.artist_id = ? 
              AND c.chart_type = 'album' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            """;
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get the list of albums that reached #1 on the weekly albums chart for an artist.
     * Returns album names sorted by the first week they reached #1 (most recent first).
     */
    public List<String> getNumberOneAlbumNames(Integer artistId) {
        String sql = """
            SELECT DISTINCT al.name, MIN(c.period_key) as first_number_one_week
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            WHERE al.artist_id = ? 
              AND c.chart_type = 'album' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            GROUP BY al.id, al.name
            ORDER BY first_number_one_week DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("name"), artistId);
    }
    
    /**
     * Get the count of distinct featured songs that reached #1 on the weekly songs chart for an artist.
     */
    public Integer getNumberOneFeaturedSongsCount(Integer artistId) {
        String sql = """
            SELECT COUNT(DISTINCT s.id) as count
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN SongFeaturedArtist sfa ON sfa.song_id = s.id
            WHERE sfa.artist_id = ? 
              AND c.chart_type = 'song' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            """;
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get the total number of weeks an artist had featured songs at #1 on the weekly songs chart.
     */
    public Integer getNumberOneFeaturedWeeksCount(Integer artistId) {
        String sql = """
            SELECT COUNT(*) as count
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN SongFeaturedArtist sfa ON sfa.song_id = s.id
            WHERE sfa.artist_id = ? 
              AND c.chart_type = 'song' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            """;
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get the list of featured songs that reached #1 on the weekly songs chart for an artist.
     * Returns song names (with primary artist name) sorted by the first week they reached #1 (most recent first).
     */
    public List<String> getNumberOneFeaturedSongNames(Integer artistId) {
        String sql = """
            SELECT DISTINCT s.name || ' (by ' || ar.name || ')' as display_name, 
                   MIN(c.period_key) as first_number_one_week
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            INNER JOIN SongFeaturedArtist sfa ON sfa.song_id = s.id
            WHERE sfa.artist_id = ? 
              AND c.chart_type = 'song' 
              AND c.period_type = 'weekly'
              AND ce.position = 1
            GROUP BY s.id, s.name, ar.name
            ORDER BY first_number_one_week DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("display_name"), artistId);
    }
    
    // ========== WEEKLY NUMBER ONES ==========
    
    /**
     * Get all #1 runs for songs or albums, most recent first.
     * Each run is a set of consecutive weeks where the same item was at #1.
     * Returns runs with cumulative week counts.
     */
    public List<NumberOneRunDTO> getNumberOneRuns(String type) {
        boolean isSong = "song".equals(type);
        
        // Get all #1 entries ordered chronologically (ASC for building runs, we reverse at the end)
        String sql;
        if (isSong) {
            sql = """
                SELECT 
                    s.id,
                    s.name,
                    ar.name as artist_name,
                    ar.id as artist_id,
                    ar.gender_id,
                    (CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                    s.album_id,
                    (CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END) as album_has_image,
                    c.period_start_date,
                    c.period_end_date
                FROM ChartEntry ce
                INNER JOIN Chart c ON ce.chart_id = c.id
                INNER JOIN Song s ON ce.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                WHERE c.chart_type = 'song'
                  AND c.period_type = 'weekly'
                  AND ce.position = 1
                ORDER BY c.period_start_date ASC
                """;
        } else {
            sql = """
                SELECT 
                    al.id,
                    al.name,
                    ar.name as artist_name,
                    ar.id as artist_id,
                    ar.gender_id,
                    CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                    NULL as album_id,
                    0 as album_has_image,
                    c.period_start_date,
                    c.period_end_date
                FROM ChartEntry ce
                INNER JOIN Chart c ON ce.chart_id = c.id
                INNER JOIN Album al ON ce.album_id = al.id
                INNER JOIN Artist ar ON al.artist_id = ar.id
                WHERE c.chart_type = 'album'
                  AND c.period_type = 'weekly'
                  AND ce.position = 1
                ORDER BY c.period_start_date ASC
                """;
        }
        
        // Fetch all #1 week entries chronologically
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        
        if (rows.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Build runs: consecutive weeks with the same item
        List<NumberOneRunDTO> runs = new ArrayList<>();
        
        Integer currentId = null;
        String currentStartDate = null;
        String currentEndDate = null;
        String currentName = null;
        String currentArtistName = null;
        Integer currentArtistId = null;
        Integer currentGenderId = null;
        boolean currentHasImage = false;
        Integer currentAlbumId = null;
        boolean currentAlbumHasImage = false;
        int runWeeks = 0;
        
        // Track cumulative weeks per item (id -> total weeks)
        Map<Integer, Integer> cumulativeMap = new HashMap<>();
        
        for (Map<String, Object> row : rows) {
            Integer id = ((Number) row.get("id")).intValue();
            
            if (currentId != null && id.equals(currentId)) {
                // Same item, extend the run
                runWeeks++;
                currentEndDate = (String) row.get("period_end_date");
            } else {
                // Different item or first entry - close previous run if any
                if (currentId != null) {
                    int cumulative = cumulativeMap.getOrDefault(currentId, 0) + runWeeks;
                    cumulativeMap.put(currentId, cumulative);
                    
                    NumberOneRunDTO dto = buildRunDTO(currentId, currentName, currentArtistName,
                            currentArtistId, currentGenderId, currentHasImage, isSong ? "song" : "album",
                            currentAlbumId, currentAlbumHasImage, runWeeks,
                            currentStartDate, currentEndDate, cumulative);
                    runs.add(dto);
                }
                
                // Start new run
                currentId = id;
                currentName = (String) row.get("name");
                currentArtistName = (String) row.get("artist_name");
                currentArtistId = ((Number) row.get("artist_id")).intValue();
                currentGenderId = row.get("gender_id") != null ? ((Number) row.get("gender_id")).intValue() : null;
                currentHasImage = ((Number) row.get("has_image")).intValue() == 1;
                currentAlbumId = row.get("album_id") != null ? ((Number) row.get("album_id")).intValue() : null;
                currentAlbumHasImage = ((Number) row.get("album_has_image")).intValue() == 1;
                currentStartDate = (String) row.get("period_start_date");
                currentEndDate = (String) row.get("period_end_date");
                runWeeks = 1;
            }
        }
        
        // Close the last run
        if (currentId != null) {
            int cumulative = cumulativeMap.getOrDefault(currentId, 0) + runWeeks;
            cumulativeMap.put(currentId, cumulative);
            
            NumberOneRunDTO dto = buildRunDTO(currentId, currentName, currentArtistName,
                    currentArtistId, currentGenderId, currentHasImage, isSong ? "song" : "album",
                    currentAlbumId, currentAlbumHasImage, runWeeks,
                    currentStartDate, currentEndDate, cumulative);
            runs.add(dto);
        }
        
        // Reverse to show most recent first
        Collections.reverse(runs);
        
        // Second pass: set grand total weeks at #1 on every run.
        // cumulativeMap now holds the final cumulative total for each item,
        // which equals the all-time total since we processed every week.
        for (NumberOneRunDTO run : runs) {
            run.setTotalWeeks(cumulativeMap.getOrDefault(run.getId(), run.getRunWeeks()));
        }
        
        return runs;
    }
    
    private NumberOneRunDTO buildRunDTO(Integer id, String name, String artistName,
            Integer artistId, Integer genderId, boolean hasImage, String type,
            Integer albumId, boolean albumHasImage, int runWeeks,
            String startDate, String endDate, int cumulativeWeeks) {
        NumberOneRunDTO dto = new NumberOneRunDTO();
        dto.setId(id);
        dto.setName(name);
        dto.setArtistName(artistName);
        dto.setArtistId(artistId);
        dto.setGenderId(genderId);
        dto.setHasImage(hasImage);
        dto.setType(type);
        dto.setAlbumId(albumId);
        dto.setAlbumHasImage(albumHasImage);
        dto.setRunWeeks(runWeeks);
        dto.setCumulativeWeeks(cumulativeWeeks);
        
        // Format dates from yyyy-MM-dd to dd/MM/yyyy
        dto.setRunStartDate(formatDateForDisplay(startDate));
        dto.setRunEndDate(formatDateForDisplay(endDate));
        
        return dto;
    }
    
    private String formatDateForDisplay(String isoDate) {
        if (isoDate == null || isoDate.isEmpty()) return "";
        try {
            LocalDate date = LocalDate.parse(isoDate);
            return String.format("%02d/%02d/%d", date.getDayOfMonth(), date.getMonthValue(), date.getYear());
        } catch (Exception e) {
            return isoDate;
        }
    }

    /**
     * Returns ALL songs for an album with their weekly chart stats.
     * Uncharted songs have charted=false and null chart fields.
     * Sorted by track_number ASC.
     */
    public List<Map<String, Object>> getAlbumAllSongsWithWeeklyStats(int albumId) {
        String allSongsSql =
            "SELECT s.id, s.name, s.track_number, " +
            "       COALESCE(s.release_date, a.release_date) AS release_date " +
            "FROM Song s " +
            "JOIN Album a ON s.album_id = a.id " +
            "WHERE s.album_id = ? " +
            "ORDER BY s.track_number ASC, s.name ASC";

        String chartedSql =
            "SELECT s.id AS song_id, MIN(ce.position) AS peak_position, COUNT(ce.id) AS total_weeks " +
            "FROM ChartEntry ce " +
            "JOIN Chart c ON ce.chart_id = c.id " +
            "JOIN Song s ON ce.song_id = s.id " +
            "WHERE s.album_id = ? AND c.chart_type = 'song' AND c.period_type = 'weekly' " +
            "GROUP BY s.id";

        List<Map<String, Object>> allSongs = jdbcTemplate.queryForList(allSongsSql, albumId);
        List<Map<String, Object>> chartedStats = jdbcTemplate.queryForList(chartedSql, albumId);

        Map<Integer, Map<String, Object>> chartedBySongId = new HashMap<>();
        for (Map<String, Object> row : chartedStats) {
            int songId = ((Number) row.get("song_id")).intValue();
            chartedBySongId.put(songId, row);
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> song : allSongs) {
            int songId = ((Number) song.get("id")).intValue();
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", songId);
            entry.put("name", song.get("name"));
            entry.put("trackNumber", song.get("track_number"));
            entry.put("releaseDate", getReleaseDate(songId, "song"));

            Map<String, Object> chartStat = chartedBySongId.get(songId);
            if (chartStat != null) {
                int peak = ((Number) chartStat.get("peak_position")).intValue();
                int totalWeeks = ((Number) chartStat.get("total_weeks")).intValue();
                int weeksAtPeak = countWeeksAtPosition(songId, peak, "song") != null
                    ? countWeeksAtPosition(songId, peak, "song") : 0;
                entry.put("charted", true);
                entry.put("peakPosition", peak);
                entry.put("weeksAtPeak", weeksAtPeak);
                entry.put("totalWeeks", totalWeeks);
                entry.put("peakDate", getFirstPeakDate(songId, peak, "song"));
                entry.put("debutDate", getDebutDate(songId, "song"));
                entry.put("peakWeek", getFirstPeakWeek(songId, peak, "song"));
                entry.put("debutWeek", getDebutWeek(songId, "song"));
                entry.put("peakNumberOne", peak == 1);
            } else {
                entry.put("charted", false);
                entry.put("peakPosition", null);
                entry.put("weeksAtPeak", null);
                entry.put("totalWeeks", null);
                entry.put("peakDate", null);
                entry.put("debutDate", null);
                entry.put("peakWeek", null);
                entry.put("debutWeek", null);
                entry.put("peakNumberOne", false);
            }
            entry.put("inItunes", resolveSongItunesPresence(songId));
            result.add(entry);
        }
        return result;
    }
}
