package library.service;

import library.dto.ChartEntryDTO;
import library.dto.ChartGenerationProgressDTO;
import library.dto.ChartHistoryDTO;
import library.dto.ChartRunDTO;
import library.entity.Chart;
import library.entity.ChartEntry;
import library.repository.ChartEntryRepository;
import library.repository.ChartRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class ChartService {
    
    private static final int TOP_SONGS_COUNT = 20;
    private static final int TOP_ALBUMS_COUNT = 10;
    
    private final ChartRepository chartRepository;
    private final ChartEntryRepository chartEntryRepository;
    private final JdbcTemplate jdbcTemplate;
    
    // Progress tracking for bulk generation
    private final ConcurrentHashMap<String, ChartGenerationProgressDTO> generationProgress = new ConcurrentHashMap<>();
    
    public ChartService(ChartRepository chartRepository, ChartEntryRepository chartEntryRepository, JdbcTemplate jdbcTemplate) {
        this.chartRepository = chartRepository;
        this.chartEntryRepository = chartEntryRepository;
        this.jdbcTemplate = jdbcTemplate;
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
     * Get a chart by type and period key.
     */
    public Optional<Chart> getChart(String chartType, String periodKey) {
        return chartRepository.findByChartTypeAndPeriodKey(chartType, periodKey);
    }
    
    /**
     * Get the previous chart (for navigation).
     */
    public Optional<Chart> getPreviousChart(String chartType, String periodKey) {
        return chartRepository.findPreviousChart(chartType, periodKey);
    }
    
    /**
     * Get the next chart (for navigation).
     */
    public Optional<Chart> getNextChart(String chartType, String periodKey) {
        return chartRepository.findNextChart(chartType, periodKey);
    }
    
    /**
     * Check if a week has completely passed (i.e., we are past the end date).
     * Charts should only be generated for weeks that have fully ended.
     */
    public boolean isWeekComplete(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate endDate = dateRange[1];
        LocalDate today = LocalDate.now();
        return today.isAfter(endDate);
    }
    
    /**
     * Generate a weekly song chart for a specific period.
     */
    @Transactional
    public Chart generateWeeklySongChart(String periodKey) {
        // Check if chart already exists
        Optional<Chart> existing = chartRepository.findByChartTypeAndPeriodKey("song", periodKey);
        if (existing.isPresent()) {
            return existing.get();
        }
        
        // Parse period key to get date range
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate startDate = dateRange[0];
        LocalDate endDate = dateRange[1];
        
        // Create the chart record
        Chart chart = new Chart("song", periodKey, startDate, endDate);
        chart = chartRepository.save(chart);
        
        // Query top 20 songs for this period
        // Tie-breaker: if same play count, the song that reached it first (earlier last scrobble) wins
        String sql = """
            SELECT s.id as song_id, COUNT(*) as play_count, MAX(scr.scrobble_date) as last_scrobble
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE DATE(scr.scrobble_date) >= ? AND DATE(scr.scrobble_date) <= ?
              AND scr.song_id IS NOT NULL
            GROUP BY s.id
            ORDER BY play_count DESC, last_scrobble ASC
            LIMIT ?
            """;
        
        Integer chartId = chart.getId();
        List<ChartEntry> entries = jdbcTemplate.query(sql, 
            (rs, rowNum) -> {
                int position = rowNum + 1;
                return ChartEntry.forSong(chartId, position, rs.getInt("song_id"), rs.getInt("play_count"));
            },
            startDate.toString(), endDate.toString(), TOP_SONGS_COUNT);
        
        // Save all entries
        chartEntryRepository.saveAll(entries);
        
        return chart;
    }
    
    /**
     * Generate a weekly album chart for a specific period.
     */
    @Transactional
    public Chart generateWeeklyAlbumChart(String periodKey) {
        // Check if chart already exists
        Optional<Chart> existing = chartRepository.findByChartTypeAndPeriodKey("album", periodKey);
        if (existing.isPresent()) {
            return existing.get();
        }
        
        // Parse period key to get date range
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate startDate = dateRange[0];
        LocalDate endDate = dateRange[1];
        
        // Create the chart record
        Chart chart = new Chart("album", periodKey, startDate, endDate);
        chart = chartRepository.save(chart);
        
        // Query top 10 albums for this period
        // Tie-breaker: if same play count, the album that reached it first (earlier last scrobble) wins
        String sql = """
            SELECT s.album_id as album_id, COUNT(*) as play_count, MAX(scr.scrobble_date) as last_scrobble
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE DATE(scr.scrobble_date) >= ? AND DATE(scr.scrobble_date) <= ?
              AND scr.song_id IS NOT NULL
              AND s.album_id IS NOT NULL
            GROUP BY s.album_id
            ORDER BY play_count DESC, last_scrobble ASC
            LIMIT ?
            """;
        
        Integer chartId = chart.getId();
        List<ChartEntry> entries = jdbcTemplate.query(sql, 
            (rs, rowNum) -> {
                int position = rowNum + 1;
                return ChartEntry.forAlbum(chartId, position, rs.getInt("album_id"), rs.getInt("play_count"));
            },
            startDate.toString(), endDate.toString(), TOP_ALBUMS_COUNT);
        
        // Save all entries
        chartEntryRepository.saveAll(entries);
        
        return chart;
    }
    
    /**
     * Generate both song and album charts for a specific period.
     * @throws IllegalArgumentException if the week has not yet completed
     */
    @Transactional
    public void generateWeeklyCharts(String periodKey) {
        if (!isWeekComplete(periodKey)) {
            LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
            throw new IllegalArgumentException(
                "Cannot generate chart for week " + periodKey + ": week has not ended yet. " +
                "The week ends on " + dateRange[1] + ". Please wait until after that date.");
        }
        generateWeeklySongChart(periodKey);
        generateWeeklyAlbumChart(periodKey);
    }
    
    /**
     * Get weekly chart with full statistics for display.
     */
    public List<ChartEntryDTO> getWeeklyChartWithStats(String periodKey) {
        Optional<Chart> chartOpt = chartRepository.findByChartTypeAndPeriodKey("song", periodKey);
        if (chartOpt.isEmpty()) {
            return Collections.emptyList();
        }
        
        Chart chart = chartOpt.get();
        
        // Get chart entries with song details
        List<Object[]> rawEntries = chartEntryRepository.findEntriesWithSongDetailsRaw(chart.getId());
        
        // Get previous chart for last week comparison
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
        
        // Get all charts for calculating peak, weeks on chart, etc.
        List<Chart> allCharts = chartRepository.findAllByChartTypeOrderByPeriodStartDateAsc("song");
        Map<String, Integer> chartIdToPeriodIndex = new HashMap<>();
        for (int i = 0; i < allCharts.size(); i++) {
            chartIdToPeriodIndex.put(allCharts.get(i).getPeriodKey(), i);
        }
        int currentChartIndex = chartIdToPeriodIndex.getOrDefault(periodKey, -1);
        
        List<ChartEntryDTO> result = new ArrayList<>();
        
        for (Object[] row : rawEntries) {
            ChartEntryDTO dto = new ChartEntryDTO();
            dto.setPosition((Integer) row[2]);
            dto.setSongId((Integer) row[3]);
            dto.setPlayCount((Integer) row[5]);
            dto.setSongName((String) row[6]);
            dto.setArtistName((String) row[7]);
            dto.setHasImage(((Number) row[8]).intValue() == 1);
            dto.setArtistId((Integer) row[9]);
            
            Integer songId = dto.getSongId();
            
            // Last week position
            if (lastWeekPositions.containsKey(songId)) {
                dto.setLastWeekPosition(lastWeekPositions.get(songId));
                dto.setLastWeekPlayCount(lastWeekPlayCounts.get(songId));
            } else {
                // Check if this is a re-entry (was on any previous chart but not last week)
                boolean wasOnPreviousChart = checkIfSongWasOnPreviousChart(songId, periodKey, prevChartOpt.map(Chart::getPeriodKey).orElse(null));
                if (wasOnPreviousChart) {
                    dto.setLastWeekPosition(-1); // -1 indicates re-entry
                }
                // else null = new entry
            }
            
            // Calculate peak, times at peak, and weeks on chart
            calculateChartRunStats(dto, songId, periodKey, currentChartIndex);
            
            result.add(dto);
        }
        
        return result;
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
     * Check if an album was on any chart before the previous chart (for re-entry detection).
     */
    private boolean checkIfAlbumWasOnPreviousChart(Integer albumId, String currentPeriodKey, String previousPeriodKey) {
        String sql = """
            SELECT COUNT(*) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ?
              AND c.chart_type = 'album'
              AND c.period_start_date < (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'album')
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
              AND c.period_start_date <= (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'album')
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
              AND c.period_start_date < (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'song')
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
              AND c.period_start_date <= (SELECT period_start_date FROM Chart WHERE period_key = ? AND chart_type = 'song')
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
        
        // Get all charts and this song's positions
        List<Chart> allCharts = chartRepository.findAllByChartTypeOrderByPeriodStartDateAsc("song");
        
        String historySql = """
            SELECT c.period_key, ce.position
            FROM Chart c
            LEFT JOIN ChartEntry ce ON c.id = ce.chart_id AND ce.song_id = ?
            WHERE c.chart_type = 'song'
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
                
                boolean isCurrent = periodKey.equals(currentPeriodKey);
                weeks.add(new ChartRunDTO.ChartRunWeek(periodKey, position, isCurrent));
                
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
     * Get all weeks that have scrobbles but no chart generated yet.
     * Only returns weeks that have completely passed (not the current ongoing week).
     */
    public List<String> getWeeksWithoutCharts() {
        String sql = """
            SELECT DISTINCT strftime('%Y-W%W', scr.scrobble_date) as period_key
            FROM Scrobble scr
            WHERE scr.scrobble_date IS NOT NULL
              AND scr.song_id IS NOT NULL
              AND strftime('%Y-W%W', scr.scrobble_date) NOT IN (
                  SELECT period_key FROM Chart WHERE chart_type = 'song'
              )
            ORDER BY period_key ASC
            """;
        List<String> allWeeks = jdbcTemplate.queryForList(sql, String.class);
        
        // Filter out weeks that haven't completed yet
        return allWeeks.stream()
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
     * Parse a period key (e.g., "2024-W48") to a date range.
     * Uses SQLite's %W week numbering convention to match how weeks are grouped in the database:
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
            SELECT s.single_cover
            FROM ChartEntry ce
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE ce.chart_id = ? AND ce.position = 1
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, byte[].class, chartOpt.get().getId());
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
        
        // Get song chart history for this artist
        String songSql = """
            SELECT s.id, s.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.artist_id = ? AND c.chart_type = 'song'
            GROUP BY s.id, s.name
            """;
        
        List<Map<String, Object>> songRows = jdbcTemplate.queryForList(songSql, artistId);
        for (Map<String, Object> row : songRows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            
            // Count weeks at peak
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDateSong(songId);
            String peakDate = getFirstPeakDateSong(songId, peakPosition);
            String debutDate = getDebutDateSong(songId);
            String peakWeek = getFirstPeakWeekSong(songId, peakPosition);
            String debutWeek = getDebutWeekSong(songId);
            
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
        
        // Get album chart history for this artist
        String albumSql = """
            SELECT al.id, al.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            WHERE al.artist_id = ? AND c.chart_type = 'album'
            GROUP BY al.id, al.name
            """;
        
        List<Map<String, Object>> albumRows = jdbcTemplate.queryForList(albumSql, artistId);
        for (Map<String, Object> row : albumRows) {
            Integer albumId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            
            // Count weeks at peak
            Integer weeksAtPeak = countWeeksAtPositionAlbum(albumId, peakPosition);
            String releaseDate = getReleaseDateAlbum(albumId);
            String peakDate = getFirstPeakDateAlbum(albumId, peakPosition);
            String debutDate = getDebutDateAlbum(albumId);
            String peakWeek = getFirstPeakWeekAlbum(albumId, peakPosition);
            String debutWeek = getDebutWeekAlbum(albumId);
            
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
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.artist_id = ? AND c.chart_type = 'song'
            GROUP BY s.id, s.name
            """;
        
        List<Map<String, Object>> songRows = jdbcTemplate.queryForList(songSql, artistId);
        for (Map<String, Object> row : songRows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDateSong(songId);
            String peakDate = getFirstPeakDateSong(songId, peakPosition);
            String debutDate = getDebutDateSong(songId);
            String peakWeek = getFirstPeakWeekSong(songId, peakPosition);
            String debutWeek = getDebutWeekSong(songId);
            
            result.add(new ChartHistoryDTO(
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
            ));
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
            WHERE al.artist_id = ? AND c.chart_type = 'album'
            GROUP BY al.id, al.name
            """;
        
        List<Map<String, Object>> albumRows = jdbcTemplate.queryForList(albumSql, artistId);
        for (Map<String, Object> row : albumRows) {
            Integer albumId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPositionAlbum(albumId, peakPosition);
            String releaseDate = getReleaseDateAlbum(albumId);
            String peakDate = getFirstPeakDateAlbum(albumId, peakPosition);
            String debutDate = getDebutDateAlbum(albumId);
            String peakWeek = getFirstPeakWeekAlbum(albumId, peakPosition);
            String debutWeek = getDebutWeekAlbum(albumId);
            
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
            WHERE s.album_id = ? AND c.chart_type = 'song'
            GROUP BY s.id, s.name
            """;
        
        List<Map<String, Object>> songRows = jdbcTemplate.queryForList(songSql, albumId);
        for (Map<String, Object> row : songRows) {
            Integer songId = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDateSong(songId);
            String peakDate = getFirstPeakDateSong(songId, peakPosition);
            String debutDate = getDebutDateSong(songId);
            String peakWeek = getFirstPeakWeekSong(songId, peakPosition);
            String debutWeek = getDebutWeekSong(songId);
            
            result.add(new ChartHistoryDTO(
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
            ));
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
            WHERE al.id = ? AND c.chart_type = 'album'
            GROUP BY al.id, al.name
            """;
        
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, albumId);
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        for (Map<String, Object> row : rows) {
            Integer id = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPositionAlbum(id, peakPosition);
            String releaseDate = getReleaseDateAlbum(id);
            String peakDate = getFirstPeakDateAlbum(id, peakPosition);
            String debutDate = getDebutDateAlbum(id);
            String peakWeek = getFirstPeakWeekAlbum(id, peakPosition);
            String debutWeek = getDebutWeekAlbum(id);
            
            result.add(new ChartHistoryDTO(
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
            ));
        }
        
        return result;
    }
    
    /**
     * Get chart history for a song (the song's own chart entries).
     */
    public List<ChartHistoryDTO> getSongChartHistory(Integer songId) {
        String sql = """
            SELECT s.id, s.name, MIN(ce.position) as peak_position, 
                   COUNT(*) as total_weeks
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.id = ? AND c.chart_type = 'song'
            GROUP BY s.id, s.name
            """;
        
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, songId);
        List<ChartHistoryDTO> result = new ArrayList<>();
        
        for (Map<String, Object> row : rows) {
            Integer id = ((Number) row.get("id")).intValue();
            Integer peakPosition = ((Number) row.get("peak_position")).intValue();
            Integer totalWeeks = ((Number) row.get("total_weeks")).intValue();
            Integer weeksAtPeak = countWeeksAtPosition(id, peakPosition, "song");
            String releaseDate = getReleaseDateSong(id);
            String peakDate = getFirstPeakDateSong(id, peakPosition);
            String debutDate = getDebutDateSong(id);
            String peakWeek = getFirstPeakWeekSong(id, peakPosition);
            String debutWeek = getDebutWeekSong(id);
            
            result.add(new ChartHistoryDTO(
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
            ));
        }
        
        return result;
    }
    
    /**
     * Count how many weeks a song spent at a specific position.
     */
    private Integer countWeeksAtPosition(Integer songId, Integer position, String chartType) {
        String sql = """
            SELECT COUNT(*) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ? AND ce.position = ? AND c.chart_type = ?
            """;
        return jdbcTemplate.queryForObject(sql, Integer.class, songId, position, chartType);
    }
    
    /**
     * Count how many weeks an album spent at a specific position.
     */
    private Integer countWeeksAtPositionAlbum(Integer albumId, Integer position) {
        String sql = """
            SELECT COUNT(*) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ? AND ce.position = ? AND c.chart_type = 'album'
            """;
        return jdbcTemplate.queryForObject(sql, Integer.class, albumId, position);
    }
    
    /**
     * Get the first date a song reached its peak position.
     */
    private String getFirstPeakDateSong(Integer songId, Integer position) {
        String sql = """
            SELECT MIN(c.period_start_date) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ? AND ce.position = ? AND c.chart_type = 'song'
            """;
        String dateStr = jdbcTemplate.queryForObject(sql, String.class, songId, position);
        return formatPeakDate(dateStr);
    }
    
    /**
     * Get the first date an album reached its peak position.
     */
    private String getFirstPeakDateAlbum(Integer albumId, Integer position) {
        String sql = """
            SELECT MIN(c.period_start_date) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ? AND ce.position = ? AND c.chart_type = 'album'
            """;
        String dateStr = jdbcTemplate.queryForObject(sql, String.class, albumId, position);
        return formatPeakDate(dateStr);
    }
    
    /**
     * Get the period key for when a song first reached its peak position.
     */
    private String getFirstPeakWeekSong(Integer songId, Integer position) {
        String sql = """
            SELECT c.period_key FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ? AND ce.position = ? AND c.chart_type = 'song'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, String.class, songId, position);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get the period key for when an album first reached its peak position.
     */
    private String getFirstPeakWeekAlbum(Integer albumId, Integer position) {
        String sql = """
            SELECT c.period_key FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ? AND ce.position = ? AND c.chart_type = 'album'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, String.class, albumId, position);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get the first date a song appeared on the chart (debut date).
     */
    private String getDebutDateSong(Integer songId) {
        String sql = """
            SELECT MIN(c.period_start_date) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ? AND c.chart_type = 'song'
            """;
        String dateStr = jdbcTemplate.queryForObject(sql, String.class, songId);
        return formatPeakDate(dateStr);
    }
    
    /**
     * Get the period key for when a song first appeared on the chart (debut week).
     */
    private String getDebutWeekSong(Integer songId) {
        String sql = """
            SELECT c.period_key FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.song_id = ? AND c.chart_type = 'song'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, String.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get the first date an album appeared on the chart (debut date).
     */
    private String getDebutDateAlbum(Integer albumId) {
        String sql = """
            SELECT MIN(c.period_start_date) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ? AND c.chart_type = 'album'
            """;
        String dateStr = jdbcTemplate.queryForObject(sql, String.class, albumId);
        return formatPeakDate(dateStr);
    }
    
    /**
     * Get the period key for when an album first appeared on the chart (debut week).
     */
    private String getDebutWeekAlbum(Integer albumId) {
        String sql = """
            SELECT c.period_key FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.album_id = ? AND c.chart_type = 'album'
            ORDER BY c.period_start_date ASC
            LIMIT 1
            """;
        try {
            return jdbcTemplate.queryForObject(sql, String.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Format a date string (yyyy-MM-dd) to a display format (d MMM yyyy).
     */
    private String formatPeakDate(String dateStr) {
        if (dateStr == null) return null;
        try {
            LocalDate date = LocalDate.parse(dateStr);
            return date.format(java.time.format.DateTimeFormatter.ofPattern("d MMM yyyy"));
        } catch (Exception e) {
            return dateStr;
        }
    }
    
    /**
     * Get the release date for a song (formatted).
     */
    private String getReleaseDateSong(Integer songId) {
        String sql = "SELECT release_date FROM Song WHERE id = ?";
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, songId);
            return formatPeakDate(dateStr);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get the release date for an album (formatted).
     */
    private String getReleaseDateAlbum(Integer albumId) {
        String sql = "SELECT release_date FROM Album WHERE id = ?";
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, albumId);
            return formatPeakDate(dateStr);
        } catch (Exception e) {
            return null;
        }
    }
}
