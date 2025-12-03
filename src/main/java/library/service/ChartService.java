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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
     * Get the latest weekly chart (period_type IS NULL).
     */
    public Optional<Chart> getLatestWeeklyChart(String chartType) {
        return chartRepository.findLatestWeeklyChart(chartType);
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
            SELECT c.period_key, ce.position
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
            Integer weeksAtPeak = countWeeksAtPosition(songId, peakPosition, "song");
            String releaseDate = getReleaseDate(songId, "song");
            String peakDate = getFirstPeakDate(songId, peakPosition, "song");
            String debutDate = getDebutDate(songId, "song");
            String peakWeek = getFirstPeakWeek(songId, peakPosition, "song");
            String debutWeek = getDebutWeek(songId, "song");
            
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
            WHERE s.id = ? AND c.chart_type = 'song' AND c.period_type = 'weekly'
            GROUP BY s.id, s.name
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
            SELECT MIN(c.period_start_date) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND ce.position = ? AND c.chart_type = ? AND c.period_type = 'weekly'
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
     * @param itemId The song or album ID
     * @param chartType "song" or "album"
     */
    private String getDebutDate(Integer itemId, String chartType) {
        String idColumn = "song".equals(chartType) ? "song_id" : "album_id";
        String sql = String.format("""
            SELECT MIN(c.period_start_date) FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND c.chart_type = ? AND c.period_type = 'weekly'
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
        String table = "song".equals(itemType) ? "Song" : "Album";
        String sql = String.format("SELECT release_date FROM %s WHERE id = ?", table);
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, itemId);
            return formatPeakDate(dateStr);
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
    
    // ==================== Seasonal/Yearly Chart Methods ====================
    
    public static final int SEASONAL_YEARLY_SONGS_COUNT = 30;
    public static final int SEASONAL_ALBUMS_COUNT = 5;
    public static final int YEARLY_ALBUMS_COUNT = 10;
    
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
     * Check if a year has completely passed.
     */
    public boolean isYearComplete(String periodKey) {
        try {
            int year = Integer.parseInt(periodKey);
            LocalDate endOfYear = LocalDate.of(year, 12, 31);
            return LocalDate.now().isAfter(endOfYear);
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
     * Get chart entries for a seasonal/yearly chart with song/album details.
     */
    public List<ChartEntryDTO> getSeasonalYearlyChartEntries(String periodType, String chartType, String periodKey) {
        String itemTable = "song".equals(chartType) ? "Song" : "Album";
        String itemIdCol = "song".equals(chartType) ? "song_id" : "album_id";
        // For songs, use COALESCE to fallback to album image if song has no single_cover
        String imageCol = "song".equals(chartType) 
            ? "COALESCE(item.single_cover, (SELECT image FROM Album WHERE id = item.album_id))" 
            : "item.image";
        
        String sql = String.format("""
            SELECT ce.position, ce.%s as item_id, 
                   item.name as item_name,
                   ar.id as artist_id, ar.name as artist_name,
                   %s as item_image,
                   %s
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN %s item ON ce.%s = item.id
            INNER JOIN Artist ar ON item.artist_id = ar.id
            WHERE c.period_type = ? AND c.chart_type = ? AND c.period_key = ?
            ORDER BY ce.position ASC
            """,
            itemIdCol,
            imageCol,
            "song".equals(chartType) ? "item.album_id, (SELECT name FROM Album WHERE id = item.album_id) as album_name" : "NULL as album_id, NULL as album_name",
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
            } else {
                dto.setAlbumId(rs.getInt("item_id"));
                dto.setAlbumName(rs.getString("item_name"));
            }
            
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setHasImage(rs.getBytes("item_image") != null);
            
            return dto;
        }, periodType, chartType, periodKey);
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
        String sql = String.format("""
            SELECT ce.position, c.period_key
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            WHERE ce.%s = ? AND c.period_type = ? AND c.chart_type = ? AND c.is_finalized = 1
            ORDER BY c.period_start_date DESC
            """, idColumn);
        
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
     * Get seasonal chart history for a song (for detail page display).
     * @deprecated Use {@link #getChartHistoryForItem(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getSeasonalChartHistoryForSong(Integer songId) {
        return getChartHistoryForItem(songId, "song", "seasonal");
    }
    
    /**
     * Get yearly chart history for a song (for detail page display).
     * @deprecated Use {@link #getChartHistoryForItem(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getYearlyChartHistoryForSong(Integer songId) {
        return getChartHistoryForItem(songId, "song", "yearly");
    }
    
    /**
     * Get seasonal chart history for an album (for detail page display).
     * @deprecated Use {@link #getChartHistoryForItem(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getSeasonalChartHistoryForAlbum(Integer albumId) {
        return getChartHistoryForItem(albumId, "album", "seasonal");
    }
    
    /**
     * Get yearly chart history for an album (for detail page display).
     * @deprecated Use {@link #getChartHistoryForItem(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getYearlyChartHistoryForAlbum(Integer albumId) {
        return getChartHistoryForItem(albumId, "album", "yearly");
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
        
        String sql = String.format("""
            SELECT ce.position, c.period_key, %s.id as item_id, %s.name as item_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN %s %s ON ce.%s = %s.id
            WHERE %s.artist_id = ? AND c.period_type = ? AND c.chart_type = ? AND c.is_finalized = 1
            ORDER BY c.period_key DESC, ce.position ASC
            """, itemAlias, itemAlias, itemTable, itemAlias, idColumn, itemAlias, itemAlias);
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("position", rs.getInt("position"));
            String periodKey = rs.getString("period_key");
            entry.put("periodKey", periodKey);
            entry.put("displayName", "seasonal".equals(periodType) ? formatSeasonPeriodKey(periodKey) : periodKey);
            entry.put(itemIdKey, rs.getInt("item_id"));
            entry.put(itemNameKey, rs.getString("item_name"));
            return entry;
        }, artistId, periodType, chartType);
    }

    /**
     * Get seasonal chart history for all songs by an artist.
     * @deprecated Use {@link #getArtistChartHistoryByPeriodType(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getSeasonalChartHistoryForArtist(Integer artistId) {
        return getArtistChartHistoryByPeriodType(artistId, "song", "seasonal");
    }
    
    /**
     * Get yearly chart history for all songs by an artist.
     * @deprecated Use {@link #getArtistChartHistoryByPeriodType(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getYearlyChartHistoryForArtist(Integer artistId) {
        return getArtistChartHistoryByPeriodType(artistId, "song", "yearly");
    }
    
    /**
     * Get seasonal chart history for all albums by an artist.
     * @deprecated Use {@link #getArtistChartHistoryByPeriodType(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getSeasonalAlbumChartHistoryForArtist(Integer artistId) {
        return getArtistChartHistoryByPeriodType(artistId, "album", "seasonal");
    }
    
    /**
     * Get yearly chart history for all albums by an artist.
     * @deprecated Use {@link #getArtistChartHistoryByPeriodType(Integer, String, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getYearlyAlbumChartHistoryForArtist(Integer artistId) {
        return getArtistChartHistoryByPeriodType(artistId, "album", "yearly");
    }
    
    /**
     * Get chart history for songs in an album on a specific period type.
     * @param albumId The album ID
     * @param periodType "seasonal" or "yearly"
     */
    public List<Map<String, Object>> getAlbumSongsChartHistoryByPeriodType(Integer albumId, String periodType) {
        String sql = """
            SELECT ce.position, c.period_key, s.id as song_id, s.name as song_name
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            WHERE s.album_id = ? AND c.period_type = ? AND c.chart_type = 'song' AND c.is_finalized = 1
            ORDER BY c.period_key DESC, ce.position ASC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("position", rs.getInt("position"));
            String periodKey = rs.getString("period_key");
            entry.put("periodKey", periodKey);
            entry.put("displayName", "seasonal".equals(periodType) ? formatSeasonPeriodKey(periodKey) : periodKey);
            entry.put("songId", rs.getInt("song_id"));
            entry.put("songName", rs.getString("song_name"));
            return entry;
        }, albumId, periodType);
    }

    /**
     * Get seasonal chart history for songs in an album.
     * @deprecated Use {@link #getAlbumSongsChartHistoryByPeriodType(Integer, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getSeasonalChartHistoryForAlbumSongs(Integer albumId) {
        return getAlbumSongsChartHistoryByPeriodType(albumId, "seasonal");
    }
    
    /**
     * Get yearly chart history for songs in an album.
     * @deprecated Use {@link #getAlbumSongsChartHistoryByPeriodType(Integer, String)} instead.
     */
    @Deprecated
    public List<Map<String, Object>> getYearlyChartHistoryForAlbumSongs(Integer albumId) {
        return getAlbumSongsChartHistoryByPeriodType(albumId, "yearly");
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
     * Check if a season has scrobble data.
     */
    public boolean hasSeasonData(String periodKey) {
        LocalDate[] dateRange = parseSeasonPeriodKeyToDateRange(periodKey);
        if (dateRange == null) return false;
        
        String sql = "SELECT COUNT(*) FROM Scrobble WHERE scrobble_date >= ? AND scrobble_date <= ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, 
            dateRange[0].toString(), dateRange[1].toString());
        return count != null && count > 0;
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
     * Check if a year has scrobble data.
     */
    public boolean hasYearData(String periodKey) {
        if (periodKey == null) return false;
        String sql = "SELECT COUNT(*) FROM Scrobble WHERE strftime('%Y', scrobble_date) = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, periodKey);
        return count != null && count > 0;
    }
    
    /**
     * Get all seasons that have scrobble data (for chart list page).
     */
    public List<Map<String, Object>> getAllSeasonsWithData() {
        String sql = """
            SELECT DISTINCT
                CASE 
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) = 12 
                        THEN (CAST(strftime('%Y', scrobble_date) AS INTEGER) + 1) || '-Winter'
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (1, 2) 
                        THEN strftime('%Y', scrobble_date) || '-Winter'
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (3, 4, 5) 
                        THEN strftime('%Y', scrobble_date) || '-Spring'
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (6, 7, 8) 
                        THEN strftime('%Y', scrobble_date) || '-Summer'
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (9, 10, 11) 
                        THEN strftime('%Y', scrobble_date) || '-Fall'
                END as period_key,
                CASE 
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) = 12 
                        THEN (CAST(strftime('%Y', scrobble_date) AS INTEGER) + 1) * 10 + 1
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (1, 2) 
                        THEN CAST(strftime('%Y', scrobble_date) AS INTEGER) * 10 + 1
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (3, 4, 5) 
                        THEN CAST(strftime('%Y', scrobble_date) AS INTEGER) * 10 + 2
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (6, 7, 8) 
                        THEN CAST(strftime('%Y', scrobble_date) AS INTEGER) * 10 + 3
                    WHEN CAST(strftime('%m', scrobble_date) AS INTEGER) IN (9, 10, 11) 
                        THEN CAST(strftime('%Y', scrobble_date) AS INTEGER) * 10 + 4
                END as sort_order
            FROM Scrobble
            WHERE scrobble_date IS NOT NULL
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
     * Get all years that have scrobble data (for chart list page).
     */
    public List<Map<String, Object>> getAllYearsWithData() {
        String sql = """
            SELECT DISTINCT strftime('%Y', scrobble_date) as period_key
            FROM Scrobble
            WHERE scrobble_date IS NOT NULL
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
}
