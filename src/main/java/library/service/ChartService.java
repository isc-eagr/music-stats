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
     * Get the latest finalized weekly chart.
     */
    public Optional<Chart> getLatestFinalizedWeeklyChart(String chartType) {
        return chartRepository.findLatestFinalizedWeeklyChart(chartType);
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
     * Get the current incomplete week's period key.
     * Returns the period key for the week that contains today's date.
     * If we're before the first Monday of the year (W00 days), returns the last week of the previous year.
     */
    public String getCurrentWeekPeriodKey() {
        LocalDate today = LocalDate.now();
        int year = today.getYear();

        // Find the first Monday of the year (matches SQLite's %W week 01)
        LocalDate jan1 = LocalDate.of(year, 1, 1);
        LocalDate firstMonday = jan1.with(java.time.temporal.TemporalAdjusters.nextOrSame(DayOfWeek.MONDAY));

        // Check if we're before the first Monday (would be week 00)
        // In this case, we should return the last week of the previous year since W00 is merged into it
        if (today.isBefore(firstMonday)) {
            return getLastWeekOfPreviousYear(year);
        }

        // Calculate week number: (days since first Monday / 7) + 1
        long daysSinceFirstMonday = java.time.temporal.ChronoUnit.DAYS.between(firstMonday, today);
        int weekNumber = (int)(daysSinceFirstMonday / 7) + 1;

        return String.format("%d-W%02d", year, weekNumber);
    }

    /**
     * Get the last week's period key of the previous year.
     * Used when current date falls in "W00" territory.
     */
    private String getLastWeekOfPreviousYear(int currentYear) {
        int prevYear = currentYear - 1;
        LocalDate dec31 = LocalDate.of(prevYear, 12, 31);
        LocalDate jan1PrevYear = LocalDate.of(prevYear, 1, 1);
        LocalDate firstMondayPrevYear = jan1PrevYear.with(java.time.temporal.TemporalAdjusters.nextOrSame(DayOfWeek.MONDAY));

        long daysSinceFirstMonday = java.time.temporal.ChronoUnit.DAYS.between(firstMondayPrevYear, dec31);
        int lastWeekNum = (int) (daysSinceFirstMonday / 7) + 1;

        return String.format("%d-W%02d", prevYear, lastWeekNum);
    }

    /**
     * Get the next week's period key from a given period key.
     * If the next week would be W00, returns the last week of the previous year (which already covers those dates).
     */
    public String getNextWeekPeriodKey(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate endDate = dateRange[1];
        // Start of next week is end date + 1 day
        LocalDate nextWeekStart = endDate.plusDays(1);

        int year = nextWeekStart.getYear();
        LocalDate jan1 = LocalDate.of(year, 1, 1);
        LocalDate firstMonday = jan1.with(java.time.temporal.TemporalAdjusters.nextOrSame(DayOfWeek.MONDAY));

        // Check if we're before the first Monday (would be week 00)
        // W00 dates are covered by the last week of the previous year, so return that instead
        if (nextWeekStart.isBefore(firstMonday)) {
            return getLastWeekOfPreviousYear(year);
        }

        // Calculate week number
        long daysSinceFirstMonday = java.time.temporal.ChronoUnit.DAYS.between(firstMonday, nextWeekStart);
        int weekNumber = (int)(daysSinceFirstMonday / 7) + 1;

        return String.format("%d-W%02d", year, weekNumber);
    }

    /**
     * Format a period key for display (e.g., "2024-W48" -> "Nov 25 - Dec 1, 2024").
     */
    public String formatPeriodKey(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate start = dateRange[0];
        LocalDate end = dateRange[1];
        java.time.format.DateTimeFormatter monthDay = java.time.format.DateTimeFormatter.ofPattern("MMM d");
        java.time.format.DateTimeFormatter full = java.time.format.DateTimeFormatter.ofPattern("MMM d, yyyy");

        if (start.getYear() == end.getYear()) {
            return start.format(monthDay) + " - " + end.format(full);
        } else {
            return start.format(full) + " - " + end.format(full);
        }
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
            dto.setAlbumId((Integer) row[4]);
            dto.setPlayCount((Integer) row[5]);
            dto.setSongName((String) row[6]);
            dto.setArtistName((String) row[7]);
            dto.setHasImage(((Number) row[8]).intValue() == 1);
            dto.setArtistId((Integer) row[9]);
            dto.setAlbumName((String) row[10]); // Album name for playlist export
            dto.setGenderId(row[11] != null ? ((Number) row[11]).intValue() : null);
            dto.setAlbumHasImage(row[12] != null && ((Number) row[12]).intValue() == 1);

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
            dto.setGenderId(row[10] != null ? ((Number) row[10]).intValue() : null);
            
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
     * This shows what the chart would look like based on scrobbles so far.
     * Only shows basic info: artist, album, song, plays - no chart history stats.
     */
    public List<ChartEntryDTO> getWeeklySongChartPreview(String periodKey) {
        LocalDate[] dateRange = parsePeriodKeyToDateRange(periodKey);
        LocalDate startDate = dateRange[0];
        LocalDate endDate = dateRange[1];

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
                MAX(CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END) as album_has_image
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            WHERE DATE(scr.scrobble_date) >= ? AND DATE(scr.scrobble_date) <= ?
              AND scr.song_id IS NOT NULL
            GROUP BY s.id
            ORDER BY play_count DESC, MAX(scr.scrobble_date) ASC
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
            result.add(dto);
        }, startDate.toString(), endDate.toString(), TOP_SONGS_COUNT);

        return result;
    }

    /**
     * Get a PREVIEW of the weekly album chart for an in-progress week (chart not yet generated).
     * This shows what the chart would look like based on scrobbles so far.
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
                MAX(CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END) as has_image
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Album al ON s.album_id = al.id
            INNER JOIN Artist ar ON al.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            WHERE DATE(scr.scrobble_date) >= ? AND DATE(scr.scrobble_date) <= ?
              AND scr.song_id IS NOT NULL
              AND s.album_id IS NOT NULL
            GROUP BY al.id
            ORDER BY play_count DESC, MAX(scr.scrobble_date) ASC
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
     * Get all weeks that have scrobbles but no chart generated yet.
     * Only returns weeks that have completely passed (not the current ongoing week).
     * Excludes W00 (days before first Monday of year) since those are covered by the last week of the previous year.
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
     * Get all weeks that have scrobble data (for regeneration).
     * Excludes W00 (days before first Monday are covered by the last week of previous year).
     * Only includes weeks that have completely passed.
     */
    public List<String> getAllWeeksWithScrobbleData() {
        String sql = """
            SELECT DISTINCT strftime('%Y-W%W', scr.scrobble_date) as period_key
            FROM Scrobble scr
            WHERE scr.scrobble_date IS NOT NULL
              AND scr.song_id IS NOT NULL
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
     * Deletes ALL existing weekly charts first, then regenerates from scratch using scrobble data.
     * This ensures any buggy charts (like W00) are cleaned up.
     * Returns a session ID for tracking progress.
     */
    public String startBulkRegeneration() {
        String sessionId = UUID.randomUUID().toString();
        
        // Get all weeks from scrobble data (properly filtered, excluding W00)
        List<String> weeksToGenerate = getAllWeeksWithScrobbleData();

        ChartGenerationProgressDTO progress = new ChartGenerationProgressDTO(
            weeksToGenerate.size(), 0, "Deleting existing charts...", false
        );
        generationProgress.put(sessionId, progress);

        // Start regeneration in a separate thread
        Thread generationThread = new Thread(() -> {
            try {
                // First, delete ALL weekly charts (including any buggy ones like W00)
                deleteAllWeeklyCharts();
                
                // Now generate fresh charts from scrobble data
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
    public static final int SEASONAL_ALBUMS_COUNT = 10;
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

        String sql = String.format("""
            SELECT ce.position, ce.%s as item_id, 
                   item.name as item_name,
                   ar.id as artist_id, ar.name as artist_name,
                   ar.gender_id as gender_id,
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
            ? "(SELECT COUNT(*) FROM Scrobble sc WHERE sc.song_id = item.id AND DATE(sc.scrobble_date) >= c.period_start_date AND DATE(sc.scrobble_date) <= c.period_end_date)"
            : "(SELECT COUNT(*) FROM Scrobble sc INNER JOIN Song s ON sc.song_id = s.id WHERE s.album_id = item.id AND DATE(sc.scrobble_date) >= c.period_start_date AND DATE(sc.scrobble_date) <= c.period_end_date)";
        
        String sql = String.format("""
            SELECT ce.position, ce.%s as item_id, 
                   item.name as item_name,
                   ar.id as artist_id, ar.name as artist_name,
                   ar.gender_id as gender_id,
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
                   item.album_id, (SELECT name FROM Album WHERE id = item.album_id) as album_name
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
    
    /**
     * Get all years that have weekly chart data (for "Most Weeks at Position" page).
     */
    public List<Map<String, Object>> getChartYearsForMostWeeks() {
        String sql = """
            SELECT DISTINCT strftime('%Y', c.period_start_date) as year
            FROM Chart c
            WHERE c.period_type = 'weekly'
            ORDER BY year DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            String year = rs.getString("year");
            Map<String, Object> map = new HashMap<>();
            map.put("year", year);
            map.put("displayName", year);
            return map;
        });
    }
    
    /**
     * Get songs or albums with the most weeks at a given position threshold.
     * For example: type="song", maxPosition=1 returns songs with most weeks at #1.
     * type="song", maxPosition=5 returns songs with most weeks in top 5.
     * 
     * @param type "song" or "album"
     * @param maxPosition The maximum position to count (1 = #1 only, 5 = top 5, etc.)
     * @param year Optional year filter. If null, returns all-time stats.
     * @return List of entries sorted by weeks count descending, then by earliest appearance.
     */
    public List<MostWeeksEntryDTO> getMostWeeksAtPosition(String type, int maxPosition, Integer year) {
        List<MostWeeksEntryDTO> results = new ArrayList<>();
        
        if ("song".equals(type)) {
            results = getMostWeeksSongs(maxPosition, year);
        } else if ("album".equals(type)) {
            results = getMostWeeksAlbums(maxPosition, year);
        }
        
        // Assign ranks
        int rank = 1;
        for (MostWeeksEntryDTO entry : results) {
            entry.setRank(rank++);
        }
        
        return results;
    }
    
    /**
     * Get songs with the most weeks at a given position threshold.
     */
    private List<MostWeeksEntryDTO> getMostWeeksSongs(int maxPosition, Integer year) {
        StringBuilder sql = new StringBuilder();
        sql.append("""
            SELECT 
                s.id,
                s.name,
                ar.name as artist_name,
                ar.id as artist_id,
                ar.gender_id,
                COUNT(*) as weeks_count,
                MIN(ce.position) as peak_position,
                MAX(CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                MIN(c.period_start_date) as first_appearance,
                s.album_id,
                MAX(CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END) as album_has_image
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE c.chart_type = 'song'
              AND c.period_type = 'weekly'
              AND ce.position <= ?
            """);
        
        List<Object> params = new ArrayList<>();
        params.add(maxPosition);
        
        if (year != null) {
            sql.append(" AND strftime('%Y', c.period_start_date) = ?");
            params.add(String.valueOf(year));
        }
        
        sql.append("""
            GROUP BY s.id, s.name, ar.name, ar.id, s.album_id
            ORDER BY weeks_count DESC, first_appearance ASC
            """);
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            MostWeeksEntryDTO dto = new MostWeeksEntryDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setWeeksCount(rs.getInt("weeks_count"));
            dto.setPeakPosition(rs.getInt("peak_position"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            dto.setType("song");
            dto.setAlbumId(rs.getObject("album_id") != null ? rs.getInt("album_id") : null);
            dto.setAlbumHasImage(rs.getInt("album_has_image") == 1);
            return dto;
        }, params.toArray());
    }

    /**
     * Get albums with the most weeks at a given position threshold.
     */
    private List<MostWeeksEntryDTO> getMostWeeksAlbums(int maxPosition, Integer year) {
        StringBuilder sql = new StringBuilder();
        sql.append("""
            SELECT 
                al.id,
                al.name,
                ar.name as artist_name,
                ar.id as artist_id,
                ar.gender_id,
                COUNT(*) as weeks_count,
                MIN(ce.position) as peak_position,
                CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                MIN(c.period_start_date) as first_appearance
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Album al ON ce.album_id = al.id
            INNER JOIN Artist ar ON al.artist_id = ar.id
            WHERE c.chart_type = 'album'
              AND c.period_type = 'weekly'
              AND ce.position <= ?
            """);
        
        List<Object> params = new ArrayList<>();
        params.add(maxPosition);
        
        if (year != null) {
            sql.append(" AND strftime('%Y', c.period_start_date) = ?");
            params.add(String.valueOf(year));
        }
        
        sql.append("""
            GROUP BY al.id, al.name, ar.name, ar.id
            ORDER BY weeks_count DESC, first_appearance ASC
            """);
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            MostWeeksEntryDTO dto = new MostWeeksEntryDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setWeeksCount(rs.getInt("weeks_count"));
            dto.setPeakPosition(rs.getInt("peak_position"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            dto.setType("album");
            return dto;
        }, params.toArray());
    }
    
    /**
     * Get artists with the most songs (hits) that reached a given position threshold.
     * Only considers weekly song charts.
     * 
     * @param maxPosition The maximum position to count (1 = #1, 5 = top 5, etc.)
     * @param year Optional year filter. If null, returns all-time stats.
     * @return List of artists sorted by songs count descending, then by total weeks.
     */
    public List<MostHitsEntryDTO> getMostHits(int maxPosition, Integer year) {
        StringBuilder sql = new StringBuilder();
        sql.append("""
            SELECT 
                ar.id as artist_id,
                ar.name as artist_name,
                ar.gender_id,
                COUNT(DISTINCT ce.song_id) as songs_count,
                COUNT(*) as total_weeks,
                CASE WHEN ar.image IS NOT NULL THEN 1 ELSE 0 END as has_image
            FROM ChartEntry ce
            INNER JOIN Chart c ON ce.chart_id = c.id
            INNER JOIN Song s ON ce.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            WHERE c.chart_type = 'song'
              AND c.period_type = 'weekly'
              AND ce.position <= ?
            """);
        
        List<Object> params = new ArrayList<>();
        params.add(maxPosition);
        
        if (year != null) {
            sql.append(" AND strftime('%Y', c.period_start_date) = ?");
            params.add(String.valueOf(year));
        }
        
        sql.append("""
            GROUP BY ar.id, ar.name
            ORDER BY songs_count DESC, total_weeks DESC
            """);
        
        List<MostHitsEntryDTO> results = jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            MostHitsEntryDTO dto = new MostHitsEntryDTO();
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setSongsCount(rs.getInt("songs_count"));
            dto.setTotalWeeks(rs.getInt("total_weeks"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            return dto;
        }, params.toArray());
        
        // Assign ranks
        int rank = 1;
        for (MostHitsEntryDTO entry : results) {
            entry.setRank(rank++);
        }
        
        return results;
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
            return entry;
        }, artistId, periodType);
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
}
