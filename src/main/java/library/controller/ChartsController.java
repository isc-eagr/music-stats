package library.controller;

import library.dto.AlbumChartRunDTO;
import library.dto.ChartEntryDTO;
import library.dto.ChartGenerationProgressDTO;
import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartRunDTO;
import library.dto.ChartSongOverviewRowDTO;
import library.dto.MostHitsEntryDTO;
import library.dto.MostWeeksEntryDTO;
import library.entity.Chart;
import library.service.ChartService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Controller for the Charts feature - weekly top songs/albums rankings.
 */
@Controller
@RequestMapping("/charts")
public class ChartsController {

    private static final int WEEKLY_OVERVIEW_PAGE_SIZE = 100;
    
    private final ChartService chartService;
    
    public ChartsController(ChartService chartService) {
        this.chartService = chartService;
    }
    
    /**
     * Weekly charts landing page - redirects to latest chart or shows empty state.
     */
    @GetMapping("/weekly")
    public String weeklyCharts(Model model) {
        Optional<Chart> latestChart = chartService.getLatestWeeklyChart("song");
        
        if (latestChart.isPresent()) {
            return "redirect:/charts/weekly/" + latestChart.get().getPeriodKey();
        }
        
        // No charts exist yet - show empty state
        model.addAttribute("currentSection", "weekly-charts");
        model.addAttribute("hasChart", false);
        model.addAttribute("missingWeeksCount", chartService.getWeeksWithoutCharts().size());
        
        return "charts/weekly";
    }
    
    /**
     * Display a specific week's chart.
     */
    @GetMapping("/weekly/{periodKey}")
    public String weeklyChart(@PathVariable String periodKey,
                              @RequestParam(required = false) Boolean preview,
                              Model model) {
        Optional<Chart> chartOpt = chartService.getChart("song", periodKey);
        
        if (chartOpt.isEmpty()) {
            // Chart doesn't exist for this week - check if we can show a preview
            boolean weekComplete = chartService.isWeekComplete(periodKey);
            boolean isPreviewMode = Boolean.TRUE.equals(preview) && !weekComplete;

            if (isPreviewMode) {
                // Show preview of in-progress week
                List<ChartEntryDTO> entries = chartService.getWeeklySongChartPreview(periodKey);
                List<ChartEntryDTO> albumEntries = chartService.getWeeklyAlbumChartPreview(periodKey);

                // Get formatted period for display
                String formattedPeriod = chartService.formatPeriodKey(periodKey);

                // Get previous chart for navigation (go back from preview)
                Optional<Chart> prevChart = chartService.getLatestWeeklyChart("song");

                model.addAttribute("currentSection", "weekly-charts");
                model.addAttribute("hasChart", false);
                model.addAttribute("isPreview", true);
                model.addAttribute("entries", entries);
                model.addAttribute("albumEntries", albumEntries);
                model.addAttribute("periodKey", periodKey);
                model.addAttribute("formattedPeriod", formattedPeriod);
                model.addAttribute("prevPeriodKey", prevChart.map(Chart::getPeriodKey).orElse(null));
                model.addAttribute("nextPeriodKey", null); // No next from preview (already at current week)
                model.addAttribute("missingWeeksCount", chartService.getWeeksWithoutCharts().size());

                // Get #1 for display
                if (!entries.isEmpty()) {
                    model.addAttribute("numberOneSong", entries.get(0));
                }
                if (!albumEntries.isEmpty()) {
                    model.addAttribute("numberOneAlbum", albumEntries.get(0));
                }

                return "charts/weekly";
            }

            // Not a preview - show empty state with option to view preview if week is in progress
            model.addAttribute("currentSection", "weekly-charts");
            model.addAttribute("hasChart", false);
            model.addAttribute("isPreview", false);
            model.addAttribute("periodKey", periodKey);
            model.addAttribute("weekComplete", weekComplete);
            model.addAttribute("missingWeeksCount", chartService.getWeeksWithoutCharts().size());
            return "charts/weekly";
        }
        
        Chart chart = chartOpt.get();
        List<ChartEntryDTO> entries = chartService.getWeeklyChartWithStats(periodKey);
        List<ChartEntryDTO> albumEntries = chartService.getWeeklyAlbumChartWithStats(periodKey);
        
        // Navigation
        Optional<Chart> prevChart = chartService.getPreviousChart("song", periodKey);
        Optional<Chart> nextChart = chartService.getNextChart("song", periodKey);
        
        // Determine if "next" should show Preview button (current incomplete week)
        String nextPeriodKey = null;
        boolean nextIsPreview = false;

        if (nextChart.isPresent()) {
            // There's a next chart - regular navigation
            nextPeriodKey = nextChart.get().getPeriodKey();
        } else {
            // No next chart exists - check if the next week is the current incomplete week
            String potentialNextWeek = chartService.getNextWeekPeriodKey(periodKey);
            boolean nextWeekComplete = chartService.isWeekComplete(potentialNextWeek);

            if (!nextWeekComplete) {
                // The next week is in progress - show Preview button
                nextPeriodKey = potentialNextWeek;
                nextIsPreview = true;
            }
        }

        model.addAttribute("currentSection", "weekly-charts");
        model.addAttribute("hasChart", true);
        model.addAttribute("isPreview", false);
        model.addAttribute("chart", chart);
        model.addAttribute("entries", entries);
        model.addAttribute("albumEntries", albumEntries);
        model.addAttribute("periodKey", periodKey);
        model.addAttribute("formattedPeriod", chart.getFormattedPeriod());
        model.addAttribute("prevPeriodKey", prevChart.map(Chart::getPeriodKey).orElse(null));
        model.addAttribute("nextPeriodKey", nextPeriodKey);
        model.addAttribute("nextIsPreview", nextIsPreview);
        model.addAttribute("missingWeeksCount", chartService.getWeeksWithoutCharts().size());
        
        // Get #1 song info for the header image
        if (!entries.isEmpty()) {
            ChartEntryDTO numberOne = entries.get(0);
            model.addAttribute("numberOneSong", numberOne);
        }
        
        // Get #1 album info for the header image
        if (!albumEntries.isEmpty()) {
            ChartEntryDTO numberOneAlbum = albumEntries.get(0);
            model.addAttribute("numberOneAlbum", numberOneAlbum);
        }
        
        return "charts/weekly";
    }
    
    /**
     * API: Get chart run data for a song (for expandable row).
     */
    @GetMapping("/weekly/{periodKey}/song/{songId}/run")
    @ResponseBody
    public ResponseEntity<ChartRunDTO> getSongChartRun(
            @PathVariable String periodKey,
            @PathVariable Integer songId) {
        try {
            ChartRunDTO chartRun = chartService.getSongChartRun(songId, periodKey);
            return ResponseEntity.ok(chartRun);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * API: Get chart run data for an album (for expandable row).
     */
    @GetMapping("/weekly/{periodKey}/album/{albumId}/run")
    @ResponseBody
    public ResponseEntity<AlbumChartRunDTO> getAlbumChartRun(
            @PathVariable String periodKey,
            @PathVariable Integer albumId) {
        try {
            AlbumChartRunDTO chartRun = chartService.getAlbumChartRun(albumId, periodKey);
            return ResponseEntity.ok(chartRun);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * API: Get chart run data for a song (all-time, no period key required).
     * Used in song detail page.
     */
    @GetMapping("/song/{songId}/run")
    @ResponseBody
    public ResponseEntity<ChartRunDTO> getSongChartRunAllTime(@PathVariable Integer songId) {
        try {
            ChartRunDTO chartRun = chartService.getSongChartRunAllTime(songId);
            return ResponseEntity.ok(chartRun);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * API: Get chart run data for an album (all-time, no period key required).
     * Used in album detail page.
     */
    @GetMapping("/album/{albumId}/run")
    @ResponseBody
    public ResponseEntity<AlbumChartRunDTO> getAlbumChartRunAllTime(@PathVariable Integer albumId) {
        try {
            AlbumChartRunDTO chartRun = chartService.getAlbumChartRun(albumId, null);
            return ResponseEntity.ok(chartRun);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * API: Generate chart for a specific week (both songs and albums).
     */
    @PostMapping("/generate/{periodKey}")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> generateChart(@PathVariable String periodKey) {
        try {
            chartService.generateWeeklyCharts(periodKey);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("periodKey", periodKey);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
    
    /**
     * API: Start bulk generation of all missing charts.
     */
    @PostMapping("/generate-all")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> generateAllCharts() {
        try {
            String sessionId = chartService.startBulkGeneration();
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("sessionId", sessionId);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
    
    /**
     * API: Get progress of bulk generation.
     */
    @GetMapping("/generate-all/progress/{sessionId}")
    @ResponseBody
    public ResponseEntity<ChartGenerationProgressDTO> getGenerationProgress(@PathVariable String sessionId) {
        ChartGenerationProgressDTO progress = chartService.getGenerationProgress(sessionId);
        return ResponseEntity.ok(progress);
    }
    
    /**
     * API: Regenerate chart for a specific week (deletes and recreates).
     */
    @PostMapping("/regenerate/{periodKey}")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> regenerateChart(@PathVariable String periodKey) {
        try {
            chartService.deleteWeeklyCharts(periodKey);
            chartService.generateWeeklyCharts(periodKey);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("periodKey", periodKey);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * API: Start bulk regeneration of ALL existing weekly charts.
     * This deletes all existing charts and regenerates them from scratch.
     */
    @PostMapping("/regenerate-all")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> regenerateAllCharts() {
        try {
            String sessionId = chartService.startBulkRegeneration();
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("sessionId", sessionId);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * API: Get count of existing weekly charts (for regeneration confirmation).
     */
    @GetMapping("/weekly/count")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getWeeklyChartsCount() {
        int count = chartService.getWeeksWithCharts().size();
        Map<String, Object> response = new HashMap<>();
        response.put("count", count);
        return ResponseEntity.ok(response);
    }

    /**
     * API: Get #1 song image for a week.
     */
    @GetMapping("/weekly/{periodKey}/image")
    @ResponseBody
    public ResponseEntity<byte[]> getNumberOneImage(@PathVariable String periodKey) {
        byte[] image = chartService.getNumberOneSongImage(periodKey);
        if (image == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok()
                .contentType(MediaType.IMAGE_JPEG)
                .body(image);
    }
    
    /**
     * Navigate to chart by date - finds the week containing the given date.
     * Uses SQLite's %W week numbering convention to match how charts are stored:
     * - Week 00: Days before the first Monday of the year
     * - Week 01: Starts from the first Monday of the year
     * - Week N: Starts from first Monday + (N-1)*7 days
     */
    @GetMapping("/weekly/by-date")
    public String weeklyChartByDate(@RequestParam String date) {
        // Convert date to week period key (YYYY-WXX format) matching SQLite's %W
        try {
            java.time.LocalDate localDate = java.time.LocalDate.parse(date);
            int year = localDate.getYear();
            
            // Find the first Monday of the year (SQLite's %W week 01)
            java.time.LocalDate jan1 = java.time.LocalDate.of(year, 1, 1);
            java.time.LocalDate firstMonday = jan1.with(java.time.temporal.TemporalAdjusters.nextOrSame(java.time.DayOfWeek.MONDAY));
            
            int week;
            if (localDate.isBefore(firstMonday)) {
                // Date is before first Monday = week 00
                week = 0;
            } else {
                // Calculate which week based on days since first Monday
                long daysSinceFirstMonday = java.time.temporal.ChronoUnit.DAYS.between(firstMonday, localDate);
                week = (int) (daysSinceFirstMonday / 7) + 1;
            }
            
            String periodKey = String.format("%d-W%02d", year, week);
            return "redirect:/charts/weekly/" + periodKey;
        } catch (Exception e) {
            return "redirect:/charts/weekly";
        }
    }
    
    // ========== SEASONAL CHARTS ==========
    
    /**
     * Seasonal charts landing page - redirects to latest chart.
     */
    @GetMapping("/seasonal")
    public String seasonalCharts(Model model) {
        Optional<Chart> latestChart = chartService.getLatestSeasonalYearlyChart("seasonal", "song");
        
        if (latestChart.isPresent()) {
            return "redirect:/charts/seasonal/" + latestChart.get().getPeriodKey();
        }
        
        // No charts exist - redirect to current season
        String currentSeason = chartService.getCurrentSeasonPeriodKey();
        return "redirect:/charts/seasonal/" + currentSeason;
    }

    /**
     * View/Edit a specific season's charts.
     */
    @GetMapping("/seasonal/{periodKey}")
    public String seasonalChart(@PathVariable String periodKey, Model model) {
        // Get or create draft charts
        Map<String, Chart> charts = chartService.createOrGetDraftCharts("seasonal", periodKey);
        Chart songChart = charts.get("song");
        Chart albumChart = charts.get("album");
        
        // For drafts, load ALL entries (unlimited). For finalized, load just 1-30.
        List<ChartEntryDTO> songEntries;
        List<ChartEntryDTO> albumEntries;
        List<ChartEntryDTO> extraSongEntries;
        
        if (Boolean.TRUE.equals(songChart.getIsFinalized())) {
            songEntries = chartService.getSeasonalYearlyChartEntries("seasonal", "song", periodKey);
            extraSongEntries = chartService.getSeasonalExtraSongEntries(periodKey);
        } else {
            // Get all entries for draft editing (no position limit), then split main vs extras
            List<ChartEntryDTO> allSongEntries = chartService.getAllChartEntriesForEdit("seasonal", "song", periodKey);
            songEntries = allSongEntries.stream()
                .filter(e -> e.getPosition() <= ChartService.SEASONAL_YEARLY_SONGS_COUNT)
                .collect(java.util.stream.Collectors.toList());
            extraSongEntries = allSongEntries.stream()
                .filter(e -> e.getPosition() > ChartService.SEASONAL_YEARLY_SONGS_COUNT)
                .collect(java.util.stream.Collectors.toList());
        }
        
        if (Boolean.TRUE.equals(albumChart.getIsFinalized())) {
            albumEntries = chartService.getSeasonalYearlyChartEntries("seasonal", "album", periodKey);
        } else {
            albumEntries = chartService.getAllChartEntriesForEdit("seasonal", "album", periodKey);
        }
        
        boolean isComplete = chartService.isSeasonComplete(periodKey);
        int mainSongCount = (int) songEntries.stream().filter(e -> e.getPosition() <= ChartService.SEASONAL_YEARLY_SONGS_COUNT).count();
        boolean songNotFinalized = !Boolean.TRUE.equals(songChart.getIsFinalized());
        boolean albumNotFinalized = !Boolean.TRUE.equals(albumChart.getIsFinalized());
        boolean songsNewlyComplete = songNotFinalized && mainSongCount == ChartService.SEASONAL_YEARLY_SONGS_COUNT;
        boolean albumsNewlyComplete = albumNotFinalized && albumEntries.size() == ChartService.SEASONAL_ALBUMS_COUNT;
        boolean songsPartial = songNotFinalized && mainSongCount > 0 && mainSongCount < ChartService.SEASONAL_YEARLY_SONGS_COUNT;
        boolean albumsPartial = albumNotFinalized && albumEntries.size() > 0 && albumEntries.size() < ChartService.SEASONAL_ALBUMS_COUNT;
        boolean canFinalize = isComplete && (songsNewlyComplete || albumsNewlyComplete) && !songsPartial && !albumsPartial;
        
        // Navigation
        String prevPeriodKey = chartService.getPreviousSeasonPeriodKey(periodKey);
        String nextPeriodKey = chartService.getNextSeasonPeriodKey(periodKey);
        // Only show nav if the adjacent season has data
        if (prevPeriodKey != null && !chartService.hasSeasonData(prevPeriodKey)) {
            prevPeriodKey = null;
        }
        if (nextPeriodKey != null && !chartService.hasSeasonData(nextPeriodKey)) {
            nextPeriodKey = null;
        }
        
        model.addAttribute("currentSection", "seasonal-charts");
        model.addAttribute("periodType", "seasonal");
        model.addAttribute("periodKey", periodKey);
        model.addAttribute("displayName", chartService.formatSeasonPeriodKey(periodKey));
        model.addAttribute("songChart", songChart);
        model.addAttribute("albumChart", albumChart);
        model.addAttribute("songEntries", songEntries);
        model.addAttribute("albumEntries", albumEntries);
        model.addAttribute("extraSongEntries", extraSongEntries);
        model.addAttribute("isComplete", isComplete);
        model.addAttribute("canFinalize", canFinalize);
        model.addAttribute("songChartFinalized", songChart.getIsFinalized());
        model.addAttribute("albumChartFinalized", albumChart.getIsFinalized());
        model.addAttribute("maxSongs", ChartService.SEASONAL_YEARLY_SONGS_COUNT);
        model.addAttribute("maxAlbums", ChartService.SEASONAL_ALBUMS_COUNT);
        model.addAttribute("maxExtraSongs", ChartService.SEASONAL_EXTRA_SONGS_COUNT);
        model.addAttribute("prevPeriodKey", prevPeriodKey);
        model.addAttribute("nextPeriodKey", nextPeriodKey);
        model.addAttribute("allSeasons", chartService.getAllSeasonsWithData());
        
        return "charts/seasonal-edit";
    }
    
    /**
     * API: Save chart entries for a seasonal chart.
     */
    @PostMapping("/seasonal/{periodKey}/save")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> saveSeasonalChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            String chartType = (String) payload.get("chartType");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rawEntries = (List<Map<String, Object>>) payload.get("entries");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rawExtraEntries = (List<Map<String, Object>>) payload.get("extraEntries");
            
            Optional<Chart> chartOpt = chartService.getSeasonalChart(chartType, periodKey);
            if (chartOpt.isEmpty()) {
                return createErrorResponse("Chart not found");
            }
            
            // Convert entries to expected format
            List<Map<String, Integer>> entries = new ArrayList<>();
            for (Map<String, Object> rawEntry : rawEntries) {
                Map<String, Integer> entry = new HashMap<>();
                entry.put("position", ((Number) rawEntry.get("position")).intValue());
                entry.put("itemId", ((Number) rawEntry.get("itemId")).intValue());
                entries.add(entry);
            }
            
            // Add extra entries (positions 31-35) if this is a song chart
            if ("song".equals(chartType) && rawExtraEntries != null) {
                int extraPosition = ChartService.SEASONAL_YEARLY_SONGS_COUNT + 1;
                for (Map<String, Object> rawEntry : rawExtraEntries) {
                    Map<String, Integer> entry = new HashMap<>();
                    entry.put("position", extraPosition++);
                    entry.put("itemId", ((Number) rawEntry.get("itemId")).intValue());
                    entries.add(entry);
                }
            }
            
            chartService.saveChartEntries(chartOpt.get().getId(), entries, chartType);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }
    
    /**
     * API: Finalize a seasonal chart.
     */
    @PostMapping("/seasonal/{periodKey}/finalize")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> finalizeSeasonalChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            Integer songChartId = ((Number) payload.get("songChartId")).intValue();
            Integer albumChartId = ((Number) payload.get("albumChartId")).intValue();
            
            chartService.finalizeChart(songChartId, albumChartId, "seasonal");
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }
    
    /**
     * API: Save and finalize a single (song or album) seasonal chart independently.
     */
    @PostMapping("/seasonal/{periodKey}/finalize-single")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> finalizeSeasonalSingleChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            Integer chartId = ((Number) payload.get("chartId")).intValue();
            chartService.finalizeSingleChart(chartId, "seasonal");
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }
    
    /**
     * API: Unlock a seasonal chart for editing (revert to draft).
     */
    @PostMapping("/seasonal/{periodKey}/unfinalize")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> unfinalizeSeasonalChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            Integer chartId = ((Number) payload.get("chartId")).intValue();
            chartService.unfinalizeChart(chartId);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }

    // ========== YEARLY CHARTS ==========
    
    /**
     * Yearly charts landing page - redirects to latest chart.
     */
    @GetMapping("/yearly")
    public String yearlyCharts(Model model) {
        Optional<Chart> latestChart = chartService.getLatestSeasonalYearlyChart("yearly", "song");
        
        if (latestChart.isPresent()) {
            return "redirect:/charts/yearly/" + latestChart.get().getPeriodKey();
        }
        
        // No charts exist - redirect to current year
        String currentYear = String.valueOf(java.time.LocalDate.now().getYear());
        return "redirect:/charts/yearly/" + currentYear;
    }

    /**
     * View/Edit a specific year's charts.
     */
    @GetMapping("/yearly/{periodKey}")
    public String yearlyChart(@PathVariable String periodKey, Model model) {
        // Get or create draft charts
        Map<String, Chart> charts = chartService.createOrGetDraftCharts("yearly", periodKey);
        Chart songChart = charts.get("song");
        Chart albumChart = charts.get("album");
        
        // For drafts, load ALL entries (unlimited). For finalized, load just 1-30.
        List<ChartEntryDTO> songEntries;
        List<ChartEntryDTO> albumEntries;
        
        if (Boolean.TRUE.equals(songChart.getIsFinalized())) {
            songEntries = chartService.getSeasonalYearlyChartEntries("yearly", "song", periodKey);
        } else {
            songEntries = chartService.getAllChartEntriesForEdit("yearly", "song", periodKey);
        }
        
        if (Boolean.TRUE.equals(albumChart.getIsFinalized())) {
            albumEntries = chartService.getSeasonalYearlyChartEntries("yearly", "album", periodKey);
        } else {
            albumEntries = chartService.getAllChartEntriesForEdit("yearly", "album", periodKey);
        }
        
        boolean isComplete = chartService.isYearComplete(periodKey);
        boolean songNotFinalized = !Boolean.TRUE.equals(songChart.getIsFinalized());
        boolean albumNotFinalized = !Boolean.TRUE.equals(albumChart.getIsFinalized());
        boolean songsNewlyComplete = songNotFinalized && songEntries.size() == ChartService.SEASONAL_YEARLY_SONGS_COUNT;
        boolean albumsNewlyComplete = albumNotFinalized && albumEntries.size() == ChartService.YEARLY_ALBUMS_COUNT;
        boolean songsPartial = songNotFinalized && songEntries.size() > 0 && songEntries.size() < ChartService.SEASONAL_YEARLY_SONGS_COUNT;
        boolean albumsPartial = albumNotFinalized && albumEntries.size() > 0 && albumEntries.size() < ChartService.YEARLY_ALBUMS_COUNT;
        boolean canFinalize = isComplete && (songsNewlyComplete || albumsNewlyComplete) && !songsPartial && !albumsPartial;
        
        // Navigation
        String prevPeriodKey = chartService.getPreviousYearPeriodKey(periodKey);
        String nextPeriodKey = chartService.getNextYearPeriodKey(periodKey);
        // Only show nav if the adjacent year has data
        if (prevPeriodKey != null && !chartService.hasYearData(prevPeriodKey)) {
            prevPeriodKey = null;
        }
        if (nextPeriodKey != null && !chartService.hasYearData(nextPeriodKey)) {
            nextPeriodKey = null;
        }
        
        model.addAttribute("currentSection", "yearly-charts");
        model.addAttribute("periodType", "yearly");
        model.addAttribute("periodKey", periodKey);
        model.addAttribute("displayName", periodKey);
        model.addAttribute("songChart", songChart);
        model.addAttribute("albumChart", albumChart);
        model.addAttribute("songEntries", songEntries);
        model.addAttribute("albumEntries", albumEntries);
        model.addAttribute("isComplete", isComplete);
        model.addAttribute("canFinalize", canFinalize);
        model.addAttribute("songChartFinalized", songChart.getIsFinalized());
        model.addAttribute("albumChartFinalized", albumChart.getIsFinalized());
        model.addAttribute("maxSongs", ChartService.SEASONAL_YEARLY_SONGS_COUNT);
        model.addAttribute("maxAlbums", ChartService.YEARLY_ALBUMS_COUNT);
        model.addAttribute("prevPeriodKey", prevPeriodKey);
        model.addAttribute("nextPeriodKey", nextPeriodKey);
        model.addAttribute("allYears", chartService.getAllYearsWithData());
        
        return "charts/yearly-edit";
    }
    
    /**
     * API: Save chart entries for a yearly chart.
     */
    @PostMapping("/yearly/{periodKey}/save")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> saveYearlyChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            String chartType = (String) payload.get("chartType");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rawEntries = (List<Map<String, Object>>) payload.get("entries");
            
            Optional<Chart> chartOpt = chartService.getYearlyChart(chartType, periodKey);
            if (chartOpt.isEmpty()) {
                return createErrorResponse("Chart not found");
            }
            
            // Convert entries to expected format
            List<Map<String, Integer>> entries = new ArrayList<>();
            for (Map<String, Object> rawEntry : rawEntries) {
                Map<String, Integer> entry = new HashMap<>();
                entry.put("position", ((Number) rawEntry.get("position")).intValue());
                entry.put("itemId", ((Number) rawEntry.get("itemId")).intValue());
                entries.add(entry);
            }
            
            chartService.saveChartEntries(chartOpt.get().getId(), entries, chartType);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }
    
    /**
     * API: Finalize a yearly chart.
     */
    @PostMapping("/yearly/{periodKey}/finalize")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> finalizeYearlyChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            Integer songChartId = ((Number) payload.get("songChartId")).intValue();
            Integer albumChartId = ((Number) payload.get("albumChartId")).intValue();
            
            chartService.finalizeChart(songChartId, albumChartId, "yearly");
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }
    
    /**
     * API: Save and finalize a single (song or album) yearly chart independently.
     */
    @PostMapping("/yearly/{periodKey}/finalize-single")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> finalizeYearlySingleChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            Integer chartId = ((Number) payload.get("chartId")).intValue();
            chartService.finalizeSingleChart(chartId, "yearly");
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }
    
    /**
     * API: Unlock a yearly chart for editing (revert to draft).
     */
    @PostMapping("/yearly/{periodKey}/unfinalize")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> unfinalizeYearlyChart(
            @PathVariable String periodKey,
            @RequestBody Map<String, Object> payload) {
        try {
            Integer chartId = ((Number) payload.get("chartId")).intValue();
            chartService.unfinalizeChart(chartId);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage());
        }
    }

    @GetMapping("/weekly/overview")
    public String weeklyOverview(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            Model model) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        String normalizedSort = normalizeWeeklyOverviewSort(normalizedOverviewTab, sort);
        String normalizedDir = normalizeWeeklyOverviewDir(dir);
        String normalizedQuery = normalizeOverviewQuery(q);

        List<ChartSongOverviewRowDTO> weeklySongRows = chartService.getChartOverviewSongRows("weekly");
        List<ChartAlbumOverviewRowDTO> weeklyAlbumRows = chartService.getChartOverviewAlbumRows(weeklySongRows);
        List<ChartArtistOverviewRowDTO> weeklyArtistRows = chartService.getChartOverviewArtistRows(weeklySongRows);

        List<ChartSongOverviewRowDTO> pagedSongRows = List.of();
        List<ChartAlbumOverviewRowDTO> pagedAlbumRows = List.of();
        List<ChartArtistOverviewRowDTO> pagedArtistRows = List.of();
        int activeTotalCount;

        switch (normalizedOverviewTab) {
            case "album" -> {
                activeTotalCount = countFilteredAlbumOverviewRows(weeklyAlbumRows, normalizedQuery);
                pagedAlbumRows = pageAlbumOverviewRows(weeklyAlbumRows, normalizedQuery, normalizedSort, normalizedDir, 0, WEEKLY_OVERVIEW_PAGE_SIZE);
            }
            case "artist" -> {
                activeTotalCount = countFilteredArtistOverviewRows(weeklyArtistRows, normalizedQuery);
                pagedArtistRows = pageArtistOverviewRows(weeklyArtistRows, normalizedQuery, normalizedSort, normalizedDir, 0, WEEKLY_OVERVIEW_PAGE_SIZE);
            }
            default -> {
                activeTotalCount = countFilteredSongOverviewRows(weeklySongRows, normalizedQuery);
                pagedSongRows = pageSongOverviewRows(weeklySongRows, normalizedQuery, normalizedSort, normalizedDir, 0, WEEKLY_OVERVIEW_PAGE_SIZE);
            }
        }

        model.addAttribute("currentSection", "weekly-overview-charts");
        model.addAttribute("periodType", "weekly");
        model.addAttribute("overviewTab", normalizedOverviewTab);
        model.addAttribute("songRows", pagedSongRows);
        model.addAttribute("albumRows", pagedAlbumRows);
        model.addAttribute("artistRows", pagedArtistRows);
        model.addAttribute("songCount", weeklySongRows.size());
        model.addAttribute("albumCount", weeklyAlbumRows.size());
        model.addAttribute("artistCount", weeklyArtistRows.size());
        model.addAttribute("pageTitle", "Weekly Chart Overview");
        model.addAttribute("pageSubtitle", "Every finalized weekly song chart rolled up into song, album, and artist stats.");
        model.addAttribute("unitLabel", "Weeks");
        model.addAttribute("mainChartUrl", "/charts/weekly");
        model.addAttribute("mainChartLabel", "Weekly Charts");
        model.addAttribute("weeklyInfiniteScrollEnabled", true);
        model.addAttribute("pageSize", WEEKLY_OVERVIEW_PAGE_SIZE);
        model.addAttribute("activeTotalCount", activeTotalCount);
        model.addAttribute("selectedSort", normalizedSort);
        model.addAttribute("selectedDir", normalizedDir);
        model.addAttribute("searchQuery", normalizedQuery);

        return "charts/overview";
    }

    @GetMapping("/weekly/overview/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> weeklyOverviewData(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "100") int size) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        String normalizedSort = normalizeWeeklyOverviewSort(normalizedOverviewTab, sort);
        String normalizedDir = normalizeWeeklyOverviewDir(dir);
        String normalizedQuery = normalizeOverviewQuery(q);
        int safeSize = Math.min(Math.max(size, 1), 250);

        Map<String, Object> result = new HashMap<>();

        switch (normalizedOverviewTab) {
            case "album" -> {
                List<ChartAlbumOverviewRowDTO> weeklyAlbumRows = chartService.getChartOverviewAlbumRows("weekly");
                int totalCount = countFilteredAlbumOverviewRows(weeklyAlbumRows, normalizedQuery);
                List<ChartAlbumOverviewRowDTO> entries = pageAlbumOverviewRows(weeklyAlbumRows, normalizedQuery, normalizedSort, normalizedDir, page, safeSize);
                result.put("entries", entries);
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
            case "artist" -> {
                List<ChartArtistOverviewRowDTO> weeklyArtistRows = chartService.getChartOverviewArtistRows("weekly");
                int totalCount = countFilteredArtistOverviewRows(weeklyArtistRows, normalizedQuery);
                List<ChartArtistOverviewRowDTO> entries = pageArtistOverviewRows(weeklyArtistRows, normalizedQuery, normalizedSort, normalizedDir, page, safeSize);
                result.put("entries", entries);
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
            default -> {
                List<ChartSongOverviewRowDTO> weeklySongRows = chartService.getChartOverviewSongRows("weekly");
                int totalCount = countFilteredSongOverviewRows(weeklySongRows, normalizedQuery);
                List<ChartSongOverviewRowDTO> entries = pageSongOverviewRows(weeklySongRows, normalizedQuery, normalizedSort, normalizedDir, page, safeSize);
                result.put("entries", entries);
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
        }

        result.put("nextPage", page + 1);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/seasonal/overview")
    public String seasonalOverview(
            @RequestParam(defaultValue = "song") String overviewTab,
            Model model) {
        return renderChartOverview("seasonal", overviewTab, model);
    }

    @GetMapping("/yearly/overview")
    public String yearlyOverview(
            @RequestParam(defaultValue = "song") String overviewTab,
            Model model) {
        return renderChartOverview("yearly", overviewTab, model);
    }

    /**
     * Most Weeks at Position page - shows songs/albums with most weeks at #1, top 5, etc.
     */
    @GetMapping("/most-weeks")
    public String mostWeeksAtPosition(
            @RequestParam(defaultValue = "song") String type,
            @RequestParam(defaultValue = "1") Integer position,
            @RequestParam(required = false) Integer year,
            @RequestParam(defaultValue = "weeks") String sort,
            @RequestParam(defaultValue = "desc") String dir,
            Model model) {

        // Validate position based on type
        // Songs: 1, 5, 10, 20 allowed
        // Albums: 1, 5, 10 allowed
        if ("album".equals(type) && position > 10) {
            return "redirect:/charts/most-weeks?type=album&position=1" + (year != null ? "&year=" + year : "");
        }

        if (!List.of("weeks", "name", "artist", "peak").contains(sort)) sort = "weeks";
        if (!List.of("asc", "desc").contains(dir)) dir = "desc";

        final int PAGE_SIZE = 50;
        List<MostWeeksEntryDTO> entries = chartService.getMostWeeksAtPosition(type, position, year, 0, PAGE_SIZE, sort, dir);
        int totalCount = chartService.getMostWeeksCount(type, position, year);
        List<Map<String, Object>> years = chartService.getChartYearsForMostWeeks();

        model.addAttribute("currentSection", "most-weeks-charts");
        model.addAttribute("entries", entries);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("pageSize", PAGE_SIZE);
        model.addAttribute("years", years);
        model.addAttribute("selectedType", type);
        model.addAttribute("selectedPosition", position);
        model.addAttribute("selectedYear", year);
        model.addAttribute("selectedSort", sort);
        model.addAttribute("selectedDir", dir);

        return "charts/most-weeks";
    }

    /**
     * JSON endpoint for infinite-scroll pagination on the Most Weeks at Position page.
     */
    @GetMapping("/most-weeks/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> mostWeeksData(
            @RequestParam(defaultValue = "song") String type,
            @RequestParam(defaultValue = "1") Integer position,
            @RequestParam(required = false) Integer year,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(defaultValue = "weeks") String sort,
            @RequestParam(defaultValue = "desc") String dir) {

        if ("album".equals(type) && position > 10) {
            position = 1;
        }
        if (!List.of("weeks", "name", "artist", "peak").contains(sort)) sort = "weeks";
        if (!List.of("asc", "desc").contains(dir)) dir = "desc";
        // Cap page size to prevent abuse
        int safeSize = Math.min(size, 200);

        List<MostWeeksEntryDTO> entries = chartService.getMostWeeksAtPosition(type, position, year, page, safeSize, sort, dir);
        int totalCount = chartService.getMostWeeksCount(type, position, year);

        Map<String, Object> result = new HashMap<>();
        result.put("entries", entries);
        result.put("totalCount", totalCount);
        result.put("hasMore", (long)(page + 1) * safeSize < totalCount);
        result.put("nextPage", page + 1);

        return ResponseEntity.ok(result);
    }
    
    /**
     * Most Hits page - shows artists with most songs that reached #1, top 5, top 10, or top 20.
     */
    @GetMapping("/most-hits")
    public String mostHits(
            @RequestParam(defaultValue = "1") Integer position,
            @RequestParam(required = false) Integer year,
            @RequestParam(defaultValue = "songs") String sort,
            @RequestParam(defaultValue = "desc") String dir,
            Model model) {

        if (!List.of("songs", "weeks", "artist").contains(sort)) sort = "songs";
        if (!List.of("asc", "desc").contains(dir)) dir = "desc";

        final int PAGE_SIZE = 50;
        List<MostHitsEntryDTO> entries = chartService.getMostHits(position, year, 0, PAGE_SIZE, sort, dir);
        int totalCount = chartService.getMostHitsCount(position, year);
        List<Map<String, Object>> years = chartService.getChartYearsForMostWeeks();

        model.addAttribute("currentSection", "most-hits-charts");
        model.addAttribute("entries", entries);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("pageSize", PAGE_SIZE);
        model.addAttribute("years", years);
        model.addAttribute("selectedPosition", position);
        model.addAttribute("selectedYear", year);
        model.addAttribute("selectedSort", sort);
        model.addAttribute("selectedDir", dir);

        return "charts/most-hits";
    }

    /**
     * JSON endpoint for infinite-scroll pagination on the Most Hits page.
     */
    @GetMapping("/most-hits/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> mostHitsData(
            @RequestParam(defaultValue = "1") Integer position,
            @RequestParam(required = false) Integer year,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(defaultValue = "songs") String sort,
            @RequestParam(defaultValue = "desc") String dir) {

        if (!List.of("songs", "weeks", "artist").contains(sort)) sort = "songs";
        if (!List.of("asc", "desc").contains(dir)) dir = "desc";
        int safeSize = Math.min(size, 200);

        List<MostHitsEntryDTO> entries = chartService.getMostHits(position, year, page, safeSize, sort, dir);
        int totalCount = chartService.getMostHitsCount(position, year);

        Map<String, Object> result = new HashMap<>();
        result.put("entries", entries);
        result.put("totalCount", totalCount);
        result.put("hasMore", (long)(page + 1) * safeSize < totalCount);
        result.put("nextPage", page + 1);

        return ResponseEntity.ok(result);
    }
    
    /**
     * Weekly Number Ones page - shows all #1 runs on the weekly chart for songs and albums.
     */
    @GetMapping("/number-ones")
    public String weeklyNumberOnes(
            @RequestParam(defaultValue = "song") String type,
            Model model) {
        
        List<library.dto.NumberOneRunDTO> runs = chartService.getNumberOneRuns(type);
        
        model.addAttribute("currentSection", "number-ones-charts");
        model.addAttribute("runs", runs);
        model.addAttribute("selectedType", type);
        
        return "charts/number-ones";
    }
    
    // ========== HELPER METHODS ==========

    /**
     * API: Get past chart appearances for a song or album in a given period type.
     * Used by the chart editor to show prior chart history on each item.
     */
    @GetMapping("/api/history/{periodType}/{chartType}/{itemId}")
    @ResponseBody
    public ResponseEntity<List<Map<String, Object>>> getItemChartHistory(
            @PathVariable String periodType,
            @PathVariable String chartType,
            @PathVariable Integer itemId,
            @RequestParam(required = false, defaultValue = "") String excludePeriodKey) {
        List<Map<String, Object>> history = chartService.getPastChartAppearancesForItem(itemId, chartType, periodType, excludePeriodKey);
        return ResponseEntity.ok(history);
    }

    private String renderChartOverview(String periodType, String overviewTab, Model model) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        List<ChartSongOverviewRowDTO> songRows = chartService.getChartOverviewSongRows(periodType);
        List<ChartAlbumOverviewRowDTO> albumRows = chartService.getChartOverviewAlbumRows(songRows);
        List<ChartArtistOverviewRowDTO> artistRows = chartService.getChartOverviewArtistRows(songRows);

        model.addAttribute("currentSection", switch (periodType) {
            case "seasonal" -> "seasonal-overview-charts";
            case "yearly" -> "yearly-overview-charts";
            default -> "weekly-overview-charts";
        });
        model.addAttribute("periodType", periodType);
        model.addAttribute("overviewTab", normalizedOverviewTab);
        model.addAttribute("songRows", songRows);
        model.addAttribute("albumRows", albumRows);
        model.addAttribute("artistRows", artistRows);
        model.addAttribute("songCount", songRows.size());
        model.addAttribute("albumCount", albumRows.size());
        model.addAttribute("artistCount", artistRows.size());
        model.addAttribute("pageTitle", switch (periodType) {
            case "seasonal" -> "Seasonal Chart Overview";
            case "yearly" -> "Yearly Chart Overview";
            default -> "Weekly Chart Overview";
        });
        model.addAttribute("pageSubtitle", switch (periodType) {
            case "seasonal" -> "Finalized seasonal song charts rolled up into song, album, and artist stats.";
            case "yearly" -> "Finalized yearly song charts rolled up into song, album, and artist stats.";
            default -> "Every finalized weekly song chart rolled up into song, album, and artist stats.";
        });
        model.addAttribute("unitLabel", switch (periodType) {
            case "seasonal" -> "Seasons";
            case "yearly" -> "Years";
            default -> "Weeks";
        });
        model.addAttribute("mainChartUrl", "/charts/" + periodType);
        model.addAttribute("mainChartLabel", switch (periodType) {
            case "seasonal" -> "Seasonal Charts";
            case "yearly" -> "Yearly Charts";
            default -> "Weekly Charts";
        });
        model.addAttribute("weeklyInfiniteScrollEnabled", false);
        model.addAttribute("selectedSort", null);
        model.addAttribute("selectedDir", null);
        model.addAttribute("searchQuery", "");
        model.addAttribute("activeTotalCount", 0);
        model.addAttribute("pageSize", 0);
        return "charts/overview";
    }

    private String normalizeOverviewQuery(String query) {
        if (query == null) {
            return "";
        }
        return query.trim();
    }

    private String normalizeWeeklyOverviewSort(String overviewTab, String sort) {
        List<String> allowedSorts = switch (overviewTab) {
            case "album" -> List.of("artist", "album", "songs", "weeks", "peak", "numberOnes", "atNumberOne", "debutPeriod", "lastPeriod");
            case "artist" -> List.of("artist", "songs", "weeks", "peak", "numberOnes", "atNumberOne", "debutPeriod", "lastPeriod");
            default -> List.of("artist", "song", "weeks", "peak", "atPeak", "debutPeriod", "debutPosition", "lastPeriod", "peakPeriod");
        };

        if (sort == null || !allowedSorts.contains(sort)) {
            return "debutPeriod";
        }
        return sort;
    }

    private String normalizeWeeklyOverviewDir(String dir) {
        return "asc".equalsIgnoreCase(dir) ? "asc" : "desc";
    }

    private int countFilteredSongOverviewRows(List<ChartSongOverviewRowDTO> rows, String query) {
        return filterSongOverviewRows(rows, query).size();
    }

    private int countFilteredAlbumOverviewRows(List<ChartAlbumOverviewRowDTO> rows, String query) {
        return filterAlbumOverviewRows(rows, query).size();
    }

    private int countFilteredArtistOverviewRows(List<ChartArtistOverviewRowDTO> rows, String query) {
        return filterArtistOverviewRows(rows, query).size();
    }

    private List<ChartSongOverviewRowDTO> pageSongOverviewRows(List<ChartSongOverviewRowDTO> rows, String query, String sort, String dir, int page, int pageSize) {
        List<ChartSongOverviewRowDTO> filteredRows = filterSongOverviewRows(rows, query);
        filteredRows.sort(buildSongOverviewComparator(sort, dir));
        return paginateRows(filteredRows, page, pageSize);
    }

    private List<ChartAlbumOverviewRowDTO> pageAlbumOverviewRows(List<ChartAlbumOverviewRowDTO> rows, String query, String sort, String dir, int page, int pageSize) {
        List<ChartAlbumOverviewRowDTO> filteredRows = filterAlbumOverviewRows(rows, query);
        filteredRows.sort(buildAlbumOverviewComparator(sort, dir));
        return paginateRows(filteredRows, page, pageSize);
    }

    private List<ChartArtistOverviewRowDTO> pageArtistOverviewRows(List<ChartArtistOverviewRowDTO> rows, String query, String sort, String dir, int page, int pageSize) {
        List<ChartArtistOverviewRowDTO> filteredRows = filterArtistOverviewRows(rows, query);
        filteredRows.sort(buildArtistOverviewComparator(sort, dir));
        return paginateRows(filteredRows, page, pageSize);
    }

    private List<ChartSongOverviewRowDTO> filterSongOverviewRows(List<ChartSongOverviewRowDTO> rows, String query) {
        List<ChartSongOverviewRowDTO> filteredRows = new ArrayList<>(rows);
        if (query == null || query.isBlank()) {
            return filteredRows;
        }

        String normalizedQuery = query.toLowerCase(Locale.ROOT);
        filteredRows.removeIf(row -> !containsOverviewValue(row.getArtistName(), normalizedQuery)
                && !containsOverviewValue(row.getSongTitle(), normalizedQuery)
                && !containsOverviewValue(row.getAlbumName(), normalizedQuery));
        return filteredRows;
    }

    private List<ChartAlbumOverviewRowDTO> filterAlbumOverviewRows(List<ChartAlbumOverviewRowDTO> rows, String query) {
        List<ChartAlbumOverviewRowDTO> filteredRows = new ArrayList<>(rows);
        if (query == null || query.isBlank()) {
            return filteredRows;
        }

        String normalizedQuery = query.toLowerCase(Locale.ROOT);
        filteredRows.removeIf(row -> !containsOverviewValue(row.getArtistName(), normalizedQuery)
                && !containsOverviewValue(row.getAlbumName(), normalizedQuery));
        return filteredRows;
    }

    private List<ChartArtistOverviewRowDTO> filterArtistOverviewRows(List<ChartArtistOverviewRowDTO> rows, String query) {
        List<ChartArtistOverviewRowDTO> filteredRows = new ArrayList<>(rows);
        if (query == null || query.isBlank()) {
            return filteredRows;
        }

        String normalizedQuery = query.toLowerCase(Locale.ROOT);
        filteredRows.removeIf(row -> !containsOverviewValue(row.getArtistName(), normalizedQuery));
        return filteredRows;
    }

    private boolean containsOverviewValue(String value, String normalizedQuery) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(normalizedQuery);
    }

    private Comparator<ChartSongOverviewRowDTO> buildSongOverviewComparator(String sort, String dir) {
        Comparator<ChartSongOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(ChartSongOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
            case "song" -> Comparator.comparing(ChartSongOverviewRowDTO::getSongTitle, String.CASE_INSENSITIVE_ORDER);
            case "peak" -> Comparator.comparingInt(ChartSongOverviewRowDTO::getPeakPosition);
            case "atPeak" -> Comparator.comparingInt(ChartSongOverviewRowDTO::getSpanAtPeak);
            case "debutPeriod" -> Comparator.comparing(ChartSongOverviewRowDTO::getFirstAppearanceSortValue, Comparator.nullsLast(String::compareTo));
            case "debutPosition" -> Comparator.comparing(ChartSongOverviewRowDTO::getDebutPosition, Comparator.nullsLast(Integer::compareTo));
            case "lastPeriod" -> Comparator.comparing(ChartSongOverviewRowDTO::getLastAppearanceSortValue, Comparator.nullsLast(String::compareTo));
            case "peakPeriod" -> Comparator.comparing(ChartSongOverviewRowDTO::getPeakAppearanceSortValue, Comparator.nullsLast(String::compareTo));
            default -> Comparator.comparingInt(ChartSongOverviewRowDTO::getTotalChartSpan);
        };

        comparator = switch (sort) {
            case "artist" -> comparator.thenComparing(ChartSongOverviewRowDTO::getSongTitle, String.CASE_INSENSITIVE_ORDER);
            case "song" -> comparator.thenComparing(ChartSongOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
            default -> comparator.thenComparing(ChartSongOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER)
                    .thenComparing(ChartSongOverviewRowDTO::getSongTitle, String.CASE_INSENSITIVE_ORDER);
        };

        return "asc".equals(dir) ? comparator : comparator.reversed();
    }

    private Comparator<ChartAlbumOverviewRowDTO> buildAlbumOverviewComparator(String sort, String dir) {
        Comparator<ChartAlbumOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
            case "album" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getAlbumName, String.CASE_INSENSITIVE_ORDER);
            case "songs" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "numberOnes" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getNumberOneSongsCount);
            case "atNumberOne" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalSpanAtNumberOne);
            case "debutPeriod" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getFirstDebutSortValue, Comparator.nullsLast(String::compareTo));
            case "lastPeriod" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getLastAppearanceSortValue, Comparator.nullsLast(String::compareTo));
            default -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan);
        };

        comparator = switch (sort) {
            case "artist" -> comparator.thenComparing(ChartAlbumOverviewRowDTO::getAlbumName, String.CASE_INSENSITIVE_ORDER);
            case "album" -> comparator.thenComparing(ChartAlbumOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
            default -> comparator.thenComparing(ChartAlbumOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER)
                    .thenComparing(ChartAlbumOverviewRowDTO::getAlbumName, String.CASE_INSENSITIVE_ORDER);
        };

        return "asc".equals(dir) ? comparator : comparator.reversed();
    }

    private Comparator<ChartArtistOverviewRowDTO> buildArtistOverviewComparator(String sort, String dir) {
        Comparator<ChartArtistOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(ChartArtistOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
            case "songs" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparing(ChartArtistOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "numberOnes" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getNumberOneSongsCount);
            case "atNumberOne" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalSpanAtNumberOne);
            case "debutPeriod" -> Comparator.comparing(ChartArtistOverviewRowDTO::getFirstDebutSortValue, Comparator.nullsLast(String::compareTo));
            case "lastPeriod" -> Comparator.comparing(ChartArtistOverviewRowDTO::getLastAppearanceSortValue, Comparator.nullsLast(String::compareTo));
            default -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan);
        };

        comparator = comparator.thenComparing(ChartArtistOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
        return "asc".equals(dir) ? comparator : comparator.reversed();
    }

    private <T> List<T> paginateRows(List<T> rows, int page, int pageSize) {
        int safePage = Math.max(page, 0);
        int offset = safePage * pageSize;
        if (offset >= rows.size()) {
            return List.of();
        }
        int endIndex = Math.min(offset + pageSize, rows.size());
        return new ArrayList<>(rows.subList(offset, endIndex));
    }

    private String normalizeOverviewTab(String overviewTab) {
        if (overviewTab == null) {
            return "song";
        }
        return switch (overviewTab.toLowerCase()) {
            case "album", "artist" -> overviewTab.toLowerCase();
            default -> "song";
        };
    }

    private ResponseEntity<Map<String, Object>> createErrorResponse(String errorMessage) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", errorMessage);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
