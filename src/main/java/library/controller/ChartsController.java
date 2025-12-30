package library.controller;

import library.dto.AlbumChartRunDTO;
import library.dto.ChartEntryDTO;
import library.dto.ChartGenerationProgressDTO;
import library.dto.ChartRunDTO;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Controller for the Charts feature - weekly top songs/albums rankings.
 */
@Controller
@RequestMapping("/charts")
public class ChartsController {
    
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

                model.addAttribute("currentSection", "weekly-charts");
                model.addAttribute("hasChart", false);
                model.addAttribute("isPreview", true);
                model.addAttribute("entries", entries);
                model.addAttribute("albumEntries", albumEntries);
                model.addAttribute("periodKey", periodKey);
                model.addAttribute("formattedPeriod", formattedPeriod);
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
        
        model.addAttribute("currentSection", "weekly-charts");
        model.addAttribute("hasChart", true);
        model.addAttribute("chart", chart);
        model.addAttribute("entries", entries);
        model.addAttribute("albumEntries", albumEntries);
        model.addAttribute("periodKey", periodKey);
        model.addAttribute("formattedPeriod", chart.getFormattedPeriod());
        model.addAttribute("prevPeriodKey", prevChart.map(Chart::getPeriodKey).orElse(null));
        model.addAttribute("nextPeriodKey", nextChart.map(Chart::getPeriodKey).orElse(null));
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
            // Get all entries for draft editing (no position limit)
            songEntries = chartService.getAllChartEntriesForEdit("seasonal", "song", periodKey);
            extraSongEntries = java.util.Collections.emptyList();
        }
        
        if (Boolean.TRUE.equals(albumChart.getIsFinalized())) {
            albumEntries = chartService.getSeasonalYearlyChartEntries("seasonal", "album", periodKey);
        } else {
            albumEntries = chartService.getAllChartEntriesForEdit("seasonal", "album", periodKey);
        }
        
        boolean isComplete = chartService.isSeasonComplete(periodKey);
        // Can finalize only if exactly the right counts
        int mainSongCount = (int) songEntries.stream().filter(e -> e.getPosition() <= ChartService.SEASONAL_YEARLY_SONGS_COUNT).count();
        int extraSongCount = (int) songEntries.stream().filter(e -> e.getPosition() > ChartService.SEASONAL_YEARLY_SONGS_COUNT).count();
        boolean canFinalize = isComplete 
            && mainSongCount == ChartService.SEASONAL_YEARLY_SONGS_COUNT
            && albumEntries.size() == ChartService.SEASONAL_ALBUMS_COUNT;
        
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
        boolean canFinalize = isComplete 
            && songEntries.size() == ChartService.SEASONAL_YEARLY_SONGS_COUNT
            && albumEntries.size() == ChartService.YEARLY_ALBUMS_COUNT;
        
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
     * Most Weeks at Position page - shows songs/albums with most weeks at #1, top 5, etc.
     */
    @GetMapping("/most-weeks")
    public String mostWeeksAtPosition(
            @RequestParam(defaultValue = "song") String type,
            @RequestParam(defaultValue = "1") Integer position,
            @RequestParam(required = false) Integer year,
            Model model) {
        
        // Validate position based on type
        // Songs: 1, 5, 10, 20 allowed
        // Albums: 1, 5, 10 allowed
        if ("album".equals(type) && position > 10) {
            // Redirect to position 1 if invalid for albums
            return "redirect:/charts/most-weeks?type=album&position=1" + (year != null ? "&year=" + year : "");
        }
        
        List<MostWeeksEntryDTO> entries = chartService.getMostWeeksAtPosition(type, position, year);
        List<Map<String, Object>> years = chartService.getChartYearsForMostWeeks();
        
        model.addAttribute("currentSection", "most-weeks-charts");
        model.addAttribute("entries", entries);
        model.addAttribute("years", years);
        model.addAttribute("selectedType", type);
        model.addAttribute("selectedPosition", position);
        model.addAttribute("selectedYear", year);
        
        return "charts/most-weeks";
    }
    
    /**
     * Most Hits page - shows artists with most songs that reached #1, top 5, top 10, or top 20.
     */
    @GetMapping("/most-hits")
    public String mostHits(
            @RequestParam(defaultValue = "1") Integer position,
            @RequestParam(required = false) Integer year,
            Model model) {
        
        List<MostHitsEntryDTO> entries = chartService.getMostHits(position, year);
        List<Map<String, Object>> years = chartService.getChartYearsForMostWeeks();
        
        model.addAttribute("currentSection", "most-hits-charts");
        model.addAttribute("entries", entries);
        model.addAttribute("years", years);
        model.addAttribute("selectedPosition", position);
        model.addAttribute("selectedYear", year);
        
        return "charts/most-hits";
    }
    
    // ========== HELPER METHODS ==========
    
    private ResponseEntity<Map<String, Object>> createErrorResponse(String errorMessage) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", errorMessage);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
