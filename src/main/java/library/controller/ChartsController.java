package library.controller;

import library.dto.ChartEntryDTO;
import library.dto.ChartGenerationProgressDTO;
import library.dto.ChartRunDTO;
import library.entity.Chart;
import library.service.ChartService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

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
        Optional<Chart> latestChart = chartService.getLatestChart("song");
        
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
    public String weeklyChart(@PathVariable String periodKey, Model model) {
        Optional<Chart> chartOpt = chartService.getChart("song", periodKey);
        
        if (chartOpt.isEmpty()) {
            // Chart doesn't exist for this week
            model.addAttribute("currentSection", "weekly-charts");
            model.addAttribute("hasChart", false);
            model.addAttribute("periodKey", periodKey);
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
}
