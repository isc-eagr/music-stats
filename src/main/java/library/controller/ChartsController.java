package library.controller;

import library.dto.AlbumChartRunDTO;
import library.dto.ChartEntryDTO;
import library.dto.ChartGenerationProgressDTO;
import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartRunDTO;
import library.dto.ChartSongOverviewRowDTO;
import library.entity.Chart;
import library.service.AppConfigService;
import library.service.BillboardHot100Service;
import library.service.ChartService;
import library.service.PcService;
import library.service.TrlService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Controller for the Charts feature - weekly top songs/albums rankings.
 */
@Controller
@RequestMapping("/charts")
public class ChartsController {

    private static final int WEEKLY_OVERVIEW_PAGE_SIZE = 100;
    private static final int SEASONAL_SONG_OVERVIEW_PAGE_SIZE = 100;
    private static final Pattern OVERVIEW_RANGE_PATTERN = Pattern.compile("^(-?\\d+(?:\\.\\d+)?)\\s*-\\s*(-?\\d+(?:\\.\\d+)?)$");
    private static final Pattern OVERVIEW_COMPARE_PATTERN = Pattern.compile("^(<=|>=|=|<|>)?\\s*(-?\\d+(?:\\.\\d+)?)$");

    private record OverviewSortSpec(String sort, String dir) {}
    
    private final ChartService chartService;
    private final AppConfigService appConfigService;
    private final BillboardHot100Service billboardHot100Service;
    private final PcService pcService;
    private final TrlService trlService;
    
    public ChartsController(ChartService chartService, AppConfigService appConfigService, BillboardHot100Service billboardHot100Service,
                             PcService pcService, TrlService trlService) {
        this.chartService = chartService;
        this.appConfigService = appConfigService;
        this.billboardHot100Service = billboardHot100Service;
        this.pcService = pcService;
        this.trlService = trlService;
    }
    
    /**
     * Weekly charts landing page - redirects to latest chart or shows empty state.
     */
    @GetMapping("/weekly")
    public String weeklyCharts(@RequestParam(required = false) String view, Model model) {
        Optional<Chart> latestChart = chartService.getLatestWeeklyChart("song");
        String selectedView = normalizeWeeklyView(view);
        
        if (latestChart.isPresent()) {
            return "redirect:/charts/weekly/" + latestChart.get().getPeriodKey()
                    + ("albums".equals(selectedView) ? "?view=albums" : "");
        }
        
        // No charts exist yet - show empty state
        model.addAttribute("currentSection", "weekly-charts");
        model.addAttribute("hasChart", false);
        model.addAttribute("missingWeeksCount", chartService.getWeeksWithoutCharts().size());
        model.addAttribute("selectedView", selectedView);
        
        return "charts/weekly";
    }
    
    /**
     * Display a specific week's chart.
     */
    @GetMapping("/weekly/{periodKey}")
    public String weeklyChart(@PathVariable String periodKey,
                              @RequestParam(required = false) Boolean preview,
                              @RequestParam(required = false) String view,
                              Model model) {
        String selectedView = normalizeWeeklyView(view);
        Set<String> availableDates = buildWeeklyDateOptions(periodKey);
        String selectedDate = getWeeklyPeriodEndDate(periodKey);
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
                model.addAttribute("selectedView", selectedView);
                model.addAttribute("availableDates", availableDates);
                model.addAttribute("selectedDate", selectedDate);

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
            model.addAttribute("selectedView", selectedView);
            model.addAttribute("availableDates", availableDates);
            model.addAttribute("selectedDate", selectedDate);
            return "charts/weekly";
        }
        
        Chart chart = chartOpt.get();
        List<ChartEntryDTO> entries = chartService.getWeeklyChartWithStats(periodKey);
        List<ChartEntryDTO> albumEntries = chartService.getWeeklyAlbumChartWithStats(periodKey);
        List<ChartEntryDTO> fallOffEntries = chartService.getWeeklyChartFallOffs(periodKey, entries);
        List<ChartEntryDTO> albumFallOffEntries = chartService.getWeeklyAlbumChartFallOffs(periodKey, albumEntries);
        
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
        model.addAttribute("fallOffEntries", fallOffEntries);
        model.addAttribute("albumFallOffEntries", albumFallOffEntries);
        model.addAttribute("periodKey", periodKey);
        model.addAttribute("formattedPeriod", chart.getFormattedPeriod());
        model.addAttribute("prevPeriodKey", prevChart.map(Chart::getPeriodKey).orElse(null));
        model.addAttribute("nextPeriodKey", nextPeriodKey);
        model.addAttribute("nextIsPreview", nextIsPreview);
        model.addAttribute("missingWeeksCount", chartService.getWeeksWithoutCharts().size());
        model.addAttribute("selectedView", selectedView);
        model.addAttribute("availableDates", availableDates);
        model.addAttribute("selectedDate", selectedDate);
        
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
     * API: Get Billboard Hot 100 chart run for a song.
     * Used in detail pages for expandable chart run rows.
     */
    @GetMapping("/bb/song/{songId}/run")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getBillboardSongChartRun(@PathVariable Integer songId) {
        try {
            List<Map<String, Object>> rawRun = billboardHot100Service.getChartRunBySongId(songId);
            return ResponseEntity.ok(buildChartRunResponse(rawRun, "week"));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * API: Get Vato's Cuntdown chart run for a song.
     * Used in detail pages for expandable chart run rows.
     */
    @GetMapping("/pc/song/{songId}/run")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getPcSongChartRun(@PathVariable Integer songId) {
        try {
            List<Map<String, Object>> rawRun = pcService.getChartRunBySongId(songId);
            return ResponseEntity.ok(buildChartRunResponse(rawRun, "day"));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * API: Get TRL chart run for a song.
     * Used in detail pages for expandable chart run rows.
     */
    @GetMapping("/trl/song/{songId}/run")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getTrlSongChartRun(@PathVariable Integer songId) {
        try {
            List<Map<String, Object>> rawRun = trlService.getChartRunBySongId(songId);
            return ResponseEntity.ok(buildChartRunResponse(rawRun, "day"));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Transforms a raw chart run list (chart_date, on_chart, position) into
     * the standard JSON response format used by detail page chart run renderers.
     */
    private Map<String, Object> buildChartRunResponse(List<Map<String, Object>> rawRun, String unit) {
        List<Map<String, Object>> weeks = new ArrayList<>();
        int top1 = 0, top5 = 0, top10 = 0, top20 = 0;
        for (Map<String, Object> entry : rawRun) {
            int onChart = entry.get("on_chart") != null ? ((Number) entry.get("on_chart")).intValue() : 0;
            Integer position = entry.get("position") != null ? ((Number) entry.get("position")).intValue() : null;
            String date = (String) entry.get("chart_date");
            Map<String, Object> week = new LinkedHashMap<>();
            week.put("onChart", onChart == 1);
            week.put("position", position);
            week.put("periodKey", date);
            week.put("display", position != null ? String.valueOf(position) : "");
            week.put("dateRange", date);
            weeks.add(week);
            if (onChart == 1 && position != null) {
                if (position <= 1) top1++;
                if (position <= 5) top5++;
                if (position <= 10) top10++;
                if (position <= 20) top20++;
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("weeks", weeks);
        response.put("weeksAtTop1", top1);
        response.put("weeksAtTop5", top5);
        response.put("weeksAtTop10", top10);
        response.put("weeksAtTop20", top20);
        response.put("unit", unit);
        return response;
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
    public String weeklyChartByDate(@RequestParam String date,
                                    @RequestParam(required = false) String view) {
        // Convert date to week period key (YYYY-WXX format) matching SQLite's %W
        try {
            java.time.LocalDate localDate = java.time.LocalDate.parse(date);
            int year = localDate.getYear();
            String selectedView = normalizeWeeklyView(view);
            
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
            return "redirect:/charts/weekly/" + periodKey + ("albums".equals(selectedView) ? "?view=albums" : "");
        } catch (Exception e) {
            return "redirect:/charts/weekly" + ("albums".equals(normalizeWeeklyView(view)) ? "?view=albums" : "");
        }
    }

    private String normalizeWeeklyView(String view) {
        return "albums".equalsIgnoreCase(view) ? "albums" : "songs";
    }

    private Set<String> buildWeeklyDateOptions(String currentPeriodKey) {
        Set<String> periodKeys = new TreeSet<>(chartService.getExistingChartPeriodKeys("song"));
        if (currentPeriodKey != null && !currentPeriodKey.isBlank()) {
            periodKeys.add(currentPeriodKey);
        }

        Set<String> availableDates = new LinkedHashSet<>();
        for (String periodKey : periodKeys) {
            availableDates.add(getWeeklyPeriodEndDate(periodKey));
        }
        return availableDates;
    }

    private String getWeeklyPeriodEndDate(String periodKey) {
        try {
            String[] parts = periodKey.split("-W");
            int year = Integer.parseInt(parts[0]);
            int week = Integer.parseInt(parts[1]);

            LocalDate jan1 = LocalDate.of(year, 1, 1);
            LocalDate firstMonday = jan1.with(TemporalAdjusters.nextOrSame(DayOfWeek.MONDAY));
            LocalDate weekStart = week == 0 ? jan1 : firstMonday.plusWeeks(week - 1L);
            return weekStart.plusDays(6).toString();
        } catch (Exception ignored) {
            return LocalDate.now().toString();
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
            @RequestParam(required = false) String sort2,
            @RequestParam(required = false) String dir2,
            @RequestParam(required = false) String sort3,
            @RequestParam(required = false) String dir3,
            Model model) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        String normalizedQuery = normalizeOverviewQuery(q);
        List<OverviewSortSpec> sortSpecs = normalizeWeeklyOverviewSortSpecs(normalizedOverviewTab, sort, dir, sort2, dir2, sort3, dir3);
        String normalizedSort = sortSpecs.get(0).sort();
        String normalizedDir = sortSpecs.get(0).dir();
        int pageSize = appConfigService.getWeeklyOverviewPageSize();

        List<ChartSongOverviewRowDTO> weeklySongRows = chartService.getChartOverviewSongRows("weekly");
        List<ChartAlbumOverviewRowDTO> weeklyAlbumRows = chartService.getChartOverviewAlbumRows("weekly");
        List<ChartArtistOverviewRowDTO> weeklyArtistRows = chartService.getChartOverviewArtistRows(weeklySongRows, weeklyAlbumRows);

        List<ChartSongOverviewRowDTO> pagedSongRows = List.of();
        List<ChartAlbumOverviewRowDTO> pagedAlbumRows = List.of();
        List<ChartArtistOverviewRowDTO> pagedArtistRows = List.of();
        int activeTotalCount;

        switch (normalizedOverviewTab) {
            case "album" -> {
                activeTotalCount = weeklyAlbumRows.size();
                List<ChartAlbumOverviewRowDTO> sortedRows = new ArrayList<>(weeklyAlbumRows);
                sortedRows.sort(buildAlbumOverviewComparator(sortSpecs));
                pagedAlbumRows = paginateRows(sortedRows, 0, pageSize);
            }
            case "artist" -> {
                activeTotalCount = weeklyArtistRows.size();
                List<ChartArtistOverviewRowDTO> sortedRows = new ArrayList<>(weeklyArtistRows);
                sortedRows.sort(buildArtistOverviewComparator(sortSpecs));
                pagedArtistRows = paginateRows(sortedRows, 0, pageSize);
            }
            default -> {
                activeTotalCount = weeklySongRows.size();
                List<ChartSongOverviewRowDTO> sortedRows = new ArrayList<>(weeklySongRows);
                sortedRows.sort(buildSongOverviewComparator(sortSpecs));
                pagedSongRows = paginateRows(sortedRows, 0, pageSize);
            }
        }

        model.addAttribute("currentSection", "weekly-overview-charts");
        model.addAttribute("periodType", "weekly");
        model.addAttribute("overviewTab", normalizedOverviewTab);
        model.addAttribute("songRows", pagedSongRows);
        model.addAttribute("albumRows", pagedAlbumRows);
        model.addAttribute("artistRows", pagedArtistRows);
        model.addAttribute("pageTitle", "Weekly Chart Overview");
        model.addAttribute("mainChartUrl", "/charts/weekly");
        model.addAttribute("mainChartLabel", "Weekly Charts");
        model.addAttribute("weeklyInfiniteScrollEnabled", true);
        model.addAttribute("serverInfiniteScrollEnabled", true);
        model.addAttribute("overviewDataPath", "/charts/weekly/overview/data");
        model.addAttribute("pageSize", pageSize);
        model.addAttribute("activeTotalCount", activeTotalCount);
        model.addAttribute("selectedSort", normalizedSort);
        model.addAttribute("selectedDir", normalizedDir);
        model.addAttribute("searchQuery", normalizedQuery);
        model.addAttribute("pageSizeConfig", appConfigService.getPageSizeConfig());

        return "charts/overview";
    }

    @GetMapping("/weekly/overview/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> weeklyOverviewData(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            @RequestParam(required = false) String sort2,
            @RequestParam(required = false) String dir2,
            @RequestParam(required = false) String sort3,
            @RequestParam(required = false) String dir3,
            @RequestParam(name = "filter", required = false) List<String> columnFilters,
            @RequestParam(required = false) Integer topSong,
            @RequestParam(required = false) Integer topAlbum,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(required = false) Integer size) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        List<OverviewSortSpec> sortSpecs = normalizeWeeklyOverviewSortSpecs(normalizedOverviewTab, sort, dir, sort2, dir2, sort3, dir3);
        String normalizedQuery = normalizeOverviewQuery(q);
        List<String> normalizedFilters = normalizeOverviewColumnFilters(columnFilters);
        int safeSize = appConfigService.normalizePageSize(size, appConfigService.getWeeklyOverviewPageSize(), 1, 250);

        Map<String, Object> result = new HashMap<>();

        switch (normalizedOverviewTab) {
            case "album" -> {
                List<ChartAlbumOverviewRowDTO> weeklyAlbumRows = chartService.getChartOverviewAlbumRows("weekly");
                List<ChartAlbumOverviewRowDTO> filteredRows = filterWeeklyAlbumOverviewRows(weeklyAlbumRows, normalizedQuery, normalizedFilters);
                filteredRows.sort(buildAlbumOverviewComparator(sortSpecs));
                int totalCount = filteredRows.size();
                List<ChartAlbumOverviewRowDTO> entries = paginateRows(filteredRows, page, safeSize);
                result.put("entries", entries);
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
            case "artist" -> {
                List<ChartArtistOverviewRowDTO> weeklyArtistRows = chartService.getChartOverviewArtistRows("weekly");
                List<ChartArtistOverviewRowDTO> filteredRows = filterWeeklyArtistOverviewRows(weeklyArtistRows, normalizedQuery, normalizedFilters, topSong, topAlbum);
                filteredRows.sort(buildArtistOverviewComparator(sortSpecs));
                int totalCount = filteredRows.size();
                List<ChartArtistOverviewRowDTO> entries = paginateRows(filteredRows, page, safeSize);
                result.put("entries", entries);
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
            default -> {
                List<ChartSongOverviewRowDTO> weeklySongRows = chartService.getChartOverviewSongRows("weekly");
                List<ChartSongOverviewRowDTO> filteredRows = filterWeeklySongOverviewRows(weeklySongRows, normalizedQuery, normalizedFilters);
                filteredRows.sort(buildSongOverviewComparator(sortSpecs));
                int totalCount = filteredRows.size();
                List<ChartSongOverviewRowDTO> entries = paginateRows(filteredRows, page, safeSize);
                result.put("entries", entries);
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
        }

        result.put("nextPage", page + 1);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{periodType}/overview/song-export")
    @ResponseBody
    public ResponseEntity<List<ChartSongOverviewRowDTO>> songOverviewExport(
            @PathVariable String periodType,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            @RequestParam(required = false) String sort2,
            @RequestParam(required = false) String dir2,
            @RequestParam(required = false) String sort3,
            @RequestParam(required = false) String dir3) {
        if (!List.of("weekly", "seasonal", "yearly").contains(periodType)) {
            return ResponseEntity.badRequest().build();
        }

        List<ChartSongOverviewRowDTO> rows = chartService.getChartOverviewSongRows(periodType);
        if (!"weekly".equals(periodType)) {
            return ResponseEntity.ok(rows);
        }

        List<OverviewSortSpec> sortSpecs = normalizeWeeklyOverviewSortSpecs("song", sort, dir, sort2, dir2, sort3, dir3);
        List<ChartSongOverviewRowDTO> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(buildSongOverviewComparator(sortSpecs));
        return ResponseEntity.ok(sortedRows);
    }

    @GetMapping("/seasonal/overview")
    public String seasonalOverview(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            @RequestParam(required = false) String sort2,
            @RequestParam(required = false) String dir2,
            @RequestParam(required = false) String sort3,
            @RequestParam(required = false) String dir3,
            Model model) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        return renderSeasonalOverview(normalizedOverviewTab, q, sort, dir, sort2, dir2, sort3, dir3, model);
    }

    @GetMapping("/yearly/overview")
    public String yearlyOverview(
            @RequestParam(defaultValue = "song") String overviewTab,
            Model model) {
        return renderChartOverview("yearly", overviewTab, model);
    }

    @GetMapping("/seasonal/overview/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> seasonalOverviewData(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            @RequestParam(required = false) String sort2,
            @RequestParam(required = false) String dir2,
            @RequestParam(required = false) String sort3,
            @RequestParam(required = false) String dir3,
            @RequestParam(name = "filter", required = false) List<String> columnFilters,
            @RequestParam(required = false) Integer topSong,
            @RequestParam(required = false) Integer topAlbum,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(required = false) Integer size) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);

        List<OverviewSortSpec> sortSpecs = normalizeWeeklyOverviewSortSpecs(normalizedOverviewTab, sort, dir, sort2, dir2, sort3, dir3);
        String normalizedQuery = normalizeOverviewQuery(q);
        List<String> normalizedFilters = normalizeOverviewColumnFilters(columnFilters);
        int safeSize = appConfigService.normalizePageSize(size, appConfigService.getSeasonalOverviewPageSize(), 1, 250);

        Map<String, Object> result = new HashMap<>();

        switch (normalizedOverviewTab) {
            case "album" -> {
                List<ChartAlbumOverviewRowDTO> seasonalAlbumRows = chartService.getChartOverviewAlbumRows("seasonal");
                List<ChartAlbumOverviewRowDTO> filteredRows = filterWeeklyAlbumOverviewRows(seasonalAlbumRows, normalizedQuery, normalizedFilters);
                filteredRows.sort(buildAlbumOverviewComparator(sortSpecs));
                int totalCount = filteredRows.size();
                result.put("entries", paginateRows(filteredRows, page, safeSize));
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
            case "artist" -> {
                List<ChartArtistOverviewRowDTO> seasonalArtistRows = chartService.getChartOverviewArtistRows("seasonal");
                List<ChartArtistOverviewRowDTO> filteredRows = filterWeeklyArtistOverviewRows(seasonalArtistRows, normalizedQuery, normalizedFilters, topSong, topAlbum);
                filteredRows.sort(buildArtistOverviewComparator(sortSpecs));
                int totalCount = filteredRows.size();
                result.put("entries", paginateRows(filteredRows, page, safeSize));
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
            default -> {
                List<ChartSongOverviewRowDTO> seasonalSongRows = chartService.getChartOverviewSongRows("seasonal");
                List<ChartSongOverviewRowDTO> filteredRows = filterWeeklySongOverviewRows(seasonalSongRows, normalizedQuery, normalizedFilters);
                filteredRows.sort(buildSongOverviewComparator(sortSpecs));
                int totalCount = filteredRows.size();
                result.put("entries", paginateRows(filteredRows, page, safeSize));
                result.put("totalCount", totalCount);
                result.put("hasMore", (long) (page + 1) * safeSize < totalCount);
            }
        }

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
        List<ChartAlbumOverviewRowDTO> albumRows = chartService.getChartOverviewAlbumRows(periodType);
        List<ChartArtistOverviewRowDTO> artistRows = chartService.getChartOverviewArtistRows(songRows, albumRows);

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
        model.addAttribute("pageTitle", switch (periodType) {
            case "seasonal" -> "Seasonal Chart Overview";
            case "yearly" -> "Yearly Chart Overview";
            default -> "Weekly Chart Overview";
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
        model.addAttribute("serverInfiniteScrollEnabled", false);
        model.addAttribute("overviewDataPath", "/charts/weekly/overview/data");
        model.addAttribute("pageSizeConfig", appConfigService.getPageSizeConfig());
        return "charts/overview";
    }

    private String renderSeasonalOverview(String overviewTab,
                                          String q,
                                          String sort,
                                          String dir,
                                          String sort2,
                                          String dir2,
                                          String sort3,
                                          String dir3,
                                          Model model) {
        List<ChartSongOverviewRowDTO> seasonalSongRows = chartService.getChartOverviewSongRows("seasonal");
        List<ChartAlbumOverviewRowDTO> seasonalAlbumRows = chartService.getChartOverviewAlbumRows("seasonal");
        List<ChartArtistOverviewRowDTO> seasonalArtistRows = chartService.getChartOverviewArtistRows(seasonalSongRows, seasonalAlbumRows);
        List<OverviewSortSpec> sortSpecs = normalizeWeeklyOverviewSortSpecs(overviewTab, sort, dir, sort2, dir2, sort3, dir3);
        String normalizedSort = sortSpecs.get(0).sort();
        String normalizedDir = sortSpecs.get(0).dir();
        String normalizedQuery = normalizeOverviewQuery(q);
        int pageSize = appConfigService.getSeasonalOverviewPageSize();

        List<ChartSongOverviewRowDTO> pagedSongRows = List.of();
        List<ChartAlbumOverviewRowDTO> pagedAlbumRows = List.of();
        List<ChartArtistOverviewRowDTO> pagedArtistRows = List.of();
        int activeTotalCount;

        switch (overviewTab) {
            case "album" -> {
                activeTotalCount = seasonalAlbumRows.size();
                List<ChartAlbumOverviewRowDTO> sortedRows = new ArrayList<>(seasonalAlbumRows);
                sortedRows.sort(buildAlbumOverviewComparator(sortSpecs));
                pagedAlbumRows = paginateRows(sortedRows, 0, pageSize);
            }
            case "artist" -> {
                activeTotalCount = seasonalArtistRows.size();
                List<ChartArtistOverviewRowDTO> sortedRows = new ArrayList<>(seasonalArtistRows);
                sortedRows.sort(buildArtistOverviewComparator(sortSpecs));
                pagedArtistRows = paginateRows(sortedRows, 0, pageSize);
            }
            default -> {
                activeTotalCount = seasonalSongRows.size();
                List<ChartSongOverviewRowDTO> sortedRows = new ArrayList<>(seasonalSongRows);
                sortedRows.sort(buildSongOverviewComparator(sortSpecs));
                pagedSongRows = paginateRows(sortedRows, 0, pageSize);
            }
        }

        model.addAttribute("currentSection", "seasonal-overview-charts");
        model.addAttribute("periodType", "seasonal");
        model.addAttribute("overviewTab", overviewTab);
        model.addAttribute("songRows", pagedSongRows);
        model.addAttribute("albumRows", pagedAlbumRows);
        model.addAttribute("artistRows", pagedArtistRows);
        model.addAttribute("pageTitle", "Seasonal Chart Overview");
        model.addAttribute("mainChartUrl", "/charts/seasonal");
        model.addAttribute("mainChartLabel", "Seasonal Charts");
        model.addAttribute("weeklyInfiniteScrollEnabled", false);
        model.addAttribute("serverInfiniteScrollEnabled", true);
        model.addAttribute("overviewDataPath", "/charts/seasonal/overview/data");
        model.addAttribute("pageSize", pageSize);
        model.addAttribute("activeTotalCount", activeTotalCount);
        model.addAttribute("selectedSort", normalizedSort);
        model.addAttribute("selectedDir", normalizedDir);
        model.addAttribute("searchQuery", normalizedQuery);
        model.addAttribute("pageSizeConfig", appConfigService.getPageSizeConfig());
        return "charts/overview";
    }

    private String normalizeOverviewQuery(String query) {
        if (query == null) {
            return "";
        }
        return query.trim();
    }

    private List<String> normalizeOverviewColumnFilters(List<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return List.of();
        }
        return filters.stream()
                .map(filter -> filter == null ? "" : filter.trim())
                .toList();
    }

    private String normalizeWeeklyOverviewSort(String overviewTab, String sort) {
        List<String> allowedSorts = switch (overviewTab) {
            case "album" -> List.of("artist", "album", "weeks", "peak", "atPeak", "debutPeriod", "debutPosition", "peakPeriod", "lastPeriod");
            case "artist" -> List.of("artist", "songs", "weeks", "peak", "numberOnes", "atNumberOne", "albums", "albumWeeks", "albumPeak", "albumNumberOnes", "albumAtNumberOne");
            default -> List.of("artist", "song", "weeks", "peak", "atPeak", "debutPeriod", "debutPosition", "lastPeriod", "peakPeriod");
        };

        if (sort == null || !allowedSorts.contains(sort)) {
            return "artist".equals(overviewTab) ? "songs" : "debutPeriod";
        }
        return sort;
    }

    private String normalizeWeeklyOverviewDir(String dir) {
        return "asc".equalsIgnoreCase(dir) ? "asc" : "desc";
    }

    private List<OverviewSortSpec> normalizeWeeklyOverviewSortSpecs(String overviewTab, String sort, String dir, String sort2, String dir2, String sort3, String dir3) {
        List<OverviewSortSpec> sortSpecs = new ArrayList<>();
        sortSpecs.add(new OverviewSortSpec(normalizeWeeklyOverviewSort(overviewTab, sort), normalizeWeeklyOverviewDir(dir)));
        addWeeklyOverviewSortSpec(sortSpecs, overviewTab, sort2, dir2);
        addWeeklyOverviewSortSpec(sortSpecs, overviewTab, sort3, dir3);
        return sortSpecs;
    }

    private void addWeeklyOverviewSortSpec(List<OverviewSortSpec> sortSpecs, String overviewTab, String sort, String dir) {
        if (sort == null || sort.isBlank()) {
            return;
        }
        String normalizedSort = normalizeWeeklyOverviewSort(overviewTab, sort);
        boolean exists = sortSpecs.stream().anyMatch(spec -> spec.sort().equals(normalizedSort));
        if (!exists) {
            sortSpecs.add(new OverviewSortSpec(normalizedSort, normalizeWeeklyOverviewDir(dir)));
        }
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

    private List<ChartSongOverviewRowDTO> filterWeeklySongOverviewRows(List<ChartSongOverviewRowDTO> rows, String query, List<String> columnFilters) {
        List<ChartSongOverviewRowDTO> filteredRows = filterSongOverviewRows(rows, query);
        if (columnFilters == null || columnFilters.stream().allMatch(String::isBlank)) {
            return filteredRows;
        }

        filteredRows.removeIf(row -> !matchesWeeklySongOverviewFilters(row, columnFilters));
        return filteredRows;
    }

    private List<ChartAlbumOverviewRowDTO> filterWeeklyAlbumOverviewRows(List<ChartAlbumOverviewRowDTO> rows, String query, List<String> columnFilters) {
        List<ChartAlbumOverviewRowDTO> filteredRows = filterAlbumOverviewRows(rows, query);
        if (columnFilters == null || columnFilters.stream().allMatch(String::isBlank)) {
            return filteredRows;
        }

        filteredRows.removeIf(row -> !matchesWeeklyAlbumOverviewFilters(row, columnFilters));
        return filteredRows;
    }

    private List<ChartArtistOverviewRowDTO> filterWeeklyArtistOverviewRows(List<ChartArtistOverviewRowDTO> rows, String query, List<String> columnFilters, Integer topSongThreshold, Integer topAlbumThreshold) {
        List<ChartArtistOverviewRowDTO> filteredRows = filterArtistOverviewRows(rows, query);
        if (columnFilters == null || columnFilters.stream().allMatch(String::isBlank)) {
            return filteredRows;
        }

        filteredRows.removeIf(row -> !matchesWeeklyArtistOverviewFilters(row, columnFilters, topSongThreshold, topAlbumThreshold));
        return filteredRows;
    }

    private boolean matchesWeeklySongOverviewFilters(ChartSongOverviewRowDTO row, List<String> filters) {
        return matchesTextOverviewFilter(getOverviewFilterValue(filters, 2), row.getArtistName(), row.getArtistName())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 3), row.getSongTitle(), row.getSongTitle())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 4), row.getTotalChartSpan())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 5), row.getPeakPosition())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 6), row.getSpanAtPeak())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 7), row.getFirstAppearanceLabel(), row.getFirstAppearanceSortValue())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 8), row.getDebutPosition())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 9), row.getPeakAppearanceLabel(), row.getPeakAppearanceSortValue())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 10), row.getLastAppearanceLabel(), row.getLastAppearanceSortValue());
    }

    private boolean matchesWeeklyAlbumOverviewFilters(ChartAlbumOverviewRowDTO row, List<String> filters) {
        return matchesTextOverviewFilter(getOverviewFilterValue(filters, 1), row.getArtistName(), row.getArtistName())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 2), row.getAlbumName(), row.getAlbumName())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 3), row.getTotalChartSpan())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 4), row.getHighestPeak())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 5), row.getSpanAtPeak())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 6), row.getFirstDebutDate(), row.getFirstDebutSortValue())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 7), row.getDebutPosition())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 8), row.getPeakAppearanceDate(), row.getPeakAppearanceSortValue())
                && matchesTextOverviewFilter(getOverviewFilterValue(filters, 9), row.getLastAppearanceDate(), row.getLastAppearanceSortValue());
    }

    private boolean matchesWeeklyArtistOverviewFilters(ChartArtistOverviewRowDTO row, List<String> filters, Integer topSongThreshold, Integer topAlbumThreshold) {
        return matchesTextOverviewFilter(getOverviewFilterValue(filters, 1), row.getArtistName(), row.getArtistName())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 2), resolveArtistThresholdValue(row.getTopSongCounts(), topSongThreshold, row.getChartedSongsCount()))
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 3), resolveArtistThresholdValue(row.getTopSongWeeks(), topSongThreshold, row.getTotalChartSpan()))
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 4), row.getNumberOneSongsCount())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 5), row.getTotalSpanAtNumberOne())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 6), resolveArtistThresholdValue(row.getTopAlbumCounts(), topAlbumThreshold, row.getChartedAlbumsCount()))
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 7), resolveArtistThresholdValue(row.getTopAlbumWeeks(), topAlbumThreshold, row.getAlbumTotalChartSpan()))
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 8), row.getNumberOneAlbumsCount())
                && matchesNumericOverviewFilter(getOverviewFilterValue(filters, 9), row.getAlbumTotalSpanAtNumberOne());
    }

    private String getOverviewFilterValue(List<String> filters, int index) {
        if (filters == null || index < 0 || index >= filters.size()) {
            return "";
        }
        return filters.get(index);
    }

    private int resolveArtistThresholdValue(int[] values, Integer threshold, int fallbackValue) {
        if (values == null || threshold == null || threshold < 1 || threshold >= values.length) {
            return fallbackValue;
        }
        return values[threshold];
    }

    private boolean matchesTextOverviewFilter(String query, String displayValue, String rawValue) {
        if (query == null || query.isBlank()) {
            return true;
        }
        String normalizedQuery = query.toLowerCase(Locale.ROOT);
        return (displayValue != null && displayValue.toLowerCase(Locale.ROOT).contains(normalizedQuery))
                || (rawValue != null && rawValue.toLowerCase(Locale.ROOT).contains(normalizedQuery));
    }

    private boolean matchesNumericOverviewFilter(String query, Number value) {
        if (query == null || query.isBlank()) {
            return true;
        }
        if (value == null) {
            return false;
        }

        String normalizedQuery = query.trim();
        Matcher rangeMatcher = OVERVIEW_RANGE_PATTERN.matcher(normalizedQuery);
        if (rangeMatcher.matches()) {
            double min = Double.parseDouble(rangeMatcher.group(1));
            double max = Double.parseDouble(rangeMatcher.group(2));
            double numericValue = value.doubleValue();
            return numericValue >= min && numericValue <= max;
        }

        Matcher compareMatcher = OVERVIEW_COMPARE_PATTERN.matcher(normalizedQuery);
        if (!compareMatcher.matches()) {
            return false;
        }

        String operator = compareMatcher.group(1) == null ? "=" : compareMatcher.group(1);
        double target = Double.parseDouble(compareMatcher.group(2));
        double numericValue = value.doubleValue();
        return switch (operator) {
            case "<" -> numericValue < target;
            case "<=" -> numericValue <= target;
            case ">" -> numericValue > target;
            case ">=" -> numericValue >= target;
            default -> numericValue == target;
        };
    }

    private Comparator<ChartSongOverviewRowDTO> buildSongOverviewComparator(String sort, String dir) {
        return buildSongOverviewComparator(List.of(new OverviewSortSpec(sort, dir)));
    }

    private Comparator<ChartSongOverviewRowDTO> buildSongOverviewComparator(List<OverviewSortSpec> sortSpecs) {
        Comparator<ChartSongOverviewRowDTO> comparator = buildSongOverviewSortComparator(sortSpecs.get(0));
        for (int index = 1; index < sortSpecs.size(); index++) {
            comparator = comparator.thenComparing(buildSongOverviewSortComparator(sortSpecs.get(index)));
        }
        return comparator.thenComparing(ChartSongOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(ChartSongOverviewRowDTO::getSongTitle, String.CASE_INSENSITIVE_ORDER);
    }

    private Comparator<ChartSongOverviewRowDTO> buildSongOverviewSortComparator(OverviewSortSpec sortSpec) {
        Comparator<ChartSongOverviewRowDTO> comparator = switch (sortSpec.sort()) {
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

        return "asc".equals(sortSpec.dir()) ? comparator : comparator.reversed();
    }

    private Comparator<ChartAlbumOverviewRowDTO> buildAlbumOverviewComparator(String sort, String dir) {
        return buildAlbumOverviewComparator(List.of(new OverviewSortSpec(sort, dir)));
    }

    private Comparator<ChartAlbumOverviewRowDTO> buildAlbumOverviewComparator(List<OverviewSortSpec> sortSpecs) {
        Comparator<ChartAlbumOverviewRowDTO> comparator = buildAlbumOverviewSortComparator(sortSpecs.get(0));
        for (int index = 1; index < sortSpecs.size(); index++) {
            comparator = comparator.thenComparing(buildAlbumOverviewSortComparator(sortSpecs.get(index)));
        }
        return comparator.thenComparing(ChartAlbumOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(ChartAlbumOverviewRowDTO::getAlbumName, String.CASE_INSENSITIVE_ORDER);
    }

    private Comparator<ChartAlbumOverviewRowDTO> buildAlbumOverviewSortComparator(OverviewSortSpec sortSpec) {
        Comparator<ChartAlbumOverviewRowDTO> comparator = switch (sortSpec.sort()) {
            case "artist" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
            case "album" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getAlbumName, String.CASE_INSENSITIVE_ORDER);
            case "peak" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "atPeak" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getSpanAtPeak);
            case "debutPeriod" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getFirstDebutSortValue, Comparator.nullsLast(String::compareTo));
            case "debutPosition" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getDebutPosition, Comparator.nullsLast(Integer::compareTo));
            case "peakPeriod" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getPeakAppearanceSortValue, Comparator.nullsLast(String::compareTo));
            case "lastPeriod" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getLastAppearanceSortValue, Comparator.nullsLast(String::compareTo));
            default -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan);
        };

        return "asc".equals(sortSpec.dir()) ? comparator : comparator.reversed();
    }

    private Comparator<ChartArtistOverviewRowDTO> buildArtistOverviewComparator(String sort, String dir) {
        return buildArtistOverviewComparator(List.of(new OverviewSortSpec(sort, dir)));
    }

    private Comparator<ChartArtistOverviewRowDTO> buildArtistOverviewComparator(List<OverviewSortSpec> sortSpecs) {
        Comparator<ChartArtistOverviewRowDTO> comparator = buildArtistOverviewSortComparator(sortSpecs.get(0));
        for (int index = 1; index < sortSpecs.size(); index++) {
            comparator = comparator.thenComparing(buildArtistOverviewSortComparator(sortSpecs.get(index)));
        }
        return comparator.thenComparing(ChartArtistOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
    }

    private Comparator<ChartArtistOverviewRowDTO> buildArtistOverviewSortComparator(OverviewSortSpec sortSpec) {
        Comparator<ChartArtistOverviewRowDTO> comparator = switch (sortSpec.sort()) {
            case "artist" -> Comparator.comparing(ChartArtistOverviewRowDTO::getArtistName, String.CASE_INSENSITIVE_ORDER);
            case "songs" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getChartedSongsCount);
            case "albums" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getChartedAlbumsCount);
            case "peak" -> Comparator.comparing(ChartArtistOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "albumPeak" -> Comparator.comparing(ChartArtistOverviewRowDTO::getAlbumHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "numberOnes" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getNumberOneSongsCount);
            case "albumNumberOnes" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getNumberOneAlbumsCount);
            case "atNumberOne" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalSpanAtNumberOne);
            case "albumAtNumberOne" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getAlbumTotalSpanAtNumberOne);
            case "albumWeeks" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getAlbumTotalChartSpan);
            default -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan);
        };

        return "asc".equals(sortSpec.dir()) ? comparator : comparator.reversed();
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
