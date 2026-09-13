package library.controller;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.BillboardHot100OverviewRowDTO;
import library.service.AppConfigService;
import library.service.BillboardHot100Service;
import library.service.OverviewFilterService;
import library.service.OverviewCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/misc/billboard-hot-100")
public class BillboardHot100Controller {

    private final AppConfigService appConfigService;
    private final BillboardHot100Service billboardHot100Service;
    private final OverviewFilterService overviewFilterService;
    private final OverviewCacheService overviewCacheService;

    public BillboardHot100Controller(AppConfigService appConfigService, BillboardHot100Service billboardHot100Service) {
        this(appConfigService, billboardHot100Service, new OverviewFilterService(), new OverviewCacheService());
    }

    public BillboardHot100Controller(AppConfigService appConfigService, BillboardHot100Service billboardHot100Service,
                                     OverviewFilterService overviewFilterService) {
        this(appConfigService, billboardHot100Service, overviewFilterService, new OverviewCacheService());
    }

    @Autowired
    public BillboardHot100Controller(AppConfigService appConfigService, BillboardHot100Service billboardHot100Service,
                                     OverviewFilterService overviewFilterService, OverviewCacheService overviewCacheService) {
        this.appConfigService = appConfigService;
        this.billboardHot100Service = billboardHot100Service;
        this.overviewFilterService = overviewFilterService;
        this.overviewCacheService = overviewCacheService;
    }

    @GetMapping
    public String overview(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(required = false) Integer size,
            @RequestParam(defaultValue = "firstWeek") String sort,
            @RequestParam(defaultValue = "desc") String dir,
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam MultiValueMap<String, String> filterParams,
            Model model) {
        String safeOverviewTab = normalizeOverviewTab(overviewTab);
        int safeSize = normalizeSize(size);
        String safeSort = normalizeSort(safeOverviewTab, sort);
        String safeDir = normalizeDir(dir);
        int resultTotal;
        List<?> pageEntries;
        if ("album".equals(safeOverviewTab)) {
            List<ChartAlbumOverviewRowDTO> rows = billboardHot100Service.sortAlbumOverviewRows(
                    overviewFilterService.filter("billboard", "album", billboardAlbumOverviewRows(), filterParams), safeSort, safeDir);
            resultTotal = rows.size();
            pageEntries = rows;
        } else if ("artist".equals(safeOverviewTab)) {
            List<ChartArtistOverviewRowDTO> rows = billboardHot100Service.sortArtistOverviewRows(
                    overviewFilterService.filter("billboard", "artist", billboardArtistOverviewRows(includeFeatured), filterParams), safeSort, safeDir);
            resultTotal = rows.size();
            pageEntries = rows;
        } else {
            List<BillboardHot100OverviewRowDTO> rows = billboardHot100Service.sortOverviewRows(
                    overviewFilterService.filter("billboard", "song", billboardSongOverviewRows(), filterParams), safeSort, safeDir);
            resultTotal = rows.size();
            pageEntries = rows;
        }
        int totalPages = Math.max(1, (int) Math.ceil(resultTotal / (double) safeSize));
        int safePage = Math.max(1, Math.min(page, totalPages));

        if ("album".equals(safeOverviewTab)) {
            model.addAttribute("albumEntries", paginate(pageEntries, safePage, safeSize));
        } else if ("artist".equals(safeOverviewTab)) {
            model.addAttribute("artistEntries", paginate(pageEntries, safePage, safeSize));
        } else {
            model.addAttribute("entries", paginate(pageEntries, safePage, safeSize));
        }
        model.addAttribute("page", safePage);
        model.addAttribute("size", safeSize);
        model.addAttribute("sort", safeSort);
        model.addAttribute("dir", safeDir);
        model.addAttribute("q", q);
        model.addAttribute("overviewTab", safeOverviewTab);
        model.addAttribute("selectedIncludeFeatured", includeFeatured);
        model.addAttribute("overviewFilterFields", overviewFilterService.fieldsFor("billboard", safeOverviewTab));
        model.addAttribute("resultTotal", resultTotal);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("hasPrev", safePage > 1);
        model.addAttribute("hasNext", safePage < totalPages);
        model.addAttribute("currentSection", "billboard-hot100");
        return "misc/billboard-hot100";
    }

    @GetMapping("/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> overviewData(
            @RequestParam(defaultValue = "2") int page,
            @RequestParam(required = false) Integer size,
            @RequestParam(defaultValue = "firstWeek") String sort,
            @RequestParam(defaultValue = "desc") String dir,
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam MultiValueMap<String, String> filterParams) {
        String safeOverviewTab = normalizeOverviewTab(overviewTab);
        int safeSize = normalizeSize(size);
        int safePage = Math.max(1, page);
        String safeSort = normalizeSort(safeOverviewTab, sort);
        String safeDir = normalizeDir(dir);

        Map<String, Object> result = new LinkedHashMap<>();
        int resultTotal;

        if ("album".equals(safeOverviewTab)) {
            List<ChartAlbumOverviewRowDTO> rows = billboardHot100Service.sortAlbumOverviewRows(
                    overviewFilterService.filter("billboard", "album", billboardAlbumOverviewRows(), filterParams), safeSort, safeDir);
            resultTotal = rows.size();
            result.put("entries", paginate(rows, safePage, safeSize));
        } else if ("artist".equals(safeOverviewTab)) {
            List<ChartArtistOverviewRowDTO> rows = billboardHot100Service.sortArtistOverviewRows(
                    overviewFilterService.filter("billboard", "artist", billboardArtistOverviewRows(includeFeatured), filterParams), safeSort, safeDir);
            resultTotal = rows.size();
            result.put("entries", paginate(rows, safePage, safeSize));
        } else {
            List<BillboardHot100OverviewRowDTO> rows = billboardHot100Service.sortOverviewRows(
                    overviewFilterService.filter("billboard", "song", billboardSongOverviewRows(), filterParams), safeSort, safeDir);
            resultTotal = rows.size();
            result.put("entries", paginate(rows, safePage, safeSize));
        }

        result.put("totalCount", resultTotal);
        result.put("hasMore", (long) safePage * safeSize < resultTotal);
        result.put("nextPage", safePage + 1);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/import")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> importCharts() {
        try {
            return ResponseEntity.ok(billboardHot100Service.importIncrementalCharts());
        } catch (Exception e) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("ok", false);
            result.put("error", e.getMessage() != null ? e.getMessage() : "Billboard import failed");
            return ResponseEntity.internalServerError().body(result);
        }
    }

    @PostMapping("/auto-link")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> autoLink() {
        return ResponseEntity.ok(billboardHot100Service.autoLinkExactMatches());
    }

    @PostMapping("/normalize-case")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> normalizeCase() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("updatedRows", billboardHot100Service.normalizeCaseDifferences());
        return ResponseEntity.ok(result);
    }

    @PostMapping("/match")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> matchRawGroup(
            @RequestParam String rawArtist,
            @RequestParam String rawSong,
            @RequestParam Integer songId) {
        return ResponseEntity.ok(billboardHot100Service.matchRawGroup(rawArtist, rawSong, songId));
    }

    @GetMapping("/recaps")
    public String recaps(@RequestParam(required = false) String date, Model model) {
        List<String> availableDates = billboardHot100Service.getAvailableChartDates();

        String effectiveDate = date;
        if ((effectiveDate == null || effectiveDate.isBlank()) && !availableDates.isEmpty()) {
            effectiveDate = availableDates.get(availableDates.size() - 1);
        } else if (effectiveDate != null && !effectiveDate.isBlank()) {
            effectiveDate = billboardHot100Service.findClosestChartDate(effectiveDate);
        }

        if (effectiveDate != null) {
            List<Map<String, Object>> countdown = billboardHot100Service.getCountdownForDate(effectiveDate);
            model.addAttribute("countdown", countdown);
            model.addAttribute("fallOffs", billboardHot100Service.getFallOffsForDate(effectiveDate, countdown));
        }

        model.addAttribute("availableDates", availableDates);
        model.addAttribute("selectedDate", effectiveDate);
        model.addAttribute("currentSection", "billboard-hot100-recaps");
        return "misc/billboard-hot100-recaps";
    }

    @GetMapping("/recaps/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> recapsData(@RequestParam String date) {
        String effectiveDate = billboardHot100Service.findClosestChartDate(date);
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> countdown = effectiveDate != null ? billboardHot100Service.getCountdownForDate(effectiveDate) : List.of();
        result.put("date", effectiveDate);
        result.put("prevDate", effectiveDate != null ? billboardHot100Service.getPrevChartDate(effectiveDate) : null);
        result.put("nextDate", effectiveDate != null ? billboardHot100Service.getNextChartDate(effectiveDate) : null);
        result.put("entries", countdown);
        result.put("fallOffs", effectiveDate != null ? billboardHot100Service.getFallOffsForDate(effectiveDate, countdown) : List.of());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/chart-run")
    @ResponseBody
    public List<Map<String, Object>> getChartRun(
            @RequestParam(required = false) Integer songId,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String song) {
        if (songId != null) {
            return billboardHot100Service.getChartRunBySongId(songId);
        }
        if (artist != null && song != null) {
            return billboardHot100Service.getChartRunByNames(artist, song);
        }
        return List.of();
    }

    private int normalizeSize(Integer size) {
        return appConfigService.normalizePageSize(size, appConfigService.getBillboardOverviewPageSize(), 100, 500);
    }

    private List<BillboardHot100OverviewRowDTO> billboardSongOverviewRows() {
        return overviewCacheService.get("billboard:song", billboardHot100Service::getAllOverviewRows);
    }

    private List<ChartAlbumOverviewRowDTO> billboardAlbumOverviewRows() {
        return overviewCacheService.get("billboard:album",
                () -> billboardHot100Service.getAlbumOverviewRows(billboardSongOverviewRows()));
    }

    private List<ChartArtistOverviewRowDTO> billboardArtistOverviewRows(boolean includeFeatured) {
        return overviewCacheService.get("billboard:artist:" + includeFeatured,
                () -> billboardHot100Service.getArtistOverviewRows(billboardSongOverviewRows(), includeFeatured));
    }

    private List<?> paginate(List<?> rows, int page, int size) {
        int fromIndex = Math.min(Math.max(0, (page - 1) * size), rows.size());
        int toIndex = Math.min(rows.size(), fromIndex + size);
        return rows.subList(fromIndex, toIndex);
    }

    private String normalizeSort(String overviewTab, String sort) {
        if ("album".equals(overviewTab)) {
            return switch (sort) {
                case "artist", "album", "songs", "weeks", "peak", "numberOnes", "weeksAtNumberOne", "firstWeek", "lastWeek" -> sort;
                default -> "weeks";
            };
        }
        if ("artist".equals(overviewTab)) {
            return switch (sort) {
                case "artist", "songs", "weeks", "peak", "numberOnes", "weeksAtNumberOne", "firstWeek", "lastWeek" -> sort;
                default -> "weeks";
            };
        }
        return switch (sort) {
            case "weeks", "peak", "weeksAtPeak", "peakWeek", "firstWeek", "lastWeek", "song", "artist" -> sort;
            default -> "firstWeek";
        };
    }

    private String normalizeDir(String dir) {
        return "asc".equalsIgnoreCase(dir) ? "asc" : "desc";
    }

    private String normalizeOverviewTab(String overviewTab) {
        if ("album".equalsIgnoreCase(overviewTab)) {
            return "album";
        }
        if ("artist".equalsIgnoreCase(overviewTab)) {
            return "artist";
        }
        return "song";
    }

}
