package library.controller;

import library.service.AppConfigService;
import library.service.ChartOverviewService;
import library.service.ChartOverviewService.Chart;
import library.service.ChartOverviewService.SortSpec;
import library.service.OverviewFilterService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The shared Chart Overview page for Weekly, Hot 100, TRL and Vato's Cuntdown.
 * All view state (chart, tab, view, sort levels, filters) lives in the URL.
 */
@Controller
@RequestMapping("/charts/overview")
public class ChartOverviewController {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final ChartOverviewService chartOverviewService;
    private final OverviewFilterService overviewFilterService;
    private final AppConfigService appConfigService;

    public ChartOverviewController(ChartOverviewService chartOverviewService, OverviewFilterService overviewFilterService,
                                   AppConfigService appConfigService) {
        this.chartOverviewService = chartOverviewService;
        this.overviewFilterService = overviewFilterService;
        this.appConfigService = appConfigService;
    }

    @GetMapping
    public String overview(@RequestParam(required = false) String chart,
                           @RequestParam(defaultValue = "song") String tab,
                           @RequestParam(required = false) String view,
                           @RequestParam(name = "sort", required = false) List<String> sort,
                           @RequestParam(defaultValue = "false") boolean includeFeatured,
                           @RequestParam(required = false) Integer topSong,
                           @RequestParam(required = false) Integer topAlbum,
                           @RequestParam MultiValueMap<String, String> params,
                           Model model) {
        Chart selected = Chart.from(chart);
        String safeTab = ChartOverviewService.normalizeTab(tab);
        List<SortSpec> sorts = ChartOverviewService.parseSorts(safeTab, sort);
        int size = pageSize(selected, null);
        ChartOverviewService.Page page = chartOverviewService.page(selected, safeTab, sorts, includeFeatured, topSong, topAlbum, params, 0, size);

        model.addAttribute("currentSection", currentSection(selected));
        model.addAttribute("chart", selected);
        model.addAttribute("charts", Chart.values());
        model.addAttribute("tab", safeTab);
        model.addAttribute("view", normalizeView(safeTab, view));
        model.addAttribute("includeFeatured", includeFeatured);
        model.addAttribute("overviewFilterFields", overviewFilterService.fieldsFor(selected.source(), safeTab));
        // Serialized here (records and int[] included) and closed-tag-safe for the inline <script>.
        model.addAttribute("pageStateJson", JSON.writeValueAsString(pageState(selected, safeTab, sorts, page, size)).replace("</", "<\\/"));
        return "charts/chart-overview";
    }

    @GetMapping("/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> data(@RequestParam(required = false) String chart,
                                                    @RequestParam(defaultValue = "song") String tab,
                                                    @RequestParam(name = "sort", required = false) List<String> sort,
                                                    @RequestParam(defaultValue = "false") boolean includeFeatured,
                                                    @RequestParam(required = false) Integer topSong,
                                                    @RequestParam(required = false) Integer topAlbum,
                                                    @RequestParam(defaultValue = "0") int page,
                                                    @RequestParam(required = false) Integer size,
                                                    @RequestParam(defaultValue = "false") boolean all,
                                                    @RequestParam(defaultValue = "false") boolean fresh,
                                                    @RequestParam MultiValueMap<String, String> params) {
        Chart selected = Chart.from(chart);
        if (fresh) {
            chartOverviewService.invalidate(selected);
        }
        String safeTab = ChartOverviewService.normalizeTab(tab);
        List<SortSpec> sorts = ChartOverviewService.parseSorts(safeTab, sort);
        int safeSize = all ? Integer.MAX_VALUE : pageSize(selected, size);
        ChartOverviewService.Page result = chartOverviewService.page(selected, safeTab, sorts, includeFeatured, topSong, topAlbum,
                params, all ? 0 : page, safeSize);
        return ResponseEntity.ok(pageState(selected, safeTab, sorts, result, safeSize));
    }

    /** Chart runs for the run row and the cards, several rows per request. */
    @GetMapping("/runs")
    @ResponseBody
    public Map<String, List<ChartOverviewService.RunPoint>> runs(@RequestParam(required = false) String chart,
                                                                  @RequestParam(name = "key", required = false) List<String> keys) {
        List<String> limited = keys == null ? List.of() : keys.stream().limit(60).toList();
        return chartOverviewService.runs(Chart.from(chart), limited);
    }

    private Map<String, Object> pageState(Chart chart, String tab, List<SortSpec> sorts, ChartOverviewService.Page page, int size) {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("chart", chart.key());
        state.put("chartLabel", chart.label());
        state.put("unit", chart.unit());
        state.put("tiers", chart.tiers());
        state.put("tab", tab);
        List<Map<String, Object>> sortState = new ArrayList<>();
        for (SortSpec spec : sorts) {
            sortState.add(Map.of("key", spec.key(), "dir", spec.ascending() ? "asc" : "desc"));
        }
        state.put("sorts", sortState);
        state.put("rows", page.rows());
        state.put("totalCount", page.totalCount());
        state.put("hasMore", page.hasMore());
        state.put("page", page.page());
        state.put("pageSize", size);
        if (page.summary() != null) {
            state.put("summary", page.summary());
        }
        return state;
    }

    private int pageSize(Chart chart, Integer requested) {
        int configured = switch (chart) {
            case WEEKLY -> appConfigService.getWeeklyOverviewPageSize();
            case HOT100 -> appConfigService.getBillboardOverviewPageSize();
            case TRL -> appConfigService.getTrlOverviewPageSize();
            case CUNTDOWN -> appConfigService.getPcOverviewPageSize();
        };
        return appConfigService.normalizePageSize(requested, configured, 1, 500);
    }

    static String normalizeView(String tab, String view) {
        if ("artist".equals(tab)) {
            return "table".equals(view) ? "table" : "leaderboard";
        }
        return "cards".equals(view) ? "cards" : "table";
    }

    static String currentSection(Chart chart) {
        return switch (chart) {
            case WEEKLY -> "weekly-overview-charts";
            case HOT100 -> "billboard-hot100";
            case TRL -> "trl";
            case CUNTDOWN -> "pc";
        };
    }
}
