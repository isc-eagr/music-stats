package library.controller;

import jakarta.servlet.http.HttpServletRequest;
import library.service.ChartOverviewService.Chart;
import library.service.NumberOneService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * The shared Number Ones page: every run at #1 on the Weekly chart (songs and albums), the Hot 100,
 * TRL and Vato's Cuntdown. Each chart has its own path so the menu can link to it.
 */
@Controller
@RequestMapping("/charts")
public class NumberOnesController {

    private final NumberOneService numberOneService;

    public NumberOnesController(NumberOneService numberOneService) {
        this.numberOneService = numberOneService;
    }

    @GetMapping({"/weekly/number-ones", "/hot100/number-ones", "/trl/number-ones", "/cuntdown/number-ones"})
    public String numberOnes(HttpServletRequest request, @RequestParam(defaultValue = "song") String type, Model model) {
        Chart chart = chartFromPath(request.getRequestURI());
        String safeType = chart == Chart.WEEKLY && "album".equals(type) ? "album" : "song";
        model.addAttribute("currentSection", currentSection(chart));
        model.addAttribute("chart", chart);
        model.addAttribute("pageTitle", pageTitle(chart));
        model.addAttribute("charts", Chart.values());
        model.addAttribute("selectedType", safeType);
        model.addAttribute("runs", numberOneService.runs(chart, safeType));
        return "charts/number-ones";
    }

    /** Old Weekly Number Ones URL, kept so bookmarks land on the new path. */
    @GetMapping("/number-ones")
    public String legacyWeeklyNumberOnes(@RequestParam(defaultValue = "song") String type) {
        return "redirect:/charts/weekly/number-ones?type=" + ("album".equals(type) ? "album" : "song");
    }

    static Chart chartFromPath(String uri) {
        String path = uri == null ? "" : uri;
        for (Chart chart : Chart.values()) {
            if (path.endsWith("/charts/" + chart.key() + "/number-ones")) return chart;
        }
        return Chart.WEEKLY;
    }

    static String pageTitle(Chart chart) {
        return switch (chart) {
            case WEEKLY -> "Weekly Number Ones";
            case HOT100 -> "Billboard Hot 100 Number Ones";
            case TRL -> "TRL Number Ones";
            case CUNTDOWN -> "Vato's Cuntdown Number Ones";
        };
    }

    static String currentSection(Chart chart) {
        return "number-ones-" + chart.key();
    }
}
