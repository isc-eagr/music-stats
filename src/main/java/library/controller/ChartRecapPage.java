package library.controller;

import library.service.ChartOverviewService;
import org.springframework.ui.Model;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Model for the shared recap page (charts/recap.html + js/chart-recap.js) used by the Billboard Hot 100, TRL
 * and Vato's Cuntdown recaps. The page loads each date from {@code base + "/recaps/data"} and chart runs from
 * {@code base + "/chart-run"}; label, day/week unit and the chart-run tiers come from the chart overview config.
 */
final class ChartRecapPage {

    static final String VIEW = "charts/recap";

    private ChartRecapPage() {
    }

    static void populate(Model model, ChartOverviewService.Chart chart, String base, List<String> availableDates, String selectedDate) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("key", chart.key());
        config.put("label", chart.label());
        config.put("unit", chart.unit());
        config.put("tiers", Arrays.stream(chart.tiers()).boxed().toList());
        config.put("base", base);
        config.put("availableDates", availableDates);
        config.put("selectedDate", selectedDate);

        model.addAttribute("chart", chart);
        model.addAttribute("recapConfig", config);
    }
}
