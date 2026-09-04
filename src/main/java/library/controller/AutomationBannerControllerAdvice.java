package library.controller;

import library.service.ChartService;
import library.service.PlayAutomationStateService;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

import java.util.List;

@ControllerAdvice
public class AutomationBannerControllerAdvice {

    private final PlayAutomationStateService automationStateService;
    private final ChartService chartService;

    public AutomationBannerControllerAdvice(PlayAutomationStateService automationStateService, ChartService chartService) {
        this.automationStateService = automationStateService;
        this.chartService = chartService;
    }

    @ModelAttribute
    public void addAutomationBannerState(Model model) {
        model.addAttribute("playAutomationBannerState", automationStateService.getBannerState());

        List<String> missingWeeks = chartService.getWeeksWithoutCharts();
        WeeklyChartBanner weeklyChartBanner = missingWeeks.isEmpty()
                ? null
                : new WeeklyChartBanner(missingWeeks.size(), "/charts/weekly/" + missingWeeks.getFirst());
        model.addAttribute("weeklyChartBanner", weeklyChartBanner);
    }

    public record WeeklyChartBanner(int count, String href) {
    }
}
