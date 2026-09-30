package library.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import library.service.ChartService;
import library.service.PlayAutomationStateService;
import org.springframework.stereotype.Component;
import org.springframework.ui.ModelMap;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import java.util.List;

/**
 * Adds the automation and missing-weekly-chart banners to rendered pages.
 * Runs after the handler and only when an HTML view is about to render, so JSON endpoints,
 * image requests and redirects skip the banner queries entirely.
 */
@Component
public class AutomationBannerInterceptor implements HandlerInterceptor {

    private final PlayAutomationStateService automationStateService;
    private final ChartService chartService;

    public AutomationBannerInterceptor(PlayAutomationStateService automationStateService, ChartService chartService) {
        this.automationStateService = automationStateService;
        this.chartService = chartService;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
                           ModelAndView modelAndView) {
        if (rendersView(modelAndView)) {
            addAutomationBannerState(modelAndView.getModelMap());
        }
    }

    public void addAutomationBannerState(ModelMap model) {
        model.addAttribute("playAutomationBannerState", automationStateService.getBannerState());

        List<String> missingWeeks = chartService.getWeeksWithoutCharts();
        WeeklyChartBanner weeklyChartBanner = missingWeeks.isEmpty()
                ? null
                : new WeeklyChartBanner(missingWeeks.size(), "/charts/weekly/" + missingWeeks.getFirst());
        model.addAttribute("weeklyChartBanner", weeklyChartBanner);
    }

    static boolean rendersView(ModelAndView modelAndView) {
        if (modelAndView == null || !modelAndView.hasView()) {
            return false;
        }
        String viewName = modelAndView.getViewName();
        if (viewName != null) {
            return !viewName.startsWith("redirect:") && !viewName.startsWith("forward:");
        }
        return !(modelAndView.getView() instanceof RedirectView);
    }

    public record WeeklyChartBanner(int count, String href) {
    }
}
