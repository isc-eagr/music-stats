package library.controller;

import library.service.ChartService;
import library.service.PlayAutomationStateService;
import org.junit.jupiter.api.Test;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AutomationBannerInterceptorTest {

    private final PlayAutomationStateService automationStateService = mock(PlayAutomationStateService.class);
    private final ChartService chartService = mock(ChartService.class);
    private final AutomationBannerInterceptor interceptor =
            new AutomationBannerInterceptor(automationStateService, chartService);

    @Test
    void exposesPersistentBannerForTheOldestMissingWeeklyChart() {
        when(chartService.getWeeksWithoutCharts()).thenReturn(List.of("2026-W31", "2026-W33"));
        ExtendedModelMap model = new ExtendedModelMap();

        interceptor.addAutomationBannerState(model);

        AutomationBannerInterceptor.WeeklyChartBanner banner =
                (AutomationBannerInterceptor.WeeklyChartBanner) model.get("weeklyChartBanner");
        assertThat(banner.count()).isEqualTo(2);
        assertThat(banner.href()).isEqualTo("/charts/weekly/2026-W31");
    }

    @Test
    void omitsMissingWeeklyChartBannerOnceAllChartsAreGenerated() {
        when(chartService.getWeeksWithoutCharts()).thenReturn(List.of());
        ExtendedModelMap model = new ExtendedModelMap();

        interceptor.addAutomationBannerState(model);

        assertThat(model).containsEntry("weeklyChartBanner", null);
    }

    @Test
    void addsBannersWhenAnHtmlViewRenders() {
        when(chartService.getWeeksWithoutCharts()).thenReturn(List.of("2026-W31"));
        ModelAndView page = new ModelAndView("albums/list");

        interceptor.postHandle(null, null, null, page);

        assertThat(page.getModel()).containsKeys("playAutomationBannerState", "weeklyChartBanner");
    }

    @Test
    void skipsBannerQueriesForJsonAndImageResponses() {
        // @ResponseBody / ResponseEntity handlers reach postHandle without a ModelAndView.
        interceptor.postHandle(null, null, null, null);

        verifyNoInteractions(automationStateService, chartService);
    }

    @Test
    void skipsBannerQueriesForRedirects() {
        interceptor.postHandle(null, null, null, new ModelAndView("redirect:/charts/seasonal/2026-Summer"));
        interceptor.postHandle(null, null, null, new ModelAndView(new RedirectView("/albums")));

        verifyNoInteractions(automationStateService, chartService);
    }
}
