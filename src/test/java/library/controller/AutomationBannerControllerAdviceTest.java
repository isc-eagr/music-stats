package library.controller;

import library.service.ChartService;
import library.service.PlayAutomationStateService;
import org.junit.jupiter.api.Test;
import org.springframework.ui.ExtendedModelMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AutomationBannerControllerAdviceTest {

    @Test
    void exposesPersistentBannerForTheOldestMissingWeeklyChart() {
        PlayAutomationStateService automationStateService = mock(PlayAutomationStateService.class);
        ChartService chartService = mock(ChartService.class);
        when(chartService.getWeeksWithoutCharts()).thenReturn(List.of("2026-W31", "2026-W33"));

        AutomationBannerControllerAdvice advice =
                new AutomationBannerControllerAdvice(automationStateService, chartService);
        ExtendedModelMap model = new ExtendedModelMap();

        advice.addAutomationBannerState(model);

        AutomationBannerControllerAdvice.WeeklyChartBanner banner =
                (AutomationBannerControllerAdvice.WeeklyChartBanner) model.get("weeklyChartBanner");
        assertThat(banner.count()).isEqualTo(2);
        assertThat(banner.href()).isEqualTo("/charts/weekly/2026-W31");
    }

    @Test
    void omitsMissingWeeklyChartBannerOnceAllChartsAreGenerated() {
        PlayAutomationStateService automationStateService = mock(PlayAutomationStateService.class);
        ChartService chartService = mock(ChartService.class);
        when(chartService.getWeeksWithoutCharts()).thenReturn(List.of());

        AutomationBannerControllerAdvice advice =
                new AutomationBannerControllerAdvice(automationStateService, chartService);
        ExtendedModelMap model = new ExtendedModelMap();

        advice.addAutomationBannerState(model);

        assertThat(model).containsEntry("weeklyChartBanner", null);
    }
}
