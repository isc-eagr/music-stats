package library;

import library.controller.ChartsController;
import library.dto.ChartEntryDTO;
import library.service.AppConfigService;
import library.service.BillboardHot100Service;
import library.service.ChartService;
import library.service.PcService;
import library.service.TrlService;
import org.junit.jupiter.api.Test;
import org.springframework.ui.ExtendedModelMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChartsControllerTest {

    @Test
    @SuppressWarnings("unchecked")
    void weeklyPreviewKeepsTopTwentySeparateFromThirtyContenders() {
        ChartService chartService = mock(ChartService.class);
        ChartsController controller = new ChartsController(
                chartService,
                mock(AppConfigService.class),
                mock(BillboardHot100Service.class),
                mock(PcService.class),
                mock(TrlService.class));

        List<ChartEntryDTO> preview = new ArrayList<>();
        for (int position = 1; position <= 50; position++) {
            ChartEntryDTO entry = new ChartEntryDTO();
            entry.setPosition(position);
            entry.setSongId(position);
            preview.add(entry);
        }

        when(chartService.getExistingChartPeriodKeys("song")).thenReturn(Set.of());
        when(chartService.getChart("song", "2026-W34")).thenReturn(Optional.empty());
        when(chartService.isWeekComplete("2026-W34")).thenReturn(false);
        when(chartService.getWeeklySongChartPreview("2026-W34", 50)).thenReturn(preview);
        when(chartService.getWeeklyAlbumChartPreview("2026-W34")).thenReturn(List.of());
        when(chartService.formatPeriodKey("2026-W34")).thenReturn("Aug 24 - Aug 30, 2026");
        when(chartService.getLatestWeeklyChart("song")).thenReturn(Optional.empty());
        when(chartService.getWeeksWithoutCharts()).thenReturn(List.of());

        ExtendedModelMap model = new ExtendedModelMap();
        String view = controller.weeklyChart("2026-W34", true, "songs", model);

        assertThat(view).isEqualTo("charts/weekly");
        assertThat((List<ChartEntryDTO>) model.get("entries"))
                .hasSize(20)
                .extracting(ChartEntryDTO::getPosition)
                .containsExactlyElementsOf(java.util.stream.IntStream.rangeClosed(1, 20).boxed().toList());
        assertThat((List<ChartEntryDTO>) model.get("contenderEntries"))
                .hasSize(30)
                .extracting(ChartEntryDTO::getPosition)
                .containsExactlyElementsOf(java.util.stream.IntStream.rangeClosed(21, 50).boxed().toList());
        verify(chartService).getWeeklySongChartPreview("2026-W34", 50);
    }
}
