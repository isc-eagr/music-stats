package library;

import library.controller.ChartsController;
import library.dto.ChartEntryDTO;
import library.dto.ChartSongOverviewRowDTO;
import library.service.AppConfigService;
import library.service.BillboardHot100Service;
import library.service.ChartService;
import library.service.PcService;
import library.service.TrlService;
import org.junit.jupiter.api.Test;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.util.LinkedMultiValueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

class ChartsControllerTest {

    @Test
    @SuppressWarnings("unchecked")
    void weeklyOverviewFiltersBeforeInitialPaginationAndDataPaging() {
        ChartService chartService = mock(ChartService.class);
        AppConfigService configService = mock(AppConfigService.class);
        ChartsController controller = new ChartsController(
                chartService, configService, mock(BillboardHot100Service.class), mock(PcService.class), mock(TrlService.class));
        List<ChartSongOverviewRowDTO> rows = List.of(
                overviewSong("A", "First", 8), overviewSong("A", "Second", 7),
                overviewSong("A", "Third", 6), overviewSong("B", "Ignored", 5));
        when(chartService.getChartOverviewSongRows("weekly")).thenReturn(rows);
        when(configService.getWeeklyOverviewPageSize()).thenReturn(2);
        when(configService.normalizePageSize(2, 2, 1, 250)).thenReturn(2);

        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("artist", "1");
        ExtendedModelMap model = new ExtendedModelMap();

        controller.weeklyOverview("song", null, "weeks", "desc", null, null, null, null, false, null, null, params, model);

        assertThat((Integer) model.get("activeTotalCount")).isEqualTo(3);
        assertThat((List<ChartSongOverviewRowDTO>) model.get("songRows"))
                .extracting(ChartSongOverviewRowDTO::getSongTitle).containsExactly("First", "Second");

        Map<String, Object> data = controller.weeklyOverviewData("song", null, "weeks", "desc", null, null, null, null,
                false, List.of(), null, null, 1, 2, params).getBody();
        assertThat(data).containsEntry("totalCount", 3).containsEntry("hasMore", false);
        assertThat((List<ChartSongOverviewRowDTO>) data.get("entries"))
                .extracting(ChartSongOverviewRowDTO::getSongTitle).containsExactly("Third");
        verify(chartService, times(2)).getChartOverviewSongRows("weekly");
        verify(chartService, never()).getChartOverviewAlbumRows("weekly");
        verify(chartService, never()).getChartOverviewArtistRows("weekly", false);
    }

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

    private ChartSongOverviewRowDTO overviewSong(String artist, String title, int span) {
        ChartSongOverviewRowDTO row = new ChartSongOverviewRowDTO();
        row.setArtistName(artist);
        row.setArtistId("A".equals(artist) ? 1 : 2);
        row.setSongTitle(title);
        row.setTotalChartSpan(span);
        row.setPeakPosition(1);
        row.setSpanAtPeak(1);
        row.setFirstAppearanceSortValue("2025-W01");
        return row;
    }
}
