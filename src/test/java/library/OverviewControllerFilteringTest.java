package library;

import library.controller.ChartsController;
import library.controller.ChartOverviewController;
import library.controller.ChartOverviewRedirectController;
import library.dto.ChartSongOverviewRowDTO;
import library.dto.BillboardHot100OverviewRowDTO;
import library.dto.PcOverviewRowDTO;
import library.entity.TrlDebut;
import library.service.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class OverviewControllerFilteringTest {
    @ParameterizedTest
    @ValueSource(strings = {"seasonal", "yearly"})
    void namedFiltersReachInitialPageDataAndPlaylistExport(String source) throws Exception {
        var config = config();
        var service = mock(ChartService.class);
        var first = song("Alpha", 1);
        var second = song("Bravo", 2);
        var third = song("Charlie", 3);
        when(service.getChartOverviewSongRows(source)).thenReturn(List.of(first, second, third));
        var controller = new ChartsController(service, config, mock(BillboardHot100Service.class), mock(PcService.class), mock(TrlService.class));
        var mvc = MockMvcBuilders.standaloneSetup(controller).build();
        String path = "/charts/" + source + "/overview";
        var page = mvc.perform(get(path).param("view", "table").param("totalChartSpan", "2").param("totalChartSpanMode", "gte")
                        .param("sort", "song").param("dir", "asc"))
                .andExpect(status().isOk()).andExpect(model().attribute("activeTotalCount", 2)).andReturn();
        assertThat(page.getModelAndView().getModel().get("songRows")).isEqualTo(List.of(second));
        mvc.perform(get(path + "/data").param("totalChartSpan", "2").param("totalChartSpanMode", "gte")
                        .param("sort", "song").param("dir", "asc").param("page", "1").param("size", "1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.totalCount").value(2))
                .andExpect(jsonPath("$.entries[0].songTitle").value("Charlie"))
                .andExpect(jsonPath("$.hasMore").value(false));
        mvc.perform(get(path + "/song-export").param("totalChartSpan", "2").param("totalChartSpanMode", "gte")
                        .param("sort", "song").param("dir", "desc"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].songTitle").value("Charlie"));
        mvc.perform(get(path + "/data").param("songTitle", "no match").param("page", "0"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.totalCount").value(0))
                .andExpect(jsonPath("$.entries").isEmpty());
        verify(service, times(4)).getChartOverviewSongRows(source);
    }

    @Test
    void countdownSearchFindsRowsBeyondFirstPage() throws Exception {
        var service = mock(PcService.class);
        var other = new PcOverviewRowDTO(); other.setSongTitle("Other");
        var match = new PcOverviewRowDTO(); match.setSongTitle("Needle");
        when(service.getOverviewRows()).thenReturn(List.of(other, match));
        var mvc = overview(mock(TrlService.class), service, mock(BillboardHot100Service.class));
        mvc.perform(get("/charts/overview/data").param("chart", "cuntdown").param("q", "needle").param("page", "0"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.rows[0].title").value("Needle"))
                .andExpect(jsonPath("$.totalCount").value(1));
    }

    @Test
    void hallOfFameFilterReachesTheTrlOverview() throws Exception {
        var service = mock(TrlService.class);
        var active = new TrlDebut(); active.setSongTitle("Active");
        var retired = new TrlDebut(); retired.setSongTitle("Retired"); retired.setRetired(true);
        when(service.getAllDebuts()).thenReturn(List.of(active, retired));
        var mvc = overview(service, mock(PcService.class), mock(BillboardHot100Service.class));
        mvc.perform(get("/charts/overview/data").param("chart", "trl").param("retired", "true"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.rows[0].title").value("Retired"))
                .andExpect(jsonPath("$.rows[0].retired").value(true))
                .andExpect(jsonPath("$.totalCount").value(1));
    }

    @Test
    void billboardNamedFiltersReachTheOverviewData() throws Exception {
        var service = mock(BillboardHot100Service.class);
        var first = new BillboardHot100OverviewRowDTO();
        first.setSongTitle("First"); first.setResolvedArtistId(1); first.setWeeksOnChart(4);
        var second = new BillboardHot100OverviewRowDTO();
        second.setSongTitle("Second"); second.setResolvedArtistId(2); second.setWeeksOnChart(8);
        when(service.getAllOverviewRows()).thenReturn(List.of(first, second));
        var mvc = overview(mock(TrlService.class), mock(PcService.class), service);
        mvc.perform(get("/charts/overview/data").param("chart", "hot100").param("artist", "2"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.totalCount").value(1))
                .andExpect(jsonPath("$.rows[0].title").value("Second"));
        verify(service).getAllOverviewRows();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"/charts/weekly/overview,weekly", "/misc/trl,trl", "/misc/vatos-cuntdown,cuntdown",
            "/misc/pc,cuntdown", "/misc/billboard-hot-100,hot100"})
    void oldOverviewUrlsRedirectToTheSharedPage(String path, String chart) throws Exception {
        var mvc = MockMvcBuilders.standaloneSetup(new ChartOverviewRedirectController()).build();
        mvc.perform(get(path).param("overviewTab", "artist").param("q", "bad bunny").param("sort", "days"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/charts/overview?chart=" + chart + "&tab=artist&q=bad%20bunny"));
    }

    private org.springframework.test.web.servlet.MockMvc overview(TrlService trl, PcService pc, BillboardHot100Service billboard) {
        var service = new ChartOverviewService(mock(ChartService.class), trl, pc, billboard, new OverviewFilterService(), new OverviewCacheService());
        return MockMvcBuilders.standaloneSetup(new ChartOverviewController(service, new OverviewFilterService(), config())).build();
    }

    private AppConfigService config() {
        var config = mock(AppConfigService.class);
        when(config.getWeeklyOverviewPageSize()).thenReturn(1);
        when(config.getSeasonalOverviewPageSize()).thenReturn(1);
        when(config.getBillboardOverviewPageSize()).thenReturn(1);
        when(config.normalizePageSize(any(), anyInt(), anyInt(), anyInt())).thenReturn(1);
        return config;
    }

    private ChartSongOverviewRowDTO song(String name, int span) {
        var row = new ChartSongOverviewRowDTO();
        row.setSongTitle(name); row.setArtistName("Artist"); row.setTotalChartSpan(span);
        row.setFirstAppearanceSortValue("2026-01-01");
        return row;
    }
}
