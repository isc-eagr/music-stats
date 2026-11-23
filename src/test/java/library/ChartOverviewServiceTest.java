package library;

import library.controller.ChartOverviewController;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartOverviewRows.ArtistRow;
import library.dto.ChartOverviewRows.SongRow;
import library.dto.ChartSongOverviewRowDTO;
import library.entity.TrlDebut;
import library.service.AppConfigService;
import library.service.BillboardHot100Service;
import library.service.ChartOverviewService;
import library.service.ChartOverviewService.Chart;
import library.service.ChartOverviewService.SortSpec;
import library.service.ChartService;
import library.service.OverviewCacheService;
import library.service.OverviewFilterService;
import library.service.PcService;
import library.service.TrlService;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.LinkedMultiValueMap;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

class ChartOverviewServiceTest {

    private final ChartService chartService = mock(ChartService.class);
    private final TrlService trlService = mock(TrlService.class);
    private final ChartOverviewService service = new ChartOverviewService(chartService, trlService, mock(PcService.class),
            mock(BillboardHot100Service.class), new OverviewFilterService(), new OverviewCacheService());

    @Test
    void weeklySongsSortByEveryLevelFilterAndSummarize() {
        when(chartService.getChartOverviewSongRows("weekly")).thenReturn(List.of(
                weekly(1, "Bravo", "gender-male", 10, 1, "2020-01-05"),
                weekly(2, "Alpha", "gender-female", 10, 3, "2021-02-07"),
                weekly(3, "Charlie", "gender-male", 4, 12, "2019-03-03")));

        List<SortSpec> sorts = ChartOverviewService.parseSorts("song", List.of("span:desc", "title:asc", "bogus:asc", "span:asc"));
        assertThat(sorts).containsExactly(new SortSpec("span", false), new SortSpec("title", true));

        ChartOverviewService.Page page = service.page(Chart.WEEKLY, "song", sorts, false, null, null, new LinkedMultiValueMap<>(), 0, 2);
        assertThat(page.rows()).extracting(row -> ((SongRow) row).title()).containsExactly("Alpha", "Bravo");
        assertThat(page.totalCount()).isEqualTo(3);
        assertThat(page.hasMore()).isTrue();

        Map<String, Object> summary = page.summary();
        assertThat(summary.get("genders")).isEqualTo(Map.of("male", 2, "female", 1, "other", 0));
        assertThat(((int[]) ((Map<?, ?>) summary.get("peakSpread")).get("male"))).containsExactly(1, 0, 0, 1);
        assertThat(((int[]) ((Map<?, ?>) summary.get("peakSpread")).get("female"))).containsExactly(0, 1, 0, 0);
        assertThat(summary).doesNotContainKeys("numberOnes", "spanTotal", "longest", "mostAtOne");

        SongRow first = (SongRow) page.rows().get(0);
        assertThat(first.gender()).isEqualTo("female");
        assertThat(first.debutDate()).isEqualTo("2021-02-07");
        assertThat(first.tiers()).containsExactly(0, 2, 5);

        LinkedMultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("q", "charl");
        ChartOverviewService.Page searched = service.page(Chart.WEEKLY, "song", sorts, false, null, null, filters, 0, 50);
        assertThat(searched.rows()).extracting(row -> ((SongRow) row).title()).containsExactly("Charlie");
    }

    @Test
    void weeklyFiltersBeforePagingAndPagesTheFilteredRows() {
        ChartSongOverviewRowDTO first = weekly(1, "First", "gender-male", 8, 1, "2025-01-05");
        ChartSongOverviewRowDTO second = weekly(2, "Second", "gender-male", 7, 1, "2025-01-12");
        ChartSongOverviewRowDTO third = weekly(3, "Third", "gender-male", 6, 1, "2025-01-19");
        ChartSongOverviewRowDTO other = weekly(4, "Ignored", "gender-male", 5, 1, "2025-01-26");
        List.of(first, second, third).forEach(row -> row.setArtistId(1));
        other.setArtistId(2);
        when(chartService.getChartOverviewSongRows("weekly")).thenReturn(List.of(first, second, third, other));
        LinkedMultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("artist", "1");
        List<SortSpec> sorts = List.of(new SortSpec("span", false));

        ChartOverviewService.Page firstPage = service.page(Chart.WEEKLY, "song", sorts, false, null, null, filters, 0, 2);
        assertThat(firstPage.totalCount()).isEqualTo(3);
        assertThat(firstPage.rows()).extracting(row -> ((SongRow) row).title()).containsExactly("First", "Second");
        ChartOverviewService.Page secondPage = service.page(Chart.WEEKLY, "song", sorts, false, null, null, filters, 1, 2);
        assertThat(secondPage.rows()).extracting(row -> ((SongRow) row).title()).containsExactly("Third");
        assertThat(secondPage.hasMore()).isFalse();
        assertThat(secondPage.summary()).isNull();
    }

    @Test
    void trlSongsKeepActualDaysAndBlankPeaksSortLast() {
        TrlDebut withGap = trl(7, "Only You", 37, 35, 2);
        TrlDebut noPeak = trl(8, "Unknown", 3, 3, null);
        TrlDebut top = trl(9, "Hollaback Girl", 50, 51, 1);
        when(trlService.getAllDebuts()).thenReturn(List.of(withGap, noPeak, top));

        ChartOverviewService.Page asc = service.page(Chart.TRL, "song", List.of(new SortSpec("peak", true)), false, null, null,
                new LinkedMultiValueMap<>(), 0, 10);
        assertThat(asc.rows()).extracting(row -> ((SongRow) row).title()).containsExactly("Hollaback Girl", "Only You", "Unknown");
        ChartOverviewService.Page desc = service.page(Chart.TRL, "song", List.of(new SortSpec("peak", false)), false, null, null,
                new LinkedMultiValueMap<>(), 0, 10);
        assertThat(desc.rows()).extracting(row -> ((SongRow) row).title()).containsExactly("Only You", "Hollaback Girl", "Unknown");

        SongRow gap = (SongRow) asc.rows().get(1);
        assertThat(gap.span()).isEqualTo(37);
        assertThat(gap.actualSpan()).isEqualTo(35);
        assertThat(gap.runKey()).isEqualTo("7");
        assertThat(gap.debutKey()).isEqualTo("2004-11-10");
    }

    @Test
    void artistLeaderboardsSplitByGenderAndUseTopThresholds() {
        ChartArtistOverviewRowDTO male = artist("Rauw Alejandro", "gender-male", 429, 17, new int[]{35, 17, 25});
        ChartArtistOverviewRowDTO female = artist("Rihanna", "gender-female", 203, 9, new int[]{26, 9, 14});
        ChartArtistOverviewRowDTO group = artist("Paramore", null, 108, 4, new int[]{17, 4, 8});
        when(chartService.getChartOverviewSongRows("weekly")).thenReturn(List.of());
        when(chartService.getChartOverviewAlbumRows("weekly")).thenReturn(List.of());
        when(chartService.getChartOverviewArtistRows(eq("weekly"), any(), any(), anyBoolean())).thenReturn(List.of(male, female, group));

        ChartOverviewService.Page page = service.page(Chart.WEEKLY, "artist", ChartOverviewService.parseSorts("artist", null),
                false, 2, null, new LinkedMultiValueMap<>(), 0, 10);
        assertThat(page.rows()).extracting(row -> ((ArtistRow) row).artistName()).containsExactly("Rauw Alejandro", "Rihanna", "Paramore");
        assertThat(((ArtistRow) page.rows().get(0)).songs()).isEqualTo(25);

        Map<String, Object> summary = page.summary();
        assertThat(summary.get("spanByGender")).isEqualTo(Map.of("male", 429, "female", 203, "other", 108));
        assertThat(((int[]) ((Map<?, ?>) summary.get("peakSpread")).get("female"))).containsExactly(1, 0, 0, 0);
        assertThat((List<?>) summary.get("topMale")).extracting(row -> ((ArtistRow) row).artistName()).containsExactly("Rauw Alejandro");
        assertThat((List<?>) summary.get("topFemale")).extracting(row -> ((ArtistRow) row).artistName()).containsExactly("Rihanna");
    }

    @Test
    void controllerServesPageAndDataForEveryChart() throws Exception {
        AppConfigService config = mock(AppConfigService.class);
        when(config.normalizePageSize(any(), anyInt(), anyInt(), anyInt())).thenReturn(50);
        when(trlService.getAllDebuts()).thenReturn(List.of(trl(7, "Only You", 37, 35, 2)));
        var mvc = MockMvcBuilders.standaloneSetup(new ChartOverviewController(service, new OverviewFilterService(), config)).build();

        mvc.perform(get("/charts/overview").param("chart", "trl").param("view", "cards"))
                .andExpect(status().isOk())
                .andExpect(view().name("charts/chart-overview"))
                .andExpect(model().attribute("view", "cards"))
                .andExpect(model().attribute("currentSection", "trl"));

        mvc.perform(get("/charts/overview/data").param("chart", "trl").param("sort", "actualSpan:asc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.chart").value("trl"))
                .andExpect(jsonPath("$.unit").value("day"))
                .andExpect(jsonPath("$.sorts[0].key").value("actualSpan"))
                .andExpect(jsonPath("$.rows[0].actualSpan").value(35))
                .andExpect(jsonPath("$.summary.total").value(1));

        mvc.perform(get("/charts/overview").param("tab", "artist"))
                .andExpect(model().attribute("view", "leaderboard"));
    }

    private static ChartSongOverviewRowDTO weekly(int id, String title, String genderClass, int span, int peak, String debut) {
        ChartSongOverviewRowDTO row = new ChartSongOverviewRowDTO();
        row.setSongId(id);
        row.setSongTitle(title);
        row.setArtistName("Artist " + id);
        row.setGenderClass(genderClass);
        row.setTotalChartSpan(span);
        row.setPeakPosition(peak);
        row.setFirstAppearanceSortValue(debut);
        row.setSpanAtTop5(2);
        row.setSpanAtTop10(5);
        return row;
    }

    private static TrlDebut trl(int id, String title, int days, int actual, Integer peak) {
        TrlDebut debut = new TrlDebut();
        debut.setId(id);
        debut.setSongTitle(title);
        debut.setArtistName("Artist " + id);
        debut.setDaysOnCountdown(days);
        debut.setActualDays(actual);
        debut.setPeakPosition(peak);
        debut.setDebutDate("2004-11-10");
        debut.setGenderClass("gender-female");
        return debut;
    }

    private static ChartArtistOverviewRowDTO artist(String name, String genderClass, int span, int numberOnes, int[] topCounts) {
        ChartArtistOverviewRowDTO row = new ChartArtistOverviewRowDTO();
        row.setHighestPeak(numberOnes > 5 ? 1 : 3);
        row.setArtistName(name);
        row.setGenderClass(genderClass);
        row.setTotalChartSpan(span);
        row.setNumberOneSongsCount(numberOnes);
        row.setChartedSongsCount(topCounts[0]);
        row.setTopSongCounts(topCounts);
        return row;
    }
}
