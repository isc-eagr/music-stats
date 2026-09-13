package library.service;

import library.dto.ChartSongOverviewRowDTO;
import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.BillboardHot100OverviewRowDTO;
import library.dto.PcOverviewRowDTO;
import library.entity.TrlDebut;
import org.junit.jupiter.api.Test;
import org.springframework.util.LinkedMultiValueMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OverviewFilterServiceTest {

    private final OverviewFilterService service = new OverviewFilterService();

    @Test
    void everyOverviewUsesWeeklyFilterVocabularyForItsAvailableMetrics() {
        for (String source : List.of("weekly", "seasonal", "yearly", "pc", "trl", "billboard")) {
            for (String tab : List.of("song", "album", "artist")) {
                assertThat(service.fieldsFor(source, tab)).isNotEmpty();
                assertThat(service.fieldsFor(source, tab)).anySatisfy(field -> {
                    assertThat(field.name()).isEqualTo("artist");
                    assertThat(field.type()).isEqualTo("artist");
                });
                assertThat(service.fieldsFor(source, tab)).extracting(OverviewFilterService.FilterField::name)
                        .contains("gender", "genre", "subgenre", "country", "ethnicity", "language", "tag");
            }
        }
        assertThat(service.fieldsFor("weekly", "album")).extracting(OverviewFilterService.FilterField::name)
                .contains("artist", "albumName", "totalChartSpan", "highestPeak", "spanAtPeak", "debutPosition", "firstAppearance", "peakAppearance", "lastAppearance",
                        "gender", "genre", "subgenre", "country", "ethnicity", "language", "tag");
        assertThat(service.fieldsFor("weekly", "artist")).extracting(OverviewFilterService.FilterField::name)
                .contains("artist", "chartedSongsCount", "totalChartSpan", "numberOneSongsCount", "totalSpanAtNumberOne", "chartedAlbumsCount", "albumTotalChartSpan", "numberOneAlbumsCount", "albumTotalSpanAtNumberOne",
                        "gender", "genre", "subgenre", "country", "ethnicity", "language", "tag");
    }

    @Test
    void dateFilterUsesDisplayedWeekEndAndRejectsInvalidDates() {
        var row = song("Artist", "Song", 5, "2026-09-07");
        row.setFirstAppearanceLabel("Sep 7 - Sep 13, 2026");
        var params = new LinkedMultiValueMap<String, String>();
        params.set("firstAppearance", "13/09/2026");
        assertThat(service.filter("weekly", "song", List.of(row), params)).containsExactly(row);
        params.set("firstAppearance", "07/09/2026");
        assertThat(service.filter("weekly", "song", List.of(row), params)).isEmpty();
        params.set("firstAppearance", "31/02/2026");
        assertThat(service.filter("weekly", "song", List.of(row), params)).isEmpty();
    }

    @Test
    void seasonalAlbumDebutMatchesItsSeasonLabel() {
        var row = new ChartAlbumOverviewRowDTO();
        row.setFirstDebutDate("Winter 2025");
        row.setFirstDebutSortValue("2025-01-01");
        var params = new LinkedMultiValueMap<String, String>();
        params.set("firstAppearance", "winter");
        assertThat(service.filter("seasonal", "album", List.of(row), params)).containsExactly(row);
    }

    @Test
    void artistCountFiltersUseSelectedTopThresholds() {
        var row = new ChartArtistOverviewRowDTO();
        row.setChartedSongsCount(10);
        row.setTopSongCounts(new int[] {0, 2, 4});
        row.setAlbumTotalChartSpan(20);
        row.setTopAlbumWeeks(new int[] {0, 3, 8});
        var params = new LinkedMultiValueMap<String, String>();
        params.set("topSong", "1"); params.set("chartedSongsCount", "2");
        params.set("topAlbum", "2"); params.set("albumTotalChartSpan", "8");
        assertThat(service.filter("weekly", "artist", List.of(row), params)).containsExactly(row);
        params.set("chartedSongsCount", "10");
        assertThat(service.filter("weekly", "artist", List.of(row), params)).isEmpty();
    }

    @Test
    void supportsExclusionEmptyModesAndBothBooleanValues() {
        var row = song("Artist", "Song", 5, null);
        var params = new LinkedMultiValueMap<String, String>();
        row.setArtistId(7);
        params.set("artist", "7"); params.set("artistMode", "excludes");
        assertThat(service.filter("weekly", "song", List.of(row), params)).isEmpty();
        params.clear(); params.set("firstAppearanceMode", "isnull");
        assertThat(service.filter("weekly", "song", List.of(row), params)).containsExactly(row);
        params.set("firstAppearanceMode", "isnotnull");
        assertThat(service.filter("weekly", "song", List.of(row), params)).isEmpty();
        params.clear(); params.add("retired", "true"); params.add("retired", "false");
        var retired = new TrlDebut(); retired.setRetired(true);
        var active = new TrlDebut();
        assertThat(service.filter("trl", "song", List.of(retired, active), params)).containsExactly(retired, active);
    }

    @Test
    void filtersChartRowsWithRepeatedTextAndNumericRangeParameters() {
        ChartSongOverviewRowDTO first = song("Ariana Grande", "One", 12, "2025-01-03");
        ChartSongOverviewRowDTO second = song("Beyonce", "Two", 4, "2024-01-03");
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        first.setArtistId(1);
        second.setArtistId(2);
        params.add("artist", "1");
        params.add("artist", "2");
        params.add("totalChartSpan", "5");
        params.add("totalChartSpanTo", "15");
        params.add("totalChartSpanMode", "between");

        assertThat(service.filter("weekly", "song", List.of(first, second), params)).containsExactly(first);
    }

    @Test
    void parsesMexicoDisplayDatesForDateRanges() {
        ChartSongOverviewRowDTO first = song("Ariana Grande", "One", 12, "2025-01-03");
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("firstAppearance", "01/01/2025");
        params.add("firstAppearanceTo", "31/01/2025");
        params.add("firstAppearanceMode", "between");

        assertThat(service.filter("weekly", "song", List.of(first), params)).containsExactly(first);
    }

    @Test
    void filtersTrlHallOfFameOnTheServer() {
        TrlDebut retired = new TrlDebut();
        retired.setRetired(true);
        TrlDebut active = new TrlDebut();
        active.setRetired(false);
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("retired", "true");

        assertThat(service.filter("trl", "song", List.of(retired, active), params)).containsExactly(retired);
        assertThat(service.fieldsFor("pc", "song")).extracting(OverviewFilterService.FilterField::name)
                .contains("artist", "songTitle", "totalChartSpan");
        assertThat(service.fieldsFor("trl", "song")).extracting(OverviewFilterService.FilterField::name)
                .contains("artist", "songTitle", "totalChartSpan", "actualDays", "retired");
    }

    @Test
    void artistIdsFilterEveryExternalOverviewSource() {
        var params = new LinkedMultiValueMap<String, String>();
        params.add("artist", "42");
        var pc = new PcOverviewRowDTO(); pc.setResolvedArtistId(42);
        var trl = new TrlDebut(); trl.setResolvedArtistId(42);
        var billboard = new BillboardHot100OverviewRowDTO(); billboard.setResolvedArtistId(42);

        assertThat(service.filter("pc", "song", List.of(pc), params)).containsExactly(pc);
        assertThat(service.filter("trl", "song", List.of(trl), params)).containsExactly(trl);
        assertThat(service.filter("billboard", "song", List.of(billboard), params)).containsExactly(billboard);
    }

    private ChartSongOverviewRowDTO song(String artist, String title, int span, String firstAppearance) {
        ChartSongOverviewRowDTO row = new ChartSongOverviewRowDTO();
        row.setArtistName(artist);
        row.setSongTitle(title);
        row.setTotalChartSpan(span);
        row.setFirstAppearanceSortValue(firstAppearance);
        return row;
    }
}
