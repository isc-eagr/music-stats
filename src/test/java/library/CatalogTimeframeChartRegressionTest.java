package library;

import library.dto.ChartEntryDTO;
import library.dto.GenderCardDTO;
import library.dto.GenreCardDTO;
import library.dto.NumberOneRunDTO;
import library.dto.TimeframeCardDTO;
import library.dto.TimeframeResultDTO;
import library.repository.ChartEntryRepository;
import library.repository.ChartRepository;
import library.repository.GenreRepository;
import library.repository.LookupRepository;
import library.service.AppConfigService;
import library.service.CatalogWinningPeriodService;
import library.service.ChartService;
import library.service.GenderService;
import library.service.GenreService;
import library.service.ItunesService;
import library.service.SongLinkService;
import library.service.TimeframeService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CatalogTimeframeChartRegressionTest {

    @Test
    void genderCatalogAggregatesEffectiveGenderCountsAndTopItems() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            GenderService service = new GenderService(
                    new LookupRepository(db.jdbcTemplate),
                    db.jdbcTemplate,
                    new CatalogWinningPeriodService(db.jdbcTemplate));

            List<GenderCardDTO> genders = service.getGenders(null, "plays", "desc");

            assertThat(genders).extracting(GenderCardDTO::getName)
                    .containsExactly("Male", "Female", "Other");

            GenderCardDTO female = byName(genders, GenderCardDTO::getName).get("Female");
            assertThat(female.getPlayCount()).isEqualTo(6);
            assertThat(female.getVatitoPlayCount()).isEqualTo(4);
            assertThat(female.getRobertloverPlayCount()).isEqualTo(2);
            assertThat(female.getArtistCount()).isEqualTo(2);
            assertThat(female.getAlbumCount()).isEqualTo(2);
            assertThat(female.getSongCount()).isEqualTo(4);
            assertThat(female.getTopArtistName()).isEqualTo("Selena");
            assertThat(female.getTopAlbumName()).isEqualTo("Amor Prohibido");
            assertThat(female.getTopSongName()).isEqualTo("Bidi Bidi Bom Bom");
            assertThat(female.getWinningDaysCount()).isGreaterThan(0);

            GenderCardDTO male = byName(genders, GenderCardDTO::getName).get("Male");
            assertThat(male.getPlayCount()).isEqualTo(6);
            assertThat(male.getTopArtistName()).isEqualTo("Bad Bunny");
            assertThat(male.getTopSongName()).isEqualTo("Titi Me Pregunto");
        }
    }

    @Test
    void genreCatalogRespectsSongAlbumArtistOverridesAndTopItems() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            GenreService service = new GenreService(
                    mock(GenreRepository.class),
                    new LookupRepository(db.jdbcTemplate),
                    db.jdbcTemplate,
                    new CatalogWinningPeriodService(db.jdbcTemplate));

            List<GenreCardDTO> genres = service.getGenres(null, "plays", "desc");

            assertThat(genres).extracting(GenreCardDTO::getName)
                    .containsExactly("Pop", "Rock", "Dance");

            Map<String, GenreCardDTO> byName = byName(genres, GenreCardDTO::getName);
            GenreCardDTO pop = byName.get("Pop");
            assertThat(pop.getPlayCount()).isEqualTo(7);
            assertThat(pop.getSongCount()).isEqualTo(3);
            assertThat(pop.getTopArtistName()).isEqualTo("Bad Bunny");
            assertThat(pop.getTopAlbumName()).isEqualTo("Un Verano Sin Ti");
            assertThat(pop.getTopSongName()).isEqualTo("Titi Me Pregunto");

            GenreCardDTO rock = byName.get("Rock");
            assertThat(rock.getPlayCount()).isEqualTo(4);
            assertThat(rock.getAlbumCount()).isEqualTo(3);
            assertThat(rock.getTopSongName()).isEqualTo("Bidi Bidi Bom Bom");

            GenreCardDTO dance = byName.get("Dance");
            assertThat(dance.getPlayCount()).isEqualTo(1);
            assertThat(dance.getTopSongName()).isEqualTo("No Me Queda Mas");
        }
    }

    @Test
    void timeframeMonthsAggregateCountsWinnersAndTopItemsWithFilters() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            TimeframeService service = new TimeframeService(db.jdbcTemplate);

            TimeframeResultDTO result = service.getTimeframeCardsWithCount(
                    "months",
                    null, null, null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null,
                    3, null,
                    null, null,
                    null, null, null, null, null, null, null, null, null, null,
                    null, null,
                    null, null,
                    "plays", "desc", 0, 10);

            assertThat(result.getTotalCount()).isEqualTo(2);
            assertThat(result.getTimeframes()).extracting(TimeframeCardDTO::getPeriodKey)
                    .containsExactly("2024-01", "2024-03");

            TimeframeCardDTO january = result.getTimeframes().get(0);
            assertThat(january.getPeriodDisplayName()).isEqualTo("January 2024");
            assertThat(january.getListenedDateFrom()).isEqualTo("2024-01-01");
            assertThat(january.getListenedDateTo()).isEqualTo("2024-01-31");
            assertThat(january.getPlayCount()).isEqualTo(5);
            assertThat(january.getArtistCount()).isEqualTo(2);
            assertThat(january.getAlbumCount()).isEqualTo(2);
            assertThat(january.getSongCount()).isEqualTo(3);
            assertThat(january.getTopArtistName()).isEqualTo("Selena");
            assertThat(january.getTopSongName()).isEqualTo("Bidi Bidi Bom Bom");

            TimeframeCardDTO march = result.getTimeframes().get(1);
            assertThat(march.getPlayCount()).isEqualTo(3);
            assertThat(march.getWinningGenderName()).isEqualTo("Male");
            assertThat(march.getWinningGenreName()).isEqualTo("Pop");
            assertThat(march.getWinningLanguageName()).isEqualTo("Spanish");
            assertThat(march.getWinningCountry()).isEqualTo("Puerto Rico");
            assertThat(march.getTopArtistName()).isEqualTo("Bad Bunny");
            assertThat(march.getTopAlbumName()).isEqualTo("Un Verano Sin Ti");
            assertThat(march.getTopSongName()).isEqualTo("Titi Me Pregunto");
        }
    }

    @Test
    void chartPreviewUsesCurrentWeekPlaysAndPriorChartHistory() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartService service = chartService(db);

            List<ChartEntryDTO> songs = service.getWeeklySongChartPreview("2024-W09");
            assertThat(songs).hasSize(1);

            ChartEntryDTO titi = songs.get(0);
            assertThat(titi.getPosition()).isEqualTo(1);
            assertThat(titi.getSongName()).isEqualTo("Titi Me Pregunto");
            assertThat(titi.getArtistName()).isEqualTo("Bad Bunny");
            assertThat(titi.getPlayCount()).isEqualTo(3);
            assertThat(titi.getGenreName()).isEqualTo("Pop");
            assertThat(titi.getLastWeekPosition()).isEqualTo(-1);
            assertThat(titi.getPeakPosition()).isEqualTo(1);
            assertThat(titi.getWeeksAtPeak()).isEqualTo(1);
            assertThat(titi.getWeeksOnChart()).isEqualTo(1);

            List<ChartEntryDTO> albums = service.getWeeklyAlbumChartPreview("2024-W09");
            assertThat(albums).hasSize(1);
            ChartEntryDTO verano = albums.get(0);
            assertThat(verano.getPosition()).isEqualTo(1);
            assertThat(verano.getAlbumName()).isEqualTo("Un Verano Sin Ti");
            assertThat(verano.getPlayCount()).isEqualTo(3);
            assertThat(verano.getPeakPosition()).isNull();
            assertThat(verano.getWeeksOnChart()).isZero();
        }
    }

    @Test
    void chartNumberOneHelpersReturnCountsNamesAndRunsInDisplayDateFormat() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartService service = chartService(db);

            assertThat(service.getNumberOneSongsCount(1)).isEqualTo(1);
            assertThat(service.getNumberOneWeeksCount(1)).isEqualTo(1);
            assertThat(service.getNumberOneSongNames(1)).containsExactly("Bidi Bidi Bom Bom");

            assertThat(service.getNumberOneSongsCount(2)).isEqualTo(1);
            assertThat(service.getNumberOneSongNames(2)).containsExactly("Titi Me Pregunto");

            assertThat(service.getNumberOneAlbumsCount(1)).isEqualTo(1);
            assertThat(service.getNumberOneAlbumNames(1)).containsExactly("Amor Prohibido");

            List<NumberOneRunDTO> runs = service.getNumberOneRuns("song");
            assertThat(runs).extracting(NumberOneRunDTO::getName)
                    .containsExactly("Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertThat(runs.get(0).getRunStartDate()).isEqualTo("08/01/2024");
            assertThat(runs.get(0).getRunEndDate()).isEqualTo("14/01/2024");
            assertThat(runs.get(0).getTotalWeeks()).isEqualTo(1);
            assertThat(runs.get(1).getRunStartDate()).isEqualTo("01/01/2024");
        }
    }

    private static ChartService chartService(TestDatabaseSupport db) {
        AppConfigService appConfigService = mock(AppConfigService.class);
        when(appConfigService.isCombineLinkedSongsEnabled()).thenReturn(false);

        return new ChartService(
                mock(ChartRepository.class),
                mock(ChartEntryRepository.class),
                db.jdbcTemplate,
                mock(ItunesService.class),
                appConfigService,
                mock(SongLinkService.class));
    }

    private static <T> Map<String, T> byName(List<T> rows, Function<T, String> keyExtractor) {
        return rows.stream().collect(Collectors.toMap(keyExtractor, Function.identity()));
    }
}
