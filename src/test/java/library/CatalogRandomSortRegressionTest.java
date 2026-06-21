package library;

import library.repository.EthnicityRepository;
import library.repository.GenreRepository;
import library.repository.LanguageRepository;
import library.repository.LookupRepository;
import library.repository.SubGenreRepository;
import library.service.CatalogWinningPeriodService;
import library.service.CountryService;
import library.service.EthnicityService;
import library.service.GenderService;
import library.service.GenreService;
import library.service.LanguageService;
import library.service.SubGenreService;
import library.service.TimeframeService;
import library.service.YearService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class CatalogRandomSortRegressionTest {

    @Test
    void secondaryCatalogsAcceptRandomSort() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            LookupRepository lookupRepository = new LookupRepository(db.jdbcTemplate);
            CatalogWinningPeriodService winningPeriodService = new CatalogWinningPeriodService(db.jdbcTemplate);

            assertThat(new CountryService(db.jdbcTemplate, winningPeriodService)
                    .getCountries(null, "random", "asc")).isNotEmpty();
            assertThat(new EthnicityService(mock(EthnicityRepository.class), lookupRepository, db.jdbcTemplate, winningPeriodService)
                    .getEthnicities(null, "random", "asc")).isNotEmpty();
            assertThat(new GenderService(lookupRepository, db.jdbcTemplate, winningPeriodService)
                    .getGenders(null, "random", "asc")).isNotEmpty();
            assertThat(new GenreService(mock(GenreRepository.class), lookupRepository, db.jdbcTemplate, winningPeriodService)
                    .getGenres(null, "random", "asc")).isNotEmpty();
            assertThat(new LanguageService(mock(LanguageRepository.class), lookupRepository, db.jdbcTemplate, winningPeriodService)
                    .getLanguages(null, "random", "asc")).isNotEmpty();
            assertThat(new SubGenreService(mock(SubGenreRepository.class), lookupRepository, db.jdbcTemplate, winningPeriodService)
                    .getSubGenres(null, null, "random", "asc")).isNotEmpty();
            assertThat(new YearService(db.jdbcTemplate).getListenYears("random", "asc")).isNotEmpty();
            assertThat(new YearService(db.jdbcTemplate).getReleaseYears("random", "asc")).isNotEmpty();
            assertThat(new TimeframeService(db.jdbcTemplate).getTimeframeCardsWithCount(
                    "months",
                    null, null, null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null,
                    1, null,
                    null, null,
                    null, null, null, null, null, null, null, null, null, null,
                    null, null,
                    null, null,
                    "random", "asc", 0, 10).getTimeframes()).isNotEmpty();
        }
    }

    @Test
    void seededRandomSortIsDeterministicForSecondaryCatalogs() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            LookupRepository lookupRepository = new LookupRepository(db.jdbcTemplate);
            CatalogWinningPeriodService winningPeriodService = new CatalogWinningPeriodService(db.jdbcTemplate);

            CountryService countryService = new CountryService(db.jdbcTemplate, winningPeriodService);
            assertThat(countryService.getCountries(null, "random", "asc", 8675309).stream()
                    .map(country -> country.getName())
                    .toList())
                    .containsExactlyElementsOf(countryService.getCountries(null, "random", "asc", 8675309).stream()
                            .map(country -> country.getName())
                            .toList());

            GenderService genderService = new GenderService(lookupRepository, db.jdbcTemplate, winningPeriodService);
            assertThat(genderService.getGenders(null, "random", "asc", 8675309).stream()
                    .map(gender -> gender.getId())
                    .toList())
                    .containsExactlyElementsOf(genderService.getGenders(null, "random", "asc", 8675309).stream()
                            .map(gender -> gender.getId())
                            .toList());
        }
    }

    @Test
    void seededRandomSortIsDeterministicForYearsAndTimeframes() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            YearService yearService = new YearService(db.jdbcTemplate);
            assertThat(yearService.getListenYears("random", "asc", 12345).stream()
                    .map(year -> year.getYear())
                    .toList())
                    .containsExactlyElementsOf(yearService.getListenYears("random", "asc", 12345).stream()
                            .map(year -> year.getYear())
                            .toList());
            assertThat(yearService.getReleaseYears("random", "asc", 12345).stream()
                    .map(year -> year.getYear())
                    .toList())
                    .containsExactlyElementsOf(yearService.getReleaseYears("random", "asc", 12345).stream()
                            .map(year -> year.getYear())
                            .toList());

            TimeframeService timeframeService = new TimeframeService(db.jdbcTemplate);
            assertThat(timeframeService.getTimeframeCardsWithCount(
                    "months",
                    null, null, null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null,
                    1, null,
                    null, null,
                    null, null, null, null, null, null, null, null, null, null,
                    null, null,
                    null, null,
                    "random", "asc", 12345, 0, 10).getTimeframes().stream()
                    .map(timeframe -> timeframe.getPeriodKey())
                    .toList())
                    .containsExactlyElementsOf(timeframeService.getTimeframeCardsWithCount(
                            "months",
                            null, null, null, null, null, null, null, null, null, null,
                            null, null, null, null, null, null,
                            1, null,
                            null, null,
                            null, null, null, null, null, null, null, null, null, null,
                            null, null,
                            null, null,
                            "random", "asc", 12345, 0, 10).getTimeframes().stream()
                            .map(timeframe -> timeframe.getPeriodKey())
                            .toList());
        }
    }
}
