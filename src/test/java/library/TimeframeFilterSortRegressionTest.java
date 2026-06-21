package library;

import library.dto.TimeframeCardDTO;
import library.dto.TimeframeResultDTO;
import library.service.TimeframeService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static library.TestDatabaseSupport.mapOf;
import static org.assertj.core.api.Assertions.assertThat;

class TimeframeFilterSortRegressionTest {

    @Test
    void everyPeriodTypeSupportsCoreFiltering() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            for (String periodType : List.of("days", "weeks", "months", "seasons", "years", "decades")) {
                TimeframeResultDTO result = timeframes(db, mapOf(
                        "periodType", periodType,
                        "playsMin", 1,
                        "sortBy", "plays",
                        "sortDir", "desc"));

                assertThat(result.getTimeframes())
                        .as("period type %s", periodType)
                        .isNotEmpty()
                        .allSatisfy(card -> assertThat(card.getPlayCount()).isGreaterThan(0));
            }
        }
    }

    @Test
    void timeframeWinningAttributeFiltersCoverIncludesExcludesAndMultiCriteria() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertTimeframeKeys(db, mapOf(
                    "periodType", "months",
                    "dateFrom", "01/03/2024",
                    "dateTo", "31/03/2024",
                    "winningGender", List.of(1), "winningGenderMode", "includes",
                    "winningGenre", List.of(1), "winningGenreMode", "includes",
                    "winningEthnicity", List.of(1), "winningEthnicityMode", "includes",
                    "winningLanguage", List.of(1), "winningLanguageMode", "includes",
                    "winningCountry", List.of("Puerto Rico"), "winningCountryMode", "includes",
                    "playsMin", 3,
                    "sortBy", "plays", "sortDir", "desc"),
                    "2024-03");

            assertTimeframeKeys(db, mapOf(
                    "periodType", "months",
                    "dateFrom", "01/03/2024",
                    "dateTo", "31/03/2024",
                    "winningCountry", List.of("Mexico"), "winningCountryMode", "excludes",
                    "winningGenre", List.of(2), "winningGenreMode", "excludes",
                    "playsMin", 3),
                    "2024-03");
        }
    }

    @Test
    void timeframeSummaryFiltersCoverAllNumericBoundsAndMaleDays() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertTimeframeKeys(db, mapOf(
                    "periodType", "months",
                    "artistCountMin", 2, "artistCountMax", 2,
                    "albumCountMin", 2, "albumCountMax", 2,
                    "songCountMin", 3, "songCountMax", 3,
                    "playsMin", 5, "playsMax", 5,
                    "timeMin", 1000L, "timeMax", 1200L,
                    "maleArtistPctMin", 40.0, "maleArtistPctMax", 60.0,
                    "maleAlbumPctMin", 40.0, "maleAlbumPctMax", 60.0,
                    "maleSongPctMin", 60.0, "maleSongPctMax", 70.0,
                    "malePlayPctMin", 50.0, "malePlayPctMax", 70.0,
                    "maleTimePctMin", 50.0, "maleTimePctMax", 60.0,
                    "dateFrom", "01/01/2024", "dateTo", "31/01/2024",
                    "maleDaysMin", 3, "maleDaysMax", 3,
                    "sortBy", "plays", "sortDir", "desc"),
                    "2024-01");
        }
    }

    @Test
    void timeframeSortOptionsWorkOneAtATime() {
        List<String> sortOptions = List.of(
                "plays", "time", "artists", "albums", "songs",
                "maleartistpct", "malealbumpct", "malesongpct", "maleplaypct", "maletimepct", "maledays", "random");

        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            for (String sortBy : sortOptions) {
                TimeframeResultDTO result = timeframes(db, mapOf(
                        "periodType", "months",
                        "playsMin", 1,
                        "sortBy", sortBy,
                        "sortDir", "desc"));
                assertThat(result.getTimeframes())
                        .as("timeframe sort %s", sortBy)
                        .isNotEmpty();
            }

            assertTimeframeKeys(db, mapOf(
                    "periodType", "months",
                    "playsMin", 1,
                    "sortBy", "plays",
                    "sortDir", "desc"),
                    "2024-01", "2024-03", "2024-04", "2024-02", "2024-05");
        }
    }

    private static void assertTimeframeKeys(TestDatabaseSupport db, Map<String, Object> overrides, String... expectedKeys) {
        List<String> actual = timeframes(db, overrides).getTimeframes().stream()
                .map(TimeframeCardDTO::getPeriodKey)
                .toList();
        assertThat(actual).containsExactly(expectedKeys);
    }

    @SuppressWarnings("unchecked")
    private static TimeframeResultDTO timeframes(TestDatabaseSupport db, Map<String, Object> overrides) {
        TimeframeService service = new TimeframeService(db.jdbcTemplate);
        String periodType = (String) overrides.getOrDefault("periodType", "months");
        String sortBy = (String) overrides.getOrDefault("sortBy", "plays");
        String sortDir = (String) overrides.getOrDefault("sortDir", "desc");
        int page = (Integer) overrides.getOrDefault("page", 0);
        int perPage = (Integer) overrides.getOrDefault("perPage", 20);

        return service.getTimeframeCardsWithCount(
                periodType,
                (List<Integer>) overrides.get("winningGender"), (String) overrides.get("winningGenderMode"),
                (List<Integer>) overrides.get("winningGenre"), (String) overrides.get("winningGenreMode"),
                (List<Integer>) overrides.get("winningEthnicity"), (String) overrides.get("winningEthnicityMode"),
                (List<Integer>) overrides.get("winningLanguage"), (String) overrides.get("winningLanguageMode"),
                (List<String>) overrides.get("winningCountry"), (String) overrides.get("winningCountryMode"),
                (Integer) overrides.get("artistCountMin"), (Integer) overrides.get("artistCountMax"),
                (Integer) overrides.get("albumCountMin"), (Integer) overrides.get("albumCountMax"),
                (Integer) overrides.get("songCountMin"), (Integer) overrides.get("songCountMax"),
                (Integer) overrides.get("playsMin"), (Integer) overrides.get("playsMax"),
                (Long) overrides.get("timeMin"), (Long) overrides.get("timeMax"),
                (Double) overrides.get("maleArtistPctMin"), (Double) overrides.get("maleArtistPctMax"),
                (Double) overrides.get("maleAlbumPctMin"), (Double) overrides.get("maleAlbumPctMax"),
                (Double) overrides.get("maleSongPctMin"), (Double) overrides.get("maleSongPctMax"),
                (Double) overrides.get("malePlayPctMin"), (Double) overrides.get("malePlayPctMax"),
                (Double) overrides.get("maleTimePctMin"), (Double) overrides.get("maleTimePctMax"),
                (String) overrides.get("dateFrom"), (String) overrides.get("dateTo"),
                (Integer) overrides.get("maleDaysMin"), (Integer) overrides.get("maleDaysMax"),
                sortBy, sortDir, page, perPage);
    }
}
