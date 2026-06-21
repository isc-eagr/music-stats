package library;

import library.dto.AlbumStatsRow;
import library.dto.ArtistStatsRow;
import library.dto.SongStatsRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static library.TestDatabaseSupport.albumQueryWith;
import static library.TestDatabaseSupport.artistQueryWith;
import static library.TestDatabaseSupport.mapOf;
import static library.TestDatabaseSupport.songQueryWith;
import static org.assertj.core.api.Assertions.assertThat;

class CatalogFilterRegressionTest {

    @Test
    void artistLookupTagImageThemeAndItunesFiltersCoverAllModes() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf("name", "sel"), "Selena");

            assertArtistNames(db, mapOf("genderIds", List.of(2), "genderMode", "includes"), "Guest Singer", "Selena");
            assertArtistNames(db, mapOf("genderIds", List.of(2), "genderMode", "excludes"),
                    "Bad Bunny", "Legacy Legend", "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("genreMode", "isnull"), "Mystery Artist");
            assertArtistNames(db, mapOf("genreMode", "isnotnull"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Selena", "The Static Hearts");

            assertArtistNames(db, mapOf("ethnicityIds", List.of(1), "ethnicityMode", "includes"),
                    "Bad Bunny", "Guest Singer", "Selena");
            assertArtistNames(db, mapOf("subgenreIds", List.of(2), "subgenreMode", "includes"),
                    "Legacy Legend", "The Static Hearts");
            assertArtistNames(db, mapOf("languageIds", List.of(2), "languageMode", "includes"),
                    "Legacy Legend", "The Static Hearts");
            assertArtistNames(db, mapOf("countries", List.of("Mexico"), "countryMode", "includes"),
                    "Legacy Legend", "Selena");
            assertArtistNames(db, mapOf("countries", List.of("Mexico"), "countryMode", "excludes"),
                    "Bad Bunny", "Guest Singer", "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("countryMode", "isnull"), "Mystery Artist");

            assertArtistNames(db, mapOf("tagIds", List.of(10), "tagMode", "includes"), "Selena");
            assertArtistNames(db, mapOf("tagIds", List.of(10), "tagMode", "excludes"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("tagMode", "isnotnull"), "Bad Bunny", "Selena");
            assertArtistNames(db, mapOf("tagMode", "isnull"),
                    "Guest Singer", "Legacy Legend", "Mystery Artist", "The Static Hearts");

            assertArtistNames(db, mapOf("imageTheme", 1, "imageThemeMode", "has"), "Selena");
            assertArtistNames(db, mapOf("imageTheme", 1, "imageThemeMode", "doesntHave"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("itunesIdsJson", "[1,2]", "inItunes", "true"), "Bad Bunny", "Selena");
            assertArtistNames(db, mapOf("itunesIdsJson", "[1,2]", "inItunes", "false"),
                    "Guest Singer", "Legacy Legend", "Mystery Artist", "The Static Hearts");
        }
    }

    @Test
    void artistDateNumericBooleanAndPlayFiltersCoverCatalogBranches() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf("birthDate", "1994-03-10", "birthDateMode", "exact"), "Bad Bunny");
            assertArtistNames(db, mapOf("birthDate", "1980-01-01", "birthDateMode", "gte"),
                    "Bad Bunny", "Guest Singer", "The Static Hearts");
            assertArtistNames(db, mapOf("birthDateFrom", "1970-01-01", "birthDateTo", "1985-01-01", "birthDateMode", "between"),
                    "Selena", "The Static Hearts");
            assertArtistNames(db, mapOf("birthDateMode", "isnull"), "Mystery Artist");
            assertArtistNames(db, mapOf("deathDate", "2020-12-31", "deathDateMode", "exact"), "Legacy Legend");
            assertArtistNames(db, mapOf("deathDateMode", "isnotnull"), "Legacy Legend");

            assertArtistNames(db, mapOf("ageMin", 70), "Legacy Legend");
            assertArtistNames(db, mapOf("ageMax", 40), "Bad Bunny", "Guest Singer");
            assertArtistNames(db, mapOf("organized", "true"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Selena");
            assertArtistNames(db, mapOf("organized", "false"), "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("imageCountMin", 2), "Selena");
            assertArtistNames(db, mapOf("imageCountMax", 0),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("isBand", "true"), "The Static Hearts");
            assertArtistNames(db, mapOf("isBand", "false"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Mystery Artist", "Selena");

            assertArtistNames(db, mapOf("accounts", List.of("robertlover"), "accountMode", "includes"),
                    "Bad Bunny", "Selena");
            assertArtistNames(db, mapOf("listenedDateFrom", "2024-03-01", "listenedDateTo", "2024-03-31"), "Bad Bunny");
            assertArtistNames(db, mapOf("firstListenedDate", "2024-01-01", "firstListenedDateMode", "exact"), "Selena");
            assertArtistNames(db, mapOf("lastListenedDate", "2024-05-01", "lastListenedDateMode", "exact"), "Legacy Legend");
            assertArtistNames(db, mapOf("playCountMin", 5), "Bad Bunny", "Selena");
            assertArtistNames(db, mapOf("playCountMax", 0), "Guest Singer", "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("albumCountMin", 1),
                    "Bad Bunny", "Legacy Legend", "Mystery Artist", "Selena", "The Static Hearts");
            assertArtistNames(db, mapOf("songCountMax", 0), "Guest Singer");
            assertArtistNames(db, mapOf("itunesPresenceMin", 100, "itunesSongIdsJson", "[3,4]"), "Bad Bunny");
        }
    }

    @Test
    void albumFiltersCoverLookupsDatesCountsChartsAndBooleanBranches() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertAlbumNames(db, mapOf("name", "verano"), "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("artistName", List.of(1)), "Amor Prohibido");
            assertAlbumNames(db, mapOf("genreIds", List.of(2), "genreMode", "includes"),
                    "Amor Prohibido", "Legacy Collection", "Silent Record");
            assertAlbumNames(db, mapOf("genreMode", "isnull"), "Unknown Album");
            assertAlbumNames(db, mapOf("subgenreIds", List.of(2), "subgenreMode", "includes"),
                    "Amor Prohibido", "Legacy Collection", "Silent Record");
            assertAlbumNames(db, mapOf("languageIds", List.of(2), "languageMode", "includes"),
                    "Legacy Collection", "Silent Record");
            assertAlbumNames(db, mapOf("genderIds", List.of(2), "genderMode", "includes"), "Amor Prohibido");
            assertAlbumNames(db, mapOf("ethnicityIds", List.of(2), "ethnicityMode", "includes"),
                    "Legacy Collection", "Silent Record");
            assertAlbumNames(db, mapOf("countries", List.of("Mexico"), "countryMode", "excludes"),
                    "Silent Record", "Un Verano Sin Ti", "Unknown Album");
            assertAlbumNames(db, mapOf("tagIds", List.of(10), "tagMode", "includes"), "Amor Prohibido");
            assertAlbumNames(db, mapOf("tagMode", "isnull"), "Legacy Collection", "Silent Record", "Unknown Album");

            assertAlbumNames(db, mapOf("releaseDate", "2022-05-06", "releaseDateMode", "exact"), "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("releaseDateMode", "isnull"), "Unknown Album");
            assertAlbumNames(db, mapOf("birthDate", "1970-01-01", "birthDateMode", "gte"),
                    "Amor Prohibido", "Silent Record", "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("deathDateMode", "isnotnull"), "Legacy Collection");
            assertAlbumNames(db, mapOf("ageMin", 70), "Legacy Collection");
            assertAlbumNames(db, mapOf("ageAtReleaseMax", 23), "Amor Prohibido");

            assertAlbumNames(db, mapOf("accounts", List.of("robertlover"), "accountMode", "includes"),
                    "Amor Prohibido", "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("firstListenedDate", "2024-01-01", "firstListenedDateMode", "exact"), "Amor Prohibido");
            assertAlbumNames(db, mapOf("lastListenedDate", "2024-05-01", "lastListenedDateMode", "exact"), "Legacy Collection");
            assertAlbumNames(db, mapOf("listenedDateFrom", "2024-03-01", "listenedDateTo", "2024-03-31"), "Un Verano Sin Ti");

            assertAlbumNames(db, mapOf("organized", "false"), "Silent Record", "Unknown Album");
            assertAlbumNames(db, mapOf("imageCountMin", 2), "Amor Prohibido");
            assertAlbumNames(db, mapOf("hasFeaturedArtists", "true"), "Amor Prohibido");
            assertAlbumNames(db, mapOf("hasFeaturedArtists", "false"),
                    "Legacy Collection", "Silent Record", "Un Verano Sin Ti", "Unknown Album");
            assertAlbumNames(db, mapOf("isBand", "true"), "Silent Record");
            assertAlbumNames(db, mapOf("itunesIdsJson", "[1,5]", "inItunes", "true"), "Amor Prohibido", "Legacy Collection");
            assertAlbumNames(db, mapOf("playCountMin", 5), "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("songCountMin", 2), "Amor Prohibido", "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("lengthMode", "null"), "Unknown Album");
            assertAlbumNames(db, mapOf("lengthMin", 400, "lengthMode", "range"), "Amor Prohibido", "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("lastFullListenDate", "2024-02-01", "lastFullListenDateMode", "exact"), "Amor Prohibido");
            assertAlbumNames(db, mapOf("lastFullListenDateMode", "isnull"),
                    "Silent Record", "Un Verano Sin Ti", "Unknown Album");
            assertAlbumNames(db, mapOf("itunesPresenceMin", 100, "itunesSongIdsJson", "[1,2,3,4,8]"),
                    "Amor Prohibido", "Legacy Collection", "Un Verano Sin Ti");

            assertAlbumNames(db, mapOf("weeklyChartPeak", 1, "weeklyChartPeakMode", "exact"), "Amor Prohibido");
            assertAlbumNames(db, mapOf("seasonalChartPeak", 1), "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("yearlyChartPeak", 2), "Amor Prohibido");
        }
    }

    @Test
    void songFiltersCoverLookupsDatesCountsChartsCountdownsAndBooleanBranches() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertSongNames(db, mapOf("name", "titi"), "Titi Me Pregunto");
            assertSongNames(db, mapOf("artistName", List.of(1)), "Bidi Bidi Bom Bom", "No Me Queda Mas", "Standalone Jam");
            assertSongNames(db, mapOf("albumName", "Verano"), "Ojitos Lindos", "Titi Me Pregunto");
            assertSongNames(db, mapOf("genreIds", List.of(2), "genreMode", "includes"),
                    "Bidi Bidi Bom Bom", "Old Hit", "Quiet Track");
            assertSongNames(db, mapOf("genreMode", "isnull"), "Unknown Silence");
            assertSongNames(db, mapOf("subgenreIds", List.of(3), "subgenreMode", "includes"),
                    "No Me Queda Mas", "Ojitos Lindos", "Titi Me Pregunto");
            assertSongNames(db, mapOf("languageIds", List.of(2), "languageMode", "includes"),
                    "No Me Queda Mas", "Old Hit", "Quiet Track");
            assertSongNames(db, mapOf("genderIds", List.of(2), "genderMode", "includes"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Standalone Jam");
            assertSongNames(db, mapOf("ethnicityIds", List.of(2), "ethnicityMode", "includes"), "Old Hit", "Quiet Track");
            assertSongNames(db, mapOf("countries", List.of("Mexico"), "countryMode", "includes"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Old Hit", "Standalone Jam");
            assertSongNames(db, mapOf("countryMode", "isnull"), "Unknown Silence");
            assertSongNames(db, mapOf("tagIds", List.of(10), "tagMode", "includes"), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("tagMode", "isnotnull"), "Bidi Bidi Bom Bom", "Titi Me Pregunto");

            assertSongNames(db, mapOf("releaseDate", "1995-01-01", "releaseDateMode", "exact"), "Standalone Jam");
            assertSongNames(db, mapOf("releaseDateMode", "isnull"), "Unknown Silence");
            assertSongNames(db, mapOf("firstListenedDate", "2024-01-01", "firstListenedDateMode", "exact"), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("lastListenedDate", "2024-05-01", "lastListenedDateMode", "exact"), "Old Hit");
            assertSongNames(db, mapOf("listenedDateFrom", "2024-03-01", "listenedDateTo", "2024-03-31"), "Titi Me Pregunto");
            assertSongNames(db, mapOf("birthDate", "1950-01-01", "birthDateMode", "exact"), "Old Hit");
            assertSongNames(db, mapOf("deathDateMode", "isnotnull"), "Old Hit");
            assertSongNames(db, mapOf("ageMin", 70), "Old Hit");
            assertSongNames(db, mapOf("ageAtReleaseMax", 23), "Bidi Bidi Bom Bom", "No Me Queda Mas", "Standalone Jam");

            assertSongNames(db, mapOf("organized", "false"), "Quiet Track", "Standalone Jam", "Unknown Silence");
            assertSongNames(db, mapOf("imageCountMin", 1), "Bidi Bidi Bom Bom", "No Me Queda Mas");
            assertSongNames(db, mapOf("hasFeaturedArtists", "true"), "No Me Queda Mas");
            assertSongNames(db, mapOf("hasFeaturedArtists", "false"),
                    "Bidi Bidi Bom Bom", "Ojitos Lindos", "Old Hit", "Quiet Track", "Standalone Jam", "Titi Me Pregunto", "Unknown Silence");
            assertSongNames(db, mapOf("isBand", "true"), "Quiet Track");
            assertSongNames(db, mapOf("isSingle", "true"), "Old Hit", "Standalone Jam");
            assertSongNames(db, mapOf("itunesIdsJson", "[1,3]", "inItunes", "true"), "Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertSongNames(db, mapOf("playCountMin", 5), "Titi Me Pregunto");
            assertSongNames(db, mapOf("trackNumber", 2, "trackNumberMode", "exact"), "No Me Queda Mas", "Ojitos Lindos");
            assertSongNames(db, mapOf("trackNumberMode", "isnull"), "Standalone Jam", "Unknown Silence");
            assertSongNames(db, mapOf("lengthMode", "null"), "Unknown Silence");
            assertSongNames(db, mapOf("lengthMin", 240, "lengthMode", "range"), "Ojitos Lindos", "Old Hit", "Titi Me Pregunto");

            assertSongNames(db, mapOf("weeklyChartPeak", 1, "weeklyChartPeakMode", "exact", "includeExpensiveStats", true),
                    "Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertSongNames(db, mapOf("weeklyChartWeeks", 2, "includeExpensiveStats", true), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("seasonalChartPeak", 1, "includeExpensiveStats", true), "Titi Me Pregunto");
            assertSongNames(db, mapOf("yearlyChartPeak", 2, "includeExpensiveStats", true), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("trlPeak", 1, "trlPeakMode", "exact", "includeExpensiveStats", true), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("trlDays", 2, "includeExpensiveStats", true), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("trlDaysAtPeak", 1, "trlDaysAtPeakMode", "exact", "includeExpensiveStats", true), "Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertSongNames(db, mapOf("vatosCuntdownPeak", 1, "vatosCuntdownPeakMode", "exact", "includeExpensiveStats", true), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("vatosCuntdownDaysAtPeak", 2, "vatosCuntdownDaysAtPeakMode", "exact", "includeExpensiveStats", true), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("billboardPeak", 1, "billboardPeakMode", "exact", "includeExpensiveStats", true), "Titi Me Pregunto");
            assertSongNames(db, mapOf("billboardWeeks", 20, "includeExpensiveStats", true), "Titi Me Pregunto");
            assertSongNames(db, mapOf("billboardWeeksAtPeak", 3, "billboardWeeksAtPeakMode", "exact", "includeExpensiveStats", true), "Titi Me Pregunto");
            assertSongNames(db, mapOf("songIdsFilter", List.of(1, 3)), "Bidi Bidi Bom Bom", "Titi Me Pregunto");
        }
    }

    @Test
    void combinedFiltersCatchParameterOrderingRegressionsAcrossCatalogs() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf(
                    "genderIds", List.of(2), "genderMode", "includes",
                    "tagIds", List.of(10), "tagMode", "includes",
                    "accounts", List.of("vatito"), "accountMode", "includes",
                    "listenedDateFrom", "2024-01-01", "listenedDateTo", "2024-02-28",
                    "playCountMin", 3),
                    "Selena");

            assertAlbumNames(db, mapOf(
                    "genreIds", List.of(2), "genreMode", "includes",
                    "genderIds", List.of(2), "genderMode", "includes",
                    "hasFeaturedArtists", "true",
                    "releaseDateFrom", "1990-01-01", "releaseDateTo", "1999-12-31", "releaseDateMode", "between",
                    "playCountMin", 4),
                    "Amor Prohibido");

            assertSongNames(db, mapOf(
                    "genreIds", List.of(3), "genreMode", "includes",
                    "languageIds", List.of(2), "languageMode", "includes",
                    "hasFeaturedArtists", "true",
                    "trackNumber", 2, "trackNumberMode", "exact",
                    "lengthMin", 180, "lengthMax", 220, "lengthMode", "range"),
                    "No Me Queda Mas");
        }
    }

    @Test
    void remainingCatalogFilterBoundsAndWindowsAreCovered() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf("albumCountMax", 0), "Guest Singer");
            assertArtistNames(db, mapOf("songCountMin", 3), "Selena");
            assertArtistNames(db, mapOf(
                    "firstListenedDateFrom", "2024-01-01",
                    "firstListenedDateTo", "2024-01-04",
                    "firstListenedDateMode", "between"),
                    "Bad Bunny", "Selena");
            assertArtistNames(db, mapOf(
                    "lastListenedDateFrom", "2024-04-01",
                    "lastListenedDateTo", "2024-05-01",
                    "lastListenedDateMode", "between"),
                    "Legacy Legend", "Selena");
            assertArtistNames(db, mapOf(
                    "deathDateFrom", "2020-01-01",
                    "deathDateTo", "2021-01-01",
                    "deathDateMode", "between"),
                    "Legacy Legend");
            assertThat(db.artistRepository.findArtistsWithStats(artistQueryWith(mapOf(
                    "itunesPresenceMax", 0,
                    "itunesSongIdsJson", "[3,4]"))))
                    .extracting(ArtistStatsRow::name)
                    .contains("Legacy Legend", "Mystery Artist", "Selena", "The Static Hearts");

            assertAlbumNames(db, mapOf("imageCountMax", 0), "Legacy Collection", "Silent Record", "Un Verano Sin Ti", "Unknown Album");
            assertAlbumNames(db, mapOf("ageAtReleaseMin", 25), "Silent Record", "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("playCountMax", 1), "Legacy Collection", "Silent Record", "Unknown Album");
            assertAlbumNames(db, mapOf("songCountMax", 1), "Legacy Collection", "Silent Record", "Unknown Album");
            assertAlbumNames(db, mapOf("lengthMax", 240, "lengthMode", "range"), "Legacy Collection", "Silent Record", "Unknown Album");
            assertAlbumNames(db, mapOf("weeklyChartWeeks", 1), "Amor Prohibido");
            assertAlbumNames(db, mapOf("weeklyChartPeakWeeks", 1, "weeklyChartPeakWeeksMode", "exact"), "Amor Prohibido");
            assertAlbumNames(db, mapOf(
                    "weeklyChartPeak", 1,
                    "weeklyChartDateFrom", "2024-01-01",
                    "weeklyChartDateTo", "2024-01-07"),
                    "Amor Prohibido");
            assertAlbumNames(db, mapOf("seasonalChartSeasons", 1), "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf(
                    "seasonalChartPeak", 1,
                    "seasonalChartDateFrom", "2024-03-01",
                    "seasonalChartDateTo", "2024-05-31"),
                    "Un Verano Sin Ti");
            assertAlbumNames(db, mapOf("yearlyChartYears", 1), "Amor Prohibido");
            assertAlbumNames(db, mapOf(
                    "yearlyChartPeak", 2,
                    "yearlyChartDateFrom", "2024-01-01",
                    "yearlyChartDateTo", "2024-12-31"),
                    "Amor Prohibido");
            assertAlbumNames(db, mapOf(
                    "lastFullListenDateFrom", "2024-02-01",
                    "lastFullListenDateTo", "2024-05-01",
                    "lastFullListenDateMode", "between"),
                    "Amor Prohibido", "Legacy Collection");
            assertAlbumNames(db, mapOf(
                    "itunesPresenceMax", 0,
                    "itunesSongIdsJson", "[1,2,3,4,8]"),
                    "Silent Record", "Unknown Album");

            assertSongNames(db, mapOf("imageCountMax", 0),
                    "Ojitos Lindos", "Old Hit", "Quiet Track", "Standalone Jam", "Titi Me Pregunto", "Unknown Silence");
            assertSongNames(db, mapOf("ageAtReleaseMin", 24),
                    "Ojitos Lindos", "Old Hit", "Quiet Track", "Titi Me Pregunto");
            assertSongNames(db, mapOf("playCountMax", 1),
                    "No Me Queda Mas", "Ojitos Lindos", "Old Hit", "Quiet Track", "Unknown Silence");
            assertSongNames(db, mapOf("lengthMax", 200, "lengthMode", "range"),
                    "No Me Queda Mas", "Quiet Track", "Standalone Jam");
            assertSongNames(db, mapOf("trackNumberMode", "isnotnull"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Old Hit", "Quiet Track", "Titi Me Pregunto");
            assertSongNames(db, mapOf(
                    "firstListenedDateFrom", "2024-01-01",
                    "firstListenedDateTo", "2024-01-03",
                    "firstListenedDateMode", "between"),
                    "Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertSongNames(db, mapOf(
                    "lastListenedDateFrom", "2024-04-01",
                    "lastListenedDateTo", "2024-05-01",
                    "lastListenedDateMode", "between"),
                    "Old Hit", "Standalone Jam");
            assertSongNames(db, mapOf(
                    "deathDateFrom", "2020-01-01",
                    "deathDateTo", "2021-01-01",
                    "deathDateMode", "between"),
                    "Old Hit");
            assertSongNames(db, mapOf("weeklyChartPeakWeeks", 1, "weeklyChartPeakWeeksMode", "exact", "includeExpensiveStats", true),
                    "Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertSongNames(db, mapOf(
                    "trlPeak", 1,
                    "trlDateFrom", "2024-01-01",
                    "trlDateTo", "2024-01-31",
                    "includeExpensiveStats", true),
                    "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("vatosCuntdownDays", 1, "includeExpensiveStats", true),
                    "Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertSongNames(db, mapOf(
                    "billboardPeak", 1,
                    "billboardDateFrom", "2024-03-11",
                    "billboardDateTo", "2024-03-25",
                    "includeExpensiveStats", true),
                    "Titi Me Pregunto");
            assertSongNames(db, mapOf("seasonalChartSeasons", 1, "includeExpensiveStats", true), "Titi Me Pregunto");
            assertSongNames(db, mapOf("yearlyChartYears", 1, "includeExpensiveStats", true), "Bidi Bidi Bom Bom");
            assertSongNames(db, mapOf("itunesIdsJson", "[1,3]", "inItunes", "false"),
                    "No Me Queda Mas", "Ojitos Lindos", "Old Hit", "Quiet Track", "Standalone Jam", "Unknown Silence");
        }
    }

    private static void assertArtistNames(TestDatabaseSupport db, Map<String, Object> overrides, String... expectedNames) {
        List<String> actual = db.artistRepository.findArtistsWithStats(artistQueryWith(overrides)).stream()
                .map(ArtistStatsRow::name)
                .toList();
        assertThat(actual).containsExactly(expectedNames);
    }

    private static void assertAlbumNames(TestDatabaseSupport db, Map<String, Object> overrides, String... expectedNames) {
        List<String> actual = db.albumRepository.findAlbumsWithStats(albumQueryWith(overrides)).stream()
                .map(AlbumStatsRow::name)
                .toList();
        assertThat(actual).containsExactly(expectedNames);
    }

    private static void assertSongNames(TestDatabaseSupport db, Map<String, Object> overrides, String... expectedNames) {
        List<String> actual = db.songRepository.findSongsWithStats(songQueryWith(overrides)).stream()
                .map(SongStatsRow::name)
                .toList();
        assertThat(actual).containsExactly(expectedNames);
    }
}
