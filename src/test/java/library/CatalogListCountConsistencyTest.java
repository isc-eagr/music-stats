package library;

import library.dto.AlbumStatsQuery;
import library.dto.AlbumStatsRow;
import library.dto.ArtistStatsQuery;
import library.dto.ArtistStatsRow;
import library.dto.SongStatsQuery;
import library.dto.SongStatsRow;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static library.TestDatabaseSupport.albumQueryWith;
import static library.TestDatabaseSupport.artistQueryWith;
import static library.TestDatabaseSupport.mapOf;
import static library.TestDatabaseSupport.songQueryWith;
import static org.assertj.core.api.Assertions.assertThat;

class CatalogListCountConsistencyTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("artistFilterCases")
    void artistListTotalAndGenderCountsMatchFilteredRows(String caseName, Map<String, Object> overrides) {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ArtistStatsQuery query = artistQueryWith(overrides);
            List<ArtistStatsRow> rows = db.artistRepository.findArtistsWithStats(query);

            assertThat(db.artistRepository.countArtistsWithFilters(query))
                    .as(caseName + " total count")
                    .isEqualTo(rows.size());
            assertThat(db.artistRepository.countArtistsByGenderWithFilters(query))
                    .as(caseName + " gender chips")
                    .isEqualTo(countBy(rows, ArtistStatsRow::genderId));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("albumFilterCases")
    void albumListTotalAndGenderCountsMatchFilteredRows(String caseName, Map<String, Object> overrides) {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            AlbumStatsQuery query = albumQueryWith(overrides);
            List<AlbumStatsRow> rows = db.albumRepository.findAlbumsWithStats(query);

            assertThat(countAlbums(db, query))
                    .as(caseName + " total count")
                    .isEqualTo(rows.size());
            assertThat(countAlbumsByGender(db, query))
                    .as(caseName + " gender chips")
                    .isEqualTo(countBy(rows, AlbumStatsRow::genderId));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("songFilterCases")
    void songListTotalAndGenderCountsMatchFilteredRows(String caseName, Map<String, Object> overrides) {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            SongStatsQuery query = songQueryWith(overrides);
            List<SongStatsRow> rows = db.songRepository.findSongsWithStats(query);

            assertThat(countSongs(db, query))
                    .as(caseName + " total count")
                    .isEqualTo(rows.size());
            assertThat(countSongsByGender(db, query))
                    .as(caseName + " gender chips")
                    .isEqualTo(countBy(rows, SongStatsRow::genderId));
        }
    }

    private static Stream<Arguments> artistFilterCases() {
        return namedCases(
                entry("name", mapOf("name", "sel")),
                entry("gender includes", mapOf("genderIds", List.of(2), "genderMode", "includes")),
                entry("gender excludes keeps nulls", mapOf("genderIds", List.of(2), "genderMode", "excludes")),
                entry("gender is null", mapOf("genderMode", "isnull")),
                entry("gender is not null", mapOf("genderMode", "isnotnull")),
                entry("ethnicity includes", mapOf("ethnicityIds", List.of(1), "ethnicityMode", "includes")),
                entry("genre includes", mapOf("genreIds", List.of(2), "genreMode", "includes")),
                entry("genre is null", mapOf("genreMode", "isnull")),
                entry("subgenre includes", mapOf("subgenreIds", List.of(2), "subgenreMode", "includes")),
                entry("language includes", mapOf("languageIds", List.of(2), "languageMode", "includes")),
                entry("country includes", mapOf("countries", List.of("Mexico"), "countryMode", "includes")),
                entry("country excludes", mapOf("countries", List.of("Mexico"), "countryMode", "excludes")),
                entry("country is null", mapOf("countryMode", "isnull")),
                entry("tag includes", mapOf("tagIds", List.of(10), "tagMode", "includes")),
                entry("tag excludes", mapOf("tagIds", List.of(10), "tagMode", "excludes")),
                entry("tag is null", mapOf("tagMode", "isnull")),
                entry("tag is not null", mapOf("tagMode", "isnotnull")),
                entry("death date exact", mapOf("deathDate", "2020-12-31", "deathDateMode", "exact")),
                entry("death date between", mapOf("deathDateFrom", "2020-01-01", "deathDateTo", "2021-01-01", "deathDateMode", "between")),
                entry("birth date exact", mapOf("birthDate", "1994-03-10", "birthDateMode", "exact")),
                entry("birth date between", mapOf("birthDateFrom", "1970-01-01", "birthDateTo", "1985-01-01", "birthDateMode", "between")),
                entry("age min", mapOf("ageMin", 70)),
                entry("age max", mapOf("ageMax", 40)),
                entry("organized true", mapOf("organized", "true")),
                entry("organized false", mapOf("organized", "false")),
                entry("image count min", mapOf("imageCountMin", 2)),
                entry("image count max", mapOf("imageCountMax", 0)),
                entry("image theme has", mapOf("imageTheme", 1, "imageThemeMode", "has")),
                entry("image theme does not have", mapOf("imageTheme", 1, "imageThemeMode", "doesntHave")),
                entry("is band true", mapOf("isBand", "true")),
                entry("itunes in", mapOf("itunesIdsJson", "[1,2]", "inItunes", "true")),
                entry("itunes not in", mapOf("itunesIdsJson", "[1,2]", "inItunes", "false")),
                entry("account includes", mapOf("accounts", List.of("robertlover"), "accountMode", "includes")),
                entry("account excludes", mapOf("accounts", List.of("robertlover"), "accountMode", "excludes")),
                entry("listened date range", mapOf("listenedDateFrom", "2024-03-01", "listenedDateTo", "2024-03-31")),
                entry("account excludes with listened date", mapOf("accounts", List.of("robertlover"), "accountMode", "excludes", "listenedDateFrom", "2024-03-01", "listenedDateTo", "2024-03-31")),
                entry("first listened exact", mapOf("firstListenedDate", "2024-01-01", "firstListenedDateMode", "exact")),
                entry("first listened between", mapOf("firstListenedDateFrom", "2024-01-01", "firstListenedDateTo", "2024-01-04", "firstListenedDateMode", "between")),
                entry("last listened exact", mapOf("lastListenedDate", "2024-05-01", "lastListenedDateMode", "exact")),
                entry("last listened between", mapOf("lastListenedDateFrom", "2024-04-01", "lastListenedDateTo", "2024-05-01", "lastListenedDateMode", "between")),
                entry("play count min", mapOf("playCountMin", 5)),
                entry("play count max", mapOf("playCountMax", 0)),
                entry("play count scoped by account and date", mapOf("accounts", List.of("vatito"), "accountMode", "includes", "listenedDateFrom", "2024-01-01", "listenedDateTo", "2024-02-28", "playCountMin", 3)),
                entry("album count min", mapOf("albumCountMin", 1)),
                entry("album count max", mapOf("albumCountMax", 0)),
                entry("song count min", mapOf("songCountMin", 3)),
                entry("song count max", mapOf("songCountMax", 0)),
                entry("itunes presence min", mapOf("itunesPresenceMin", 100, "itunesSongIdsJson", "[3,4]")),
                entry("itunes presence max", mapOf("itunesPresenceMax", 0, "itunesSongIdsJson", "[3,4]"))
        );
    }

    private static Stream<Arguments> albumFilterCases() {
        return namedCases(
                entry("name", mapOf("name", "verano")),
                entry("artist", mapOf("artistName", List.of(1))),
                entry("genre includes", mapOf("genreIds", List.of(2), "genreMode", "includes")),
                entry("genre excludes", mapOf("genreIds", List.of(1), "genreMode", "excludes")),
                entry("genre is null", mapOf("genreMode", "isnull")),
                entry("subgenre includes", mapOf("subgenreIds", List.of(2), "subgenreMode", "includes")),
                entry("language includes", mapOf("languageIds", List.of(2), "languageMode", "includes")),
                entry("gender includes", mapOf("genderIds", List.of(2), "genderMode", "includes")),
                entry("gender excludes keeps nulls", mapOf("genderIds", List.of(2), "genderMode", "excludes")),
                entry("ethnicity includes", mapOf("ethnicityIds", List.of(2), "ethnicityMode", "includes")),
                entry("country excludes", mapOf("countries", List.of("Mexico"), "countryMode", "excludes")),
                entry("tag includes", mapOf("tagIds", List.of(10), "tagMode", "includes")),
                entry("tag is null", mapOf("tagMode", "isnull")),
                entry("release date exact", mapOf("releaseDate", "2022-05-06", "releaseDateMode", "exact")),
                entry("release date between", mapOf("releaseDateFrom", "1994-03-13", "releaseDateTo", "2022-05-06", "releaseDateMode", "between")),
                entry("release date is null", mapOf("releaseDateMode", "isnull")),
                entry("first listened exact", mapOf("firstListenedDate", "2024-01-01", "firstListenedDateMode", "exact")),
                entry("last listened exact", mapOf("lastListenedDate", "2024-05-01", "lastListenedDateMode", "exact")),
                entry("listened date range", mapOf("listenedDateFrom", "2024-03-01", "listenedDateTo", "2024-03-31")),
                entry("account includes", mapOf("accounts", List.of("robertlover"), "accountMode", "includes")),
                entry("account excludes", mapOf("accounts", List.of("robertlover"), "accountMode", "excludes")),
                entry("account excludes with play count", mapOf("accounts", List.of("robertlover"), "accountMode", "excludes", "playCountMin", 1)),
                entry("organized false", mapOf("organized", "false")),
                entry("image count min", mapOf("imageCountMin", 2)),
                entry("image count max", mapOf("imageCountMax", 0)),
                entry("has featured artists", mapOf("hasFeaturedArtists", "true")),
                entry("has no featured artists", mapOf("hasFeaturedArtists", "false")),
                entry("is band true", mapOf("isBand", "true")),
                entry("itunes in", mapOf("itunesIdsJson", "[1,5]", "inItunes", "true")),
                entry("itunes not in", mapOf("itunesIdsJson", "[1,5]", "inItunes", "false")),
                entry("age min", mapOf("ageMin", 70)),
                entry("age at release max", mapOf("ageAtReleaseMax", 23)),
                entry("birth date gte", mapOf("birthDate", "1970-01-01", "birthDateMode", "gte")),
                entry("death date is not null", mapOf("deathDateMode", "isnotnull")),
                entry("play count min", mapOf("playCountMin", 5)),
                entry("play count max", mapOf("playCountMax", 1)),
                entry("song count min", mapOf("songCountMin", 2)),
                entry("song count max", mapOf("songCountMax", 1)),
                entry("length is null", mapOf("lengthMode", "null")),
                entry("length range", mapOf("lengthMin", 400, "lengthMode", "range")),
                entry("weekly chart exact peak", mapOf("weeklyChartPeak", 1, "weeklyChartPeakMode", "exact")),
                entry("weekly chart date window", mapOf("weeklyChartPeak", 1, "weeklyChartDateFrom", "2024-01-01", "weeklyChartDateTo", "2024-01-07")),
                entry("weekly chart peak weeks", mapOf("weeklyChartPeakWeeks", 1, "weeklyChartPeakWeeksMode", "exact")),
                entry("seasonal chart peak", mapOf("seasonalChartPeak", 1)),
                entry("seasonal chart date window", mapOf("seasonalChartPeak", 1, "seasonalChartDateFrom", "2024-03-01", "seasonalChartDateTo", "2024-05-31")),
                entry("seasonal chart season", mapOf("seasonalChartPeak", 1, "seasonalChartSeason", "Spring")),
                entry("yearly chart peak", mapOf("yearlyChartPeak", 2)),
                entry("yearly chart date window", mapOf("yearlyChartPeak", 2, "yearlyChartDateFrom", "2024-01-01", "yearlyChartDateTo", "2024-12-31")),
                entry("last full listen exact", mapOf("lastFullListenDate", "2024-02-01", "lastFullListenDateMode", "exact")),
                entry("last full listen between", mapOf("lastFullListenDateFrom", "2024-02-01", "lastFullListenDateTo", "2024-05-01", "lastFullListenDateMode", "between")),
                entry("last full listen is null", mapOf("lastFullListenDateMode", "isnull")),
                entry("itunes presence min", mapOf("itunesPresenceMin", 100, "itunesSongIdsJson", "[1,2,3,4,8]")),
                entry("itunes presence max", mapOf("itunesPresenceMax", 0, "itunesSongIdsJson", "[1,2,3,4,8]"))
        );
    }

    private static Stream<Arguments> songFilterCases() {
        return namedCases(
                entry("name", mapOf("name", "titi")),
                entry("artist", mapOf("artistName", List.of(1))),
                entry("album name", mapOf("albumName", "Verano")),
                entry("genre includes", mapOf("genreIds", List.of(2), "genreMode", "includes")),
                entry("genre excludes", mapOf("genreIds", List.of(1), "genreMode", "excludes")),
                entry("genre is null", mapOf("genreMode", "isnull")),
                entry("subgenre includes", mapOf("subgenreIds", List.of(3), "subgenreMode", "includes")),
                entry("language includes", mapOf("languageIds", List.of(2), "languageMode", "includes")),
                entry("gender includes", mapOf("genderIds", List.of(2), "genderMode", "includes")),
                entry("gender excludes keeps nulls", mapOf("genderIds", List.of(2), "genderMode", "excludes")),
                entry("ethnicity includes", mapOf("ethnicityIds", List.of(2), "ethnicityMode", "includes")),
                entry("country includes", mapOf("countries", List.of("Mexico"), "countryMode", "includes")),
                entry("country is null", mapOf("countryMode", "isnull")),
                entry("tag includes", mapOf("tagIds", List.of(10), "tagMode", "includes")),
                entry("tag is not null", mapOf("tagMode", "isnotnull")),
                entry("release date exact", mapOf("releaseDate", "1995-01-01", "releaseDateMode", "exact")),
                entry("release date between", mapOf("releaseDateFrom", "1994-03-13", "releaseDateTo", "1995-01-01", "releaseDateMode", "between")),
                entry("release date is null", mapOf("releaseDateMode", "isnull")),
                entry("first listened exact", mapOf("firstListenedDate", "2024-01-01", "firstListenedDateMode", "exact")),
                entry("first listened between", mapOf("firstListenedDateFrom", "2024-01-01", "firstListenedDateTo", "2024-01-03", "firstListenedDateMode", "between")),
                entry("last listened exact", mapOf("lastListenedDate", "2024-05-01", "lastListenedDateMode", "exact")),
                entry("last listened between", mapOf("lastListenedDateFrom", "2024-04-01", "lastListenedDateTo", "2024-05-01", "lastListenedDateMode", "between")),
                entry("listened date range", mapOf("listenedDateFrom", "2024-03-01", "listenedDateTo", "2024-03-31")),
                entry("account includes", mapOf("accounts", List.of("robertlover"), "accountMode", "includes")),
                entry("account excludes", mapOf("accounts", List.of("robertlover"), "accountMode", "excludes")),
                entry("account excludes with play count", mapOf("accounts", List.of("robertlover"), "accountMode", "excludes", "playCountMin", 1)),
                entry("organized false", mapOf("organized", "false")),
                entry("image count min", mapOf("imageCountMin", 1)),
                entry("image count max", mapOf("imageCountMax", 0)),
                entry("has featured artists", mapOf("hasFeaturedArtists", "true")),
                entry("has no featured artists", mapOf("hasFeaturedArtists", "false")),
                entry("is band true", mapOf("isBand", "true")),
                entry("is single true", mapOf("isSingle", "true")),
                entry("itunes in", mapOf("itunesIdsJson", "[1,3]", "inItunes", "true")),
                entry("itunes not in", mapOf("itunesIdsJson", "[1,3]", "inItunes", "false")),
                entry("age min", mapOf("ageMin", 70)),
                entry("age at release max", mapOf("ageAtReleaseMax", 23)),
                entry("birth date exact", mapOf("birthDate", "1950-01-01", "birthDateMode", "exact")),
                entry("death date is not null", mapOf("deathDateMode", "isnotnull")),
                entry("play count min", mapOf("playCountMin", 5)),
                entry("play count max", mapOf("playCountMax", 1)),
                entry("track number exact", mapOf("trackNumber", 2, "trackNumberMode", "exact")),
                entry("track number is null", mapOf("trackNumberMode", "isnull")),
                entry("track number is not null", mapOf("trackNumberMode", "isnotnull")),
                entry("length is null", mapOf("lengthMode", "null")),
                entry("length range", mapOf("lengthMin", 240, "lengthMode", "range")),
                entry("weekly chart exact peak", mapOf("weeklyChartPeak", 1, "weeklyChartPeakMode", "exact", "includeExpensiveStats", true)),
                entry("weekly chart date window", mapOf("weeklyChartPeak", 1, "weeklyChartDateFrom", "2024-01-08", "weeklyChartDateTo", "2024-01-14", "includeExpensiveStats", true)),
                entry("weekly chart weeks", mapOf("weeklyChartWeeks", 2, "includeExpensiveStats", true)),
                entry("weekly chart peak weeks", mapOf("weeklyChartPeakWeeks", 1, "weeklyChartPeakWeeksMode", "exact", "includeExpensiveStats", true)),
                entry("trl peak", mapOf("trlPeak", 1, "trlPeakMode", "exact", "includeExpensiveStats", true)),
                entry("trl date window", mapOf("trlPeak", 1, "trlDateFrom", "2024-01-01", "trlDateTo", "2024-01-31", "includeExpensiveStats", true)),
                entry("trl days at peak", mapOf("trlDaysAtPeak", 1, "trlDaysAtPeakMode", "exact", "includeExpensiveStats", true)),
                entry("vatos cuntdown peak", mapOf("vatosCuntdownPeak", 1, "vatosCuntdownPeakMode", "exact", "includeExpensiveStats", true)),
                entry("vatos cuntdown days", mapOf("vatosCuntdownDays", 1, "includeExpensiveStats", true)),
                entry("vatos cuntdown days at peak", mapOf("vatosCuntdownDaysAtPeak", 2, "vatosCuntdownDaysAtPeakMode", "exact", "includeExpensiveStats", true)),
                entry("billboard peak", mapOf("billboardPeak", 1, "billboardPeakMode", "exact", "includeExpensiveStats", true)),
                entry("billboard date window", mapOf("billboardPeak", 1, "billboardDateFrom", "2024-03-11", "billboardDateTo", "2024-03-25", "includeExpensiveStats", true)),
                entry("billboard weeks", mapOf("billboardWeeks", 20, "includeExpensiveStats", true)),
                entry("billboard weeks at peak", mapOf("billboardWeeksAtPeak", 3, "billboardWeeksAtPeakMode", "exact", "includeExpensiveStats", true)),
                entry("seasonal chart peak", mapOf("seasonalChartPeak", 1, "includeExpensiveStats", true)),
                entry("seasonal chart date window", mapOf("seasonalChartPeak", 1, "seasonalChartDateFrom", "2024-03-01", "seasonalChartDateTo", "2024-05-31", "includeExpensiveStats", true)),
                entry("seasonal chart season", mapOf("seasonalChartPeak", 1, "seasonalChartSeason", "Spring", "includeExpensiveStats", true)),
                entry("yearly chart peak", mapOf("yearlyChartPeak", 2, "includeExpensiveStats", true)),
                entry("yearly chart date window", mapOf("yearlyChartPeak", 2, "yearlyChartDateFrom", "2024-01-01", "yearlyChartDateTo", "2024-12-31", "includeExpensiveStats", true))
        );
    }

    private static Map.Entry<String, Map<String, Object>> entry(String name, Map<String, Object> overrides) {
        return Map.entry(name, overrides);
    }

    @SafeVarargs
    private static Stream<Arguments> namedCases(Map.Entry<String, Map<String, Object>>... cases) {
        return Stream.of(cases).map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
    }

    private static <T> Map<Integer, Long> countBy(List<T> rows, Function<T, Integer> genderExtractor) {
        Map<Integer, Long> counts = new LinkedHashMap<>();
        for (T row : rows) {
            Integer genderId = genderExtractor.apply(row);
            counts.merge(genderId, 1L, Long::sum);
        }
        return counts;
    }

    private static long countAlbums(TestDatabaseSupport db, AlbumStatsQuery q) {
        return db.albumRepository.countAlbumsWithFilters(q.name(), q.artistName(), q.genreIds(), q.genreMode(),
                q.subgenreIds(), q.subgenreMode(), q.languageIds(), q.languageMode(), q.genderIds(), q.genderMode(),
                q.ethnicityIds(), q.ethnicityMode(), q.countries(), q.countryMode(), q.tagIds(), q.tagMode(),
                q.accounts(), q.accountMode(), q.releaseDate(), q.releaseDateFrom(), q.releaseDateTo(), q.releaseDateMode(),
                q.firstListenedDate(), q.firstListenedDateFrom(), q.firstListenedDateTo(), q.firstListenedDateMode(),
                q.lastListenedDate(), q.lastListenedDateFrom(), q.lastListenedDateTo(), q.lastListenedDateMode(),
                q.listenedDateFrom(), q.listenedDateTo(), q.organized(), q.imageCountMin(), q.imageCountMax(),
                q.hasFeaturedArtists(), q.isBand(), q.itunesIdsJson(), q.inItunes(), q.ageMin(), q.ageMax(), q.ageMode(),
                q.ageAtReleaseMin(), q.ageAtReleaseMax(), q.birthDate(), q.birthDateFrom(), q.birthDateTo(), q.birthDateMode(),
                q.deathDate(), q.deathDateFrom(), q.deathDateTo(), q.deathDateMode(), q.playCountMin(), q.playCountMax(),
                q.songCountMin(), q.songCountMax(), q.lengthMin(), q.lengthMax(), q.lengthMode(), q.weeklyChartPeak(),
                q.weeklyChartPeakMode(), q.weeklyChartWeeks(), q.weeklyChartPeakWeeks(), q.weeklyChartPeakWeeksMode(),
                q.weeklyChartDateFrom(), q.weeklyChartDateTo(), q.weeklyChartSeason(), q.seasonalChartPeak(),
                q.seasonalChartSeasons(), q.seasonalChartDateFrom(), q.seasonalChartDateTo(), q.seasonalChartSeason(),
                q.yearlyChartPeak(), q.yearlyChartYears(), q.yearlyChartDateFrom(), q.yearlyChartDateTo(),
                q.lastFullListenDate(), q.lastFullListenDateFrom(), q.lastFullListenDateTo(), q.lastFullListenDateMode(),
                q.itunesPresenceMin(), q.itunesPresenceMax(), q.itunesSongIdsJson());
    }

    private static Map<Integer, Long> countAlbumsByGender(TestDatabaseSupport db, AlbumStatsQuery q) {
        return db.albumRepository.countAlbumsByGenderWithFilters(q.name(), q.artistName(), q.genreIds(), q.genreMode(),
                q.subgenreIds(), q.subgenreMode(), q.languageIds(), q.languageMode(), q.genderIds(), q.genderMode(),
                q.ethnicityIds(), q.ethnicityMode(), q.countries(), q.countryMode(), q.tagIds(), q.tagMode(),
                q.accounts(), q.accountMode(), q.releaseDate(), q.releaseDateFrom(), q.releaseDateTo(), q.releaseDateMode(),
                q.firstListenedDate(), q.firstListenedDateFrom(), q.firstListenedDateTo(), q.firstListenedDateMode(),
                q.lastListenedDate(), q.lastListenedDateFrom(), q.lastListenedDateTo(), q.lastListenedDateMode(),
                q.listenedDateFrom(), q.listenedDateTo(), q.organized(), q.imageCountMin(), q.imageCountMax(),
                q.hasFeaturedArtists(), q.isBand(), q.ageMin(), q.ageMax(), q.ageMode(), q.ageAtReleaseMin(),
                q.ageAtReleaseMax(), q.birthDate(), q.birthDateFrom(), q.birthDateTo(), q.birthDateMode(),
                q.deathDate(), q.deathDateFrom(), q.deathDateTo(), q.deathDateMode(), q.itunesIdsJson(), q.inItunes(),
                q.playCountMin(), q.playCountMax(), q.songCountMin(), q.songCountMax(), q.lengthMin(), q.lengthMax(),
                q.lengthMode(), q.weeklyChartPeak(), q.weeklyChartPeakMode(), q.weeklyChartWeeks(),
                q.weeklyChartPeakWeeks(), q.weeklyChartPeakWeeksMode(), q.weeklyChartDateFrom(), q.weeklyChartDateTo(),
                q.weeklyChartSeason(), q.seasonalChartPeak(), q.seasonalChartSeasons(), q.seasonalChartDateFrom(),
                q.seasonalChartDateTo(), q.seasonalChartSeason(), q.yearlyChartPeak(), q.yearlyChartYears(),
                q.yearlyChartDateFrom(), q.yearlyChartDateTo(), q.lastFullListenDate(), q.lastFullListenDateFrom(),
                q.lastFullListenDateTo(), q.lastFullListenDateMode(), q.itunesPresenceMin(), q.itunesPresenceMax(),
                q.itunesSongIdsJson());
    }

    private static long countSongs(TestDatabaseSupport db, SongStatsQuery q) {
        return db.songRepository.countSongsWithFilters(q.name(), q.artistName(), q.albumName(), q.genreIds(), q.genreMode(),
                q.subgenreIds(), q.subgenreMode(), q.languageIds(), q.languageMode(), q.genderIds(), q.genderMode(),
                q.ethnicityIds(), q.ethnicityMode(), q.countries(), q.countryMode(), q.tagIds(), q.tagMode(),
                q.accounts(), q.accountMode(), q.releaseDate(), q.releaseDateFrom(), q.releaseDateTo(), q.releaseDateMode(),
                q.firstListenedDate(), q.firstListenedDateFrom(), q.firstListenedDateTo(), q.firstListenedDateMode(),
                q.lastListenedDate(), q.lastListenedDateFrom(), q.lastListenedDateTo(), q.lastListenedDateMode(),
                q.listenedDateFrom(), q.listenedDateTo(), q.organized(), q.imageCountMin(), q.imageCountMax(),
                q.hasFeaturedArtists(), q.isBand(), q.isSingle(), q.itunesIdsJson(), q.inItunes(), q.ageMin(),
                q.ageMax(), q.ageMode(), q.ageAtReleaseMin(), q.ageAtReleaseMax(), q.birthDate(), q.birthDateFrom(),
                q.birthDateTo(), q.birthDateMode(), q.deathDate(), q.deathDateFrom(), q.deathDateTo(), q.deathDateMode(),
                q.playCountMin(), q.playCountMax(), q.trackNumber(), q.trackNumberMode(), q.lengthMin(), q.lengthMax(),
                q.lengthMode(), q.weeklyChartPeak(), q.weeklyChartPeakMode(), q.weeklyChartWeeks(),
                q.weeklyChartPeakWeeks(), q.weeklyChartPeakWeeksMode(), q.weeklyChartDateFrom(), q.weeklyChartDateTo(),
                q.weeklyChartSeason(), q.trlPeak(), q.trlPeakMode(), q.trlDays(), q.trlDaysAtPeak(),
                q.trlDaysAtPeakMode(), q.trlDateFrom(), q.trlDateTo(), q.vatosCuntdownPeak(),
                q.vatosCuntdownPeakMode(), q.vatosCuntdownDays(), q.vatosCuntdownDaysAtPeak(),
                q.vatosCuntdownDaysAtPeakMode(), q.vatosCuntdownDateFrom(), q.vatosCuntdownDateTo(),
                q.billboardPeak(), q.billboardPeakMode(), q.billboardWeeks(), q.billboardWeeksAtPeak(),
                q.billboardWeeksAtPeakMode(), q.billboardDateFrom(), q.billboardDateTo(), q.seasonalChartPeak(),
                q.seasonalChartSeasons(), q.seasonalChartDateFrom(), q.seasonalChartDateTo(), q.seasonalChartSeason(),
                q.yearlyChartPeak(), q.yearlyChartYears(), q.yearlyChartDateFrom(), q.yearlyChartDateTo());
    }

    private static Map<Integer, Long> countSongsByGender(TestDatabaseSupport db, SongStatsQuery q) {
        return db.songRepository.countSongsByGenderWithFilters(q.name(), q.artistName(), q.albumName(), q.genreIds(),
                q.genreMode(), q.subgenreIds(), q.subgenreMode(), q.languageIds(), q.languageMode(), q.genderIds(),
                q.genderMode(), q.ethnicityIds(), q.ethnicityMode(), q.countries(), q.countryMode(), q.tagIds(),
                q.tagMode(), q.accounts(), q.accountMode(), q.releaseDate(), q.releaseDateFrom(), q.releaseDateTo(),
                q.releaseDateMode(), q.firstListenedDate(), q.firstListenedDateFrom(), q.firstListenedDateTo(),
                q.firstListenedDateMode(), q.lastListenedDate(), q.lastListenedDateFrom(), q.lastListenedDateTo(),
                q.lastListenedDateMode(), q.listenedDateFrom(), q.listenedDateTo(), q.organized(), q.imageCountMin(),
                q.imageCountMax(), q.hasFeaturedArtists(), q.isBand(), q.isSingle(), q.ageMin(), q.ageMax(), q.ageMode(),
                q.ageAtReleaseMin(), q.ageAtReleaseMax(), q.birthDate(), q.birthDateFrom(), q.birthDateTo(),
                q.birthDateMode(), q.deathDate(), q.deathDateFrom(), q.deathDateTo(), q.deathDateMode(),
                q.itunesIdsJson(), q.inItunes(), q.playCountMin(), q.playCountMax(), q.trackNumber(), q.trackNumberMode(),
                q.lengthMin(), q.lengthMax(), q.lengthMode(), q.weeklyChartPeak(), q.weeklyChartPeakMode(),
                q.weeklyChartWeeks(), q.weeklyChartPeakWeeks(), q.weeklyChartPeakWeeksMode(), q.weeklyChartDateFrom(),
                q.weeklyChartDateTo(), q.weeklyChartSeason(), q.trlPeak(), q.trlPeakMode(), q.trlDays(),
                q.trlDaysAtPeak(), q.trlDaysAtPeakMode(), q.trlDateFrom(), q.trlDateTo(), q.vatosCuntdownPeak(),
                q.vatosCuntdownPeakMode(), q.vatosCuntdownDays(), q.vatosCuntdownDaysAtPeak(),
                q.vatosCuntdownDaysAtPeakMode(), q.vatosCuntdownDateFrom(), q.vatosCuntdownDateTo(),
                q.billboardPeak(), q.billboardPeakMode(), q.billboardWeeks(), q.billboardWeeksAtPeak(),
                q.billboardWeeksAtPeakMode(), q.billboardDateFrom(), q.billboardDateTo(), q.seasonalChartPeak(),
                q.seasonalChartSeasons(), q.seasonalChartDateFrom(), q.seasonalChartDateTo(), q.seasonalChartSeason(),
                q.yearlyChartPeak(), q.yearlyChartYears(), q.yearlyChartDateFrom(), q.yearlyChartDateTo());
    }
}
