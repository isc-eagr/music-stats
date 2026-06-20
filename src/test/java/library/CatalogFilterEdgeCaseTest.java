package library;

import library.dto.AlbumStatsRow;
import library.dto.ArtistStatsRow;
import library.dto.SongCardDTO;
import library.dto.SongStatsRow;
import library.repository.SongImageRepository;
import library.repository.LookupRepository;
import library.service.AppConfigService;
import library.service.ItunesService;
import library.service.SongLinkService;
import library.service.SongService;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static library.TestDatabaseSupport.albumQueryWith;
import static library.TestDatabaseSupport.artistQueryWith;
import static library.TestDatabaseSupport.mapOf;
import static library.TestDatabaseSupport.songQueryWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CatalogFilterEdgeCaseTest {

    @Test
    void emptyFilterValuesAndUnknownModesDoNotAccidentallyRemoveCatalogRows() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf("genderIds", List.of(), "genderMode", "includes"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Mystery Artist", "Selena", "The Static Hearts");
            assertArtistNames(db, mapOf("genreIds", List.of(1), "genreMode", "bogus"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Mystery Artist", "Selena", "The Static Hearts");

            assertAlbumNames(db, mapOf("artistName", List.of()),
                    "Amor Prohibido", "Legacy Collection", "Silent Record", "Un Verano Sin Ti", "Unknown Album");
            assertAlbumNames(db, mapOf("languageIds", List.of(1), "languageMode", "bogus"),
                    "Amor Prohibido", "Legacy Collection", "Silent Record", "Un Verano Sin Ti", "Unknown Album");

            assertSongNames(db, mapOf("songIdsFilter", List.of()),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Old Hit",
                    "Quiet Track", "Standalone Jam", "Titi Me Pregunto", "Unknown Silence");
            assertSongNames(db, mapOf("genreIds", List.of(1), "genreMode", "bogus"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Old Hit",
                    "Quiet Track", "Standalone Jam", "Titi Me Pregunto", "Unknown Silence");
        }
    }

    @Test
    void excludesPreserveNullEffectiveValuesUnlessIsNotNullIsExplicit() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf("genderIds", List.of(2), "genderMode", "excludes"),
                    "Bad Bunny", "Legacy Legend", "Mystery Artist", "The Static Hearts");
            assertArtistNames(db, mapOf("genderMode", "isnotnull"),
                    "Bad Bunny", "Guest Singer", "Legacy Legend", "Selena", "The Static Hearts");

            assertAlbumNames(db, mapOf("genderIds", List.of(2), "genderMode", "excludes"),
                    "Legacy Collection", "Silent Record", "Un Verano Sin Ti", "Unknown Album");
            assertAlbumNames(db, mapOf("genreIds", List.of(1), "genreMode", "excludes"),
                    "Amor Prohibido", "Legacy Collection", "Silent Record", "Unknown Album");
            assertAlbumNames(db, mapOf("subgenreIds", List.of(1), "subgenreMode", "excludes"),
                    "Amor Prohibido", "Legacy Collection", "Silent Record", "Un Verano Sin Ti", "Unknown Album");
            assertAlbumNames(db, mapOf("languageIds", List.of(1), "languageMode", "excludes"),
                    "Legacy Collection", "Silent Record", "Unknown Album");

            assertSongNames(db, mapOf("genreIds", List.of(1), "genreMode", "excludes"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Old Hit", "Quiet Track", "Unknown Silence");
            assertSongNames(db, mapOf("subgenreIds", List.of(1), "subgenreMode", "excludes"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Old Hit",
                    "Quiet Track", "Titi Me Pregunto", "Unknown Silence");
            assertSongNames(db, mapOf("languageIds", List.of(1), "languageMode", "excludes"),
                    "No Me Queda Mas", "Old Hit", "Quiet Track", "Unknown Silence");
            assertSongNames(db, mapOf("releaseDateMode", "isnotnull"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Old Hit",
                    "Quiet Track", "Standalone Jam", "Titi Me Pregunto");
        }
    }

    @Test
    void inclusiveDateBoundariesAndFallbackDatesStayStable() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf("birthDate", "1980-01-01", "birthDateMode", "lte"),
                    "Legacy Legend", "Selena", "The Static Hearts");

            assertAlbumNames(db, mapOf(
                    "releaseDateFrom", "1994-03-13",
                    "releaseDateTo", "2022-05-06",
                    "releaseDateMode", "between"),
                    "Amor Prohibido", "Silent Record", "Un Verano Sin Ti");

            assertSongNames(db, mapOf(
                    "releaseDateFrom", "1994-03-13",
                    "releaseDateTo", "1995-01-01",
                    "releaseDateMode", "between"),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Standalone Jam");

            assertSongNames(db, mapOf("listenedDateFrom", "2024-04-01", "listenedDateTo", "2024-04-02"),
                    "Standalone Jam");
        }
    }

    @Test
    void numericModeBoundariesRespectStrictAndInclusiveComparisons() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertSongNames(db, mapOf("lengthMax", 200, "lengthMode", "lt"),
                    "Quiet Track", "Standalone Jam");
            assertSongNames(db, mapOf("lengthMin", 240, "lengthMode", "gt"),
                    "Ojitos Lindos", "Titi Me Pregunto");
            assertSongNames(db, mapOf("lengthMin", 240, "lengthMode", "range"),
                    "Ojitos Lindos", "Old Hit", "Titi Me Pregunto");

            assertAlbumNames(db, mapOf("lengthMax", 200, "lengthMode", "lt"),
                    "Silent Record", "Unknown Album");
            assertAlbumNames(db, mapOf("lengthMin", 400, "lengthMode", "gt"),
                    "Amor Prohibido", "Un Verano Sin Ti");

            assertArtistNames(db, mapOf("playCountMin", 0, "playCountMax", 0),
                    "Guest Singer", "Mystery Artist", "The Static Hearts");
        }
    }

    @Test
    void contradictoryFiltersReturnEmptyResultsWithoutLeakingRows() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf(
                    "genderIds", List.of(2), "genderMode", "includes",
                    "countries", List.of("Puerto Rico"), "countryMode", "includes"));

            assertAlbumNames(db, mapOf(
                    "genreIds", List.of(1), "genreMode", "includes",
                    "genderIds", List.of(2), "genderMode", "includes"));

            assertSongNames(db, mapOf(
                    "isSingle", "true",
                    "albumName", "Verano"));
        }
    }

    @Test
    void accountExcludesAndPlayMinimumsUseTheScopedPlayStats() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf(
                    "accounts", List.of("robertlover"), "accountMode", "excludes",
                    "playCountMin", 1),
                    "Bad Bunny", "Legacy Legend", "Selena");

            assertAlbumNames(db, mapOf(
                    "accounts", List.of("robertlover"), "accountMode", "excludes",
                    "playCountMin", 1),
                    "Amor Prohibido", "Legacy Collection", "Un Verano Sin Ti");

            assertSongNames(db, mapOf(
                    "accounts", List.of("robertlover"), "accountMode", "excludes",
                    "playCountMin", 1),
                    "Bidi Bidi Bom Bom", "No Me Queda Mas", "Old Hit", "Standalone Jam", "Titi Me Pregunto");
        }
    }

    @Test
    void paginationSlicesStableNameSortsWithoutChangingFilterResults() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertArtistNames(db, mapOf("limit", 2, "offset", 2),
                    "Legacy Legend", "Mystery Artist");

            assertAlbumNames(db, mapOf("limit", 3, "offset", 1),
                    "Legacy Collection", "Silent Record", "Un Verano Sin Ti");

            assertSongNames(db, mapOf("limit", 3, "offset", 3),
                    "Old Hit", "Quiet Track", "Standalone Jam");
        }
    }

    @Test
    void chartDateWindowsAndAtPeakModesDoNotBleedAcrossPeriods() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertSongNames(db, mapOf(
                    "weeklyChartPeak", 1,
                    "weeklyChartPeakMode", "exact",
                    "weeklyChartDateFrom", "2024-01-08",
                    "weeklyChartDateTo", "2024-01-14",
                    "includeExpensiveStats", true),
                    "Bidi Bidi Bom Bom");

            assertSongNames(db, mapOf(
                    "billboardPeak", 5,
                    "billboardPeakMode", "exact",
                    "billboardWeeksAtPeak", 2,
                    "billboardWeeksAtPeakMode", "exact",
                    "includeExpensiveStats", true),
                    "Bidi Bidi Bom Bom");

            assertAlbumNames(db, mapOf(
                    "seasonalChartPeak", 1,
                    "seasonalChartSeason", "Spring"),
                    "Un Verano Sin Ti");

            assertAlbumNames(db, mapOf(
                    "seasonalChartPeak", 1,
                    "seasonalChartSeason", "Spring",
                    "yearlyChartPeak", 2));
        }
    }

    @Test
    void songRowsExposeEffectiveValuesFromSongAlbumAndArtistFallbacks() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            Map<String, SongStatsRow> byName = db.songRepository.findSongsWithStats(songQueryWith(mapOf())).stream()
                    .collect(java.util.stream.Collectors.toMap(SongStatsRow::name, row -> row));

            SongStatsRow bidi = byName.get("Bidi Bidi Bom Bom");
            assertThat(bidi.genreName()).isEqualTo("Rock");
            assertThat(bidi.subgenreName()).isEqualTo("Alt Rock");
            assertThat(bidi.languageName()).isEqualTo("Spanish");
            assertThat(bidi.ethnicityName()).isEqualTo("Latina");
            assertThat(bidi.genderName()).isEqualTo("Female");

            SongStatsRow titi = byName.get("Titi Me Pregunto");
            assertThat(titi.genreName()).isEqualTo("Pop");
            assertThat(titi.subgenreName()).isEqualTo("Dance Pop");
            assertThat(titi.languageName()).isEqualTo("Spanish");
            assertThat(titi.ethnicityName()).isEqualTo("Latina");
            assertThat(titi.genderName()).isEqualTo("Male");

            SongStatsRow noMe = byName.get("No Me Queda Mas");
            assertThat(noMe.genreName()).isEqualTo("Dance");
            assertThat(noMe.subgenreName()).isEqualTo("Dance Pop");
            assertThat(noMe.languageName()).isEqualTo("English");
            assertThat(noMe.ethnicityName()).isEqualTo("Latina");
            assertThat(noMe.genderName()).isEqualTo("Female");

            SongStatsRow ojitos = byName.get("Ojitos Lindos");
            assertThat(ojitos.genreName()).isEqualTo("Pop");
            assertThat(ojitos.genderName()).isEqualTo("Female");
        }
    }

    @Test
    void linkedSongsCombineTheirPlayStatsWhenConfigIsEnabled() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            SongLinkService songLinkService = new SongLinkService(db.jdbcTemplate);
            songLinkService.initialize();
            songLinkService.saveLinkedSongs(1, List.of(2));

            SongService uncombinedService = songService(db, songLinkService, false);
            List<SongCardDTO> uncombined = getUnfilteredSongs(uncombinedService, "plays", "desc", 0, 20);
            assertThat(uncombined)
                    .filteredOn(song -> song.getId() == 1 || song.getId() == 2)
                    .extracting(SongCardDTO::getName)
                    .containsExactly("Bidi Bidi Bom Bom", "No Me Queda Mas");

            SongService combinedService = songService(db, songLinkService, true);
            List<SongCardDTO> combined = getUnfilteredSongs(combinedService, "plays", "desc", 0, 20);

            assertThat(combined)
                    .extracting(SongCardDTO::getName)
                    .doesNotContain("Bidi Bidi Bom Bom");

            SongCardDTO linkedGroup = combined.stream()
                    .filter(song -> Boolean.TRUE.equals(song.getLinkedSongGroup()))
                    .findFirst()
                    .orElseThrow();
            assertThat(linkedGroup.getLinkedSongCount()).isEqualTo(2);
            assertThat(linkedGroup.getPlayCount()).isEqualTo(4);
            assertThat(linkedGroup.getVatitoPlayCount()).isEqualTo(3);
            assertThat(linkedGroup.getRobertloverPlayCount()).isEqualTo(1);
            assertThat(linkedGroup.getTimeListened()).isEqualTo(830L);
            assertThat(linkedGroup.getTotalPlayBreakdownItems())
                    .containsExactly("Bidi Bidi Bom Bom (Amor Prohibido): 3", "No Me Queda Mas (Amor Prohibido): 1");
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

    private static SongService songService(TestDatabaseSupport db, SongLinkService songLinkService, boolean combineLinkedSongs) {
        ItunesService itunesService = mock(ItunesService.class);
        when(itunesService.getSongPresenceById(anyList())).thenReturn(Map.of());
        AppConfigService appConfigService = mock(AppConfigService.class);
        when(appConfigService.isCombineLinkedSongsEnabled()).thenReturn(combineLinkedSongs);
        return new SongService(
                db.songRepository,
                mock(SongImageRepository.class),
                new LookupRepository(db.jdbcTemplate),
                db.jdbcTemplate,
                itunesService,
                appConfigService,
                songLinkService);
    }

    private static List<SongCardDTO> getUnfilteredSongs(SongService service, String sortBy, String sortDirection, int page, int perPage) {
        try {
            Method method = java.util.Arrays.stream(SongService.class.getMethods())
                    .filter(candidate -> candidate.getName().equals("getSongs"))
                    .findFirst()
                    .orElseThrow();
            Object[] args = new Object[method.getParameterCount()];
            Class<?>[] parameterTypes = method.getParameterTypes();
            for (int i = 0; i < parameterTypes.length; i++) {
                if (parameterTypes[i] == int.class) {
                    args[i] = 0;
                }
            }
            args[args.length - 8] = sortBy;
            args[args.length - 7] = sortDirection;
            args[args.length - 2] = page;
            args[args.length - 1] = perPage;
            @SuppressWarnings("unchecked")
            List<SongCardDTO> songs = (List<SongCardDTO>) method.invoke(service, args);
            return songs;
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to invoke SongService.getSongs from test", ex);
        }
    }
}
