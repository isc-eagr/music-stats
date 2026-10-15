package library;

import library.dto.AlbumStatsQuery;
import library.dto.ArtistStatsQuery;
import library.dto.ChartFilterDTO;
import library.repository.AlbumRepository;
import library.repository.ArtistRepositoryImpl;
import library.repository.SongRepository;
import library.service.CatalogGraphFilterResolver;
import library.service.ItunesService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.server.ResponseStatusException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CatalogGraphFilterResolverTest {
    @Test
    void unfilteredGraphsKeepTheDirectTableFastPathAndIgnoreLayoutState() {
        ArtistRepositoryImpl artists = mock(ArtistRepositoryImpl.class);
        AlbumRepository albums = mock(AlbumRepository.class);
        SongRepository songs = mock(SongRepository.class);
        ItunesService itunes = mock(ItunesService.class);
        CatalogGraphFilterResolver resolver = new CatalogGraphFilterResolver(artists, albums, songs, itunes);
        MockHttpServletRequest request = request("view", "graphs", "tab", "genre", "metric", "plays",
                "page", "2", "perpage", "1", "sortby", "length", "includeExtendedStats", "true");

        for (String source : List.of("artists", "albums", "songs")) {
            assertThat(resolver.resolve(request, source).getMatchedCatalogIdsJson()).isNull();
        }
        verifyNoInteractions(artists, albums, songs, itunes);
    }

    @Test
    void songFiltersUseFullCatalogPredicatesAndKeepTheSelectedPlayWindow() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            MockHttpServletRequest request = request("q", "Bidi", "album", "Amor Prohibido",
                    "artist", "2", "artistMode", "excludes", "tag", "10", "tagMode", "includes",
                    "genre", "2", "genreMode", "includes", "trackNumber", "1", "trackNumberMode", "exact",
                    "lengthMin", "200", "lengthMax", "250", "lengthMode", "range",
                    "account", "vatito", "accountMode", "includes",
                    "listenedDateFrom", "01/01/2024", "listenedDateTo", "31/01/2024",
                    "firstListenedDate", "01/01/2024", "firstListenedDateMode", "exact",
                    "weeklyChartPeak", "1", "weeklyChartPeakMode", "top", "playCountMin", "1",
                    "perpage", "1", "page", "9");

            ChartFilterDTO filter = resolver(db).resolve(request, "songs");
            assertThat(ids(filter)).containsExactly(1);
            assertThat(filter.getListenedDateFrom()).isEqualTo("2024-01-01");
            Map<String, Object> general = db.songRepository.getGeneralChartData(filter);
            assertThat(genderTotal(general, "songsByGender")).isEqualTo(1);
            assertThat(genderTotal(general, "playsByGender")).isEqualTo(1);
            assertThat(genderTotal(general, "listeningTimeByGender")).isEqualTo(210);
            assertThat(db.songRepository.getGenreChartData(filter)).containsKey("songsByGenre");
            assertThat(db.songRepository.getSubgenreChartData(filter)).containsKey("songsBySubgenre");
            assertThat(db.songRepository.getEthnicityChartData(filter)).containsKey("songsByEthnicity");
            assertThat(db.songRepository.getLanguageChartData(filter)).containsKey("songsByLanguage");
            assertThat(db.songRepository.getCountryChartData(filter)).containsKey("songsByCountry");
            assertThat(db.songRepository.getReleaseYearChartData(filter)).containsKey("songsByReleaseYear");
            assertThat(db.songRepository.getListenYearChartData(filter)).containsKey("songsByListenYear");
        }
    }

    @Test
    void artistFiltersDoNotReapplyArtistMetadataToSongOverrides() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartFilterDTO filter = resolver(db).resolve(request("q", "Selena", "tag", "10", "tagMode", "includes",
                    "imageTheme", "1", "imageThemeMode", "includes", "genre", "1", "genreMode", "includes",
                    "albumCountMin", "1", "songCountMin", "2", "organized", "true"), "artists");

            assertThat(ids(filter)).containsExactly(1);
            assertThat(genderTotal(db.songRepository.getGeneralChartData(filter), "songsByGender")).isEqualTo(3);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> genres = (List<Map<String, Object>>) db.songRepository.getGenreChartData(filter).get("songsByGenre");
            assertThat(genres).extracting(row -> row.get("name")).containsExactlyInAnyOrder("Pop", "Rock", "Dance");
        }
    }

    @Test
    void albumFiltersRetainReleaseDateLengthAndInheritedSongOverrides() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartFilterDTO filter = resolver(db).resolve(request("q", "Amor", "genre", "2", "genreMode", "includes",
                    "releaseDate", "13/03/1994", "releaseDateMode", "exact", "tag", "10", "tagMode", "includes",
                    "lengthMin", "400", "lengthMax", "420", "lengthMode", "range", "songCountMin", "2",
                    "ageAtReleaseMin", "22", "ageAtReleaseMax", "23"), "albums");

            assertThat(ids(filter)).containsExactly(1);
            Map<String, Object> general = db.songRepository.getGeneralChartData(filter);
            assertThat(genderTotal(general, "albumsByGender")).isEqualTo(1);
            assertThat(genderTotal(general, "songsByGender")).isEqualTo(2);
            assertThat(genderTotal(general, "playsByGender")).isEqualTo(4);
        }
    }

    @Test
    void modeOnlyNullFiltersAndEmptyMatchesStayConstrained() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartFilterDTO nullGender = resolver(db).resolve(request("genderMode", "isnull"), "songs");
            assertThat(ids(nullGender)).containsExactly(7);
            assertThat(genderTotal(db.songRepository.getGeneralChartData(nullGender), "songsByGender")).isEqualTo(1);

            ChartFilterDTO noMatches = resolver(db).resolve(request("q", "missing song"), "songs");
            assertThat(noMatches.getMatchedCatalogIdsJson()).isEqualTo("[]");
            assertThat(genderTotal(db.songRepository.getGeneralChartData(noMatches), "songsByGender")).isZero();
            assertThat(genderTotal(db.songRepository.getGeneralChartData(noMatches), "playsByGender")).isZero();
            assertThat(db.songRepository.getGenreChartData(noMatches).get("songsByGenre")).isEqualTo(List.of());
        }
    }

    @Test
    void omittedModesFollowTheSameIgnoredFilterBehaviorAsCatalogLists() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartFilterDTO filter = resolver(db).resolve(request("genre", "1", "account", "vatito"), "songs");
            assertThat(ids(filter)).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8);
            assertThat(filter.getAccounts()).isNull();
            assertThat(genderTotal(db.songRepository.getGeneralChartData(filter), "playsByGender")).isEqualTo(12);
        }
    }

    @Test
    void scopesCanExcludeMainSongsAndIncludeGroupsAndFeaturedWork() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartFilterDTO filter = resolver(db).resolve(request("q", "Guest Singer", "includeMain", "false",
                    "includeGroups", "true", "includeFeatured", "true"), "artists");
            assertThat(ids(filter)).containsExactly(4);
            Map<String, Object> general = db.songRepository.getGeneralChartData(filter);
            assertThat(genderTotal(general, "songsByGender")).isEqualTo(2);
            assertThat(genderTotal(general, "playsByGender")).isEqualTo(1);

            ChartFilterDTO noScope = resolver(db).resolve(request("includeMain", "false"), "artists");
            assertThat(noScope.getMatchedCatalogIdsJson()).isNotNull();
            assertThat(genderTotal(db.songRepository.getGeneralChartData(noScope), "playsByGender")).isZero();
        }
    }

    @Test
    void generatedItunesIdsAndAlbumFullListenDatesReachTheCatalogQuery() {
        ArtistRepositoryImpl artists = mock(ArtistRepositoryImpl.class);
        AlbumRepository albums = mock(AlbumRepository.class);
        SongRepository songs = mock(SongRepository.class);
        ItunesService itunes = mock(ItunesService.class);
        when(albums.findAlbumsWithStats(any())).thenReturn(List.of());
        when(artists.findArtistsWithStats(any())).thenReturn(List.of());
        when(itunes.getAllItunesAlbumIdsJson()).thenReturn("[1]");
        when(itunes.getAllItunesArtistIdsJson()).thenReturn("[2]");
        when(itunes.getAllItunesSongIdsJson()).thenReturn("[3]");
        CatalogGraphFilterResolver resolver = new CatalogGraphFilterResolver(artists, albums, songs, itunes);
        resolver.resolve(request("inItunes", "true", "itunesPresenceMin", "50",
                "firstFullListenDate", "14/10/2026", "firstFullListenDateMode", "gte",
                "lastFullListenDateFrom", "01/10/2026", "lastFullListenDateTo", "14/10/2026",
                "lastFullListenDateMode", "between", "fullAlbumPlaysMin", "2",
                "weeklyChartPeakWeeks", "3", "weeklyChartPeakWeeksMode", "gte"), "albums");
        ArgumentCaptor<AlbumStatsQuery> albumQuery = ArgumentCaptor.forClass(AlbumStatsQuery.class);
        verify(albums).findAlbumsWithStats(albumQuery.capture());
        assertThat(albumQuery.getValue().itunesIdsJson()).isEqualTo("[1]");
        assertThat(albumQuery.getValue().itunesSongIdsJson()).isEqualTo("[3]");
        assertThat(albumQuery.getValue().firstFullListenDate()).isEqualTo("2026-10-14");
        assertThat(albumQuery.getValue().lastFullListenDateFrom()).isEqualTo("2026-10-01");
        assertThat(albumQuery.getValue().fullAlbumPlaysMin()).isEqualTo(2);
        assertThat(albumQuery.getValue().weeklyChartPeakWeeks()).isEqualTo(3);
        assertThat(albumQuery.getValue().limit()).isEqualTo(-1);
        assertThat(albumQuery.getValue().includeExtendedStats()).isFalse();

        resolver.resolve(request("inItunes", "false", "itunesPresenceMax", "25", "imageThemeMode", "isnull"), "artists");
        ArgumentCaptor<ArtistStatsQuery> artistQuery = ArgumentCaptor.forClass(ArtistStatsQuery.class);
        verify(artists).findArtistsWithStats(artistQuery.capture());
        assertThat(artistQuery.getValue().itunesIdsJson()).isEqualTo("[2]");
        assertThat(artistQuery.getValue().itunesSongIdsJson()).isEqualTo("[3]");
        assertThat(artistQuery.getValue().imageThemeMode()).isEqualTo("isnull");
    }

    @Test
    void jsonMembershipHandlesLargeMatchingSetsWithoutSqlitePlaceholderLimits() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            String json = IntStream.rangeClosed(1, 1500).mapToObj(String::valueOf).collect(Collectors.joining(",", "[", "]"));
            ChartFilterDTO filter = ChartFilterDTO.builder().setCatalogType("songs").setMatchedCatalogIdsJson(json);
            assertThat(genderTotal(db.songRepository.getGeneralChartData(filter), "songsByGender")).isEqualTo(8);
        }
    }

    @Test
    void invalidTypedFiltersFailClearlyInsteadOfSilentlyIgnoringTheFilter() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertThatThrownBy(() -> resolver(db).resolve(request("lengthMin", "unknown"), "songs"))
                    .isInstanceOf(ResponseStatusException.class).hasMessageContaining("lengthMin");
        }
    }

    private CatalogGraphFilterResolver resolver(TestDatabaseSupport db) {
        return new CatalogGraphFilterResolver(db.artistRepository, db.albumRepository, db.songRepository, mock(ItunesService.class));
    }

    private MockHttpServletRequest request(String... pairs) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        for (int index = 0; index < pairs.length; index += 2) request.addParameter(pairs[index], pairs[index + 1]);
        return request;
    }

    private List<Integer> ids(ChartFilterDTO filter) {
        String body = filter.getMatchedCatalogIdsJson().replace("[", "").replace("]", "");
        return body.isEmpty() ? List.of() : Arrays.stream(body.split(",")).map(Integer::valueOf).toList();
    }

    private long genderTotal(Map<String, Object> data, String metric) {
        @SuppressWarnings("unchecked")
        Map<String, Number> genders = (Map<String, Number>) data.get(metric);
        return genders.values().stream().mapToLong(Number::longValue).sum();
    }
}
