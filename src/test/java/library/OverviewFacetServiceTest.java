package library;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartSongOverviewRowDTO;
import library.repository.LookupRepository;
import library.service.OverviewFacetService;
import library.service.OverviewFilterService;
import org.junit.jupiter.api.Test;
import org.springframework.util.LinkedMultiValueMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OverviewFacetServiceTest {

    @Test
    void filtersSeeCatalogEditsOnTheNextRequestWithoutCacheExpiry() {
        try (var db = TestDatabaseSupport.create()) {
            var service = new OverviewFacetService(db.jdbcTemplate, new LookupRepository(db.jdbcTemplate));
            var row = song(1);
            var params = new LinkedMultiValueMap<String, String>();
            params.add("gender", "2");
            assertThat(service.filter("song", List.of(row), params)).containsExactly(row);
            db.jdbcTemplate.update("UPDATE Song SET override_gender_id = 1 WHERE id = 1");
            assertThat(service.filter("song", List.of(row), params)).isEmpty();
        }
    }

    @Test
    void songFacetsUseOverridesAndEntityTags() {
        try (var db = TestDatabaseSupport.create()) {
            var service = new OverviewFilterService(new OverviewFacetService(
                    db.jdbcTemplate, new LookupRepository(db.jdbcTemplate)));
            var first = song(1);
            var second = song(2);
            var params = new LinkedMultiValueMap<String, String>();
            params.add("gender", "2");
            params.add("genre", "2");
            params.add("subgenre", "2");
            params.add("country", "Mexico");
            params.add("ethnicity", "1");
            params.add("language", "1");
            params.add("tag", "10");

            assertThat(service.filter("weekly", "song", List.of(first, second), params))
                    .containsExactly(first);
        }
    }

    @Test
    void artistToggleIncludesSongsWhereSelectedArtistIsFeatured() {
        try (var db = TestDatabaseSupport.create()) {
            var service = new OverviewFilterService(new OverviewFacetService(
                    db.jdbcTemplate, new LookupRepository(db.jdbcTemplate)));
            var primary = song(1);
            var featured = song(2);
            var params = new LinkedMultiValueMap<String, String>();
            params.add("artist", "4");

            assertThat(service.filter("weekly", "song", List.of(primary, featured), params)).isEmpty();
            params.set("includeFeaturedSongs", "true");
            assertThat(service.filter("weekly", "song", List.of(primary, featured), params))
                    .containsExactly(featured);
        }
    }

    @Test
    void albumAndArtistTabsUseTheirOwnTagsAndFacets() {
        try (var db = TestDatabaseSupport.create()) {
            var service = new OverviewFilterService(new OverviewFacetService(
                    db.jdbcTemplate, new LookupRepository(db.jdbcTemplate)));
            var album = new ChartAlbumOverviewRowDTO(); album.setAlbumId(1); album.setResolvedArtistId(1);
            var artist = new ChartArtistOverviewRowDTO(); artist.setResolvedArtistId(1);
            var params = new LinkedMultiValueMap<String, String>();
            params.add("tag", "10");
            params.add("genre", "2");

            assertThat(service.filter("billboard", "album", List.of(album), params)).containsExactly(album);
            params.set("genre", "1");
            assertThat(service.filter("trl", "artist", List.of(artist), params)).containsExactly(artist);
        }
    }

    @Test
    void excludesModeRetainsRowsWhoseFacetIsEmptyLikeCatalogLists() {
        try (var db = TestDatabaseSupport.create()) {
            var service = new OverviewFilterService(new OverviewFacetService(
                    db.jdbcTemplate, new LookupRepository(db.jdbcTemplate)));
            var row = song(7);
            var params = new LinkedMultiValueMap<String, String>();
            params.add("genre", "1");
            params.set("genreMode", "excludes");

            assertThat(service.filter("weekly", "song", List.of(row), params)).containsExactly(row);
        }
    }

    @Test
    void optionsAreAlphabeticalAndUseCatalogIds() {
        try (var db = TestDatabaseSupport.create()) {
            var service = new OverviewFacetService(db.jdbcTemplate, new LookupRepository(db.jdbcTemplate));
            var options = service.getOptions();

            assertThat(options.get("genre")).extracting(OverviewFacetService.Option::label)
                    .containsExactly("Dance", "Pop", "Rock");
            assertThat(options.get("country")).extracting(OverviewFacetService.Option::value)
                    .containsExactly("Colombia", "Mexico", "Puerto Rico", "United States");
            assertThat(options.get("tag")).extracting(OverviewFacetService.Option::value)
                    .containsExactly("20", "10");
        }
    }

    private ChartSongOverviewRowDTO song(int id) {
        var row = new ChartSongOverviewRowDTO();
        row.setSongId(id);
        row.setArtistId(1);
        row.setSongTitle("Song " + id);
        return row;
    }
}
