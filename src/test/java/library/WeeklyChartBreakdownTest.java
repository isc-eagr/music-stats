package library;

import library.dto.ChartBreakdownDTO;
import library.dto.ChartEntryDTO;
import library.repository.ChartEntryRepository;
import library.repository.ChartRepository;
import library.service.AppConfigService;
import library.service.ChartService;
import library.service.ItunesService;
import library.service.SongLinkService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Genre / Ethnicity / Language / Country tiles on the Weekly Chart page. */
class WeeklyChartBreakdownTest {

    private static ChartEntryDTO song(int songId, String genre) {
        ChartEntryDTO entry = new ChartEntryDTO();
        entry.setSongId(songId);
        entry.setGenreName(genre);
        return entry;
    }

    private static ChartEntryDTO album(int albumId) {
        ChartEntryDTO entry = new ChartEntryDTO();
        entry.setAlbumId(albumId);
        return entry;
    }

    private static ChartService chartService(TestDatabaseSupport db) {
        return new ChartService(mock(ChartRepository.class), mock(ChartEntryRepository.class), db.jdbcTemplate,
                mock(ItunesService.class), mock(AppConfigService.class), mock(SongLinkService.class));
    }

    @Test
    void songAttributesFollowTheOverrideChainBothWays() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            // Song 1 overrides ethnicity; album 2 overrides language for song 3, which has no override of its own
            db.jdbcTemplate.update("UPDATE Song SET override_ethnicity_id = 3 WHERE id = 1");
            db.jdbcTemplate.update("UPDATE Album SET override_language_id = 2 WHERE id = 2");

            List<ChartEntryDTO> entries = List.of(song(1, "Pop"), song(2, "Dance"), song(3, "Pop"), song(4, "Pop"), song(7, null));
            chartService(db).applyBreakdownAttributes(entries, false);

            assertThat(entries).extracting(ChartEntryDTO::getEthnicityName)
                    .containsExactly("Black", "Latina", "Latina", "Latina", null);
            // song 1: artist (Spanish); song 2: own override (English); songs 3 and 4: album override (English)
            assertThat(entries).extracting(ChartEntryDTO::getLanguageName)
                    .containsExactly("Spanish", "English", "English", "English", null);
            assertThat(entries).extracting(ChartEntryDTO::getCountryName)
                    .containsExactly("Mexico", "Mexico", "Puerto Rico", "Puerto Rico", null);
        }
    }

    @Test
    void albumLanguageUsesTheAlbumOverrideThenTheArtist() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<ChartEntryDTO> entries = List.of(album(1), album(3));
            db.jdbcTemplate.update("UPDATE Album SET override_language_id = 1 WHERE id = 3");
            chartService(db).applyBreakdownAttributes(entries, true);

            // Album 1 has no override: artist Selena (Spanish). Album 3 overrides to Spanish over its English artist.
            assertThat(entries).extracting(ChartEntryDTO::getLanguageName).containsExactly("Spanish", "Spanish");
            assertThat(entries).extracting(ChartEntryDTO::getEthnicityName).containsExactly("Latina", "White");
            assertThat(entries).extracting(ChartEntryDTO::getCountryName).containsExactly("Mexico", "United States");
        }
    }

    @Test
    void tilesRankEveryValueWithUnknownLast() {
        List<ChartEntryDTO> entries = List.of(song(1, "Pop"), song(2, "Rock"), song(3, "Pop"), song(4, null), song(5, "Pop"));

        List<ChartBreakdownDTO> tiles = ChartBreakdownDTO.forEntries(entries);

        assertThat(tiles).extracting(ChartBreakdownDTO::label).containsExactly("Genre", "Ethnicity", "Language", "Country");
        ChartBreakdownDTO genre = tiles.getFirst();
        assertThat(genre.total()).isEqualTo(5);
        assertThat(genre.slices()).containsExactly(
                new ChartBreakdownDTO.Slice("Pop", 3, 60),
                new ChartBreakdownDTO.Slice("Rock", 1, 20),
                new ChartBreakdownDTO.Slice("Unknown", 1, 20));
        assertThat(genre.top().name()).isEqualTo("Pop");
        // Nothing filled in: one Unknown slice covering the whole chart
        assertThat(tiles.get(1).slices()).containsExactly(new ChartBreakdownDTO.Slice("Unknown", 5, 100));
    }

    @Test
    void emptyChartHasNoSlices() {
        ChartBreakdownDTO genre = ChartBreakdownDTO.forEntries(List.of()).getFirst();

        assertThat(genre.total()).isZero();
        assertThat(genre.slices()).isEmpty();
        assertThat(genre.top()).isNull();
    }
}
