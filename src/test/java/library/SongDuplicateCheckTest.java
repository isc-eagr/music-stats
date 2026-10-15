package library;

import library.controller.SongController;
import library.service.SongService;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class SongDuplicateCheckTest {
    @Test
    void findsExactTitlesForTheMainArtistRegardlessOfAlbum() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            db.jdbcTemplate.update("INSERT INTO Song (artist_id, album_id, name) VALUES (1, 1, ?)", "Don't Stop & Go");
            db.jdbcTemplate.update("INSERT INTO Song (artist_id, album_id, name) VALUES (2, NULL, ?)", "A Single");

            assertThat(db.songRepository.existsByArtistAndName(1, "Don't Stop & Go")).isTrue();
            assertThat(db.songRepository.existsByArtistAndName(2, "A Single")).isTrue();
            assertThat(db.songRepository.existsByArtistAndName(2, "Don't Stop & Go")).isFalse();
            assertThat(db.songRepository.existsByArtistAndName(1, "Don't Stop & Go (Live)")).isFalse();
            assertThat(db.songRepository.existsByArtistAndName(1, "Don't Stop%")).isFalse();
            assertThat(db.songRepository.existsByArtistAndName(1, "Missing")).isFalse();
        }
    }

    @Test
    void missingArtistOrTitleDoesNotMatch() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertThat(db.songRepository.existsByArtistAndName(null, "A Single")).isFalse();
            assertThat(db.songRepository.existsByArtistAndName(1, null)).isFalse();
            assertThat(db.songRepository.existsByArtistAndName(1, " ")).isFalse();
        }
    }

    @Nested
    @ExtendWith(MockitoExtension.class)
    class EndpointChecks {
        @Mock
        private SongService songService;
        @InjectMocks
        private SongController controller;

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void duplicateEndpointReturnsCurrentResult(boolean exists) throws Exception {
            when(songService.songExists(12, "Don't Stop & Go")).thenReturn(exists);

            MockMvcBuilders.standaloneSetup(controller).build()
                    .perform(get("/songs/api/duplicate").param("artistId", "12").param("name", "Don't Stop & Go"))
                    .andExpect(status().isOk())
                    .andExpect(header().string("Cache-Control", "no-store"))
                    .andExpect(content().json("{\"exists\":" + exists + "}"));

            verify(songService).songExists(12, "Don't Stop & Go");
        }
    }
}
