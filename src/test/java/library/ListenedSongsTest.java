package library;

import library.dto.ListenedSongs;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The More panel shows the songs behind an artist's or album's first and last play. */
class ListenedSongsTest {

    @Test
    void artistFirstAndLastPlaysFollowTheListScope() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertThat(db.artistRepository.findFirstAndLastListenedSongs(1, true, false, false))
                    .isEqualTo(new ListenedSongs("Bidi Bidi Bom Bom", "Standalone Jam"));
            // Artist 4 has no songs of their own, only a featured spot on "No Me Queda Mas"
            assertThat(db.artistRepository.findFirstAndLastListenedSongs(4, true, false, false))
                    .isEqualTo(new ListenedSongs(null, null));
            assertThat(db.artistRepository.findFirstAndLastListenedSongs(4, true, false, true))
                    .isEqualTo(new ListenedSongs("No Me Queda Mas", "No Me Queda Mas"));
        }
    }

    @Test
    void albumFirstAndLastPlaysComeFromItsSongs() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertThat(db.albumRepository.findFirstAndLastListenedSongs(1))
                    .isEqualTo(new ListenedSongs("Bidi Bidi Bom Bom", "Bidi Bidi Bom Bom"));
        }
    }
}
