package library.util;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ArtistFilterModeTest {

    @Test
    void encodesOnlyTheExclusiveMode() {
        assertThat(ArtistFilterMode.encode(List.of(2, 4), "excludes"))
                .containsExactly(-2, -4);
        assertThat(ArtistFilterMode.encode(List.of(2, 4), "includes"))
                .containsExactly(2, 4);
        assertThat(ArtistFilterMode.encode(List.of(2, 4), null))
                .containsExactly(2, 4);
    }
}
