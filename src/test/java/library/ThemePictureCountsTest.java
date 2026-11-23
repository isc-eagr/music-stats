package library;

import library.service.ThemeService;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The Themes page shows how many pictures each theme holds. */
class ThemePictureCountsTest {

    @Test
    void countsPicturesPerTheme() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            // Seed: theme 1 -> artist 1, theme 2 -> artist 3
            db.jdbcTemplate.update("INSERT INTO ArtistImageTheme (artist_id, theme_id) VALUES (4, 1), (5, 1)");
            db.jdbcTemplate.update("INSERT INTO ArtistTheme (id, name, is_active) VALUES (3, 'Empty', 0)");

            Map<Integer, Integer> counts = new ThemeService(null, null, db.jdbcTemplate).getPictureCountsByTheme();

            assertThat(counts).containsEntry(1, 3).containsEntry(2, 1).doesNotContainKey(3);
        }
    }
}
