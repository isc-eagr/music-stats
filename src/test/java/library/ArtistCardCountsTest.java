package library;

import library.dto.ArtistStatsRow;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Map;

import static library.TestDatabaseSupport.artistQueryWith;
import static library.TestDatabaseSupport.mapOf;
import static org.assertj.core.api.Assertions.assertThat;

/** The Artists list card shows Songs, Feat and Albums, so the plain list query must load them too. */
class ArtistCardCountsTest {

    @ParameterizedTest(name = "includeGroups={0}, includeFeatured={1}")
    @CsvSource({"false,false", "true,false", "false,true"})
    void cardCountsMatchTheExtendedStatsWithoutLazyLoading(boolean includeGroups, boolean includeFeatured) {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<String> plain = counts(db, mapOf("includeExtendedStats", false,
                    "includeGroups", includeGroups, "includeFeatured", includeFeatured));
            List<String> extended = counts(db, mapOf("includeExtendedStats", true,
                    "includeGroups", includeGroups, "includeFeatured", includeFeatured));

            assertThat(plain).isEqualTo(extended);
            assertThat(plain).anyMatch(row -> !row.endsWith("songs=0 albums=0 feat=0"));
        }
    }

    private static List<String> counts(TestDatabaseSupport db, Map<String, Object> overrides) {
        return db.artistRepository.findArtistsWithStats(artistQueryWith(overrides)).stream()
                .map(ArtistCardCountsTest::describe)
                .toList();
    }

    private static String describe(ArtistStatsRow row) {
        return row.name() + " songs=" + row.songCount() + " albums=" + row.albumCount() + " feat=" + row.featuredSongCount();
    }
}
