package library;

import library.dto.AlbumStatsRow;
import library.dto.ArtistStatsRow;
import library.dto.SongStatsRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static library.TestDatabaseSupport.albumQueryWith;
import static library.TestDatabaseSupport.artistQueryWith;
import static library.TestDatabaseSupport.mapOf;
import static library.TestDatabaseSupport.songQueryWith;
import static org.assertj.core.api.Assertions.assertThat;

class CatalogSortRegressionTest {

    @Test
    void artistListSupportsEverySortOptionIndividually() {
        List<String> sortOptions = List.of(
                "age", "avg_length", "avg_plays", "avg_plays_album", "birth_date", "death_date",
                "songs", "featured", "albums", "plays", "time", "first_listened", "last_listened",
                "days_listened", "weeks_listened", "months_listened", "years_listened", "image_count",
                "country", "ethnicity", "featured_artist_count", "genre", "language", "legacy_plays",
                "primary_plays", "random", "solo_songs", "songs_with_features", "subgenre", "itunes_presence", "name");

        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            for (String sortBy : sortOptions) {
                assertThat(artistNames(db, mapOf("sortBy", sortBy, "sortDir", "asc", "limit", 3)))
                        .as("artist sort %s asc", sortBy)
                        .hasSizeLessThanOrEqualTo(3);
                assertThat(artistNames(db, mapOf("sortBy", sortBy, "sortDir", "desc", "limit", 3)))
                        .as("artist sort %s desc", sortBy)
                        .hasSizeLessThanOrEqualTo(3);
            }
        }
    }

    @Test
    void albumListSupportsEverySortOptionIndividually() {
        List<String> sortOptions = List.of(
                "artist", "avg_length", "avg_plays", "country", "ethnicity", "featured_artist_count",
                "genre", "language", "legacy_plays", "primary_plays", "release_date", "solo_songs",
                "song_count", "songs_with_features", "subgenre", "album_length", "plays", "time",
                "first_listened", "last_listened", "days_listened", "weeks_listened", "months_listened",
                "years_listened", "age_at_release", "birth_date", "death_date", "image_count",
                "seasonal_chart_peak", "weekly_chart_peak", "weekly_chart_weeks", "weekly_chart_peak_weeks",
                "yearly_chart_peak", "last_full_listen", "itunes_presence", "random", "name");

        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            for (String sortBy : sortOptions) {
                assertThat(albumNames(db, mapOf("sortBy", sortBy, "sortDir", "asc", "limit", 3)))
                        .as("album sort %s asc", sortBy)
                        .hasSizeLessThanOrEqualTo(3);
                assertThat(albumNames(db, mapOf("sortBy", sortBy, "sortDir", "desc", "limit", 3)))
                        .as("album sort %s desc", sortBy)
                        .hasSizeLessThanOrEqualTo(3);
            }
        }
    }

    @Test
    void songListSupportsEverySortOptionIndividually() {
        List<String> sortOptions = List.of(
                "artist", "album", "country", "ethnicity", "featured_artist_count", "release_date",
                "genre", "language", "legacy_plays", "length", "plays", "primary_plays", "track_number",
                "subgenre", "time", "first_listened", "last_listened", "days_listened", "weeks_listened",
                "months_listened", "years_listened", "age_at_release", "birth_date", "billboard_peak",
                "billboard_weeks", "billboard_weeks_at_peak", "death_date", "image_count",
                "seasonal_chart_peak", "trl_days", "trl_days_at_peak", "trl_peak", "weekly_chart_peak",
                "weekly_chart_weeks", "weekly_chart_peak_weeks", "vatos_cuntdown_days",
                "vatos_cuntdown_days_at_peak", "vatos_cuntdown_peak", "yearly_chart_peak", "random", "name");

        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            for (String sortBy : sortOptions) {
                assertThat(songNames(db, mapOf("sortBy", sortBy, "sortDirection", "asc", "limit", 3, "includeExpensiveStats", true)))
                        .as("song sort %s asc", sortBy)
                        .hasSizeLessThanOrEqualTo(3);
                assertThat(songNames(db, mapOf("sortBy", sortBy, "sortDirection", "desc", "limit", 3, "includeExpensiveStats", true)))
                        .as("song sort %s desc", sortBy)
                        .hasSizeLessThanOrEqualTo(3);
            }
        }
    }

    @Test
    void catalogSortsSupportOneTwoAndThreeLevelOrdering() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertThat(artistNames(db, mapOf("sortBy", "plays", "sortDir", "desc")))
                    .containsExactly("Selena", "Bad Bunny", "Legacy Legend", "Guest Singer", "Mystery Artist", "The Static Hearts");
            assertThat(artistNames(db, mapOf(
                    "sortBy", "country", "sortDir", "asc",
                    "sortBy2", "plays", "sortDir2", "desc",
                    "sortBy3", "name", "sortDir3", "asc")))
                    .containsExactly("Guest Singer", "Selena", "Legacy Legend", "Bad Bunny", "The Static Hearts", "Mystery Artist");

            assertThat(albumNames(db, mapOf(
                    "sortBy", "plays", "sortDir", "desc",
                    "sortBy2", "name", "sortDir2", "asc")))
                    .containsExactly("Un Verano Sin Ti", "Amor Prohibido", "Legacy Collection", "Silent Record", "Unknown Album");
            assertThat(albumNames(db, mapOf(
                    "sortBy", "genre", "sortDir", "asc",
                    "sortBy2", "song_count", "sortDir2", "desc",
                    "sortBy3", "name", "sortDir3", "asc")))
                    .containsExactly("Un Verano Sin Ti", "Amor Prohibido", "Legacy Collection", "Silent Record", "Unknown Album");

            assertThat(songNames(db, mapOf(
                    "sortBy", "plays", "sortDirection", "desc",
                    "sortBy2", "length", "sortDirection2", "desc")))
                    .containsExactly("Titi Me Pregunto", "Bidi Bidi Bom Bom", "Standalone Jam", "Old Hit",
                            "No Me Queda Mas", "Ojitos Lindos", "Quiet Track", "Unknown Silence");
            assertThat(songNames(db, mapOf(
                    "sortBy", "genre", "sortDirection", "asc",
                    "sortBy2", "plays", "sortDirection2", "desc",
                    "sortBy3", "name", "sortDirection3", "asc")))
                    .containsExactly("Unknown Silence", "No Me Queda Mas", "Titi Me Pregunto", "Standalone Jam",
                            "Ojitos Lindos", "Bidi Bidi Bom Bom", "Old Hit", "Quiet Track");
        }
    }

    private static List<String> artistNames(TestDatabaseSupport db, Map<String, Object> overrides) {
        return db.artistRepository.findArtistsWithStats(artistQueryWith(overrides)).stream()
                .map(ArtistStatsRow::name)
                .toList();
    }

    private static List<String> albumNames(TestDatabaseSupport db, Map<String, Object> overrides) {
        return db.albumRepository.findAlbumsWithStats(albumQueryWith(overrides)).stream()
                .map(AlbumStatsRow::name)
                .toList();
    }

    private static List<String> songNames(TestDatabaseSupport db, Map<String, Object> overrides) {
        return db.songRepository.findSongsWithStats(songQueryWith(overrides)).stream()
                .map(SongStatsRow::name)
                .toList();
    }
}
