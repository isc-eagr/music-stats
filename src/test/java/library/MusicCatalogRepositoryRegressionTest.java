package library;

import library.dto.AlbumStatsRow;
import library.dto.ArtistStatsRow;
import library.dto.SongStatsRow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static library.TestDatabaseSupport.albumQuery;
import static library.TestDatabaseSupport.artistQuery;
import static library.TestDatabaseSupport.songQuery;
import static library.TestDatabaseSupport.songQueryWithExpensiveStats;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeout;

class MusicCatalogRepositoryRegressionTest {

    @Test
    void songListUsesPlayCountsOverrideFallbacksGenderOverridesAndStableSorts() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<SongStatsRow> rows = db.songRepository.findSongsWithStats(songQuery("plays", "desc"));

            assertThat(rows)
                    .extracting(SongStatsRow::name)
                    .containsExactly(
                            "Titi Me Pregunto",
                            "Bidi Bidi Bom Bom",
                            "Standalone Jam",
                            "No Me Queda Mas",
                            "Old Hit",
                            "Ojitos Lindos",
                            "Quiet Track",
                            "Unknown Silence"
                    );

            Map<String, SongStatsRow> byName = indexBy(rows, SongStatsRow::name);
            assertThat(byName.get("Bidi Bidi Bom Bom").playCount()).isEqualTo(3);
            assertThat(byName.get("Bidi Bidi Bom Bom").genreName()).isEqualTo("Rock");
            assertThat(byName.get("Bidi Bidi Bom Bom").subgenreName()).isEqualTo("Alt Rock");
            assertThat(byName.get("Bidi Bidi Bom Bom").languageName()).isEqualTo("Spanish");
            assertThat(byName.get("Bidi Bidi Bom Bom").hasImage()).isTrue();
            assertThat(byName.get("Bidi Bidi Bom Bom").albumHasImage()).isTrue();

            assertThat(byName.get("No Me Queda Mas").genreName()).isEqualTo("Dance");
            assertThat(byName.get("No Me Queda Mas").subgenreName()).isEqualTo("Dance Pop");
            assertThat(byName.get("No Me Queda Mas").languageName()).isEqualTo("English");
            assertThat(byName.get("No Me Queda Mas").featuredArtistCount()).isEqualTo(1);

            List<SongStatsRow> femaleRows = db.songRepository.findSongsWithStats(
                    songQuery(List.of(2), "includes", null, null, null, null, "name", "asc"));
            assertThat(femaleRows)
                    .extracting(SongStatsRow::name)
                    .containsExactly("Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Standalone Jam");
            assertThat(indexBy(femaleRows, SongStatsRow::name).get("Ojitos Lindos").genderName()).isEqualTo("Female");
        }
    }

    @Test
    void songListScopesPlayStatsByAccountAndListenedDate() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<SongStatsRow> legacyOnly = db.songRepository.findSongsWithStats(
                    songQuery(null, null, List.of("robertlover"), "includes", null, null, "plays", "desc"));

            assertThat(legacyOnly)
                    .extracting(SongStatsRow::name)
                    .containsExactly("Titi Me Pregunto", "Bidi Bidi Bom Bom", "Standalone Jam");
            assertThat(indexBy(legacyOnly, SongStatsRow::name).get("Titi Me Pregunto").playCount()).isEqualTo(2);
            assertThat(indexBy(legacyOnly, SongStatsRow::name).get("Bidi Bidi Bom Bom").playCount()).isEqualTo(1);

            List<SongStatsRow> januaryOnly = db.songRepository.findSongsWithStats(
                    songQuery(null, null, null, null, "2024-01-01", "2024-01-31", "plays", "desc"));

            assertThat(januaryOnly)
                    .extracting(SongStatsRow::name)
                    .containsExactly("Bidi Bidi Bom Bom", "Titi Me Pregunto", "No Me Queda Mas");
            assertThat(indexBy(januaryOnly, SongStatsRow::name).get("Bidi Bidi Bom Bom").playCount()).isEqualTo(2);
            assertThat(indexBy(januaryOnly, SongStatsRow::name).get("No Me Queda Mas").lastListened())
                    .isEqualTo("2024-01-05 10:00:00");
        }
    }

    @Test
    void songListCanIncludeExpensiveChartStatsWithoutChangingBaseRows() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<SongStatsRow> rows = db.songRepository.findSongsWithStats(
                    songQueryWithExpensiveStats("weekly_chart_weeks", "desc"));

            Map<String, SongStatsRow> byName = indexBy(rows, SongStatsRow::name);
            assertThat(byName.get("Bidi Bidi Bom Bom").weeklyChartPeak()).isEqualTo(1);
            assertThat(byName.get("Bidi Bidi Bom Bom").weeklyChartWeeks()).isEqualTo(2);
            assertThat(byName.get("Titi Me Pregunto").weeklyChartPeak()).isEqualTo(1);
            assertThat(byName.get("Quiet Track").weeklyChartPeak()).isNull();
        }
    }

    @Test
    void artistListAggregatesStatsAndGenderCountsFromFilteredCatalog() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<ArtistStatsRow> rows = db.artistRepository.findArtistsWithStats(artistQuery("plays", "desc"));

            assertThat(rows)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Selena", "Bad Bunny", "Legacy Legend", "Guest Singer", "Mystery Artist", "The Static Hearts");

            Map<String, ArtistStatsRow> byName = indexBy(rows, ArtistStatsRow::name);
            assertThat(byName.get("Selena").playCount()).isEqualTo(6);
            assertThat(byName.get("Selena").songCount()).isEqualTo(3);
            assertThat(byName.get("Selena").albumCount()).isEqualTo(1);
            assertThat(byName.get("Selena").featuredArtistCount()).isEqualTo(1);
            assertThat(byName.get("Selena").imageCount()).isEqualTo(2);

            List<ArtistStatsRow> femaleRows = db.artistRepository.findArtistsWithStats(
                    artistQuery(List.of(2), "includes", null, null, null, null, "name", "asc"));
            assertThat(femaleRows)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Guest Singer", "Selena");

            Map<Integer, Long> genderCounts = db.artistRepository.countArtistsByGenderWithFilters(artistQuery("name", "asc"));
            assertThat(genderCounts).containsEntry(1, 3L).containsEntry(2, 2L);
        }
    }

    @Test
    void artistListScopesStatsByAccountAndListenedDate() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<ArtistStatsRow> rows = db.artistRepository.findArtistsWithStats(
                    artistQuery(null, null, List.of("robertlover"), "includes", null, null, "plays", "desc"));

            assertThat(rows)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Bad Bunny", "Selena");
            assertThat(indexBy(rows, ArtistStatsRow::name).get("Bad Bunny").playCount()).isEqualTo(2);
            assertThat(indexBy(rows, ArtistStatsRow::name).get("Selena").playCount()).isEqualTo(2);

            List<ArtistStatsRow> januaryOnly = db.artistRepository.findArtistsWithStats(
                    artistQuery(null, null, null, null, "2024-01-01", "2024-01-31", "plays", "desc"));

            assertThat(januaryOnly)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Selena", "Bad Bunny");
            assertThat(indexBy(januaryOnly, ArtistStatsRow::name).get("Selena").playCount()).isEqualTo(3);
            assertThat(indexBy(januaryOnly, ArtistStatsRow::name).get("Bad Bunny").playCount()).isEqualTo(2);
        }
    }

    @Test
    void albumListAggregatesStatsOverridesAndLastFullListen() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<AlbumStatsRow> rows = db.albumRepository.findAlbumsWithStats(albumQuery("plays", "desc"));

            assertThat(rows)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Un Verano Sin Ti", "Amor Prohibido", "Legacy Collection", "Silent Record", "Unknown Album");

            Map<String, AlbumStatsRow> byName = indexBy(rows, AlbumStatsRow::name);
            assertThat(byName.get("Amor Prohibido").playCount()).isEqualTo(4);
            assertThat(byName.get("Amor Prohibido").songCount()).isEqualTo(2);
            assertThat(byName.get("Amor Prohibido").albumLength()).isEqualTo(410);
            assertThat(byName.get("Amor Prohibido").genreName()).isEqualTo("Rock");
            assertThat(byName.get("Amor Prohibido").languageName()).isEqualTo("Spanish");
            assertThat(byName.get("Amor Prohibido").featuredArtistCount()).isEqualTo(1);
            assertThat(byName.get("Amor Prohibido").imageCount()).isEqualTo(2);

            List<AlbumStatsRow> fullListenRows = db.albumRepository.findAlbumsWithStats(
                    albumQuery("last_full_listen", "desc"));
            assertThat(indexBy(fullListenRows, AlbumStatsRow::name).get("Amor Prohibido").lastFullListenDate())
                    .isEqualTo("2024-02-01");
            assertThat(indexBy(fullListenRows, AlbumStatsRow::name).get("Un Verano Sin Ti").lastFullListenDate())
                    .isNull();
        }
    }

    @Test
    void albumListScopesStatsByGenderAccountAndListenedDate() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<AlbumStatsRow> femaleRows = db.albumRepository.findAlbumsWithStats(
                    albumQuery(List.of(2), "includes", null, null, null, null, "name", "asc"));
            assertThat(femaleRows)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Amor Prohibido");

            List<AlbumStatsRow> legacyOnly = db.albumRepository.findAlbumsWithStats(
                    albumQuery(null, null, List.of("robertlover"), "includes", null, null, "plays", "desc"));
            assertThat(legacyOnly)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Un Verano Sin Ti", "Amor Prohibido");
            assertThat(indexBy(legacyOnly, AlbumStatsRow::name).get("Un Verano Sin Ti").playCount()).isEqualTo(2);
            assertThat(indexBy(legacyOnly, AlbumStatsRow::name).get("Amor Prohibido").playCount()).isEqualTo(1);

            List<AlbumStatsRow> januaryOnly = db.albumRepository.findAlbumsWithStats(
                    albumQuery(null, null, null, null, "2024-01-01", "2024-01-31", "plays", "desc"));
            assertThat(januaryOnly)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Amor Prohibido", "Un Verano Sin Ti");
        }
    }

    @Test
    void representativeCatalogQueriesStayWithinPerformanceSmokeBudget() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertTimeout(Duration.ofSeconds(2), () -> {
                db.songRepository.findSongsWithStats(songQuery("plays", "desc"));
                db.songRepository.findSongsWithStats(songQueryWithExpensiveStats("weekly_chart_weeks", "desc"));
                db.artistRepository.findArtistsWithStats(artistQuery("plays", "desc"));
                db.artistRepository.countArtistsByGenderWithFilters(artistQuery("name", "asc"));
                db.albumRepository.findAlbumsWithStats(albumQuery("plays", "desc"));
                db.albumRepository.findAlbumsWithStats(albumQuery("last_full_listen", "desc"));
            });
        }
    }

    private static <T> Map<String, T> indexBy(List<T> rows, Function<T, String> keyExtractor) {
        return rows.stream().collect(Collectors.toMap(keyExtractor, Function.identity()));
    }
}
