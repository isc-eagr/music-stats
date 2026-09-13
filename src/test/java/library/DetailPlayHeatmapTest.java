package library;

import library.service.DetailPlayHeatmapService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DetailPlayHeatmapTest {
    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private DetailPlayHeatmapService service;

    @BeforeEach
    void setup() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE Artist (id INTEGER PRIMARY KEY, gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Album (id INTEGER PRIMARY KEY, artist_id INTEGER)");
        jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER, album_id INTEGER, override_gender_id INTEGER)");
        jdbc.execute("CREATE TABLE SongFeaturedArtist (song_id INTEGER, artist_id INTEGER)");
        jdbc.execute("CREATE TABLE Play (play_date TEXT, song_id INTEGER)");
        jdbc.execute("INSERT INTO Artist VALUES (1, 2), (2, 1), (3, 3)");
        jdbc.execute("INSERT INTO Album VALUES (10, 1), (20, 2)");
        jdbc.execute("INSERT INTO Song VALUES (100, 1, 10, 1), (200, 2, 20, NULL), (300, 3, NULL, NULL)");
        jdbc.execute("INSERT INTO SongFeaturedArtist VALUES (300, 1)");
        service = new DetailPlayHeatmapService(jdbc);
    }

    @AfterEach
    void cleanup() {
        dataSource.destroy();
    }

    @Test
    void scopesCountsAndCalculatedGenderPerDetailType() {
        play("2024-01-01 10:00", 100);
        play("2024-01-01 11:00", 100);
        play("2024-02-29 12:00", 200);
        play("2024-03-01 12:00", 300);

        var song = service.forSong(100, 2024);
        assertThat(song.gender()).isEqualTo("female");
        assertThat(song.totalPlays()).isEqualTo(2);
        assertThat(song.days()).hasSize(366);
        assertThat(song.days().getFirst().intensity()).isEqualTo(4);
        assertThat(song.days().get(59).total()).isZero();

        var album = service.forAlbum(10, 2024);
        assertThat(album.gender()).isEqualTo("male");
        assertThat(album.totalPlays()).isEqualTo(2);

        var artistWithFeatured = service.forArtist(1, List.of(), true, true, 2024);
        assertThat(artistWithFeatured.gender()).isEqualTo("male");
        assertThat(artistWithFeatured.totalPlays()).isEqualTo(3);
        assertThat(artistWithFeatured.days().get(60).label()).isEqualTo("01/03/2024: 1 play");

        var groupOnly = service.forArtist(1, List.of(2), false, false, 2024);
        assertThat(groupOnly.totalPlays()).isEqualTo(1);
        assertThat(groupOnly.days().get(59).date()).isEqualTo(LocalDate.of(2024, 2, 29));
    }

    @Test
    void emptyScopeAndMissingYearStillProduceAFullCalendar() {
        var heatmap = service.forArtist(1, null, false, false, 2012);
        assertThat(heatmap.gender()).isEqualTo("male");
        assertThat(heatmap.totalPlays()).isZero();
        assertThat(heatmap.days()).hasSize(366);
        assertThat(heatmap.years()).containsExactly(LocalDate.now().getYear(), 2012);
    }

    @Test
    void allDetailTemplatesExposeTheToggleAndSharedAssets() throws Exception {
        for (String type : List.of("artists", "albums", "songs")) {
            String template = Files.readString(Path.of("src/main/resources/templates", type, "detail.html"));
            assertThat(template).contains("data-plays-view=\"chart\"")
                    .contains("data-plays-view=\"heatmap\"")
                    .contains("fragments/detail-play-heatmap :: calendar")
                    .contains("detail-play-heatmap.css")
                    .contains("detail-play-heatmap.js");
        }
        String css = Files.readString(Path.of("src/main/resources/static/css/detail-play-heatmap.css"));
        assertThat(css).contains(".detail-heatmap-male .intensity-4")
                .contains(".detail-heatmap-female .intensity-4")
                .contains(".detail-heatmap-other .intensity-4")
                .contains(".plays-chart-section > [data-plays-visualization] { padding: 18px 20px 16px; }")
                .contains("linear-gradient(135deg");
        String fragment = Files.readString(Path.of("src/main/resources/templates/fragments/detail-play-heatmap.html"));
        assertThat(fragment).doesNotContain("detail-heatmap-detail");
    }

    private void play(String date, int songId) {
        jdbc.update("INSERT INTO Play VALUES (?, ?)", date, songId);
    }
}
