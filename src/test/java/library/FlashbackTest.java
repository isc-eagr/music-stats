package library;

import library.controller.PlaygroundController;
import library.service.PlaygroundService;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.springframework.ui.ExtendedModelMap;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

class FlashbackTest {
    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private PlaygroundService service;

    @BeforeEach
    void setup() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE Genre (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        jdbc.execute("CREATE TABLE Artist (id INTEGER PRIMARY KEY, name TEXT NOT NULL, genre_id INTEGER, gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Album (id INTEGER PRIMARY KEY, artist_id INTEGER, name TEXT, override_genre_id INTEGER, image BLOB)");
        jdbc.execute("CREATE TABLE AlbumImage (id INTEGER PRIMARY KEY, album_id INTEGER, image BLOB, display_order INTEGER)");
        jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER, album_id INTEGER, name TEXT NOT NULL, length_seconds INTEGER, override_genre_id INTEGER, override_gender_id INTEGER, single_cover BLOB)");
        jdbc.execute("CREATE TABLE SongImage (id INTEGER PRIMARY KEY, song_id INTEGER, image BLOB, display_order INTEGER)");
        jdbc.execute("CREATE TABLE Play (id INTEGER PRIMARY KEY, play_date TEXT, song_id INTEGER)");
        jdbc.execute("CREATE INDEX idx_play_period_year_song ON Play(SUBSTR(play_date, 1, 4), play_date, song_id) WHERE play_date IS NOT NULL");
        jdbc.execute("CREATE INDEX idx_play_songid_date ON Play(song_id, play_date)");
        jdbc.execute("CREATE INDEX idx_song_artist ON Song(artist_id)");
        jdbc.update("INSERT INTO Genre VALUES (1, 'Reggaeton'), (2, 'Small genre')");
        jdbc.update("INSERT INTO Artist (id, name, genre_id, gender_id) VALUES (1, 'Artist One', 1, 2), (2, 'Artist Two', 1, 1)");
        jdbc.update("INSERT INTO Album (id, artist_id, name, image) VALUES (1, 1, 'Artwork Album', X'03')");

        List<Object[]> songs = new ArrayList<>();
        songs.add(new Object[]{1, 1, 1, "First track", 180, null, null, new byte[]{1}});
        songs.add(new Object[]{2, 2, null, "Second track", 200, null, null, new byte[]{2}});
        songs.add(new Object[]{3, 1, 1, "Older track", 220, null, null, new byte[]{3}});
        for (int id = 4; id <= 200; id++) {
            songs.add(new Object[]{id, 1, null, "Catalog track " + id, 180, null, null, null});
        }
        songs.add(new Object[]{201, 1, null, "Overridden track", 180, 2, null, null});
        jdbc.batchUpdate("INSERT INTO Song (id, artist_id, album_id, name, length_seconds, override_genre_id, override_gender_id, single_cover) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", songs);

        jdbc.update("INSERT INTO Play (play_date, song_id) VALUES ('2023-12-31 20:00', 3)");
        jdbc.update("INSERT INTO Play (play_date, song_id) VALUES ('2024-01-01 10:00', 1), ('2024-01-01 11:00', 1)");
        jdbc.update("INSERT INTO Play (play_date, song_id) VALUES ('2024-01-02 10:00', 2), ('2024-01-02 11:00', 2), ('2024-01-02 12:00', 2)");
        jdbc.update("INSERT INTO Play (play_date, song_id) VALUES ('2024-01-03 10:00', 3)");
        jdbc.update("INSERT INTO Play (play_date, song_id) VALUES ('2024-03-01 10:00', 1)");
        service = new PlaygroundService(jdbc);
    }

    @AfterEach
    void cleanup() {
        dataSource.destroy();
    }

    @Test
    void yearReviewBuildsFactsFirstListensRankingsAndMajorGenreCharts() {
        var review = service.getFlashback("year", 2024, "winter", 1);

        assertThat(review.title()).isEqualTo("Year in Review · 2024");
        assertThat(review.startDate().toString()).isEqualTo("2024-01-01");
        assertThat(review.endDate().toString()).isEqualTo("2024-12-31");
        assertThat(review.quickFacts().totalPlays()).isEqualTo(7);
        assertThat(review.quickFacts().listeningSeconds()).isEqualTo(1360);
        assertThat(review.quickFacts().newArtists()).isEqualTo(1);
        assertThat(review.quickFacts().newSongs()).isEqualTo(2);
        assertThat(review.quickFacts().mostPlayedNewArtist().name()).isEqualTo("Artist Two");
        assertThat(review.quickFacts().mostPlayedNewArtist().plays()).isEqualTo(3);
        assertThat(review.quickFacts().mostPlayedNewSong().name()).isEqualTo("First track");
        assertThat(review.quickFacts().mostPlayedNewSong().artistName()).isEqualTo("Artist One");
        assertThat(review.quickFacts().mostPlayedNewSong().plays()).isEqualTo(3);
        assertThat(review.quickFacts().longestStreakDays()).isEqualTo(3);
        assertThat(review.quickFacts().busiestDay().toString()).isEqualTo("2024-01-02");
        assertThat(review.quickFacts().busiestDayPlays()).isEqualTo(3);
        assertThat(review.topOverall()).extracting(PlaygroundService.FlashbackTrack::songName)
                .startsWith("First track", "Second track");
        assertThat(review.topOverall().getFirst().genderClass()).isEqualTo("gender-male");
        assertThat(review.topOverall().getFirst().albumHasImage()).isTrue();
        assertThat(review.topOverall().getFirst().songHasImage()).isTrue();
        assertThat(review.topOverall().get(1).genderClass()).isEqualTo("gender-female");
        assertThat(review.topOverall().get(1).albumHasImage()).isFalse();
        assertThat(review.topOverall().get(1).songHasImage()).isTrue();
        assertThat(review.genreRankings()).extracting(PlaygroundService.GenreRanking::name)
                .containsExactly("Reggaeton");
        assertThat(review.genreRankings().getFirst().songsPlayed()).isEqualTo(3);
        assertThat(review.genreRankings().getFirst().tracks().getFirst().songName()).isEqualTo("First track");
    }

    @Test
    void seasonAndMonthReviewsUseTheirSelectedCalendarBoundaries() {
        var spring = service.getFlashback("season", 2024, "spring", 1);
        assertThat(spring.startDate().toString()).isEqualTo("2024-03-01");
        assertThat(spring.endDate().toString()).isEqualTo("2024-05-31");
        assertThat(spring.quickFacts().totalPlays()).isEqualTo(1);

        var january = service.getFlashback("month", 2024, "winter", 1);
        assertThat(january.title()).isEqualTo("Month in Review · January 2024");
        assertThat(january.quickFacts().totalPlays()).isEqualTo(6);
    }

    @Test
    void rankedTracksResolveGalleryImagesAfterRankingCandidates() {
        jdbc.update("INSERT INTO Album (id, artist_id, name) VALUES (2, 2, 'Gallery Album')");
        jdbc.update("""
                INSERT INTO Song (id, artist_id, album_id, name, length_seconds)
                VALUES (202, 2, 2, 'Gallery track', 210)
                """);
        jdbc.update("INSERT INTO AlbumImage (id, album_id, image, display_order) VALUES (1, 2, X'04', 0)");
        jdbc.update("INSERT INTO SongImage (id, song_id, image, display_order) VALUES (1, 202, X'05', 0)");
        for (int hour = 0; hour < 8; hour++) {
            jdbc.update("INSERT INTO Play (play_date, song_id) VALUES (?, 202)",
                    "2024-02-01 %02d:00".formatted(hour));
        }

        var review = service.getFlashback("year", 2024, "winter", 1);

        assertThat(review.topOverall().getFirst().songName()).isEqualTo("Gallery track");
        assertThat(review.topOverall().getFirst().albumHasImage()).isTrue();
        assertThat(review.topOverall().getFirst().songHasImage()).isTrue();
        assertThat(review.genreRankings().getFirst().tracks().getFirst().songName()).isEqualTo("Gallery track");
        assertThat(review.genreRankings().getFirst().tracks().getFirst().albumHasImage()).isTrue();
        assertThat(review.genreRankings().getFirst().tracks().getFirst().songHasImage()).isTrue();
    }

    @Test
    void flashbackTabRendersItsSummaryAndTrackCards() {
        var controller = new PlaygroundController(service);
        var model = new ExtendedModelMap();
        controller.playground(2024, "flashback", "year", "winter", 1, model);

        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var request = new MockHttpServletRequest(servletContext);
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(request, new MockHttpServletResponse());
        var context = new WebContext(exchange, Locale.ENGLISH, model);

        var document = Jsoup.parse(engine.process("playground", context));
        assertThat(document.select(".playground-tabs a.active").text()).isEqualTo("Flashback");
        assertThat(document.select(".flashback-fact")).hasSize(6);
        assertThat(document.select(".flashback-feature-copy a").eachText())
                .containsExactly("First track", "First track");
        assertThat(document.select(".flashback-featured-track.gender-male")).hasSize(2);
        assertThat(document.select(".flashback-track-row.gender-female")).isNotEmpty();
        assertThat(document.select(".flashback-overall-card .flashback-track-list")).hasSize(1);
        assertThat(document.select(".flashback-new-leader a").eachText())
                .containsExactly("Artist Two", "First track");
        assertThat(document.select(".flashback-genre-summary small").text()).isEqualTo("3 songs played");
        assertThat(document.select(".flashback-genre-card > .flashback-featured-track > .flashback-genre-summary"))
                .hasSize(1);
        assertThat(document.select(".flashback-feature-art.album-art")).isNotEmpty();
        assertThat(document.select(".flashback-track-art.album-art")).isNotEmpty();
        assertThat(document.select(".flashback-track-art.song-art")).isNotEmpty();
        assertThat(document.select(".flashback-feature-art")).allSatisfy(image -> {
            assertThat(image.hasAttr("loading")).isFalse();
            assertThat(image.hasAttr("decoding")).isFalse();
        });
        assertThat(document.select(".flashback-track-art")).allSatisfy(image -> {
            assertThat(image.attr("loading")).isEqualTo("lazy");
            assertThat(image.hasAttr("decoding")).isFalse();
        });
        assertThat(document.select(".flashback-genre-card h4").text()).isEqualTo("Reggaeton");
    }
}
