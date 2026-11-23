package library;

import library.service.ChartOverviewService;
import library.service.SeasonalWallService;
import library.service.SeasonalWallService.Tile;
import library.service.SeasonalWallService.Wall;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SeasonalWallServiceTest {

    private TestDatabaseSupport db;
    private SeasonalWallService service;

    @BeforeEach
    void setUp() {
        db = TestDatabaseSupport.create();
        // Seed already holds 2024-Spring (song 3 / album 2 at #1) and the 2024 yearly charts (#2 only).
        db.jdbcTemplate.update("""
                INSERT INTO Chart (id, chart_type, period_type, period_key, period_start_date, period_end_date, is_finalized)
                VALUES
                    (100, 'song', 'seasonal', '2024-Winter', '2023-12-01', '2024-02-29', 1),
                    (101, 'song', 'seasonal', '2024-Summer', '2024-06-01', '2024-08-31', 0),
                    (102, 'song', 'yearly', '2023', '2023-01-01', '2023-12-31', 1),
                    (103, 'song', 'seasonal', '2023-Fall', '2023-09-01', '2023-11-30', 1)
                """);
        db.jdbcTemplate.update("""
                INSERT INTO ChartEntry (id, chart_id, position, song_id, album_id, play_count)
                VALUES
                    (100, 100, 1, 4, NULL, 0),
                    (101, 100, 2, 1, NULL, 0),
                    (102, 100, 3, 3, NULL, 0),
                    (103, 100, 31, 8, NULL, 0),
                    (104, 102, 1, 1, NULL, 0),
                    (105, 103, 1, 1, NULL, 0),
                    (106, 103, 2, 3, NULL, 0)
                """);
        service = new SeasonalWallService(db.jdbcTemplate);
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    @Test
    void songWallShowsEachSeasonNumberOneWithBubblingUnderAsDotsOnly() {
        Wall wall = service.build("song");

        assertThat(wall.rows()).extracting(SeasonalWallService.Row::year).containsExactly(2024, 2023);
        List<Tile> seasons2024 = wall.rows().get(0).seasons();

        Tile winter = seasons2024.get(0);
        assertThat(winter.state()).isEqualTo("chart");
        assertThat(winter.title()).isEqualTo("Ojitos Lindos");
        assertThat(winter.link()).isEqualTo("/songs/4");
        assertThat(winter.gender()).as("song gender override wins").isEqualTo("female");
        assertThat(winter.dots()).as("position 31 is a bubbling-under dot").containsExactly("female", "female", "male", "male bub");
        assertThat(winter.maleCount()).isEqualTo(1);
        assertThat(winter.chartedCount()).isEqualTo(3);
        assertThat(winter.chartLink()).isEqualTo("/charts/seasonal/2024-Winter");

        assertThat(seasons2024.get(1).title()).isEqualTo("Titi Me Pregunto");
        assertThat(seasons2024.get(2).state()).isEqualTo("empty");
        assertThat(seasons2024.get(2).draft()).isTrue();
        assertThat(seasons2024.get(3).state()).as("after the latest season").isEqualTo("later");

        Tile year2024 = wall.rows().get(0).yearTile();
        assertThat(year2024.title()).isEqualTo("Bidi Bidi Bom Bom");
        assertThat(year2024.position()).as("the top entry even without a #1").isEqualTo(2);
        assertThat(year2024.coverUrl()).isEqualTo("/albums/1/image?thumbnail=true");
        assertThat(year2024.crowned()).isFalse();
    }

    @Test
    void seasonNumberOneThatAlsoLedTheYearIsCrowned() {
        List<Tile> seasons2023 = service.build("song").rows().get(1).seasons();

        assertThat(seasons2023.subList(0, 3)).extracting(Tile::state).containsOnly("none");
        assertThat(seasons2023.get(3).title()).isEqualTo("Bidi Bidi Bom Bom");
        assertThat(seasons2023.get(3).crowned()).isTrue();
        assertThat(service.build("song").rows().get(0).seasons()).noneMatch(Tile::crowned);
    }

    @Test
    void summaryCountsDistinctLeadersByGenderAndTheirYearlyFinish() {
        SeasonalWallService.Summary summary = service.build("song").summary();

        assertThat(summary.seasonsCharted()).isEqualTo(3);
        assertThat(summary.seasonsWaiting()).isEqualTo(1);
        assertThat(summary.firstSeason()).isEqualTo("Fall 2023");
        assertThat(summary.lastSeason()).isEqualTo("Spring 2024");
        assertThat(summary.seasonLeaders()).isEqualTo(new SeasonalWallService.GenderCounts(1, 2, 0));
        assertThat(summary.yearLeaders()).isEqualTo(new SeasonalWallService.GenderCounts(0, 1, 0));
        assertThat(summary.seasonLeadersOnYearly()).isEqualTo(1);
        assertThat(summary.seasonLeadersInYearlyTop10()).isEqualTo(1);
    }

    @Test
    void artistBoardListsEveryChartedArtistWithSeasonalAndYearlyTotals() {
        List<SeasonalWallService.ArtistRow> board = service.artistBoard(false);

        assertThat(board).extracting(SeasonalWallService.ArtistRow::name)
                .as("seasonal points first; bubbling-under-only artists still listed")
                .containsExactly("Bad Bunny", "Selena", "Legacy Legend");

        SeasonalWallService.ArtistRow badBunny = board.get(0);
        assertThat(badBunny.gender()).as("artist's own gender, not a song override").isEqualTo("male");
        // 2024-Spring #1, 2024-Winter #1 and #3, 2023-Fall #2 = 30 + 30 + 28 + 29
        assertThat(badBunny.seasonPoints()).isEqualTo(117);
        assertThat(badBunny.seasonEntries()).isEqualTo(4);
        assertThat(badBunny.seasonNumberOnes()).isEqualTo(2);
        assertThat(badBunny.seasonTop10()).isEqualTo(4);
        assertThat(badBunny.yearEntries()).isZero();
        assertThat(badBunny.firstSeason()).isEqualTo("Fall 2023");
        assertThat(badBunny.lastSeason()).isEqualTo("Spring 2024");

        SeasonalWallService.ArtistRow selena = board.get(1);
        assertThat(selena.seasonPoints()).isEqualTo(59);
        assertThat(selena.yearPoints()).as("2024 #2 and 2023 #1").isEqualTo(59);
        assertThat(selena.yearNumberOnes()).isEqualTo(1);
        assertThat(selena.coverUrl()).isEqualTo("/artists/1/image?thumbnail=true");

        SeasonalWallService.ArtistRow legacy = board.get(2);
        assertThat(legacy.bubblingUnder()).isEqualTo(1);
        assertThat(legacy.seasonPoints()).isZero();
        assertThat(legacy.seasonEntries()).isZero();
        assertThat(legacy.firstSeason()).isNull();
    }

    @Test
    void albumWallUsesAlbumCharts() {
        Wall wall = service.build("album");

        assertThat(wall.rows()).extracting(SeasonalWallService.Row::year).containsExactly(2024);
        Tile spring = wall.rows().get(0).seasons().get(1);
        assertThat(spring.title()).isEqualTo("Un Verano Sin Ti");
        assertThat(spring.link()).isEqualTo("/albums/2");
        assertThat(wall.rows().get(0).yearTile().coverUrl()).isEqualTo("/albums/1/image?thumbnail=true");
    }

    @Test
    void wallPageRendersTilesMenuAndViewToggle() {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new MockHttpServletRequest(servletContext), new MockHttpServletResponse());
        Map<String, Object> model = new HashMap<>();
        model.put("periodType", "yearly");
        model.put("overviewTab", "song");
        model.put("currentSection", "yearly-overview-charts");
        model.put("charts", ChartOverviewService.Chart.values());
        model.put("wall", service.build("song"));
        var document = Jsoup.parse(engine.process("charts/seasonal-wall", new WebContext(exchange, Locale.ENGLISH, model)));

        assertThat(document.select(".sw-grid:not(.sw-head)")).hasSize(2);
        assertThat(document.select(".sw-grid:not(.sw-head) > .sw-tile")).hasSize(10);
        assertThat(document.select(".sw-tile.sw-crowned .sw-title").text()).isEqualTo("Bidi Bidi Bom Bom");
        assertThat(document.select(".sw-tile.sw-yeartile")).hasSize(2);
        assertThat(document.select(".co-menubar a.co-tone-seasonal.active").text()).isEqualTo("Seasonal / Yearly");
        assertThat(document.select(".view-toggle a")).extracting(link -> link.attr("href"))
                .containsExactly("/charts/seasonal/overview?overviewTab=song",
                        "/charts/seasonal/overview?overviewTab=song&view=table",
                        "/charts/yearly/overview?overviewTab=song&view=table");
        assertThat(document.select(".sw-tile.sw-empty a.sw-placeholder").attr("href")).isEqualTo("/charts/seasonal/2024-Summer");
        assertThat(document.select(".sw-foot .sw-d.bub")).hasSize(1);
        assertThat(document.select("#swArtistBoard")).isEmpty();
    }

    @Test
    void featuredToggleCreditsGuestArtistsWithTheSongsPositions() {
        // Song 2 (Selena, #4 in 2024-Winter) features Guest Singer; Selena is also featured on Bad Bunny's song 3.
        db.jdbcTemplate.update("INSERT INTO ChartEntry (id, chart_id, position, song_id, album_id, play_count) VALUES (107, 100, 4, 2, NULL, 0)");
        db.jdbcTemplate.update("INSERT INTO SongFeaturedArtist (song_id, artist_id) VALUES (3, 1), (2, 1)");

        assertThat(service.artistBoard(false)).extracting(SeasonalWallService.ArtistRow::name)
                .doesNotContain("Guest Singer");

        List<SeasonalWallService.ArtistRow> board = service.artistBoard(true);
        SeasonalWallService.ArtistRow guest = board.stream().filter(a -> a.name().equals("Guest Singer")).findFirst().orElseThrow();
        assertThat(guest.seasonPoints()).isEqualTo(27);
        assertThat(guest.seasonEntries()).isEqualTo(1);
        assertThat(guest.gender()).isEqualTo("female");
        assertThat(guest.firstSeason()).isEqualTo("Winter 2024");

        SeasonalWallService.ArtistRow selena = board.stream().filter(a -> a.name().equals("Selena")).findFirst().orElseThrow();
        // Her own songs: 29 + 30 + 27 (song 2 lists her as featured on her own song, not counted twice),
        // plus Bad Bunny's song 3 she features on: 30 (Spring) + 28 (Winter) + 29 (Fall).
        assertThat(selena.seasonPoints()).isEqualTo(29 + 30 + 27 + 30 + 28 + 29);
        assertThat(selena.seasonNumberOnes()).isEqualTo(2);
        assertThat(board.stream().filter(a -> a.name().equals("Bad Bunny")).findFirst().orElseThrow().seasonPoints())
                .as("the main artist keeps the same totals").isEqualTo(117);
    }

    @Test
    void artistTabRendersOneBoardWithoutTheWall() {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new MockHttpServletRequest(servletContext), new MockHttpServletResponse());
        Map<String, Object> model = new HashMap<>();
        model.put("periodType", "seasonal");
        model.put("overviewTab", "artist");
        model.put("currentSection", "seasonal-overview-charts");
        model.put("charts", ChartOverviewService.Chart.values());
        model.put("artists", service.artistBoard(false));
        model.put("includeFeatured", true);
        var document = Jsoup.parse(engine.process("charts/seasonal-wall", new WebContext(exchange, Locale.ENGLISH, model)));

        assertThat(document.select("#swArtistBoard tbody tr")).hasSize(3);
        assertThat(document.select("#swArtistBoard tbody tr").first().attr("data-sp")).isEqualTo("117");
        assertThat(document.select("#swArtistBoard tbody tr.sw-female .sw-artist-name").text()).isEqualTo("Selena");
        assertThat(document.select(".sw-grid, .co-summary, .view-toggle")).isEmpty();
        assertThat(document.select("script[src='/js/seasonal-artists.js']")).hasSize(1);
        assertThat(document.select("#swIncludeFeatured[checked]")).hasSize(1);
    }
}
