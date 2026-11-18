package library;

import library.dto.ChartHistoryDTO;
import library.dto.ChartHistoryViewDTO;
import library.dto.ChartHistoryViewDTO.CalendarCell;
import library.dto.ChartHistoryViewDTO.CalendarGrid;
import library.dto.ChartHistoryViewDTO.HitCard;
import library.dto.ChartHistoryViewDTO.HitSection;
import library.dto.ChartHistoryViewDTO.Tile;
import library.service.DetailChartHistoryService;
import library.util.ChartAggregationUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.templateresolver.StringTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DetailChartHistoryServiceTest {

    private final DetailChartHistoryService service = new DetailChartHistoryService();

    @Test
    void artistTilesSpreadAndSectionsCoverEveryChart() {
        ChartHistoryViewDTO view = artistView();

        assertThat(view.tiles()).extracting(Tile::key)
                .containsExactly("weekly", "seasonal", "yearly", "bb", "pc", "trl");
        Tile weekly = view.tiles().get(0);
        assertThat(weekly.bestPeak()).isEqualTo(1);
        // Weeks at #1 add up across every entry that got there: Titi 4 + the album 9
        assertThat(weekly.bestCount()).isEqualTo(13);
        assertThat(weekly.bestLabel()).isEqualTo("weeks at #1");
        assertThat(weekly.stats()).extracting(s -> s.value() + " " + s.label())
                .containsExactly("3 songs", "1 album", "100 weeks", "2 at #1");
        assertThat(weekly.target()).isEqualTo("weekly");
        Tile seasonalTile = view.tiles().get(1);
        assertThat(seasonalTile.target()).isEqualTo("calendar");
        assertThat(seasonalTile.bestLabel()).isEqualTo("time at #1");
        assertThat(seasonalTile.stats()).extracting(s -> s.value() + " " + s.label())
                .containsExactly("2 seasons", "3 entries", "1 at #1");
        assertThat(view.tiles().get(2).stats().get(0).label()).isEqualTo("year");
        Tile trl = view.tiles().get(5);
        assertThat(trl.bestPeak()).isEqualTo(6);
        assertThat(trl.bestCount()).isEqualTo(2);
        assertThat(trl.bestLabel()).isEqualTo("days at #6");
        // No #1 on TRL, so the last stat counts the top 10 instead
        assertThat(trl.stats()).extracting(s -> s.value() + " " + s.label()).containsExactly("1 song", "8 days", "1 top 10");
        assertThat(view.firstTarget()).isEqualTo("weekly");

        // Where they peaked: #1, #2-5, #6-10, #11+ per chart, empty bands left out
        assertThat(view.spread()).extracting(r -> r.name())
                .containsExactly("Weekly songs", "Weekly albums", "Hot 100", "Vato's Cuntdown", "TRL");
        assertThat(view.spread().get(0).segments()).extracting(s -> s.band() + "=" + s.count())
                .containsExactly("one=1", "top5=1", "rest=1");
        assertThat(view.spread().get(0).total()).isEqualTo(3);

        HitSection weeklySection = view.sections().get(0);
        assertThat(weeklySection.hasBoth()).isTrue();
        // Sorted by peak, then weeks at peak, then weeks
        assertThat(weeklySection.songs()).extracting(HitCard::name).containsExactly("Titi", "Safaera", "Vete");
        HitCard titi = weeklySection.songs().get(0);
        assertThat(titi.runUrl()).isEqualTo("/charts/song/10/run");
        assertThat(titi.debutHref()).isEqualTo("/charts/weekly/2022-W19");
        assertThat(titi.coverUrl()).isEqualTo("/songs/10/image?thumbnail=true");
        assertThat(titi.fallbackUrl()).isEqualTo("/albums/5/image?thumbnail=true");
        assertThat(titi.initials()).isEqualTo("T");
        HitCard safaera = weeklySection.songs().get(1);
        assertThat(safaera.sub()).isEqualTo("feat. on Cardi B");
        assertThat(safaera.subHref()).isEqualTo("/artists/77");
        assertThat(safaera.coverUrl()).isEqualTo("/albums/6/image?thumbnail=true");
        assertThat(weeklySection.albums()).extracting(HitCard::runUrl).containsExactly("/charts/album/5/run");

        HitSection bb = view.sections().get(1);
        assertThat(bb.key()).isEqualTo("bb");
        assertThat(bb.songScale()).isEqualTo(100);
        HitCard bbCard = bb.songs().get(0);
        assertThat(bbCard.debutLabel()).isEqualTo("13/04/2018");
        assertThat(bbCard.debutHref()).isEqualTo("/misc/billboard-hot-100/recaps?date=2018-04-13");
        assertThat(bbCard.total()).isEqualTo(32);
        assertThat(bbCard.atPeak()).isEqualTo(1);
        // Featured song from the toggle-scoped query is labelled like the weekly cards
        assertThat(bbCard.sub()).isEqualTo("feat. on Cardi B");
        assertThat(bbCard.subHref()).isEqualTo("/artists/77");
        assertThat(view.sections().get(2).songs().get(0).sub()).isNull();
        assertThat(view.sections().get(2).unit()).isEqualTo("days");
        assertThat(view.sections().get(2).songs().get(0).total()).isEqualTo(46);
    }

    @Test
    void seasonalAndYearlySpreadCountsEachSongByItsBestPlacement() {
        ChartHistoryViewDTO view = artistView();

        // Titi #1 and Vete #12 in Summer 2022, Safaera #4 in Spring 2020; Titi #1 year-end 2022
        assertThat(view.periodSpread()).extracting(r -> r.name()).containsExactly("Seasonal songs", "Yearly songs");
        assertThat(view.periodSpread().get(0).segments()).extracting(s -> s.band() + "=" + s.count())
                .containsExactly("one=1", "top5=1", "rest=1");
        assertThat(view.periodSpread().get(0).total()).isEqualTo(3);
        assertThat(view.periodSpread().get(1).total()).isEqualTo(1);

        List<Map<String, Object>> repeats = List.of(
                placement("songId", 10, "songName", "Titi", "2022-Summer", 3),
                placement("songId", 10, "songName", "Titi", "2022-Fall", 9),
                placement("songId", 10, "songName", "Titi", "2023-Winter", 1));
        ChartHistoryViewDTO repeated = service.forArtist(1, List.of(), List.of(), repeats, List.of(), List.of(), List.of(),
                List.of(), List.of(), List.of());
        assertThat(repeated.periodSpread().get(0).total()).isEqualTo(1);
        assertThat(repeated.periodSpread().get(0).segments()).extracting(s -> s.band()).containsExactly("one");
        assertThat(repeated.spread()).isEmpty();
        assertThat(repeated.hasSpread()).isTrue();
    }

    @Test
    void artistSongScopeFollowsMainGroupsAndFeaturedToggles() {
        var mainOnly = ChartAggregationUtils.artistSongScope("s", List.of(5), null);
        assertThat(mainOnly.where()).isEqualTo("s.artist_id IN (?)");
        assertThat(mainOnly.featuredFlag()).isEqualTo("0");
        assertThat(mainOnly.params()).containsExactly(5);

        var all = ChartAggregationUtils.artistSongScope("s", List.of(5, 9), 5);
        assertThat(all.where()).contains("s.artist_id IN (?,?) OR s.id IN (SELECT sfa.song_id FROM SongFeaturedArtist sfa");
        assertThat(all.featuredFlag()).startsWith("CASE WHEN s.artist_id IN (?,?)");
        assertThat(all.params()).containsExactly(5, 9, 5, 9, 5);

        var featuredOnly = ChartAggregationUtils.artistSongScope("s", List.of(), 5);
        assertThat(featuredOnly.featuredFlag()).isEqualTo("1");
        assertThat(featuredOnly.params()).containsExactly(5);

        assertThat(ChartAggregationUtils.artistSongScope("s", List.of(), null)).isNull();
    }

    @Test
    void artistSongScopeSelectsTheRightSongs() {
        var ds = new org.springframework.jdbc.datasource.SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        try {
            var jdbc = new org.springframework.jdbc.core.JdbcTemplate(ds);
            jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER)");
            jdbc.execute("CREATE TABLE SongFeaturedArtist (song_id INTEGER, artist_id INTEGER)");
            jdbc.execute("INSERT INTO Song VALUES (1, 5), (2, 9), (3, 7), (4, 8)");
            jdbc.execute("INSERT INTO SongFeaturedArtist VALUES (3, 5)");
            var scope = ChartAggregationUtils.artistSongScope("s", List.of(5, 9), 5);
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT s.id, " + scope.featuredFlag() + " AS featured_on FROM Song s WHERE " + scope.where() + " ORDER BY s.id",
                    scope.params().toArray());
            assertThat(rows).extracting(r -> r.get("id") + ":" + r.get("featured_on")).containsExactly("1:0", "2:0", "3:1");
        } finally {
            ds.destroy();
        }
    }

    @Test
    void calendarPlacesSeasonsAndYearEndsAndFillsGapYears() {
        ChartHistoryViewDTO view = artistView();
        CalendarGrid songs = view.calendar().songs();

        assertThat(songs.years()).extracting(y -> y.year()).containsExactly(2020, 2021, 2022);
        assertThat(songs.years().get(1).cells()).allMatch(c -> !c.isCharted());
        CalendarCell summer2022 = songs.years().get(2).cells().get(2);
        assertThat(summer2022.label()).isEqualTo("Summer 2022");
        assertThat(summer2022.chartHref()).isEqualTo("/charts/seasonal/2022-Summer");
        assertThat(summer2022.items()).extracting(i -> i.name() + "#" + i.position()).containsExactly("Titi#1", "Vete#12");
        assertThat(summer2022.band()).isEqualTo("one");
        assertThat(summer2022.more()).isEqualTo(1);
        CalendarCell yearEnd = songs.years().get(2).cells().get(4);
        assertThat(yearEnd.kind()).isEqualTo("year");
        assertThat(yearEnd.chartHref()).isEqualTo("/charts/yearly/2022");
        assertThat(songs.years().get(0).cells().get(1).items().get(0).note()).isEqualTo("feat. on Cardi B");

        // Two cells hold a #1 (Summer and year-end 2022); the later one is selected
        assertThat(songs.numberOnes()).isEqualTo(2);
        assertThat(songs.seasonsCharted()).isEqualTo(2);
        assertThat(songs.selectedId()).isEqualTo(yearEnd.id());
        assertThat(view.calendar().albums()).isNull();
        assertThat(view.calendar().firstView()).isEqualTo("songs");
    }

    @Test
    void songPageUsesItsOwnStatsAndPlacements() {
        Map<String, Object> bbStats = Map.of("weeksOnChart", 40, "peakPosition", 4, "weeksAtPeak", 2,
                "debutDate", "2022-05-21", "peakDate", "2022-07-02");
        ChartHistoryViewDTO view = service.forSong(10, "Titi", 5, List.of(weekly(10, "Titi", "song", 1, 31, 4)),
                List.of(Map.of("position", 3, "periodKey", "2022-Spring")), List.of(), bbStats, null, null);

        assertThat(view.tiles()).extracting(Tile::key).containsExactly("weekly", "seasonal", "bb");
        HitCard card = view.sections().get(1).songs().get(0);
        assertThat(card.name()).isEqualTo("Titi");
        assertThat(card.peak()).isEqualTo(4);
        assertThat(card.total()).isEqualTo(40);
        assertThat(card.runUrl()).isEqualTo("/charts/bb/song/10/run");
        assertThat(card.peakLabel()).isEqualTo("02/07/2022");
        CalendarCell spring = view.calendar().songs().years().get(0).cells().get(1);
        assertThat(spring.best().name()).isEqualTo("Titi");
        assertThat(spring.best().href()).isEqualTo("/songs/10");
        assertThat(view.calendar().songs().selectedId()).isEqualTo(spring.id());
    }

    @Test
    void albumPageSkipsUnchartedSongsAndLabelsTracks() {
        Map<String, Object> charted = new HashMap<>();
        charted.put("id", 21);
        charted.put("name", "Moscow Mule");
        charted.put("trackNumber", 1);
        charted.put("releaseDate", "13-May-2022");
        // (both present in the row; neither should reach the card)
        charted.put("charted", true);
        charted.put("peakPosition", 7);
        charted.put("totalWeeks", 14);
        charted.put("weeksAtPeak", 1);
        charted.put("debutWeek", "2022-W19");
        charted.put("debutDate", "13/05/2022");
        Map<String, Object> uncharted = new HashMap<>();
        uncharted.put("id", 22);
        uncharted.put("name", "Neverita");
        uncharted.put("charted", false);

        ChartHistoryViewDTO view = service.forAlbum(5, "UVST", List.of(weekly(5, "UVST", "album", 1, 42, 9)),
                List.of(charted, uncharted), List.of(), List.of(Map.of("position", 1, "periodKey", "2022")),
                List.of(), List.of(), List.of(), List.of(), List.of());

        HitSection weeklySection = view.sections().get(0);
        assertThat(weeklySection.songs()).extracting(HitCard::name).containsExactly("Moscow Mule");
        // No track number (or the album's shared release date) under the song name on the album page
        assertThat(weeklySection.songs().get(0).sub()).isNull();
        assertThat(weeklySection.songs().get(0).fallbackUrl()).isEqualTo("/albums/5/image?thumbnail=true");
        assertThat(view.calendar().albums().years().get(0).cells().get(4).best().name()).isEqualTo("UVST");
        assertThat(view.calendar().songs()).isNull();
        assertThat(view.calendar().firstView()).isEqualTo("albums");
    }

    @Test
    void nothingChartedGivesAnEmptyView() {
        ChartHistoryViewDTO view = service.forSong(1, "Song", null, List.of(), List.of(), List.of(), null, null, null);
        assertThat(view.isEmpty()).isTrue();
        assertThat(view.spread()).isEmpty();
        assertThat(view.calendar().isPresent()).isFalse();
    }

    @Test
    void tabFragmentRendersTilesSectionsAndCalendar() {
        Document doc = renderTab(artistView());

        assertThat(doc.select(".chh-tile")).hasSize(6);
        assertThat(doc.select(".chh-tile.is-active").attr("data-chh-target")).isEqualTo("weekly");
        assertThat(doc.select("[data-chh-section=weekly]").hasAttr("hidden")).isFalse();
        assertThat(doc.select("[data-chh-section=bb]").hasAttr("hidden")).isTrue();
        assertThat(doc.select("[data-chh-section=calendar]").hasAttr("hidden")).isTrue();
        assertThat(doc.select(".chh-spread-rows > .chh-spread-row")).hasSize(7);
        assertThat(doc.select(".chh-spread-group .chh-spread-row")).hasSize(2);
        // Where they peaked comes before the chart tiles
        assertThat(doc.select(".chh-spread ~ .chh-tiles")).hasSize(1);
        assertThat(doc.select(".chh-tile").first().attr("data-chh-key")).isEqualTo("weekly");
        assertThat(doc.select("[data-chh-section=weekly] .chh-grid[data-chh-view=songs] .chh-hit")).hasSize(3);
        assertThat(doc.select("[data-chh-section=weekly] .chh-grid[data-chh-view=albums]").hasAttr("hidden")).isTrue();
        assertThat(doc.select(".chh-hit").first().attr("data-run-url")).isEqualTo("/charts/song/10/run");
        assertThat(doc.select(".chh-hit").first().attr("data-scale")).isEqualTo("20");
        assertThat(doc.select(".chh-hit .chh-num.is-one")).isNotEmpty();
        // Debut and first peak date both on the card, the peak linked to its chart and gold at #1
        var bbDates = doc.select("[data-chh-section=bb] .chh-hit-dates").first();
        assertThat(bbDates.select(".chh-hit-debut").text()).isEqualTo("Debut 13/04/2018");
        assertThat(bbDates.select("a.chh-hit-peakdate.is-one").text()).isEqualTo("Peak 13/04/2018");
        assertThat(bbDates.select("a.chh-hit-peakdate").attr("href")).isEqualTo("/misc/billboard-hot-100/recaps?date=2018-04-13");
        assertThat(doc.select(".chh-run[aria-haspopup=dialog]")).isNotEmpty();
        // Peak with its ×weeks badge, and the weeks on chart as a headline number
        var titiCard = doc.select("[data-chh-section=weekly] .chh-hit").first();
        assertThat(titiCard.select(".chh-peak-line .chh-times.is-one").text()).isEqualTo("×4");
        assertThat(titiCard.select(".chh-times").attr("title")).isEqualTo("4 weeks at #1");
        assertThat(titiCard.select(".chh-hit-total").text()).isEqualTo("31 weeks on chart");
        assertThat(doc.select("[data-chh-section=pc] .chh-hit-total").first().text()).isEqualTo("46 days on chart");
        // Tiles: ×weeks at the best peak plus the headline counts; six tiles sit in rows of three
        var weeklyTile = doc.select(".chh-tile[data-chh-key=weekly]").first();
        assertThat(weeklyTile.select(".chh-times").text()).isEqualTo("×13");
        assertThat(weeklyTile.select(".chh-times-label").text()).isEqualTo("weeks at #1");
        assertThat(weeklyTile.select(".chh-tile-stat")).extracting(e -> e.text())
                .containsExactly("3 songs", "1 album", "100 weeks", "2 at #1");
        assertThat(doc.select(".chh-tiles").attr("style")).isEqualTo("--chh-tile-cols:3");
        assertThat(doc.select(".chh-run-boxes")).isEmpty();
        assertThat(doc.select(".chh-cal-cell[data-chh-cell]")).hasSize(3);
        assertThat(doc.select(".chh-cal-cell.is-selected")).hasSize(1);
        assertThat(doc.select(".chh-period:not([hidden])")).hasSize(1);
        assertThat(doc.select(".chh-period:not([hidden]) .chh-period-label").text()).isEqualTo("2022 year-end");
        assertThat(doc.text()).doesNotContain("has not charted");
    }

    @Test
    void cardGridStaysInTheNormalLayoutFlow() throws Exception {
        // The card grid once picked up the modal guide lines' absolute positioning through a shared class name
        String css = java.nio.file.Files.readString(java.nio.file.Path.of("src/main/resources/static/css/detail-chart-history.css"));
        var rule = java.util.regex.Pattern.compile("(?m)^\\s*([^{}@/]+)\\{([^}]*)\\}");
        var matcher = rule.matcher(css);
        int gridRules = 0;
        while (matcher.find()) {
            for (String selector : matcher.group(1).split(",")) {
                if (selector.trim().matches("\\.chh-grid(\\s.*)?") || selector.trim().equals(".chh-grid")) {
                    if (selector.trim().equals(".chh-grid")) gridRules++;
                    assertThat(matcher.group(2)).as(selector.trim()).doesNotContain("position").doesNotContain("absolute");
                }
            }
        }
        assertThat(gridRules).isPositive();
        String js = java.nio.file.Files.readString(java.nio.file.Path.of("src/main/resources/static/js/detail-chart-history.js"));
        assertThat(js).doesNotContain("'chh-grid");
    }

    @Test
    void tabFragmentShowsTheEmptyState() {
        ChartHistoryViewDTO empty = service.forSong(1, "Song", null, List.of(), List.of(), List.of(), null, null, null);
        Document doc = renderTab(empty);
        assertThat(doc.select(".chh-empty").text()).isEqualTo("This artist has not charted.");
        assertThat(doc.select(".chh-tile")).isEmpty();
    }

    // ── Fixtures ──

    private ChartHistoryViewDTO artistView() {
        ChartHistoryDTO titi = weekly(10, "Titi", "song", 1, 31, 4);
        titi.setHasImage(true);
        titi.setAlbumId(5);
        titi.setDebutWeek("2022-W19");
        ChartHistoryDTO safaera = weekly(11, "Safaera", "song", 2, 19, 2);
        safaera.setAlbumId(6);
        safaera.setFeaturedOn(true);
        safaera.setSourceArtistId(77);
        safaera.setSourceArtistName("Cardi B");
        ChartHistoryDTO vete = weekly(12, "Vete", "song", 12, 8, 1);
        ChartHistoryDTO album = weekly(5, "UVST", "album", 1, 42, 9);

        List<Map<String, Object>> seasonalSongs = List.of(
                placement("songId", 10, "songName", "Titi", "2022-Summer", 1),
                placement("songId", 12, "songName", "Vete", "2022-Summer", 12),
                featured(placement("songId", 11, "songName", "Safaera", "2020-Spring", 4)));
        List<Map<String, Object>> yearlySongs = List.of(placement("songId", 10, "songName", "Titi", "2022", 1));

        Map<String, Object> featuredBb = recap(10, "I Like It", 1, 32, 1, "2018-04-13", "weeks_on_chart", "weeks_at_peak");
        featuredBb.put("owner_id", 77);
        featuredBb.put("owner_name", "Cardi B");
        featuredBb.put("featured_on", 1);
        return service.forArtist(196, new ArrayList<>(List.of(vete, titi, safaera)), List.of(album),
                seasonalSongs, yearlySongs, List.of(), List.of(),
                List.of(featuredBb),
                List.of(recap(10, "Titi", 1, 46, 9, "2022-05-20", "days_on_countdown", "days_at_peak")),
                List.of(recap(13, "MIA", 6, 8, 2, "2018-10-20", "days_on_countdown", "days_at_peak")));
    }

    private static ChartHistoryDTO weekly(int id, String name, String type, int peak, int weeks, int atPeak) {
        return new ChartHistoryDTO(id, name, "Bad Bunny", peak, atPeak, weeks, type);
    }

    private static Map<String, Object> placement(String idKey, int id, String nameKey, String name, String periodKey, int position) {
        Map<String, Object> m = new HashMap<>();
        m.put(idKey, id);
        m.put(nameKey, name);
        m.put("periodKey", periodKey);
        m.put("position", position);
        return m;
    }

    private static Map<String, Object> featured(Map<String, Object> m) {
        m.put("featuredOn", true);
        m.put("sourceArtistName", "Cardi B");
        return m;
    }

    private static Map<String, Object> recap(int songId, String name, int peak, int total, int atPeak, String debut,
                                             String totalKey, String atPeakKey) {
        Map<String, Object> m = new HashMap<>();
        m.put("song_id", songId);
        m.put("song_name", name);
        m.put("peak_position", peak);
        m.put(totalKey, total);
        m.put(atPeakKey, atPeak);
        m.put("debut_date", debut);
        m.put("peak_date", debut);
        return m;
    }

    private static Document renderTab(ChartHistoryViewDTO view) {
        var files = new ClassLoaderTemplateResolver();
        files.setPrefix("templates/");
        files.setSuffix(".html");
        files.setCheckExistence(true);
        files.setOrder(1);
        var strings = new StringTemplateResolver();
        strings.setOrder(2);
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolvers(Set.of(files, strings));
        var servletContext = new MockServletContext();
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new MockHttpServletRequest(servletContext), new MockHttpServletResponse());
        var context = new WebContext(exchange, Locale.ENGLISH, Map.of("view", view));
        return Jsoup.parse(engine.process(
                "<div th:replace=\"~{fragments/detail-chart-history :: tab(${view}, 'artist')}\"></div>", context));
    }
}
