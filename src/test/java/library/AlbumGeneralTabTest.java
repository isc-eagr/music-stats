package library;

import library.dto.AlbumSongDTO;
import library.service.AlbumService;
import library.util.AlbumGeneralSummaries;
import library.util.AlbumGeneralSummaries.SongShareSlice;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class AlbumGeneralTabTest {

    private static final Path TEMPLATE = Path.of("src/main/resources/templates/albums/detail.html");

    @Test
    void playsShareListsEverySongBiggestFirstWithEvenlyFadingShades() {
        List<AlbumSongDTO> songs = List.of(
                song(1, "Intro", 10),
                song(2, "Hit", 70),
                song(3, "Bonus", 0),
                song(4, "Deep Cut", 20));

        List<SongShareSlice> slices = AlbumGeneralSummaries.playsShare(songs);

        assertThat(slices).extracting(SongShareSlice::name).containsExactly("Hit", "Deep Cut", "Intro", "Bonus");
        assertThat(slices).extracting(SongShareSlice::songId).containsExactly(2, 4, 1, 3);
        assertThat(slices).extracting(SongShareSlice::plays).containsExactly(70, 20, 10, 0);
        assertThat(slices.get(0).percent()).isCloseTo(70.0, within(0.001));
        assertThat(slices.get(3).percent()).isZero();
        assertThat(slices.get(0).shade()).isCloseTo(0.95, within(0.001));
        assertThat(slices.get(3).shade()).isCloseTo(0.20, within(0.001));
        assertThat(slices.get(1).shade() - slices.get(2).shade())
                .isCloseTo(slices.get(0).shade() - slices.get(1).shade(), within(0.001));
    }

    @Test
    void playsShareIsEmptyWithoutPlaysAndTiesSortByName() {
        assertThat(AlbumGeneralSummaries.playsShare(List.of(song(1, "A", 0), song(2, "B", null)))).isEmpty();
        assertThat(AlbumGeneralSummaries.playsShare(null)).isEmpty();
        assertThat(AlbumGeneralSummaries.playsShare(List.of(song(1, "beta", 5), song(2, "Alpha", 5))))
                .extracting(SongShareSlice::name).containsExactly("Alpha", "beta");
    }

    @Test
    void catalogCountsItunesPresenceAndGuestArtistSongs() {
        AlbumSongDTO single = song(1, "Hit", 70);
        single.setIsSingle(true);
        single.setInItunes(true);
        single.setArtistId(5);
        AlbumSongDTO deepCut = song(2, "Deep Cut", 0);
        deepCut.setInItunes(false);
        deepCut.setArtistId(5);
        AlbumSongDTO guest = song(3, "Guest Track", 4);
        guest.setArtistId(9);

        AlbumGeneralSummaries.AlbumCatalog catalog = AlbumGeneralSummaries.catalog(List.of(single, deepCut, guest), 5);
        assertThat(catalog.songs()).isEqualTo(3);
        // iTunes presence counts only songs where it is known
        assertThat(catalog.itunesKnown()).isEqualTo(2);
        assertThat(catalog.inItunes()).isEqualTo(1);
        assertThat(catalog.byOthers()).isEqualTo(1);
        assertThat(AlbumGeneralSummaries.catalog(null, 5).songs()).isZero();

        // Rendered: solo / features, iTunes (scaled to known songs) and guest-artist bars
        Map<String, Object> model = new HashMap<>();
        model.put("albumCatalog", catalog);
        model.put("soloSongCount", 2);
        model.put("songsWithFeatCount", 1);
        var guestRef = new library.service.DetailAssociationsService.ArtistRef(9, "Guest", "f", "Female", null);
        var albumGuest = new library.service.DetailAssociationsService.AlbumGuest(guestRef, "Featured", 1, "Guest Track", "3", 3,
                4, 4, 0, 5.0, 8, 50.0);
        model.put("albumAssociations", new library.service.DetailAssociationsService.AlbumTab(
                List.of(), List.of(albumGuest, albumGuest), 74, 4, 1));
        var resolver = new org.thymeleaf.templateresolver.ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new org.thymeleaf.spring6.SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new org.springframework.mock.web.MockServletContext();
        var exchange = org.thymeleaf.web.servlet.JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new org.springframework.mock.web.MockHttpServletRequest(servletContext),
                        new org.springframework.mock.web.MockHttpServletResponse());
        Element block = Jsoup.parse(engine.process("albums/detail", java.util.Set.of("#albumCatalog"),
                new org.thymeleaf.context.WebContext(exchange, java.util.Locale.ENGLISH, model))).getElementById("albumCatalog");
        assertThat(block.select(".mm-total").text()).isEqualTo("Songs 3 · featured artists 2");
        assertThat(block.select(".mm-split-row").eachText()).containsExactly(
                "Solo 2 With features 1", "In iTunes 1 Not in iTunes 1", "Album artist 2 Other artists 1");
        assertThat(block.select(".mm-split").get(1).attr("style")).startsWith("width: 66.6");
    }

    @Test
    void albumCountBacksTheOverallStanding() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            AlbumService albumService = new AlbumService(db.albumRepository, null, null, db.jdbcTemplate, null);
            assertThat(albumService.getAlbumCount()).isEqualTo(5);
        }
    }

    @Test
    void albumGeneralTabEditsInTheSidebarAndSortsTheTracklistByColumn() throws IOException {
        String html = Files.readString(TEMPLATE, StandardCharsets.UTF_8);
        Document detail = Jsoup.parse(html);

        Element page = detail.selectFirst("div.container-wide.album-detail.ad-page");
        assertThat(page).isNotNull();
        assertThat(page.selectFirst("> .detail-grid > aside.detail-sidebar.ad-card#albumSidebar")).isNotNull();
        Element main = page.selectFirst("> .detail-grid > div.ad-main");
        assertThat(main).isNotNull();
        for (String id : List.of("generalSectionContent", "albumSongs", "playsTabContent", "featuredTabContent", "chartHistoryTabContent")) {
            assertThat(main.getElementById(id)).describedAs(id + " inside .ad-main").isNotNull();
        }

        // Profile edits in place inside the sidebar card, keeping the form action and field names
        Element sidebar = detail.getElementById("albumSidebar");
        Element form = sidebar.selectFirst("form#albumForm.ad-profile-edit");
        assertThat(form).isNotNull();
        assertThat(form.attr("method")).isEqualTo("post");
        assertThat(form.attr("th:action")).isEqualTo("@{'/albums/' + ${album.id}}");
        for (String field : List.of("name", "releaseDate", "overrideGenreId", "overrideSubgenreId",
                "overrideLanguageId", "organized", "tags")) {
            assertThat(form.select(".ad-fb[data-field=" + field + "]")).describedAs("field box for " + field).hasSize(1);
        }
        for (String name : List.of("artistId", "name", "releaseDate", "overrideGenreId", "overrideSubgenreId",
                "overrideLanguageId", "organized")) {
            assertThat(form.select("[name=" + name + "]")).describedAs("form field " + name).hasSize(1);
        }
        assertThat(form.select(".ad-fb[data-field=organized] .ad-switch input[type=checkbox]")).hasSize(1);
        assertThat(form.select("#overrideLanguageId option[value='']").attr("th:text")).contains("inheritedLanguageName");
        assertThat(sidebar.select("#deleteBtn.ad-menu-item")).hasSize(1);
        assertThat(sidebar.select("#profileMenu")).hasSize(1);
        // Overridable rows tell the shared rank row where the value comes from
        assertThat(html).contains("${album.overrideGenreId != null ? 'override' : artistName}")
                .contains("${album.overrideSubgenreId != null ? 'override' : artistName}")
                .contains("${album.overrideLanguageId != null ? 'override' : artistName}");
        assertThat(html).contains("function enterEditMode()")
                .contains("function updateProfileDirtyState()")
                .contains("saveForm.requestSubmit()")
                .contains("dateFormat: 'd/m/Y'");

        // Tracklist: header-only single-key sort, no chips, no mini bars, every row sortable
        Element songs = detail.getElementById("albumSongs");
        assertThat(songs.classNames()).contains("ad-card", "ad-section", "general-tab-section");
        assertThat(songs.select("table.ad-table")).hasSize(1);
        assertThat(songs.select("[data-sort-container]")).hasSize(1);
        assertThat(songs.select("tr[data-sort-item]")).hasSize(1);
        assertThat(songs.select(".ad-sortbar, .ad-sort-chip, .ad-mb")).isEmpty();
        assertThat(songs.select("th [data-sort-key]").eachAttr("data-sort-key")).containsExactly(
                "tracknumber", "name", "totalplays", "primaryplays", "legacyplays", "length",
                "releasedate", "firstlistened", "lastlistened", "timelistened");
        assertThat(songs.select("button.mobile-row-expand-btn")).hasSize(1);
        assertThat(html).contains("/js/detail-sort.js(v=2)")
                .contains("initDetailSort('albumSongs', { defaultKey: 'tracknumber', defaultDir: 'asc' })")
                .doesNotContain("initTopTableSort")
                .doesNotContain("top-table-sort.js");

        // Stats card with the plays split and medal year tiles; the #1 crowns moved to Chart History
        Element stats = detail.selectFirst("#generalSectionContent section.ad-card.ad-stats");
        assertThat(stats).isNotNull();
        assertThat(stats.select(".ad-share-legend [th:each]")).isNotEmpty();
        assertThat(html).contains("is-medal is-gold").contains("is-medal is-silver").contains("is-medal is-bronze")
                .doesNotContain("ad-crown").doesNotContain("numberOne");

        // Old ranking chips, General/Statistics cards and their leftovers are gone
        assertThat(html).doesNotContain("winning-chip")
                .doesNotContain("detail-stats-panel")
                .doesNotContain("attribute-text")
                .doesNotContain("formActions")
                .doesNotContain("golden-plays");

        // Shared detail styles load last, then the album-only sheet
        List<String> sheets = detail.select("link[rel=stylesheet]").eachAttr("th:href");
        assertThat(sheets.get(sheets.size() - 2)).startsWith("@{/css/detail-general.css");
        assertThat(sheets.get(sheets.size() - 1)).startsWith("@{/css/album-detail.css");
    }

    private static AlbumSongDTO song(int id, String name, Integer totalPlays) {
        AlbumSongDTO song = new AlbumSongDTO();
        song.setId(id);
        song.setName(name);
        song.setTotalPlays(totalPlays);
        return song;
    }

}
