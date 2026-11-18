package library;

import library.dto.AlbumCardDTO;
import library.dto.ArtistCardDTO;
import library.dto.SongCardDTO;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Renders the Artists, Albums and Songs list cards and checks the gender-themed card layout. */
class CatalogCardTemplateTest {

    private static final List<String> OLD_CARD_EMOJIS = List.of("🎵", "💿", "🕒",
            "📦", "📬", "🔹", "⏵", "✓", "✗");

    @Test
    void artistCardsAreGenderThemedWithSplitBarColumnsAndFooter() {
        ArtistCardDTO selena = artist(1, "Selena", "Female", 3105, 1707, 18);
        selena.setInItunes(true);
        selena.setOrganized(true);
        selena.setDeathDate(LocalDate.of(1995, 3, 31));
        ArtistCardDTO badBunny = artist(2, "Bad Bunny", "Male", 6230, 0, 64);
        badBunny.setInItunes(false);
        badBunny.setOrganized(false);
        ArtistCardDTO group = artist(3, "Fleetwood Mac", "Group", 0, 0, 0);

        Map<String, Object> model = baseModel("songs");
        model.put("artists", List.of(selena, badBunny, group));
        model.put("activeTheme", null);
        model.put("showPlayBreakdown", false);
        model.put("includeMain", true);
        model.put("includeGroups", false);
        model.put("includeFeatured", false);
        model.put("artistRankings", Map.of());

        var cards = render("artists/list", "#artistGrid", model).select(".artist-card.gc");
        assertThat(cards).hasSize(3);

        Element female = cards.get(0);
        assertThat(female.hasClass("gender-female")).isTrue();
        assertThat(female.select(".gc-media .gc-title .gc-name-text").text()).isEqualTo("Selena");
        assertThat(female.select(".gc-title .itunes-badge.in-itunes svg")).hasSize(1);
        assertThat(female.select(".gc-corner .death-overlay")).hasSize(1);
        assertThat(female.select(".gc-bar-primary").attr("style")).isEqualTo("flex: 3105 1 0");
        assertThat(female.select(".gc-bar-legacy").attr("style")).isEqualTo("flex: 1707 1 0");
        assertThat(female.select(".gc-total-num").text()).isEqualTo("4,812");
        assertThat(female.select(".gc-plays .gc-legend b").eachText()).containsExactly("3,105", "1,707");
        assertThat(female.select(".gc-cell-l").eachText()).containsExactly("Songs", "Feat. on", "Albums", "Listened");
        assertThat(female.select(".gc-cell.gc-hl .gc-cell-l").text()).isEqualTo("Songs");
        assertThat(female.select(".gc-total .card-index").text()).isEqualTo("#1");
        assertThat(female.select(".gc-foot .card-index")).isEmpty();
        assertThat(female.select(".gc-foot .gc-org").text()).isEqualTo("Organized");
        assertThat(female.select(".gc-foot .card-expand-toggle").attr("onclick")).isEqualTo("openCardModal(this)");
        assertThat(female.select(".gc-body > .card-modal-text-stats")).hasSize(1);

        Element male = cards.get(1);
        assertThat(male.hasClass("gender-male")).isTrue();
        assertThat(male.select(".itunes-badge.not-in-itunes svg")).hasSize(1);
        assertThat(male.select(".gc-corner")).isEmpty();
        assertThat(male.select(".gc-plays .gc-bar-legacy")).isEmpty();
        assertThat(male.select(".gc-plays .gc-legend-item").eachAttr("title")).containsExactly("Primary plays");
        assertThat(male.select(".gc-foot .gc-org.is-unorganized").text()).isEqualTo("Not organized");

        Element other = cards.get(2);
        assertThat(other.hasClass("gender-male") || other.hasClass("gender-female")).isFalse();
        assertThat(other.select(".itunes-badge")).isEmpty();
        assertThat(other.select(".gc-plays .gc-bar-empty")).hasSize(1);
        assertThat(other.select(".gc-cell-l").eachText()).containsExactly("Songs", "Feat. on", "Albums", "Listened");
        assertThat(other.select(".gc-cell[title=Featured songs] .gc-cell-v").text()).isEqualTo("0");

        cards.forEach(CatalogCardTemplateTest::assertNoEmojiOnCardFace);
    }

    @Test
    void artistMorePanelGroupsRanksPlaysCatalogListeningAndLife() {
        ArtistCardDTO selena = artist(1, "Selena", "Female", 3105, 1707, 18);
        selena.setGenreId(7);
        selena.setGenreName("Tejano");
        selena.setFirstListenedDate("13-Feb-2005");
        selena.setLastListenedDate("08-Nov-2026");
        selena.setBirthDate(LocalDate.of(1971, 4, 16));
        selena.setDeathDate(LocalDate.of(1995, 3, 31));
        selena.setItunesPresenceRatio(86.6);
        selena.setFirstListenedSong("Bidi Bidi Bom Bom");
        selena.setLastListenedSong("Como la Flor");
        Map<String, Object> model = baseModel("songs");
        model.put("artists", List.of(selena));
        model.put("activeTheme", null);
        model.put("showPlayBreakdown", false);
        model.put("includeMain", true);
        model.put("includeGroups", false);
        model.put("includeFeatured", false);
        model.put("artistRankings", Map.of(1, Map.of("gender", 3, "genre", 1)));

        Element panel = render("artists/list", "#artistGrid", model).selectFirst(".artist-card.gc .card-modal-text-stats");
        assertThat(panel.select(".mm-ranks .mm-rank .mm-rank-k").eachText()).containsExactly("Gender", "Genre");
        assertThat(panel.select(".mm-rank").get(1).attr("href")).contains("genre=7");
        assertThat(panel.select(".mm-rank .mm-rank-n").eachText()).containsExactly("#3", "#1");
        assertThat(panel.select(".mm-main > section .mm-cap").eachText()).containsExactly("Plays", "Catalog", "Listening", "Life");
        assertThat(panel.select(".mm-big").text()).isEqualTo("4,812");
        assertThat(panel.select(".mm-legend em").eachText()).containsExactly("65%", "35%");
        assertThat(panel.select(".mm-sec-catalog .mm-total").text()).isEqualTo("Total songs 118 including features");
        assertThat(panel.select(".mm-sec-catalog .mm-split-row").eachText())
                .containsExactly("Own songs 100 Featured on 18", "Solo 80 With features 20", "Album songs 95 Standalone 5");
        assertThat(panel.select(".mm-sec-catalog .mm-split").get(0).hasAttr("style")).isFalse();
        assertThat(panel.select(".mm-sec-catalog .mm-split").get(1).attr("style")).startsWith("width: 84.7");
        assertThat(panel.select(".mm-sec-catalog .mm-fact[title^=iTunes]").text()).isEqualTo("iTunes 86.6%");
        assertThat(panel.select(".mm-tl-mid").text()).isEqualTo("21 years, 8 months, 26 days");
        assertThat(panel.select(".mm-tl .mm-song").eachText()).containsExactly("Bidi Bidi Bom Bom", "Como la Flor");
        assertThat(panel.select(".mm-tl .mm-song").first().attr("title")).isEqualTo("Bidi Bidi Bom Bom");
        assertThat(panel.select(".mm-sec-life .mm-fact").eachText()).containsExactly("Born 16/04/1971", "Died 31/03/1995", "Age at death 23 years");
        assertThat(panel.select(".mm-foot .gc-org").text()).isEqualTo("Not organized");

        // Consistency ledger under the listening timeline; ranks load when the More panel opens
        Element ledger = panel.selectFirst(".mm-sec-listening section.cons.cons-compact");
        assertThat(ledger.attr("data-cons-url")).isEqualTo("/artists/1/consistency");
        assertThat(ledger.select("tbody th").eachText()).containsExactly("Days", "Weeks", "Months", "Years");
        assertThat(ledger.select(".cons-count").first().text()).startsWith("1 / ");
        assertThat(ledger.select("[data-cons-rank]").eachText()).containsOnly("...");
        assertThat(ledger.select("[data-cons-grank], [data-cons-gender-head], .cons-sub")).isEmpty();
    }

    @Test
    void spanLabelReadsBothDisplayDateFormats() {
        assertThat(library.util.DateFormatUtils.spanLabel("13-Feb-2005", "08-Nov-2026")).isEqualTo("21 years, 8 months, 26 days");
        assertThat(library.util.DateFormatUtils.spanLabel("01/01/2026", "15/06/2026")).isEqualTo("5 months, 14 days");
        assertThat(library.util.DateFormatUtils.spanLabel("01/01/2026", "15/01/2026")).isEqualTo("14 days");
        assertThat(library.util.DateFormatUtils.spanLabel("01/01/2025", "01/02/2026")).isEqualTo("1 year, 1 month");
        assertThat(library.util.DateFormatUtils.spanLabel("15/01/2026", "15/01/2026")).isNull();
        assertThat(library.util.DateFormatUtils.spanLabel(null, "15/01/2026")).isNull();
        assertThat(library.util.DateFormatUtils.spanLabel("-", "15/01/2026")).isNull();
    }

    @Test
    void artistSortMetricLabelHasNoTrailingColon() {
        ArtistCardDTO selena = artist(1, "Selena", "Female", 3105, 1707, 18);
        selena.setLastListenedDate("08/11/2026");
        Map<String, Object> model = baseModel("last_listened");
        model.put("artists", List.of(selena));
        model.put("activeTheme", null);
        model.put("showPlayBreakdown", false);
        model.put("includeMain", true);
        model.put("includeGroups", false);
        model.put("includeFeatured", false);
        model.put("artistRankings", Map.of());

        Element card = render("artists/list", "#artistGrid", model).selectFirst(".artist-card.gc");
        assertThat(card.select(".gc-body > .sort-metric .sort-metric-label").text()).isEqualTo("Last Listened");
        assertThat(card.select(".gc-body > .sort-metric .sort-metric-value").text()).isEqualTo("08/11/2026");
        assertThat(card.select(".gc-cell.gc-hl")).isEmpty();
    }

    @Test
    void consistencySortShowsYearsOutOfYearsSinceTheFirstPlay() {
        ArtistCardDTO selena = artist(1, "Selena", "Female", 3105, 1707, 18);
        selena.setYearsListened(19);
        selena.setFirstListenedDate("13-Feb-2005");
        Map<String, Object> model = baseModel("consistency");
        model.put("artists", List.of(selena));
        model.put("activeTheme", null);
        model.put("showPlayBreakdown", false);
        model.put("includeMain", true);
        model.put("includeGroups", false);
        model.put("includeFeatured", false);
        model.put("artistRankings", Map.of());

        Element card = render("artists/list", "#artistGrid", model).selectFirst(".artist-card.gc");
        assertThat(card.select(".gc-body > .sort-metric .sort-metric-label").text()).isEqualTo("Consistency");
        long totalYears = LocalDate.now().getYear() - 2005 + 1L;
        assertThat(card.select(".gc-body > .sort-metric .sort-metric-value").text())
                .startsWith("19 / " + totalYears + " years (");
    }

    @Test
    void albumCardPutsArtistOnTheMeltAndLengthInTheCorner() {
        AlbumCardDTO album = new AlbumCardDTO();
        album.setId(10);
        album.setName("Amor Prohibido");
        album.setArtistId(1);
        album.setArtistName("Selena");
        album.setGenderName("Female");
        album.setAlbumLengthFormatted("32:46");
        album.setSongCount(10);
        album.setPlayCount(2410);
        album.setVatitoPlayCount(1512);
        album.setRobertloverPlayCount(898);
        album.setTimeListenedFormatted("2d 4h");
        album.setInItunes(true);
        album.setOrganized(true);

        Map<String, Object> model = baseModel("time");
        model.put("albums", List.of(album));
        model.put("albumRankings", Map.of());
        model.put("includeFullListenStats", false);

        Element card = render("albums/list", "#albumGrid", model).selectFirst(".album-card.gc");
        assertThat(card.hasClass("gender-female")).isTrue();
        assertThat(card.select(".gc-title .gc-sub").text()).isEqualTo("Selena");
        assertThat(card.select(".gc-title .gc-sub").attr("href")).isEqualTo("/artists/1");
        assertThat(card.select(".gc-corner .length-overlay").text()).isEqualTo("32:46");
        assertThat(card.select(".gc-total-num").text()).isEqualTo("2,410");
        assertThat(card.select(".gc-plays .gc-legend b").eachText()).containsExactly("1,512", "898");
        assertThat(card.select(".gc-cell-l").eachText()).containsExactly("Songs", "Listened");
        assertThat(card.select(".gc-cell.gc-hl .gc-cell-l").text()).isEqualTo("Listened");
        assertThat(card.select(".gc-foot .card-expand-toggle")).hasSize(1);
        assertNoEmojiOnCardFace(card);

        Element panel = card.selectFirst(".card-modal-text-stats");
        assertThat(panel.select(".mm-main > section .mm-cap").eachText()).containsExactly("Plays", "Tracklist", "Listening", "Dates");
        assertThat(panel.select(".mm-sec-plays .full-album-plays-value")).hasSize(1);
        assertThat(panel.select(".mm-sec-dates .first-full-listen-value")).hasSize(1);
        assertThat(panel.select(".mm-sec-dates .last-full-listen-value")).hasSize(1);
        assertThat(panel.select(".mm-legend em").eachText()).containsExactly("63%", "37%");
        // No plays yet: zero counts, no totals, still one row per period
        assertThat(panel.select(".mm-sec-listening .cons-count").eachText()).containsOnly("0 / 0");
        assertThat(panel.select(".mm-sec-listening .cons-share").eachText()).containsOnly("-");
        assertThat(panel.select(".mm-sec-listening .cons").attr("data-cons-url")).isEqualTo("/albums/10/consistency");
    }

    @Test
    void songCardKeepsHoverImagesLinkedTooltipsAndSingleLength() {
        SongCardDTO song = new SongCardDTO();
        song.setId(20);
        song.setName("Como la Flor");
        song.setArtistId(1);
        song.setArtistName("Selena");
        song.setAlbumId(10);
        song.setAlbumName("Entre a Mi Mundo");
        song.setGenderName("Female");
        song.setLengthFormatted("3:05");
        song.setHasImage(true);
        song.setAlbumHasImage(true);
        song.setIsSingle(true);
        song.setInItunes(true);
        song.setOrganized(true);
        song.setPlayCount(597);
        song.setVatitoPlayCount(402);
        song.setRobertloverPlayCount(195);
        song.setTimeListenedFormatted("1d 6h");
        song.setTotalPlayBreakdownItems(List.of("Como la Flor: 500", "Como la Flor (Live): 97"));

        Map<String, Object> model = baseModel("plays");
        model.put("songs", List.of(song));
        model.put("songRankings", Map.of());

        Element card = render("songs/list", "#songGrid", model).selectFirst(".song-card.gc");
        assertThat(card.select(".gc-media .hover-image-container img")).hasSize(2);
        assertThat(card.select(".length-overlay")).hasSize(1);
        assertThat(card.select(".gc-corner .length-overlay").text()).isEqualTo("3:05");
        assertThat(card.select(".gc-title .gc-single svg")).hasSize(1);
        assertThat(card.select(".gc-title .gc-sub").text()).isEqualTo("Selena");
        assertThat(card.select(".gc-title .gc-sub2").attr("href")).isEqualTo("/albums/10");
        assertThat(card.select(".gc-total-num .linked-song-tooltip-trigger .linked-song-tooltip-item-source")).hasSize(2);
        assertThat(card.select(".gc-cell-l").eachText()).containsExactly("Listened");
        assertNoEmojiOnCardFace(card);

        Element panel = card.selectFirst(".card-modal-text-stats");
        assertThat(panel.select(".mm-sec-plays .mm-l").eachText()).containsExactly("Listened", "Length", "Feat. artists");
        assertThat(panel.select(".mm-sec-charts")).isEmpty();
    }

    @Test
    void songMorePanelShowsOnlyTheChartsThatExist() {
        SongCardDTO song = new SongCardDTO();
        song.setId(21);
        song.setName("Dreams");
        song.setGenderName("Mixed");
        song.setPlayCount(412);
        song.setVatitoPlayCount(260);
        song.setRobertloverPlayCount(152);
        song.setTimeListenedFormatted("1d 5h");
        song.setWeeklyChartPeak(4);
        song.setWeeklyChartPeakWeeks(3);
        song.setWeeklyChartWeeks(11);
        song.setYearlyChartPeak(38);
        song.setYearlyChartPeakYears(1);

        Map<String, Object> model = baseModel("plays");
        model.put("songs", List.of(song));
        model.put("songRankings", Map.of());

        Element panel = render("songs/list", "#songGrid", model).selectFirst(".song-card.gc .card-modal-text-stats");
        assertThat(panel.select(".mm-peak .mm-l").eachText()).containsExactly("Weekly peak", "Weekly chart", "Yearly peak");
        assertThat(panel.select(".mm-peak-v b").eachText()).containsExactly("#4", "11", "#38");
        assertThat(panel.select(".mm-peak-v span").eachText()).containsExactly("3 weeks at peak", "weeks on chart", "1 year");
    }

    private static void assertNoEmojiOnCardFace(Element card) {
        Element face = card.clone();
        face.select(".card-modal-text-stats").remove();
        String html = face.outerHtml();
        OLD_CARD_EMOJIS.forEach(emoji -> assertThat(html).doesNotContain(emoji));
    }

    private static ArtistCardDTO artist(int id, String name, String gender, int primary, int legacy, int featured) {
        ArtistCardDTO artist = new ArtistCardDTO();
        artist.setId(id);
        artist.setName(name);
        artist.setGenderName(gender);
        artist.setPlayCount(primary + legacy);
        artist.setVatitoPlayCount(primary);
        artist.setRobertloverPlayCount(legacy);
        artist.setSongCount(100);
        artist.setAlbumCount(10);
        artist.setFeaturedSongCount(featured);
        artist.setSoloSongCount(80);
        artist.setSongsWithFeatCount(20);
        artist.setStandaloneSongCount(5);
        artist.setImageCount(1);
        artist.setDaysListened(1);
        artist.setWeeksListened(1);
        artist.setMonthsListened(1);
        artist.setYearsListened(1);
        artist.setTimeListenedFormatted("11d 6h");
        return artist;
    }

    private static Map<String, Object> baseModel(String sortBy) {
        Map<String, Object> model = new HashMap<>();
        model.put("currentPage", 0);
        model.put("totalPages", 1);
        model.put("perPage", 40);
        model.put("startIndex", 1);
        model.put("sortBy", sortBy);
        model.put("defaultSortBy", "plays");
        return model;
    }

    private static Document render(String template, String selector, Map<String, Object> model) {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new MockHttpServletRequest(servletContext), new MockHttpServletResponse());
        var context = new WebContext(exchange, Locale.ENGLISH, model);
        return Jsoup.parse(engine.process(template, java.util.Set.of(selector), context));
    }
}
