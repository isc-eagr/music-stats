package library;

import library.dto.GenderCardDTO;
import library.dto.GenreCardDTO;
import library.dto.TimeframeCardDTO;
import library.dto.YearCardDTO;
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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Renders the leaderboard rows used by the Timeframe and Catalog list pages. */
class CatalogRowTemplateTest {

    private static final List<String> OLD_EMOJIS = List.of("🎤", "💿", "🎵", "🕒",
            "⏵", "🏆", "👁", "🎸");

    @Test
    void genreRowsShowGenderSplitMetricsTopsAndAccounts() {
        GenreCardDTO reggaeton = genre(1, "Reggaeton", 19800, 4300, 425);
        reggaeton.setHasImage(true);
        reggaeton.setVatitoPlayCount(18420);
        reggaeton.setRobertloverPlayCount(6105);
        reggaeton.setTopArtistId(7);
        reggaeton.setTopArtistName("Bad Bunny");
        reggaeton.setTopArtistGenderId(2);
        GenreCardDTO pop = genre(2, "Pop", 300, 900, 0);

        Map<String, Object> model = model("plays");
        model.put("genres", List.of(reggaeton, pop));
        List<Element> rows = render("genres/list", ".lb-list", model).select(".lb-row.gc");

        assertThat(rows).hasSize(2);
        Element first = rows.get(0);
        assertThat(first.hasClass("gender-male")).isTrue();
        // Only the artist / album / song links navigate; the card itself is not clickable
        assertThat(first.hasAttr("onclick")).isFalse();
        assertThat(first.hasClass("lb-static")).isTrue();
        assertThat(first.select(".lb-rank").text()).isEqualTo("1");
        assertThat(first.select(".lb-thumb img").attr("src")).isEqualTo("/genres/1/image");
        assertThat(first.select(".lb-m-l").eachText()).containsExactly("Artists", "Albums", "Songs", "Plays", "Time");
        assertThat(first.select(".lb-m.hl .lb-m-l").text()).isEqualTo("Plays");
        assertThat(first.select(".lb-m.hl .lb-m-v").text()).isEqualTo("24,525");
        assertThat(first.select(".lb-m.hl .lb-m-pm").text()).isEqualTo("80.73%");
        assertThat(first.select(".lb-m.hl .lb-m-pf").text()).isEqualTo("17.53%");
        assertThat(first.select(".lb-m.hl .lb-m-male").attr("style")).isEqualTo("flex: 19800 1 0");
        assertThat(first.select(".lb-foot .lb-top .lb-top-n").text()).isEqualTo("Bad Bunny");
        assertThat(first.select(".lb-foot .lb-top").hasClass("male")).isTrue();
        assertThat(first.select(".lb-foot .lb-acc").text()).isEqualTo("Primary 18,420 · Legacy 6,105");
        assertThat(first.select(".lb-mix > a.lb-m-link").eachAttr("href")).containsExactly(
                "/artists?genre=1&genreMode=includes&view=table",
                "/albums?genre=1&genreMode=includes&view=table",
                "/songs?genre=1&genreMode=includes&view=table");
        assertThat(first.select("[onclick]")).isEmpty();
        assertThat(first.select(".lb-mix > a.lb-m-link").hasAttr("target")).isFalse();
        assertThat(first.select(".lb-mix > .lb-m .lb-m-l").eachText()).containsExactly("Plays", "Time");

        Element second = rows.get(1);
        assertThat(second.hasClass("gender-female")).isTrue();
        // Rows without an image skip the thumbnail instead of showing initials.
        assertThat(second.select(".lb-thumb")).isEmpty();
        assertThat(second.select(".lb-m-other")).isEmpty();
        rows.forEach(CatalogRowTemplateTest::assertNoEmoji);
    }

    @Test
    void genderRowsShowEachGendersShareOfTheWhole() {
        GenderCardDTO male = gender(1, "Male", 600);
        GenderCardDTO female = gender(2, "Female", 400);
        Map<String, Object> model = model("plays");
        model.put("genders", List.of(male, female));

        List<Element> rows = render("genders/list", ".lb-list", model).select(".lb-row.gc");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).hasClass("gender-male")).isTrue();
        assertThat(rows.get(1).hasClass("gender-female")).isTrue();
        assertThat(rows.get(0).select(".lb-m.hl .lb-m-ps").text()).isEqualTo("60.00% of all");
        assertThat(rows.get(1).select(".lb-m.hl .lb-m-ps").text()).isEqualTo("40.00% of all");
        assertThat(rows.get(0).select(".lb-m.hl .lb-m-fill").attr("style")).isEqualTo("flex: 600 1 0");
        assertThat(rows.get(1).select(".lb-mix > a.lb-m-link").eachAttr("href")).containsExactly(
                "/artists?gender=2&genderMode=includes&view=table",
                "/albums?gender=2&genderMode=includes&view=table",
                "/songs?gender=2&genderMode=includes&view=table");
    }

    @Test
    void yearRowsLinkToListenOrReleaseFilteredTables() {
        YearCardDTO year = new YearCardDTO();
        year.setYear(2016);
        year.setPlayCount(100);
        year.setArtistCount(10);
        year.setAlbumCount(20);
        year.setSongCount(30);
        year.setTimeListenedFormatted("5h");

        Map<String, Object> model = model("plays");
        model.put("years", List.of(year));
        model.put("yearType", "listen");
        Element listenRow = render("years/list", ".lb-list", model).selectFirst(".lb-row.gc");
        assertThat(listenRow.select(".lb-mix > a.lb-m-link").eachAttr("href")).containsExactly(
                "/artists?listenedDateFrom=2016-01-01&listenedDateTo=2016-12-31&view=table",
                "/albums?listenedDateFrom=2016-01-01&listenedDateTo=2016-12-31&view=table",
                "/songs?listenedDateFrom=2016-01-01&listenedDateTo=2016-12-31&view=table");

        model.put("yearType", "release");
        Element releaseRow = render("years/list", ".lb-list", model).selectFirst(".lb-row.gc");
        // Artists have no release date filter, so that column stays plain
        assertThat(releaseRow.select(".lb-mix > a.lb-m-link .lb-m-l").eachText()).containsExactly("Albums", "Songs");
        assertThat(releaseRow.select(".lb-mix > a.lb-m-link").first().attr("href"))
                .isEqualTo("/albums?releaseDateMode=between&releaseDateFrom=01/01/2016&releaseDateTo=31/12/2016&view=table");
    }

    @Test
    void yearRowsHideLegacyAfterTheLegacyAccountStopped() {
        YearCardDTO legacyEra = new YearCardDTO();
        legacyEra.setYear(2008);
        legacyEra.setVatitoPlayCount(40);
        legacyEra.setRobertloverPlayCount(0);
        YearCardDTO later = new YearCardDTO();
        later.setYear(2016);
        later.setVatitoPlayCount(100);
        later.setRobertloverPlayCount(0);
        YearCardDTO laterWithSlip = new YearCardDTO();
        laterWithSlip.setYear(2011);
        laterWithSlip.setVatitoPlayCount(10);
        laterWithSlip.setRobertloverPlayCount(3);

        for (YearCardDTO year : List.of(legacyEra, later, laterWithSlip)) {
            year.setPlayCount(year.getVatitoPlayCount() + year.getRobertloverPlayCount());
            year.setArtistCount(1);
            year.setAlbumCount(1);
            year.setSongCount(1);
            year.setTimeListenedFormatted("1h");
        }

        Map<String, Object> model = model("plays");
        model.put("years", List.of(legacyEra, later, laterWithSlip));
        model.put("yearType", "listen");
        List<Element> rows = render("years/list", ".lb-list", model).select(".lb-row.gc");

        assertThat(rows.get(0).select(".lb-acc").text()).isEqualTo("Primary 40 · Legacy 0");
        assertThat(rows.get(1).select(".lb-acc").text()).isEqualTo("Primary 100");
        assertThat(rows.get(2).select(".lb-acc").text()).isEqualTo("Primary 10 · Legacy 3");
    }

    @Test
    void weekRowsMoveChartStatusWinnersAndTopsIntoTheFooter() {
        TimeframeCardDTO week = new TimeframeCardDTO();
        week.setPeriodKey("2026-W45");
        week.setPeriodType("weeks");
        week.setPeriodDisplayName("Week 45, 2026");
        week.setListenedDateFrom("2026-11-02");
        week.setListenedDateTo("2026-11-08");
        week.setPlayCount(642);
        week.setArtistCount(48);
        week.setAlbumCount(61);
        week.setSongCount(132);
        week.setMalePlayCount(412);
        week.setFemalePlayCount(218);
        week.setOtherPlayCount(12);
        week.setTimeListenedFormatted("1d 14h");
        week.setWinningGenderName("Male");
        week.setWinningGenreName("Reggaeton");
        week.setHasChart(true);
        week.setMaleDays(5);
        week.setTotalDays(7);
        week.setTopArtistId(7);
        week.setTopArtistName("Bad Bunny");
        week.setTopArtistGenderId(2);
        week.setTopAlbumId(11);
        week.setTopAlbumName("DeBI TiRAR MaS FOTos");
        week.setTopAlbumArtistName("Bad Bunny");
        week.setTopAlbumGenderId(2);
        week.setTopSongId(21);
        week.setTopSongName("A Poca Luz");
        week.setTopSongArtistName("Ryan Castro");
        week.setTopSongGenderId(2);
        // Chart #1 album differs from the most played one, chart #1 song is the most played one
        week.setNumberOneAlbumId(12);
        week.setNumberOneAlbumName("KAROL G - Mananeras Milagrosas");
        week.setNumberOneAlbumGenderId(1);
        week.setNumberOneSongId(21);
        week.setNumberOneSongName("Ryan Castro - A Poca Luz");
        week.setNumberOneSongGenderId(2);

        Map<String, Object> model = model("plays");
        model.put("timeframes", List.of(week));
        model.put("periodType", "weeks");
        model.put("startIndex", 1);
        model.put("currentPage", 0);
        model.put("totalPages", 1);

        Element row = render("timeframes/list", "#tfGrid", model).selectFirst("#tfGrid .lb-row.gc");
        assertThat(row.hasClass("gender-male")).isTrue();
        assertThat(row.hasAttr("onclick")).isFalse();
        assertThat(row.hasClass("lb-static")).isTrue();
        assertThat(row.select("> .lb-rank").text()).isEqualTo("1");
        assertThat(row.select(".lb-h").text()).isEqualTo("Week 45, 2026");
        assertThat(row.select(".lb-mix .lb-m.hl .lb-m-v").text()).isEqualTo("642");
        assertThat(row.select(".lb-mix > a.lb-m-link").eachAttr("href")).containsExactly(
                "/artists?listenedDateFrom=2026-11-02&listenedDateTo=2026-11-08&periodLabel=Week%2045,%202026&view=table",
                "/albums?listenedDateFrom=2026-11-02&listenedDateTo=2026-11-08&periodLabel=Week%2045,%202026&view=table",
                "/songs?listenedDateFrom=2026-11-02&listenedDateTo=2026-11-08&periodLabel=Week%2045,%202026&view=table");
        assertThat(row.select(".lb-mix > a.lb-m-link").eachAttr("target")).containsOnly("_blank");
        // One footer line: winners, number ones, then the legend and chart status at the end
        Element winners = row.selectFirst(".lb-foot.tf-foot");
        assertThat(winners.select(".tf-gender.male").text()).isEqualTo("Male 71.43% of days");
        assertThat(winners.select(".tf-gender").attr("title")).contains("5 of 7");
        assertThat(winners.select(".tf-cat.genre").text()).isEqualTo("Reggaeton");
        assertThat(winners.children().eachAttr("class")).startsWith("tf-chip tf-gender male");
        assertThat(winners.children().last().hasClass("tf-foot-end")).isTrue();
        assertThat(winners.children().last().children().last().hasClass("chart-status-badge")).isTrue();
        assertThat(winners.select(".chart-status-badge.finalized").text()).isEqualTo("Chart");

        // Number ones; the differing chart #1 album is a gold half joined to the most played album
        Element tops = row.selectFirst(".lb-foot .tf-tops");
        assertThat(tops.select("> a.tf-top .tf-top-n").eachText()).containsExactly("Bad Bunny", "Ryan Castro - A Poca Luz");
        Element songChip = tops.select("> a.tf-top").get(1);
        assertThat(songChip.attr("href")).isEqualTo("/songs/21");
        assertThat(songChip.select(".tf-ic-plays, .tf-ic-chart")).hasSize(2);
        Element albumPair = tops.selectFirst(".tf-pair");
        assertThat(albumPair.select("a").eachAttr("href")).containsExactly("/albums/11", "/albums/12");
        Element chartHalf = albumPair.selectFirst("a.tf-chart");
        assertThat(chartHalf.hasClass("female")).isTrue();
        // The trophy alone marks the chart #1 half; the footer legend spells it out
        assertThat(chartHalf.select(".tf-ic-chart")).hasSize(1);
        assertThat(chartHalf.text()).isEqualTo("KAROL G - Mananeras Milagrosas");
        assertThat(chartHalf.select(".tf-top-n").text()).isEqualTo("KAROL G - Mananeras Milagrosas");
        assertThat(albumPair.selectFirst("a:not(.tf-chart) .tf-top-n").text()).isEqualTo("Bad Bunny - DeBI TiRAR MaS FOTos");
        assertThat(winners.select("> .tf-tops")).hasSize(1);
        assertThat(winners.select(".tf-foot-end .tf-legend").text()).isEqualTo("Most played Chart #1");
        assertNoEmoji(row);
    }

    private static void assertNoEmoji(Element row) {
        String html = row.outerHtml();
        OLD_EMOJIS.forEach(emoji -> assertThat(html).doesNotContain(emoji));
    }

    private static GenreCardDTO genre(int id, String name, int malePlays, int femalePlays, int otherPlays) {
        GenreCardDTO genre = new GenreCardDTO();
        genre.setId(id);
        genre.setName(name);
        genre.setPlayCount(malePlays + femalePlays + otherPlays);
        genre.setMalePlayCount(malePlays);
        genre.setFemalePlayCount(femalePlays);
        genre.setOtherPlayCount(otherPlays);
        genre.setArtistCount(10);
        genre.setMaleArtistCount(6);
        genre.setFemaleArtistCount(4);
        genre.setOtherArtistCount(0);
        genre.setAlbumCount(5);
        genre.setSongCount(40);
        genre.setTimeListened(3600L);
        genre.setTimeListenedFormatted("1h");
        return genre;
    }

    private static GenderCardDTO gender(int id, String name, int plays) {
        GenderCardDTO gender = new GenderCardDTO();
        gender.setId(id);
        gender.setName(name);
        gender.setPlayCount(plays);
        gender.setArtistCount(plays / 10);
        gender.setAlbumCount(plays / 20);
        gender.setSongCount(plays / 5);
        gender.setTimeListened((long) plays * 200);
        gender.setTimeListenedFormatted("1d");
        return gender;
    }

    private static Map<String, Object> model(String sortBy) {
        Map<String, Object> model = new HashMap<>();
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
        return Jsoup.parse(engine.process(template, Set.of(selector), new WebContext(exchange, Locale.ENGLISH, model)));
    }
}
