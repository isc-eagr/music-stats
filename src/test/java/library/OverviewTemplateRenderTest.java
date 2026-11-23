package library;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartSongOverviewRowDTO;
import library.service.ChartOverviewService;
import library.service.OverviewFilterService;
import org.jsoup.Jsoup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

class OverviewTemplateRenderTest {
    @ParameterizedTest
    @CsvSource({"seasonal,song", "seasonal,album", "seasonal,artist", "yearly,song", "yearly,album", "yearly,artist"})
    void overviewRendersSharedFiltersAndServerSortLinks(String source, String tab) {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var request = new MockHttpServletRequest(servletContext);
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(request, new MockHttpServletResponse());
        var context = new WebContext(exchange, Locale.ENGLISH, model(source, tab));
        var document = Jsoup.parse(engine.process("charts/overview", context));
        assertThat(document.select("#overviewFilterForm")).hasSize(1);
        assertThat(document.select("#overviewFilterPanel .filter-item")).hasSize(new OverviewFilterService().fieldsFor(source, tab).size());
        assertThat(document.select("script[src='/js/overview-filters.js']")).hasSize(1);
        assertThat(document.select("script[src='/js/chip-select.js']")).hasSize(1);
        // Floating headers come from the shared .table-scroll / .sticky-head pair (js/table-scroll.js in the nav)
        assertThat(document.select("table.overview-table")).isNotEmpty().allSatisfy(table -> {
            assertThat(table.hasClass("sticky-head")).isTrue();
            assertThat(table.parent().hasClass("table-scroll")).isTrue();
        });
        assertThat(document.select("#overviewSearchForm[hidden]")).isEmpty();
        assertThat(document.select("th .sort-link")).isNotEmpty().allSatisfy(link ->
                assertThat(link.attr("href")).startsWith("/charts/" + source + "/overview?"));
        assertThat(document.select("th").stream().filter(th -> th.hasAttr("data-type")))
                .allSatisfy(th -> assertThat(th.text()).isNotBlank());
    }

    @ParameterizedTest
    @CsvSource({"WEEKLY,song,table", "WEEKLY,album,cards", "WEEKLY,artist,leaderboard", "HOT100,song,cards", "HOT100,album,table",
            "HOT100,artist,table", "TRL,song,table", "TRL,album,table", "TRL,artist,leaderboard", "CUNTDOWN,song,table",
            "CUNTDOWN,album,cards", "CUNTDOWN,artist,leaderboard"})
    void chartOverviewRendersSwitchersFiltersAndState(ChartOverviewService.Chart chart, String tab, String view) {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var request = new MockHttpServletRequest(servletContext);
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(request, new MockHttpServletResponse());
        Map<String, Object> model = new HashMap<>();
        model.put("chart", chart);
        model.put("charts", ChartOverviewService.Chart.values());
        model.put("tab", tab);
        model.put("view", view);
        model.put("includeFeatured", false);
        model.put("currentSection", "charts");
        model.put("overviewFilterFields", new OverviewFilterService().fieldsFor(chart.source(), tab));
        model.put("pageStateJson", "{\"chart\":\"" + chart.key() + "\",\"rows\":[]}");
        var document = Jsoup.parse(engine.process("charts/chart-overview", new WebContext(exchange, Locale.ENGLISH, model)));

        assertThat(document.select("#chartOverview").attr("data-chart")).isEqualTo(chart.key());
        assertThat(document.select(".co-menubar > a.co-menu-tab.active").text()).isEqualTo(chart.label());
        assertThat(document.select(".co-menu-side a.co-menu-tab.active").attr("href")).contains("tab=" + tab);
        assertThat(document.select(".co-menubar a.co-tone-seasonal")).extracting(link -> link.attr("href"))
                .containsExactly("/charts/seasonal/overview?overviewTab=" + tab);
        assertThat(document.select("#coViewToggle [data-view=" + view + "].active")).hasSize(1);
        assertThat(document.select("#overviewFilterPanel .filter-item")).hasSize(new OverviewFilterService().fieldsFor(chart.source(), tab).size());
        assertThat(document.select("script[src='/js/overview-filters.js']")).hasSize(1);
        assertThat(document.select("script[src='/js/chart-overview.js']")).hasSize(1);
        assertThat(document.select("#coSortPills")).hasSize(1);
        assertThat(document.select("#artistTopSongFilter")).hasSize(chart == ChartOverviewService.Chart.WEEKLY && "artist".equals(tab) ? 1 : 0);
        assertThat(document.select("#coPlaylist")).hasSize("song".equals(tab) ? 1 : 0);
        assertThat(document.html()).contains("window.chartOverviewState = {\"chart\":\"" + chart.key() + "\"");
    }

    private Map<String, Object> model(String source, String tab) {
        Map<String, Object> model = new HashMap<>();
        model.put("overviewTab", tab);
        model.put("currentSection", source);
        model.put("periodType", source);
        model.put("pageTitle", "Chart Overview");
        model.put("unitLabel", "Weeks");
        model.put("pageSize", 25);
        model.put("activeTotalCount", 1);
        model.put("resultTotal", 0);
        model.put("selectedSort", "artist");
        model.put("selectedDir", "asc");
        model.put("selectedIncludeFeatured", false);
        model.put("sort", "artist");
        model.put("dir", "asc");
        model.put("page", 1);
        model.put("size", 25);
        model.put("totalPages", 1);
        model.put("q", "");
        model.put("searchQuery", "");
        model.put("pageSizeConfig", Map.of("songsListPageSize", 25, "albumsListPageSize", 25));
        model.put("overviewFilterFields", new OverviewFilterService().fieldsFor(source, tab));
        model.put("songRows", List.of(new ChartSongOverviewRowDTO()));
        model.put("albumRows", List.of(new ChartAlbumOverviewRowDTO()));
        model.put("artistRows", List.of(new ChartArtistOverviewRowDTO()));
        for (String name : List.of("entries", "debuts", "albumEntries", "artistEntries", "albumOverviewRows", "artistOverviewRows")) {
            model.put(name, List.of());
        }
        return model;
    }
}
