package library;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartSongOverviewRowDTO;
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
    @CsvSource({"weekly,song", "weekly,album", "weekly,artist", "seasonal,song", "seasonal,album",
            "seasonal,artist", "yearly,song", "yearly,album", "yearly,artist",
            "pc,song", "pc,album", "pc,artist", "trl,song", "trl,album", "trl,artist",
            "billboard,song", "billboard,album", "billboard,artist"})
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
        String template = switch (source) {
            case "pc" -> "misc/pc";
            case "trl" -> "misc/trl";
            case "billboard" -> "misc/billboard-hot100";
            default -> "charts/overview";
        };
        var document = Jsoup.parse(engine.process(template, context));
        assertThat(document.select("#overviewFilterForm")).hasSize(1);
        assertThat(document.select("#overviewFilterPanel .filter-item")).hasSize(new OverviewFilterService().fieldsFor(source, tab).size());
        assertThat(document.select("script[src='/js/overview-filters.js']")).hasSize(1);
        assertThat(document.select("script[src='/js/chip-select.js']")).hasSize(1);
        assertThat(document.select("script[src='/js/overview-sticky-headers.js']")).hasSize(1);
        assertThat(document.select("#overviewSearchForm[hidden]")).isEmpty();
        if (List.of("weekly", "seasonal", "yearly").contains(source)) {
            assertThat(document.select("th .sort-link")).isNotEmpty().allSatisfy(link ->
                    assertThat(link.attr("href")).startsWith("/charts/" + source + "/overview?"));
            assertThat(document.select("th").stream().filter(th -> th.hasAttr("data-type")))
                    .allSatisfy(th -> assertThat(th.text()).isNotBlank());
        }
    }

    private Map<String, Object> model(String source, String tab) {
        Map<String, Object> model = new HashMap<>();
        model.put("overviewTab", tab);
        model.put("currentSection", source);
        model.put("periodType", source);
        model.put("pageTitle", "Chart Overview");
        model.put("unitLabel", "Weeks");
        model.put("serverInfiniteScrollEnabled", true);
        model.put("weeklyInfiniteScrollEnabled", source.equals("weekly"));
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
