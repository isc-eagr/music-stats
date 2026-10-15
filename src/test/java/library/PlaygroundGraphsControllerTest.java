package library;

import library.controller.AlbumController;
import library.controller.ArtistController;
import library.controller.PlaygroundController;
import library.controller.SongController;
import library.dto.ChartFilterDTO;
import library.service.AlbumService;
import library.service.ArtistService;
import library.service.CatalogChartService;
import library.service.CatalogGraphFilterResolver;
import library.service.PlaygroundService;
import library.service.SongService;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class PlaygroundGraphsControllerTest {
    @Test
    void graphsPageLoadsIndependentlyOfYearActivityAndFlashback() throws Exception {
        var service = mock(PlaygroundService.class);
        var charts = mock(CatalogChartService.class);
        var resolver = mock(CatalogGraphFilterResolver.class);
        var controller = new PlaygroundController(service, charts, resolver);
        var mvc = MockMvcBuilders.standaloneSetup(controller)
                .setViewResolvers(new InternalResourceViewResolver("/templates/", ".html")).build();

        var result = mvc.perform(get("/playground").param("view", "graphs").param("source", "albums"))
                .andExpect(status().isOk()).andExpect(view().name("playground"))
                .andExpect(model().attribute("graphsSource", "albums"))
                .andExpect(model().attribute("currentSection", "playground"))
                .andExpect(model().attributeDoesNotExist("heatmap", "flashback", "years"))
                .andReturn();
        verifyNoInteractions(service, charts, resolver);

        var document = render("playground", null, result);
        assertThat(document.select(".playground-tabs a.active").text()).isEqualTo("Library graphs");
        assertThat(document.select("#graphs-explorer")).hasSize(1);
        assertThat(document.select(".heatmap-panel, .flashback-controls")).isEmpty();
    }

    @Test
    void chartsUseExactCatalogFiltersAndRejectUnknownSourceOrBreakdown() throws Exception {
        var service = mock(PlaygroundService.class);
        var charts = mock(CatalogChartService.class);
        var resolver = mock(CatalogGraphFilterResolver.class);
        var filter = ChartFilterDTO.builder().setCatalogType("album");
        when(resolver.resolve(any(), eq("albums"))).thenReturn(filter);
        var data = Map.<String, Object>of("albumsByGender", Map.of("male", 2, "female", 3));
        when(charts.getGeneralChartData(filter)).thenReturn(data);
        var mvc = MockMvcBuilders.standaloneSetup(new PlaygroundController(service, charts, resolver)).build();

        mvc.perform(get("/playground/api/charts/general").param("source", "albums")
                        .param("tag", "1", "2").param("tagMode", "excludes"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.albumsByGender.female").value(3));
        Mockito.verify(resolver).resolve(any(), eq("albums"));
        Mockito.verify(charts).getGeneralChartData(filter);
        verifyNoInteractions(service);

        var invalidResolver = mock(CatalogGraphFilterResolver.class);
        var invalidCharts = mock(CatalogChartService.class);
        var invalidMvc = MockMvcBuilders.standaloneSetup(
                new PlaygroundController(service, invalidCharts, invalidResolver)).build();
        invalidMvc.perform(get("/playground/api/charts/unknown")).andExpect(status().isBadRequest());
        invalidMvc.perform(get("/playground/api/charts/general").param("source", "unknown"))
                .andExpect(status().isBadRequest());
        invalidMvc.perform(get("/playground").param("view", "graphs").param("source", "unknown"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(invalidResolver, invalidCharts, service);
    }

    @Test
    void legacyCatalogGraphUrlsPreserveRepeatedFiltersAndDropListState() throws Exception {
        var fixture = new CatalogFixture();
        var mvc = MockMvcBuilders.standaloneSetup(fixture.controllers()).build();
        for (String source : List.of("artists", "albums", "songs")) {
            mvc.perform(get("/" + source).param("view", "graphs").param("tab", "genre")
                            .param("genre", "1", "2").param("genreMode", "excludes")
                            .param("q", "AC/DC & friends").param("page", "3").param("sortby", "name"))
                    .andExpect(status().is3xxRedirection())
                    .andExpect(redirectedUrl("/playground?view=graphs&source=" + source
                            + "&tab=genre&genre=1&genre=2&genreMode=excludes&q=AC/DC%20%26%20friends"));
        }
        fixture.dependencies.values().forEach(dependency -> verifyNoInteractions(dependency));
    }

    @Test
    void catalogFilterFormsRenderAllOptionsAndSelectedValuesWithoutCatalogQueries() throws Exception {
        var fixture = new CatalogFixture();
        when(fixture.get(ArtistService.class).getGenres()).thenReturn(Map.of(3, "Rock"));
        when(fixture.get(AlbumService.class).getGenres()).thenReturn(Map.of(3, "Rock"));
        when(fixture.get(SongService.class).getGenres()).thenReturn(Map.of(3, "Rock"));
        var mvc = MockMvcBuilders.standaloneSetup(fixture.controllers()).build();
        for (String source : List.of("artists", "albums", "songs")) {
            var result = mvc.perform(get("/" + source).param("filtersOnly", "true")
                            .param("genre", "3").param("genreMode", "excludes")
                            .param("playCountMin", "25").param("firstListenedDate", "14/10/2026"))
                    .andExpect(status().isOk()).andExpect(view().name(source + "/list :: #filterForm"))
                    .andExpect(model().attribute("selectedGenres", List.of(3)))
                    .andExpect(model().attribute("playCountMin", 25)).andReturn();
            var document = render(source + "/list", Set.of("#filterForm"), result);
            assertThat(document.select("#filterForm")).hasSize(1);
            assertThat(document.select(".filter-item")).hasSizeGreaterThan(15);
            assertThat(document.select("input[name=genre][value=3][checked]")).hasSize(1);
            assertThat(document.select("input[name=playCountMin]").val()).isEqualTo("25");
            assertThat(document.select("input[name=firstListenedDate]").val()).isEqualTo("14/10/2026");
            assertThat(document.select(".artist-card, .album-card, .song-card, .app-nav")).isEmpty();
        }
        for (Object dependency : fixture.dependencies.values()) {
            assertThat(mockingDetails(dependency).getInvocations())
                    .extracting(invocation -> invocation.getMethod().getName())
                    .doesNotContain("getArtists", "getAlbums", "getSongs", "countArtists", "countAlbums", "countSongs",
                            "countArtistsByGender", "countAlbumsByGender", "countSongsByGender",
                            "getItunesArtistIdsJson", "getItunesAlbumIdsJson", "getItunesSongIdsJson");
        }
    }

    private org.jsoup.nodes.Document render(String template, Set<String> selectors,
                                            org.springframework.test.web.servlet.MvcResult result) {
        var templateResolver = new ClassLoaderTemplateResolver();
        templateResolver.setPrefix("templates/");
        templateResolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(templateResolver);
        var request = result.getRequest();
        var exchange = JakartaServletWebApplication.buildApplication(request.getServletContext())
                .buildExchange(request, new MockHttpServletResponse());
        var context = new WebContext(exchange, Locale.ENGLISH, result.getModelAndView().getModel());
        return Jsoup.parse(selectors == null ? engine.process(template, context) : engine.process(template, selectors, context));
    }

    private static class CatalogFixture {
        private final Map<Class<?>, Object> dependencies = new HashMap<>();

        Object[] controllers() throws Exception {
            var controllers = new Object[3];
            var types = List.of(ArtistController.class, AlbumController.class, SongController.class);
            for (int index = 0; index < types.size(); index++) {
                var constructor = types.get(index).getConstructors()[0];
                Object[] arguments = Arrays.stream(constructor.getParameterTypes())
                        .map(type -> dependencies.computeIfAbsent(type, Mockito::mock)).toArray();
                controllers[index] = constructor.newInstance(arguments);
            }
            return controllers;
        }

        <T> T get(Class<T> type) {
            return type.cast(dependencies.computeIfAbsent(type, Mockito::mock));
        }
    }
}
