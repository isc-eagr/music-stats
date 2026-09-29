package library;

import library.controller.PlaygroundController;
import library.service.PlaygroundService;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.springframework.web.server.ResponseStatusException;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.StringTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Locale;

import static org.assertj.core.api.Assertions.*;

class PlaygroundTest {
    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private PlaygroundService service;

    @BeforeEach
    void setup() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE Artist (id INTEGER PRIMARY KEY, gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER, override_gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Play (play_date TEXT, song_id INTEGER)");
        jdbc.execute("INSERT INTO Artist VALUES (1, 2), (2, 1), (3, 3)");
        jdbc.execute("INSERT INTO Song VALUES (1, 1, NULL), (2, 2, NULL), (3, 1, 1), (4, 3, NULL)");
        service = new PlaygroundService(jdbc);
    }

    @AfterEach
    void cleanup() {
        dataSource.destroy();
    }

    @Test
    void countsOverridesTiesUnknownAndYearBoundaries() {
        play("2023-12-31 23:59", 1);
        play("2024-01-01 00:00", 1);
        play("2024-02-29 23:59", 3);
        play("2024-02-29 23:59", 2);
        play("2024-03-01 12:00", 1);
        play("2024-03-01 12:01", 2);
        play("2024-03-02 12:00", null);
        play("2024-03-03 12:00", 4);
        play("2024-12-31 23:59", 1);
        play("2025-01-01 00:00", 2);
        var heatmap = service.getHeatmap(2024);
        assertThat(heatmap.days()).hasSize(366);
        assertThat(heatmap.totalPlays()).isEqualTo(8);
        assertThat(heatmap.activeDays()).isEqualTo(6);
        assertThat(heatmap.maxPlays()).isEqualTo(2);
        assertThat(heatmap.days().getFirst().gender()).isEqualTo("male");
        assertThat(heatmap.days().getFirst().purity()).isEqualTo("all-male");
        var leapDay = heatmap.days().get(59);
        assertThat(leapDay.date()).isEqualTo(LocalDate.of(2024, 2, 29));
        assertThat(leapDay.gender()).isEqualTo("female");
        assertThat(leapDay.purity()).isEqualTo("all-female");
        assertThat(leapDay.female()).isEqualTo(2);
        assertThat(leapDay.intensity()).isEqualTo(4);
        assertThat(leapDay.label()).startsWith("29/02/2024: 2 plays");
        assertThat(heatmap.days().get(60).gender()).isEqualTo("neutral");
        assertThat(heatmap.days().get(60).purity()).isEqualTo("mixed");
        assertThat(heatmap.days().get(61).other()).isEqualTo(1);
        assertThat(heatmap.days().get(61).gender()).isEqualTo("neutral");
        assertThat(heatmap.days().get(62).gender()).isEqualTo("neutral");
        assertThat(heatmap.days().getLast().total()).isEqualTo(1);
        assertThat(heatmap.days().get(31).monthColumn() - heatmap.days().get(30).monthColumn())
                .isEqualTo(2);
        assertThat(heatmap.monthColumns()).isGreaterThan(heatmap.weeks());
        assertThat(heatmap.monthTracks()).contains("minmax(0, 0.4fr)");
        assertThat(service.getYears()).contains(2023, 2024, 2025);
    }

    @Test
    void emptyYearsAndCalendarAlignment() {
        var heatmap = service.getHeatmap(2012);
        assertThat(heatmap.weeks()).isEqualTo(54);
        assertThat(heatmap.days().getFirst().row()).isEqualTo(8);
        assertThat(heatmap.days().getLast().column()).isEqualTo(55);
        assertThat(heatmap.days()).allSatisfy(day -> {
            assertThat(day.gender()).isEqualTo("no-plays");
            assertThat(day.intensity()).isZero();
        });
        assertThat(service.getHeatmap(2023).days()).hasSize(365);
        assertThat(heatmap.totalPlays()).isZero();
        assertThatThrownBy(() -> service.getHeatmap(10000)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void intensityUsesFourBandsRelativeToPeak() {
        for (int day = 1; day <= 4; day++) {
            for (int i = 0; i < day; i++) play("2024-01-0" + day + " 12:00", 1);
        }
        assertThat(service.getHeatmap(2024).days().subList(0, 4))
                .extracting(PlaygroundService.HeatmapDay::intensity).containsExactly(1, 2, 3, 4);
    }

    @Test
    void genderShadeGetsDarkerAsLeaderShareIncreases() {
        for (int i = 0; i < 11; i++) play("2024-04-01 12:00", 1);
        for (int i = 0; i < 9; i++) play("2024-04-01 12:00", 2);
        for (int i = 0; i < 9; i++) play("2024-04-02 12:00", 1);
        play("2024-04-02 12:00", 2);
        for (int i = 0; i < 9; i++) play("2024-04-03 12:00", 2);
        play("2024-04-03 12:00", 1);

        var days = service.getHeatmap(2024).days();
        var slightMaleLead = days.get(91);
        var strongMaleLead = days.get(92);
        var strongFemaleLead = days.get(93);
        assertThat(slightMaleLead.leaderPercent()).isEqualTo(55);
        assertThat(strongMaleLead.leaderPercent()).isEqualTo(90);
        assertThat(slightMaleLead.genderShade()).isEqualTo(1);
        assertThat(strongMaleLead.genderShade()).isEqualTo(5);
        assertThat(strongFemaleLead.gender()).isEqualTo("female");
        assertThat(strongFemaleLead.genderShade()).isEqualTo(strongMaleLead.genderShade());
        assertThat(strongMaleLead.label()).contains("90% male");
    }

    @Test
    void controllerSelectsYearAndTemplateRendersBothCalendars() throws Exception {
        play("2024-01-01 12:00", 1);
        var controller = new PlaygroundController(service);
        var model = new ExtendedModelMap();
        assertThat(controller.playground(2024, model)).isEqualTo("playground");
        assertThat(model.get("currentSection")).isEqualTo("playground");
        var defaultModel = new ExtendedModelMap();
        controller.playground(null, defaultModel);
        assertThat(((PlaygroundService.YearHeatmap) defaultModel.get("heatmap")).year())
                .isEqualTo(service.getYears().getFirst());
        assertThatThrownBy(() -> controller.playground(0, model)).isInstanceOf(ResponseStatusException.class);

        // Render the actual page without shared navigation, which requires a servlet request.
        String template = Files.readString(Path.of("src/main/resources/templates/playground.html"))
                .replaceAll("<th:block th:replace=\"[^\"]*\"></th:block>", "")
                .replace("@{/css/global.css}", "'/css/global.css'")
                .replace("@{/css/playground.css}", "'/css/playground.css'");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(new StringTemplateResolver());
        var servletContext = new MockServletContext();
        var request = new MockHttpServletRequest(servletContext);
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(request, new MockHttpServletResponse());
        var context = new WebContext(exchange, Locale.ENGLISH, model);
        var document = Jsoup.parse(engine.process(template, context));
        assertThat(document.select(".heatmap-panel")).hasSize(2);
        assertThat(document.select(".heatmap-day")).hasSize(1464);
        assertThat(document.select(".heatmap-month")).hasSize(24);
        assertThat(document.select(".heatmap-month-boundary")).isEmpty();
        assertThat(document.select(".heatmap-grid")).allSatisfy(grid ->
                assertThat(grid.attr("style")).contains("minmax(0, 0.4fr)"));
        assertThat(document.select(".heatmap-day.no-plays")).hasSize(1460);
        assertThat(document.select(".heatmap-month-card")).hasSize(24);
        assertThat(document.select(".heatmap-month-card").getFirst().select(".heatmap-day")).hasSize(31);
        assertThat(document.select(".heatmap-month-card").getFirst().select(".heatmap-month-weekday")).hasSize(7);
        assertThat(document.select(".heatmap-month-card").getFirst().select(".heatmap-day").first().className())
                .contains("male", "shade-5");
        assertThat(document.select(".heatmap-panel").get(1).select(".heatmap-month-card").first()
                .select(".heatmap-day").first().className()).doesNotContain("shade-");
        assertThat(document.select(".heatmap-panel").get(0).select(".all-male, .all-female"))
                .as("gender panel pure-day classes; first day=%s", document.select(".heatmap-panel").get(0).select(".heatmap-day").first().className())
                .isNotEmpty();
        assertThat(document.select(".heatmap-panel").get(1).select(".all-male, .all-female")).isEmpty();
        assertThat(document.select("option[selected]").val()).isEqualTo("2024");
        assertThat(document.select(".heatmap-day").first().attr("aria-label")).startsWith("01/01/2024:");
        assertThat(document.select("#history-measure option")).extracting(element -> element.attr("value"))
                .containsExactly("artists", "albums", "songs", "plays", "time");
        assertThat(document.select("#history-tooltip[hidden]")).hasSize(1);
        assertThat(document.select("#history-interval-week[hidden][disabled]")).hasSize(1);
    }

    @Test
    void yearSelectorSubmitsImmediatelyWithoutHelperCopy() throws Exception {
        String template = Files.readString(Path.of("src/main/resources/templates/playground.html"));
        var document = Jsoup.parse(template);
        assertThat(document.select("#year").attr("onchange")).isEqualTo("this.form.submit()");
        assertThat(document.select(".flashback-controls button[type=submit]")).hasSize(1)
                .extracting(element -> element.text()).containsExactly("Generate recap");
        assertThat(template).doesNotContain("A year of listening")
                .doesNotContain("Blue for male")
                .doesNotContain("Brighter green means")
                .doesNotContain("Hover, focus, or tap")
                .doesNotContain("Dates use Mexico City time");
    }

    @Test
    void flashbackTemplateShowsSixQuickFactsAndGenreRankings() throws Exception {
        String template = Files.readString(Path.of("src/main/resources/templates/playground.html"));
        var document = Jsoup.parse(template);
        assertThat(document.select(".flashback-fact")).hasSize(6);
        assertThat(document.select(".flashback-facts").text()).contains("New artists", "New songs")
                .doesNotContain("Loved tracks", "Events attended");
        assertThat(document.select("#top-overall-heading").text()).isEqualTo("Top 30 Overall");
        assertThat(document.select(".flashback-genre-grid")).hasSize(1);
        assertThat(document.select("#flashback-review-type option")).extracting(element -> element.attr("value"))
                .containsExactly("year", "season", "month");
    }

    @Test
    void genderShadesAndMobileCalendarsHaveDedicatedStyles() throws Exception {
        String stylesheet = Files.readString(Path.of("src/main/resources/static/css/playground.css"));
        assertThat(stylesheet).contains(".heatmap-day.male.shade-1 { background-color: #93c5fd; }")
                .contains(".heatmap-day.male.shade-5 { background-color: #172554; }")
                .contains(".heatmap-day.female.shade-1 { background-color: #f9a8d4; }")
                .contains(".heatmap-day.female.shade-5 { background-color: #831843; }")
                .contains("all-gender-diagonal-swipe")
                .contains("skewX(-20deg)")
                .contains(".heatmap-day.all-male::after")
                .contains("prefers-reduced-motion")
                .contains("grid-template-columns: 34px var(--month-tracks)")
                .contains("grid-template-rows: 22px repeat(7, auto)")
                .contains("width: 100%; min-width: 920px")
                .contains("aspect-ratio: 1")
                .contains(".heatmap-months { display: flex")
                .contains(".heatmap-legend i.male { background: linear-gradient")
                .contains("scroll-snap-type: x mandatory");
    }

    @Test
    void playCountColorsGetDarkerAsCountsIncrease() throws Exception {
        String stylesheet = Files.readString(Path.of("src/main/resources/static/css/playground.css"));
        assertThat(stylesheet).contains(".intensity-1 { background-color: #83ed95; }")
                .contains(".intensity-2 { background-color: #32b65b; }")
                .contains(".intensity-3 { background-color: #208047; }")
                .contains(".intensity-4 { background-color: #144b32; }");
    }

    private void play(String date, Integer songId) {
        jdbc.update("INSERT INTO Play VALUES (?, ?)", date, songId);
    }
}
