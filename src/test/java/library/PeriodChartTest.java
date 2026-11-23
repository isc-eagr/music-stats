package library;

import library.dto.ChartBreakdownDTO;
import library.dto.ChartEntryDTO;
import library.entity.Chart;
import library.service.PeriodChartService;
import library.service.PeriodChartService.Appearance;
import library.service.PeriodChartService.ItemStats;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
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

/** Seasonal / Yearly chart page: its stats service and both states of the shared template. */
class PeriodChartTest {

    private TestDatabaseSupport db;
    private PeriodChartService service;

    @BeforeEach
    void setUp() {
        db = TestDatabaseSupport.create();
        service = new PeriodChartService(db.jdbcTemplate);
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    @Test
    void statsCountPlaysInsideThePeriodAndTheWeeklyRun() {
        // Seed: song 3 is 2024-Spring #1 with 3 plays in March; song 1 is weekly #2 then #1 and 2024 yearly #2.
        Map<Integer, ItemStats> stats = service.statsForIds("song", List.of(3, 1), "seasonal", "2024-Spring", "2024-03-01", "2024-05-31");

        ItemStats spring = stats.get(3);
        assertThat(spring.plays()).isEqualTo(3);
        assertThat(spring.weeklyPeak()).isEqualTo(1);
        assertThat(spring.weeklyWeeks()).isEqualTo(1);
        assertThat(spring.seasons()).as("the chart being shown is left out").isEmpty();
        assertThat(spring.weeklyBand()).isEqualTo("co-band-1");

        ItemStats other = stats.get(1);
        assertThat(other.plays()).as("January plays fall outside Spring").isZero();
        assertThat(other.weeklyPeak()).isEqualTo(1);
        assertThat(other.weeklyWeeks()).isEqualTo(2);
        assertThat(other.yearly()).extracting(Appearance::label, Appearance::position).containsExactly(org.assertj.core.groups.Tuple.tuple("2024", 2));
    }

    @Test
    void yearlyPageSeesTheSeasonsOfItsSongs() {
        ItemStats stats = service.statsForIds("song", List.of(3), "yearly", "2024", "2024-01-01", "2024-12-31").get(3);

        assertThat(stats.plays()).isEqualTo(5);
        assertThat(stats.seasons()).extracting(Appearance::periodKey).containsExactly("2024-Spring");
        assertThat(stats.bestSeason().labelFor("2024")).isEqualTo("Spring");
        assertThat(stats.bestSeason().link()).isEqualTo("/charts/seasonal/2024-Spring");
    }

    @Test
    void appearanceLabelsKeepTheYearOnlyOutsideTheShownYear() {
        assertThat(new Appearance("seasonal", "2025-Winter", 2).labelFor("2025")).isEqualTo("Winter");
        assertThat(new Appearance("seasonal", "2026-Winter", 1).labelFor("2025")).isEqualTo("Winter 2026");
        assertThat(new Appearance("yearly", "2025", 7).labelFor("2025")).isEqualTo("2025");
    }

    @Test
    void albumStatsSumTheAlbumsSongs() {
        ItemStats stats = service.statsForIds("album", List.of(2), "seasonal", "2024-Spring", "2024-03-01", "2024-05-31").get(2);
        assertThat(stats.plays()).isEqualTo(3);
        assertThat(stats.yearly()).isEmpty();
    }

    @Test
    void genderUsesTheSongOverride() {
        // Song 4 belongs to Bad Bunny (gender 1 here) but overrides to 2
        assertThat(service.genderId("song", 4)).isEqualTo(2);
        assertThat(service.genderId("album", 1)).isEqualTo(2);
    }

    @Test
    void summaryCountsGendersAndTheArtistWithTheMostEntries() {
        List<ChartEntryDTO> entries = List.of(
                entry(1, 10, "Uno", 7, "Rauw", 2),
                entry(2, 11, "Dos", 8, "Karol", 1),
                entry(3, 12, "Tres", 8, "Karol", 1),
                entry(4, 13, "Cuatro", 7, "Rauw", 2),
                entry(5, 14, "Cinco", 9, "Mixed Band", null));
        PeriodChartService.Summary summary = service.summary(entries, Map.of(), false);

        assertThat(summary.top().getSongName()).isEqualTo("Uno");
        assertThat(summary.male()).isEqualTo(2);
        assertThat(summary.female()).isEqualTo(2);
        assertThat(summary.other()).isEqualTo(1);
        assertThat(summary.malePercent()).isEqualTo(40.0);
        assertThat(summary.leadArtist()).as("2 entries each, best position wins").isEqualTo("Rauw");
        assertThat(summary.leadCount()).isEqualTo(2);
        assertThat(summary.leadBestTitle()).isEqualTo("Uno");
        assertThat(service.summary(List.of(), Map.of(), false)).isNull();
    }

    @Test
    void displayDateUsesDayMonthYear() {
        assertThat(PeriodChartService.displayDate("2025-09-01")).isEqualTo("01/09/2025");
        assertThat(PeriodChartService.displayDate("2025-11-30 00:00:00")).isEqualTo("30/11/2025");
    }

    @Test
    void finalizedSeasonShowsSummaryChipsAndBubblingUnderWithoutEditControls() {
        Document page = render("seasonal", true);

        assertThat(page.select(".catalog-toolbar-title").text()).isEqualTo("Seasonal Chart");
        assertThat(page.select(".pd-status").text()).isEqualTo("Finalized");
        assertThat(page.select("#pdEditBtn")).hasSize(1);
        assertThat(page.select("#finalizeBtn, #pdSaveBtn, [data-pd-search]")).isEmpty();
        assertThat(page.select("#songsSection .wk-hero-title").text()).isEqualTo("Uno");
        assertThat(page.select("#songsSection .wk-gold").text()).isEqualTo("Number one song");
        assertThat(page.select("#songList tr.pd-row")).hasSize(2);
        assertThat(page.select("#songList .pd-grip, #songList .pd-edit")).isEmpty();
        assertThat(page.select("#songList tr.pd-row").first().select(".pd-chip-year").text()).isEqualTo("2025 · #7");
        assertThat(page.select("#songList tr.pd-row").first().select(".pd-chip").last().text()).isEqualTo("Winter 2026 · #2");
        assertThat(page.select("#extraList tr.pd-row .pd-pos").text()).isEqualTo("31");
        assertThat(page.select(".pd-range").text()).isEqualTo("01/09/2025 - 30/11/2025");
        assertThat(page.select("#pdJumpSeason option[selected]").text()).isEqualTo("Fall");
    }

    @Test
    void draftShowsTheEditorOnTheSamePage() {
        Document page = render("seasonal", false);

        assertThat(page.select(".pd-status").text()).isEqualTo("Draft");
        assertThat(page.select("#pdSaveBtn, #finalizeBtn")).hasSize(2);
        assertThat(page.select("#pdEditBtn, .wk-hero")).isEmpty();
        assertThat(page.select("[data-pd-progress=song] [data-pd-blocks] i")).hasSize(30);
        assertThat(page.select("[data-pd-search=song], [data-pd-search=extra], [data-pd-search=album]")).hasSize(3);
        assertThat(page.select("#songList tr.pd-row .pd-grip[draggable=true]")).hasSize(2);
        assertThat(page.select("#songList tr.pd-row button.pd-ib")).as("up, down and remove on each row").hasSize(6);
        assertThat(page.select("#songList").attr("data-pd-max")).isEqualTo("30");
        assertThat(page.select("#extraList").attr("data-pd-start")).isEqualTo("31");
        assertThat(page.select("[data-pd-count=song]").text()).isEqualTo("2/30");
        assertThat(page.select("script[src='/js/period-chart.js']")).hasSize(1);
    }

    @Test
    void yearlyPageHasSeasonsColumnAndSeasonLinks() {
        Document page = render("yearly", true);

        assertThat(page.select(".catalog-toolbar-title").text()).isEqualTo("Yearly Chart");
        assertThat(page.select("#songsChartTable thead th.pd-c-chips").text()).isEqualTo("Seasons");
        assertThat(page.select("#songsSection .wk-gold").text()).isEqualTo("Song of the year");
        assertThat(page.select(".pd-year-seasons a")).extracting(a -> a.attr("href")).containsExactly("/charts/seasonal/2025-Fall");
        assertThat(page.select("#extraList")).isEmpty();
        assertThat(page.select("#pdJumpPeriod option[selected]").text()).isEqualTo("2025");
    }

    private Document render(String periodType, boolean finalized) {
        boolean seasonal = "seasonal".equals(periodType);
        String key = seasonal ? "2025-Fall" : "2025";
        List<ChartEntryDTO> songs = List.of(entry(1, 10, "Uno", 7, "Rauw", 2), entry(2, 11, "Dos", 8, "Karol", 1));
        List<ChartEntryDTO> extras = seasonal ? List.of(entry(31, 12, "Extra", 9, "Other", 2)) : List.of();
        Map<Integer, ItemStats> stats = new HashMap<>();
        stats.put(10, new ItemStats(43, 2, 17, List.of(new Appearance("yearly", "2025", 7)), List.of(new Appearance("seasonal", "2026-Winter", 2))));
        stats.put(11, new ItemStats(34, null, 0, List.of(), List.of()));
        stats.put(12, new ItemStats(20, 1, 4, List.of(), List.of()));

        Chart songChart = new Chart();
        songChart.setId(1);
        Chart albumChart = new Chart();
        albumChart.setId(2);

        Map<String, Object> model = new HashMap<>();
        model.put("currentSection", seasonal ? "seasonal-charts" : "yearly-charts");
        model.put("periodType", periodType);
        model.put("periodKey", key);
        model.put("periodYear", "2025");
        model.put("displayName", seasonal ? "Fall 2025" : "2025");
        model.put("periodStart", seasonal ? "2025-09-01" : "2025-01-01");
        model.put("periodEnd", seasonal ? "2025-11-30" : "2025-12-31");
        model.put("periodRange", seasonal ? "01/09/2025 - 30/11/2025" : "01/01/2025 - 31/12/2025");
        model.put("songChart", songChart);
        model.put("albumChart", albumChart);
        model.put("songEntries", songs);
        model.put("albumEntries", List.of());
        model.put("extraSongEntries", extras);
        model.put("songStats", stats);
        model.put("albumStats", Map.of());
        model.put("songSummary", service.summary(songs, stats, false));
        model.put("albumSummary", null);
        model.put("songBreakdowns", ChartBreakdownDTO.forEntries(songs));
        model.put("albumBreakdowns", ChartBreakdownDTO.forEntries(List.of()));
        model.put("isComplete", true);
        model.put("canFinalize", false);
        model.put("songChartFinalized", finalized);
        model.put("albumChartFinalized", finalized);
        model.put("maxSongs", 30);
        model.put("maxAlbums", seasonal ? 5 : 10);
        model.put("maxExtraSongs", seasonal ? 5 : 0);
        model.put("prevPeriodKey", null);
        model.put("nextPeriodKey", null);
        if (seasonal) {
            model.put("allSeasons", List.of(Map.of("periodKey", "2025-Fall", "displayName", "Fall 2025")));
            model.put("seasonNames", List.of("Winter", "Spring", "Summer", "Fall"));
        } else {
            model.put("allYears", List.of(Map.of("periodKey", "2025", "displayName", "2025")));
            model.put("yearSeasons", List.of("2025-Fall"));
        }

        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new MockHttpServletRequest(servletContext), new MockHttpServletResponse());
        return Jsoup.parse(engine.process("charts/period-chart", new WebContext(exchange, Locale.ENGLISH, model)));
    }

    private static ChartEntryDTO entry(int position, int songId, String name, int artistId, String artist, Integer genderId) {
        ChartEntryDTO entry = new ChartEntryDTO();
        entry.setPosition(position);
        entry.setSongId(songId);
        entry.setSongName(name);
        entry.setArtistId(artistId);
        entry.setArtistName(artist);
        entry.setGenderId(genderId);
        entry.setPlayCount(0);
        return entry;
    }
}
