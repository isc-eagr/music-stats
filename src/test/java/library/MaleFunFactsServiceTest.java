package library;

import library.controller.PlaygroundController;
import library.service.MaleFunFactsService;
import library.service.MaleFunFactsService.FactList;
import library.service.MaleFunFactsService.MaleFunFacts;
import library.service.PlaygroundService;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.ui.ExtendedModelMap;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MaleFunFactsServiceTest {
    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private MaleFunFactsService service;

    @BeforeEach
    void setup() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE Artist (id INTEGER PRIMARY KEY, name TEXT, gender_id INTEGER, is_band INTEGER, country TEXT, birth_date TEXT, death_date TEXT)");
        jdbc.execute("CREATE TABLE Album (id INTEGER PRIMARY KEY, artist_id INTEGER, name TEXT)");
        jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER, album_id INTEGER, name TEXT, length_seconds INTEGER, override_gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Play (id INTEGER PRIMARY KEY, play_date TEXT, song_id INTEGER)");
        jdbc.execute("""
                INSERT INTO Artist VALUES (1, 'Rauw', 2, 0, 'Puerto Rico', '1993-01-10', NULL),
                    (2, 'Banda', 2, 1, 'Mexico', NULL, NULL), (3, 'Selena', 1, 0, NULL, NULL, NULL),
                    (4, 'Gone', 2, 0, 'United States', '1998-10-30', '2018-06-18')""");
        jdbc.execute("INSERT INTO Album VALUES (1, 1, 'Album One'), (2, 4, 'Album Two')");
        jdbc.execute("""
                INSERT INTO Song VALUES (1, 1, 1, 'A', 200, NULL), (2, 1, 1, 'B', 100, NULL), (3, 2, NULL, 'C', 300, NULL),
                    (4, 3, NULL, 'D', 100, NULL), (5, 3, NULL, 'E', 100, 2), (6, 4, 2, 'F', 60, NULL)""");
        String[][] plays = {
                {"2024-01-01 10:00", "1"}, {"2024-01-02 11:00", "2"}, {"2024-01-03 09:00", "1"}, {"2024-01-05 09:00", "1"},
                {"2020-05-01 10:00", "3"}, {"2020-05-01 11:00", "3"}, {"2019-02-01 08:00", "6"},
                {"2024-01-01 12:00", "4"}, {"2024-02-01 12:00", "5"}, {"2024-01-01 13:00", null},
                // The excluded initial-load day must not count anywhere.
                {"2005-02-13 04:20", "6"}, {"2005-02-13 04:20", "1"}};
        for (String[] play : plays) jdbc.update("INSERT INTO Play (play_date, song_id) VALUES (?, ?)", play[0], play[1]);
        jdbc.execute("CREATE TABLE Chart (id INTEGER PRIMARY KEY, chart_type TEXT, period_key TEXT, period_start_date TEXT, period_end_date TEXT, period_type TEXT)");
        jdbc.execute("CREATE TABLE ChartEntry (id INTEGER PRIMARY KEY, chart_id INTEGER, position INTEGER, song_id INTEGER, album_id INTEGER, play_count INTEGER)");
        jdbc.execute("""
                INSERT INTO Chart VALUES (1, 'song', '2024-W01', '2024-01-01', '2024-01-07', 'weekly'), (2, 'song', '2024-W02', '2024-01-08', '2024-01-14', 'weekly'),
                    (3, 'song', '2024-W03', '2024-01-15', '2024-01-21', 'weekly'), (4, 'song', '2024-W04', '2024-01-22', '2024-01-28', 'weekly'),
                    (5, 'song', '2005-W06', '2005-02-07', '2005-02-13', 'weekly'), (6, 'album', '2024-W01', '2024-01-01', '2024-01-07', 'weekly'),
                    (7, 'song', '2024-Winter', '2024-01-01', '2024-03-31', 'seasonal')""");
        jdbc.execute("""
                INSERT INTO ChartEntry (chart_id, position, song_id, album_id) VALUES (1, 1, 1, NULL), (1, 2, 4, NULL), (2, 1, 1, NULL), (2, 2, 3, NULL),
                    (3, 1, 4, NULL), (3, 2, 1, NULL), (4, 1, 1, NULL), (5, 1, 6, NULL), (6, 1, NULL, 1), (7, 1, 6, NULL)""");
        Clock clock = Clock.fixed(Instant.parse("2026-10-30T18:00:00Z"), ZoneId.of("America/Mexico_City"));
        service = new MaleFunFactsService(jdbc, clock);
    }

    @AfterEach
    void cleanup() {
        dataSource.destroy();
    }

    @Test
    void allTimeListsRankMalePlaysIncludingSongGenderOverrides() {
        MaleFunFacts facts = service.getFacts(null);
        assertThat(facts.stats()).extracting(MaleFunFactsService.Stat::value)
                .containsExactly("8", "4", "5", "2", "24m");
        assertThat(facts.stats().getFirst().detail()).isEqualTo("80.0% of all plays");
        assertThat(facts.split().male()).isEqualTo(80.0);
        assertThat(facts.split().female()).isEqualTo(10.0);
        assertThat(facts.split().other()).isEqualTo(10.0);
        assertThat(facts.split().solo()).isEqualTo(75.0);
        assertThat(list(facts, "Top male artists").rows().getFirst()).isEqualTo(new MaleFunFactsService.FactRow(
                "Rauw", "/artists/1", "/artists/1/image?thumbnail=true", "50.0% of male plays", "4", 0, 100));
        assertThat(list(facts, "Top male artists").rows().get(1).barWidth()).isEqualTo(50.0);
        assertThat(list(facts, "Top male songs").rows().getFirst().imageUrl()).isEqualTo("/albums/1/image?thumbnail=true");
        assertThat(list(facts, "Top male songs").rows()).filteredOn(row -> row.name().equals("C"))
                .extracting(MaleFunFactsService.FactRow::imageUrl).containsExactly("/songs/3/image?thumbnail=true");
        assertThat(list(facts, "Top male artists").rows()).extracting(MaleFunFactsService.FactRow::name).contains("Selena");
        assertThat(list(facts, "Top male songs").rows().getFirst().name()).isEqualTo("A");
        assertThat(list(facts, "Top male albums").rows()).extracting(MaleFunFactsService.FactRow::name).containsExactly("Album One", "Album Two");
        assertThat(list(facts, "Longest daily streaks").rows().getFirst()).extracting(MaleFunFactsService.FactRow::name,
                MaleFunFactsService.FactRow::detail, MaleFunFactsService.FactRow::value).containsExactly("Rauw", "01/01/2024 to 03/01/2024", "3");
        assertThat(list(facts, "Biggest single days").rows().getFirst().detail()).isEqualTo("01/05/2020");
        assertThat(list(facts, "Longest time listening").rows()).isEmpty();
        assertThat(list(facts, "One-song men").rows()).extracting(MaleFunFactsService.FactRow::name).containsExactly("Banda", "Selena", "Gone");
        assertThat(list(facts, "Deepest catalogs").rows().getFirst().value()).isEqualTo("2");
        assertThat(list(facts, "Not played in 3+ years").rows()).extracting(MaleFunFactsService.FactRow::detail)
                .containsExactly("Last played 01/05/2020", "Last played 01/02/2019");
        assertThat(list(facts, "Men who have passed").rows().getFirst().detail()).isEqualTo("Died 18/06/2018");
        assertThat(list(facts, "Born on 30/10").rows().getFirst().detail()).isEqualTo("Born 30/10/1998");
        assertThat(list(facts, "Top countries for men").rows()).extracting(MaleFunFactsService.FactRow::name)
                .containsExactly("Puerto Rico", "Mexico", "United States", "Unknown");
        assertThat(list(facts, "Milestones").rows()).extracting(MaleFunFactsService.FactRow::detail)
                .containsExactly("F · Gone", "92 plays to go");
        assertThat(facts.groups()).extracting(MaleFunFactsService.FactGroup::title)
                .containsExactly("Most played", "Always around", "Binges", "Catalog depth", "Then and now");
        assertThat(facts.groups().stream().flatMap(group -> group.lists().stream())).extracting(FactList::title).doesNotContain("First heard in 2024");
        assertThat(facts.hours().get(10).total()).isEqualTo(2);
        assertThat(facts.hours().get(10).male()).isEqualTo(2);
        assertThat(facts.weekdays().getFirst().label()).isEqualTo("Monday");
        assertThat(facts.heatmap()).hasSize(7).allSatisfy(row -> assertThat(row.cells()).hasSize(24));
        assertThat(facts.heatmap().getFirst().label()).isEqualTo("Mon");
        assertThat(facts.timeline()).extracting(MaleFunFactsService.ShareBar::label)
                .containsExactly("2019", "2020", "2021", "2022", "2023", "2024");
        assertThat(facts.timeline().getLast().height()).isEqualTo(100.0);
        assertThat(facts.timeline().get(2).total()).isZero();
    }

    @Test
    void spansSitOnASharedAxisAndYearTimelineIsMonthly() {
        for (int day = 1; day <= 10; day++) {
            jdbc.update("INSERT INTO Play (play_date, song_id) VALUES (?, 3)", String.format("2022-03-%02d 10:00", day));
        }
        MaleFunFacts facts = service.getFacts(null);
        FactList spans = list(facts, "Longest time listening");
        assertThat(spans.style()).isEqualTo("spans");
        assertThat(spans.axisStart()).isEqualTo("2019");
        assertThat(spans.axisEnd()).isEqualTo("2024");
        var banda = spans.rows().getFirst();
        assertThat(banda.name()).isEqualTo("Banda");
        assertThat(banda.barStart()).isGreaterThan(0);
        assertThat(banda.barStart() + banda.barWidth()).isLessThanOrEqualTo(100.1);
        assertThat(service.getFacts(2024).timeline()).hasSize(12).first()
                .extracting(MaleFunFactsService.ShareBar::label).isEqualTo("Jan");
    }

    @Test
    void artistPeaksCoverSingleDaysWeeksAndMonths() {
        MaleFunFacts facts = service.getFacts(null);
        assertThat(list(facts, "Biggest single weeks").rows().getFirst()).extracting(MaleFunFactsService.FactRow::name,
                MaleFunFactsService.FactRow::detail, MaleFunFactsService.FactRow::value).containsExactly("Rauw", "week of 01/01/2024", "4");
        assertThat(list(facts, "Biggest single weeks").rows()).filteredOn(row -> row.name().equals("Banda"))
                .extracting(MaleFunFactsService.FactRow::detail).containsExactly("week of 27/04/2020");
        assertThat(list(facts, "Biggest single months").rows().getFirst()).extracting(MaleFunFactsService.FactRow::detail,
                MaleFunFactsService.FactRow::value).containsExactly("January 2024", "4");
    }

    @Test
    void weeklyChartFactsSkipTheExcludedWeekAndTrackNumberOneRuns() {
        var charts = service.getFacts(null).charts();
        assertThat(charts.stats()).extracting(MaleFunFactsService.Stat::value).containsExactly("3", "1", "1", "1");
        assertThat(charts.stats().getFirst().detail()).isEqualTo("75.0% of 4 weeks");
        FactList runs = charts.lists().stream().filter(list -> list.title().equals("Longest #1 runs")).findFirst().orElseThrow();
        assertThat(runs.rows().getFirst()).extracting(MaleFunFactsService.FactRow::name, MaleFunFactsService.FactRow::detail,
                MaleFunFactsService.FactRow::value).containsExactly("A", "Rauw · 01/01/2024 to 14/01/2024", "2");
        FactList weeks = charts.lists().stream().filter(list -> list.title().equals("Most weeks on the song chart")).findFirst().orElseThrow();
        assertThat(weeks.rows()).extracting(MaleFunFactsService.FactRow::name).containsExactly("A", "C");
        assertThat(weeks.rows().getFirst().detail()).isEqualTo("Rauw · peak #1");
        assertThat(service.getFacts(2023).charts().stats().getFirst().detail()).isEqualTo("0.0% of 0 weeks");
    }

    @Test
    void songObsessionsKeepEachSongsBusiestDayAndWeek() {
        MaleFunFacts facts = service.getFacts(null);
        assertThat(list(facts, "Song obsessions: one day").rows().getFirst()).extracting(MaleFunFactsService.FactRow::name,
                MaleFunFactsService.FactRow::detail, MaleFunFactsService.FactRow::value).containsExactly("C", "Banda · 01/05/2020", "2");
        assertThat(list(facts, "Song obsessions: one week").rows().getFirst()).extracting(MaleFunFactsService.FactRow::name,
                MaleFunFactsService.FactRow::detail, MaleFunFactsService.FactRow::value).containsExactly("A", "Rauw · week of 01/01/2024", "3");
        assertThat(list(facts, "Song obsessions: one week").rows()).extracting(MaleFunFactsService.FactRow::name).doesNotHaveDuplicates();
    }

    @Test
    void monthOfYearSharesCompareMonthsAcrossAllYearsOnly() {
        var months = service.getFacts(null).monthsOfYear();
        assertThat(months).hasSize(12);
        assertThat(months.getFirst()).isEqualTo(new MaleFunFactsService.MonthTile("January", 66.7, "6", 1));
        assertThat(months.get(1).level()).isEqualTo(5);
        assertThat(months.get(2)).extracting(MaleFunFactsService.MonthTile::plays, MaleFunFactsService.MonthTile::level).containsExactly("0", 0);
        assertThat(service.getFacts(2024).monthsOfYear()).isEmpty();
    }

    @Test
    void yearModeOnlyCountsThatYearAndListsNewcomers() {
        MaleFunFacts facts = service.getFacts(2024);
        assertThat(facts.stats().getFirst().value()).isEqualTo("5");
        assertThat(facts.stats().getFirst().detail()).isEqualTo("71.4% of all plays");
        assertThat(list(facts, "First heard in 2024").rows()).extracting(MaleFunFactsService.FactRow::name)
                .containsExactly("Rauw", "Selena");
        assertThat(list(facts, "Not played in 3+ years").rows()).isEmpty();
        assertThat(list(facts, "Top countries for men").rows()).extracting(MaleFunFactsService.FactRow::name)
                .doesNotContain("Mexico");
    }

    @Test
    void controllerAndTemplateShowFiveRowsThenShowMore() {
        for (int i = 0; i < 12; i++) {
            jdbc.update("INSERT INTO Artist VALUES (?, ?, 2, 0, 'Chile', NULL, NULL)", 100 + i, "Extra " + i);
            jdbc.update("INSERT INTO Song VALUES (?, ?, NULL, 'Solo', 100, NULL)", 100 + i, 100 + i);
            jdbc.update("INSERT INTO Play (play_date, song_id) VALUES ('2025-03-03 10:00', ?)", 100 + i);
        }
        var model = new ExtendedModelMap();
        var controller = new PlaygroundController(new PlaygroundService(jdbc), service);
        assertThat(controller.playground(null, "male", "year", "winter", null, model)).isEqualTo("playground");
        assertThat(model.get("selectedYear")).isNull();

        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var html = engine.process("fragments/male-facts-view", Set.of("male-facts-view"), new Context(Locale.ENGLISH, model.asMap()));
        var document = Jsoup.parse(html);
        var topArtists = document.select(".male-facts-list").stream()
                .filter(section -> section.select("h3").text().equals("Top male artists")).findFirst().orElseThrow();
        assertThat(topArtists.select("> ol > li")).hasSize(5);
        assertThat(topArtists.select("details summary").text()).isEqualTo("Show 5 more");
        assertThat(topArtists.select("details li .male-facts-rank").first().text()).isEqualTo("6");
        assertThat(document.select("#male-facts-year option").first().text()).isEqualTo("All time");
        assertThat(document.select(".male-facts-heat-cell")).hasSize(168);
        assertThat(document.select(".male-facts-podium-card")).hasSize(3);
        assertThat(document.select(".male-facts-podium-card.place-1 strong").text()).isEqualTo("Rauw");
        assertThat(document.select(".male-facts-ring").attr("style")).contains("--m: 90.9");
        assertThat(topArtists.select(".male-facts-row-bar span").first().attr("style")).contains("width: 100.0%");
        assertThat(document.select(".style-milestones .male-facts-row-bar")).isEmpty();
        assertThat(document.select(".male-facts-months li")).hasSize(12);
        assertThat(document.select(".male-facts-chart-stats .male-facts-stat")).hasSize(4);
        assertThat(document.select(".male-facts-group .male-facts-list h3").eachText())
                .contains("Most weeks at #1: songs", "Longest #1 runs", "Most weeks at #1: albums", "Most days with a play");
        assertThat(document.select(".male-facts-group > h2").eachText())
                .containsExactly("Most played", "Always around", "Binges", "Catalog depth", "Then and now", "Men on your weekly charts");
        assertThat(topArtists.select(".male-facts-thumb.portrait")).hasSize(10);
    }

    @Test
    void activityListsCountDistinctDaysWeeksAndMonthsAgainstThePeriod() {
        MaleFunFacts facts = service.getFacts(null);
        // First counted play is 01/02/2019 (13/02/2005 is excluded); the clock says 30/10/2026.
        var days = list(facts, "Most days with a play").rows().getFirst();
        assertThat(days).extracting(MaleFunFactsService.FactRow::name, MaleFunFactsService.FactRow::value).containsExactly("Rauw", "4");
        assertThat(days.detail()).isEqualTo("0.1% of 2,829 days");
        assertThat(list(facts, "Most weeks with a play").rows().getFirst().value()).isEqualTo("1");
        assertThat(list(facts, "Most months with a play").rows().getFirst().detail()).endsWith("of 93 months");
        var yearDays = list(service.getFacts(2024), "Most days with a play").rows().getFirst();
        assertThat(yearDays.detail()).isEqualTo("1.1% of 366 days");
    }

    private static FactList list(MaleFunFacts facts, String title) {
        return facts.groups().stream().flatMap(group -> group.lists().stream())
                .filter(list -> list.title().equals(title)).findFirst().orElseThrow();
    }
}
