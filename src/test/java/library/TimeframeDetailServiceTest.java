package library;

import library.service.CatalogDetailService;
import library.service.TimeframeDetailService;
import library.service.TimeframeDetailService.Detail;
import library.service.TimeframeDetailService.Tile;
import library.service.TimeframePeriod;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TimeframeDetailServiceTest {
    private static final LocalDate TODAY = LocalDate.of(2026, 11, 18);

    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private TimeframeDetailService service;

    @BeforeEach
    void setup() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE Gender (id INTEGER PRIMARY KEY, name TEXT)");
        jdbc.execute("CREATE TABLE Genre (id INTEGER PRIMARY KEY, name TEXT)");
        jdbc.execute("CREATE TABLE SubGenre (id INTEGER PRIMARY KEY, name TEXT, parent_genre_id INTEGER)");
        jdbc.execute("CREATE TABLE Language (id INTEGER PRIMARY KEY, name TEXT)");
        jdbc.execute("CREATE TABLE Ethnicity (id INTEGER PRIMARY KEY, name TEXT)");
        jdbc.execute("CREATE TABLE Artist (id INTEGER PRIMARY KEY, name TEXT, gender_id INTEGER, country TEXT, ethnicity_id INTEGER,"
                + " genre_id INTEGER, subgenre_id INTEGER, language_id INTEGER)");
        jdbc.execute("CREATE TABLE Album (id INTEGER PRIMARY KEY, artist_id INTEGER, name TEXT, override_genre_id INTEGER,"
                + " override_subgenre_id INTEGER, override_language_id INTEGER)");
        jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER, album_id INTEGER, name TEXT, length_seconds INTEGER,"
                + " override_genre_id INTEGER, override_subgenre_id INTEGER, override_language_id INTEGER, override_gender_id INTEGER,"
                + " override_ethnicity_id INTEGER)");
        jdbc.execute("CREATE TABLE Play (id INTEGER PRIMARY KEY AUTOINCREMENT, play_date TEXT, song_id INTEGER, account TEXT)");
        jdbc.execute("CREATE TABLE Chart (id INTEGER PRIMARY KEY, chart_type TEXT, period_key TEXT, period_start_date TEXT,"
                + " period_end_date TEXT, period_type TEXT, is_finalized INTEGER)");
        jdbc.execute("CREATE TABLE ChartEntry (id INTEGER PRIMARY KEY, chart_id INTEGER, position INTEGER, song_id INTEGER,"
                + " album_id INTEGER, play_count INTEGER)");

        jdbc.execute("INSERT INTO Gender VALUES (1, 'Female'), (2, 'Male')");
        jdbc.execute("INSERT INTO Genre VALUES (1, 'Reggaeton'), (2, 'Pop')");
        jdbc.execute("INSERT INTO Language VALUES (1, 'Spanish')");
        jdbc.execute("INSERT INTO Ethnicity VALUES (1, 'Latino')");
        jdbc.execute("INSERT INTO Artist VALUES (1, 'Bad Bunny', 2, 'Puerto Rico', 1, 1, NULL, 1),"
                + " (2, 'Karol G', 1, 'Colombia', 1, 1, NULL, 1), (3, 'Selena', 1, 'United States', 1, 2, NULL, 1),"
                + " (4, 'Old Band', 2, 'Mexico', NULL, NULL, NULL, NULL)");
        jdbc.execute("INSERT INTO Album VALUES (10, 1, 'Un Verano', NULL, NULL, NULL), (20, 2, 'Bichota', NULL, NULL, NULL)");
        for (int i = 0; i < 8; i++) {
            jdbc.update("INSERT INTO Song VALUES (?, 1, 10, ?, 180, NULL, NULL, NULL, NULL, NULL)", 100 + i, "Track " + (i + 1));
        }
        jdbc.execute("INSERT INTO Song VALUES (200, 2, 20, 'Provenza', 200, NULL, NULL, NULL, NULL, NULL),"
                + " (300, 3, NULL, 'Bidi Bidi', 150, NULL, NULL, NULL, NULL, NULL),"
                + " (400, 4, NULL, 'Old Song', 240, NULL, NULL, NULL, NULL, NULL)");
        // Release dates: Un Verano's tracks inherit 2024, Provenza overrides its 2023 album, Bidi Bidi has its own
        jdbc.execute("ALTER TABLE Album ADD COLUMN release_date TEXT");
        jdbc.execute("ALTER TABLE Song ADD COLUMN release_date TEXT");
        jdbc.execute("UPDATE Album SET release_date = CASE id WHEN 10 THEN '2024-03-01' ELSE '2023-05-01' END");
        jdbc.execute("UPDATE Song SET release_date = '2024-02-01' WHERE id = 200");
        jdbc.execute("UPDATE Song SET release_date = '2024-06-01 00:00:00' WHERE id = 300");

        play("2005-02-13 12:00", 100);                                   // initial load day, never a streak
        play("2023-01-10 10:00", 400);
        play("2023-06-01 10:00", 100);
        for (int i = 0; i < 8; i++) play("2024-03-01 20:" + String.format("%02d", i * 3), 100 + i); // whole album run
        play("2024-03-01 23:50", 200);                                   // Karol G discovered, session runs past midnight
        play("2024-03-02 00:05", 200);
        play("2024-03-02 12:00", 300);                                   // Selena discovered, female-won day
        play("2024-03-02 12:03", 300);
        play("2024-03-03 10:00", 100);
        play("2024-03-04 10:00", 400);                                   // Old Band: comeback and farewell
        play("2025-01-05 10:00", 300);
        play("2025-03-01 10:00", 100);
        jdbc.execute("INSERT INTO Chart VALUES (1, 'song', '2024', '2024-01-01', '2024-12-31', 'yearly', 1)");
        jdbc.execute("INSERT INTO ChartEntry VALUES (1, 1, 1, 100, NULL, 9)");
        service = new TimeframeDetailService(jdbc);
    }

    @AfterEach
    void cleanup() {
        dataSource.destroy();
    }

    private void play(String date, int songId) {
        jdbc.update("INSERT INTO Play (play_date, song_id, account) VALUES (?, ?, 'vatito')", date, songId);
    }

    private Optional<Detail> build(String type, String key, LocalDate today) {
        return service.build(type, key, today);
    }

    @Test
    void periodsUseTheListPageKeysAndRanges() {
        // Week 00 days belong to the previous year's last week, which runs into January
        TimeframePeriod lastWeek = TimeframePeriod.of("weeks", LocalDate.of(2025, 1, 3));
        assertThat(lastWeek.key()).isEqualTo("2024-W53");
        assertThat(lastWeek.from()).isEqualTo(LocalDate.of(2024, 12, 30));
        assertThat(lastWeek.to()).isEqualTo(LocalDate.of(2025, 1, 5));
        assertThat(TimeframePeriod.parse("weeks", "2025-W11").orElseThrow().from()).isEqualTo(LocalDate.of(2025, 3, 17));
        assertThat(TimeframePeriod.parse("weeks", "2025-W11").orElseThrow().name()).isEqualTo("Week 11, 2025");
        // Winter is December of the previous year to the end of February, leap day included
        TimeframePeriod winter = TimeframePeriod.parse("seasons", "2024-Winter").orElseThrow();
        assertThat(winter.from()).isEqualTo(LocalDate.of(2023, 12, 1));
        assertThat(winter.to()).isEqualTo(LocalDate.of(2024, 2, 29));
        assertThat(winter.name()).isEqualTo("Winter 2024");
        assertThat(winter.plural()).isEqualTo("winters");
        assertThat(winter.comparison().key()).isEqualTo("2023-Winter");
        assertThat(TimeframePeriod.parse("months", "2025-03").orElseThrow().comparison().key()).isEqualTo("2024-03");
        assertThat(TimeframePeriod.parse("years", "2024").orElseThrow().comparison().key()).isEqualTo("2023");
        assertThat(TimeframePeriod.parse("decades", "2010s").orElseThrow().to()).isEqualTo(LocalDate.of(2019, 12, 31));
        assertThat(TimeframePeriod.parse("days", "2025-03-21").orElseThrow().name()).isEqualTo("21/03/2025");
        assertThat(TimeframePeriod.parse("days", "2025-03-21").orElseThrow().shortName("weeks")).isEqualTo("Fri 21");
        assertThat(TimeframePeriod.parse("years", "2024").orElseThrow().children("months")).hasSize(12);
        assertThat(TimeframePeriod.parse("weeks", "2025-W1")).isEmpty();
        assertThat(TimeframePeriod.parse("seasons", "2025-Autumn")).isEmpty();
        assertThat(TimeframePeriod.parse("decades", "2015s")).isEmpty();
        assertThat(TimeframePeriod.parse("years", "abc")).isEmpty();
    }

    @Test
    void yearHeroBreakdownsAndSlices() {
        Detail d = build("years", "2024", TODAY).orElseThrow();
        assertThat(d.plays()).isEqualTo(14);
        assertThat(d.theme()).isEqualTo("gender-male");
        assertThat(d.inProgress()).isFalse();
        assertThat(d.eyebrow()).isEqualTo("Year · listened");
        assertThat(d.prev().href()).isEqualTo("/years/2023");
        assertThat(d.next().href()).isEqualTo("/years/2025");
        assertThat(d.partOf()).extracting(TimeframeDetailService.NavLink::label).containsExactly("2020s");
        assertThat(d.contains()).extracting(TimeframeDetailService.NavLink::label).startsWith("Winter", "Spring", "Summer", "Fall", "Jan");

        assertThat(d.tiles()).extracting(Tile::label).containsExactly("Artists", "Albums", "Songs", "Plays", "Time listened",
                "New artists", "Album releases", "Song releases");
        Tile plays = d.tiles().get(3);
        assertThat(plays.value()).isEqualTo("14");
        assertThat(plays.note()).isEqualTo("▲ 600.0% vs 2023");
        assertThat(d.tiles().get(0).href()).isEqualTo("/artists?listenedDateFrom=2024-01-01&listenedDateTo=2024-12-31&periodLabel=2024&view=table");
        // Releases count the catalog, not plays; songs fall back to the album date unless they have their own
        Tile albumReleases = d.tiles().get(6), songReleases = d.tiles().get(7);
        assertThat(albumReleases.value()).isEqualTo("1");
        assertThat(albumReleases.note()).isEqualTo("1 male · 0 female");
        assertThat(albumReleases.href()).isEqualTo("/albums?releaseDateMode=between&releaseDateFrom=01/01/2024"
                + "&releaseDateTo=31/12/2024&sortby=release_date&sortdir=asc&view=table");
        assertThat(songReleases.value()).isEqualTo("10");
        assertThat(songReleases.note()).isEqualTo("8 male · 2 female");
        assertThat(songReleases.href()).startsWith("/songs?releaseDateMode=between&releaseDateFrom=01/01/2024");
        Tile none = build("years", "2025", TODAY).orElseThrow().tiles().get(6);
        assertThat(none.value()).isEqualTo("0");
        assertThat(none.href()).isNull();
        // The hero keeps only the two standings; who won, days played and the chart live in their own cards
        assertThat(d.standing()).extracting(TimeframeDetailService.Standing::value).containsExactly("#1", "#2");
        assertThat(d.standing()).extracting(TimeframeDetailService.Standing::label)
                .containsExactly("of 3 years by plays", "of 3 by male share");
        assertThat(d.standing().get(1).note()).isEqualTo("71% male plays");
        assertThat(d.standing()).allMatch(TimeframeDetailService.Standing::top);
        assertThat(d.chart().text()).isEqualTo("Yearly chart #1 · Track 1");
        assertThat(d.chart().href()).isEqualTo("/charts/yearly/2024");

        assertThat(d.gender()).extracting(CatalogDetailService.GenderRow::label).containsExactly("Artists", "Albums", "Songs", "Plays", "Time");
        assertThat(d.gender().get(3).male()).isEqualTo("10");
        assertThat(d.gender().get(3).female()).isEqualTo("4");
        assertThat(d.genres()).extracting(CatalogDetailService.BreakdownRow::name).containsExactly("Reggaeton", "Pop", "Not set");
        assertThat(d.genres().get(0).href()).isEqualTo(
                "/songs?listenedDateFrom=2024-01-01&listenedDateTo=2024-12-31&periodLabel=2024&genre=1&genreMode=includes&view=table");
        assertThat(d.mixes()).extracting(CatalogDetailService.MixCard::title).containsExactly("Languages", "Ethnicities", "Countries");

        assertThat(d.tallies()).extracting(TimeframeDetailService.Tally::label).containsExactly("Months", "Weeks", "Days");
        var days = d.tallies().get(2);
        assertThat(days.male()).isEqualTo(3);
        assertThat(days.female()).isEqualTo(1);
        assertThat(days.note()).isEqualTo("female: 02/03");
        assertThat(d.steps()).extracting(TimeframeDetailService.Step::id).containsExactly("months", "weeks");
        assertThat(d.steps().get(0).pills()).hasSize(12);
        assertThat(d.steps().get(0).pills().get(2).best()).isTrue();
        assertThat(d.steps().get(0).pills().get(2).href()).isEqualTo("/months/2024-03");
        assertThat(d.steps().get(0).pills().get(0).href()).isNull();
    }

    @Test
    void yearStreaksSessionsTopListsAndArtistLifecycles() {
        Detail d = build("years", "2024", TODAY).orElseThrow();
        var streaks = d.streaks();
        assertThat(streaks.around()).isFalse();
        assertThat(streaks.streaks().get(0).days()).isEqualTo(4);
        assertThat(streaks.streaks().get(0).range()).isEqualTo("01/03/2024 → 04/03/2024");
        assertThat(streaks.streaks().get(1).days()).isEqualTo(2);
        assertThat(streaks.ticks()).anyMatch(t -> t.kind().equals("female") && t.title().startsWith("02/03/2024"));

        // The session that starts at 23:50 keeps the 00:05 play; the album run is a whole-album run
        var sessions = d.sessions();
        assertThat(sessions.count()).isEqualTo("5");
        assertThat(sessions.longest()).isEqualTo("21m");
        assertThat(sessions.longestHref()).isEqualTo("/days/2024-03-01");
        assertThat(sessions.rows()).isEmpty();
        assertThat(sessions.runs()).hasSize(1);
        assertThat(sessions.runs().get(0).title()).isEqualTo("Un Verano · Bad Bunny");
        assertThat(sessions.runs().get(0).sub()).startsWith("whole album · 8 in a row · 01/03/2024");

        var artists = d.tops().get(0);
        assertThat(artists.all()).extracting(TimeframeDetailService.TopRow::name).containsExactly("Bad Bunny", "Karol G", "Selena", "Old Band");
        assertThat(artists.all()).extracting(TimeframeDetailService.TopRow::gender).containsExactly("m", "f", "f", "m");
        assertThat(artists.male()).extracting(TimeframeDetailService.TopRow::name).containsExactly("Bad Bunny", "Old Band");
        assertThat(artists.female()).extracting(TimeframeDetailService.TopRow::name).containsExactly("Karol G", "Selena");
        assertThat(d.topNote()).isEqualTo("2 of your top 4 artists are male");
        assertThat(d.tops().get(1).all().get(0).sub()).isEqualTo("Bad Bunny");

        var disc = d.discoveries();
        assertThat(disc.artists()).extracting(TimeframeDetailService.Discovery::name).containsExactly("Karol G", "Selena");
        assertThat(disc.artists().get(0).faded()).isTrue();
        assertThat(disc.artists().get(1).since()).isEqualTo("1");
        assertThat(disc.artists().get(1).faded()).isFalse();
        assertThat(disc.note()).isEqualTo("2 artists first played · 0 male · 2 female");
        assertThat(disc.songs()).isEmpty();

        assertThat(d.comebacks()).extracting(TimeframeDetailService.Comeback::name).containsExactly("Old Band");
        assertThat(d.comebacks().get(0).gap()).isEqualTo("1.1 yrs");
        assertThat(d.comebackNote()).isEqualTo("back after a year or more away");
        assertThat(d.farewells()).extracting(TimeframeDetailService.Farewell::name).containsExactlyInAnyOrder("Old Band", "Karol G");

        // Every artist, album and song row carries its thumbnail; a whole-album run shows the album cover
        assertThat(sessions.runs().get(0).img()).matches("/albums/\\d+/image\\?thumbnail=true");
        assertThat(artists.all()).allSatisfy(r -> assertThat(r.img()).isEqualTo(r.href() + "/image?thumbnail=true"));
        assertThat(d.tops().get(1).all().get(0).img()).startsWith("/albums/");
        assertThat(d.tops().get(2).all().get(0).img()).startsWith("/songs/");
        assertThat(disc.artists()).allSatisfy(x -> assertThat(x.img()).isEqualTo(x.href() + "/image?thumbnail=true"));
        assertThat(d.comebacks().get(0).img()).startsWith("/artists/").endsWith("/image?thumbnail=true");
        assertThat(d.farewells()).allSatisfy(x -> assertThat(x.img()).isEqualTo(x.href() + "/image?thumbnail=true"));

        // The initial load day is not a peer; 2023 is all male so 2024 is second by male share
        assertThat(d.byPlays()).extracting(CatalogDetailService.PeerRow::name).containsExactly("2024", "2023", "2025");
        assertThat(d.byMale()).extracting(CatalogDetailService.PeerRow::name).containsExactly("2023", "2024", "2025");
        // Month of year only adds something on a decade; a year's months are already the Across card
        assertThat(d.showMonth()).isFalse();
        assertThat(d.showWeek()).isTrue();
        assertThat(build("seasons", "2024-Spring", TODAY).orElseThrow().showMonth()).isFalse();
        assertThat(build("decades", "2020s", TODAY).orElseThrow().showMonth()).isTrue();
        // A week's days are its Across step, so Day of week goes too
        Detail week = build("weeks", "2024-W09", TODAY).orElseThrow();
        assertThat(week.showWeek()).isFalse();
        assertThat(week.showMonth()).isFalse();
    }

    @Test
    void dayPageAdaptsEverySection() {
        Detail d = build("days", "2024-03-01", TODAY).orElseThrow();
        assertThat(d.plays()).isEqualTo(9);
        assertThat(d.eyebrow()).isEqualTo("Day · Friday");
        assertThat(d.sub()).isEqualTo("First play 20:00 · last play 23:50");
        assertThat(d.tiles()).hasSize(8);
        assertThat(d.tiles().get(7).value()).isEqualTo("8");
        assertThat(d.tiles().get(7).href()).contains("releaseDateFrom=01/03/2024&releaseDateTo=01/03/2024");
        assertThat(d.tallies()).extracting(TimeframeDetailService.Tally::label).containsExactly("Sessions", "Hours");
        assertThat(d.tallies().get(0).note()).isEqualTo("female: 23:50 – 00:05");
        assertThat(d.steps().get(0).id()).isEqualTo("hours");
        assertThat(d.steps().get(0).pills()).hasSize(24);
        assertThat(d.sessionStrip()).hasSize(2);
        assertThat(d.sessions().rows()).extracting(TimeframeDetailService.SessionRow::range).containsExactly("20:00 – 20:21", "23:50 – 00:05");

        var listen = d.streaks().streaks().get(0);
        assertThat(d.streaks().around()).isTrue();
        assertThat(listen.label()).isEqualTo("Listening streak · 4 days");
        assertThat(listen.note()).isEqualTo("This day is day 1");
        assertThat(d.standing()).extracting(TimeframeDetailService.Standing::label).containsExactly("of 8 days by plays", "of 8 by male share");
        assertThat(d.standing().get(1).note()).isEqualTo("89% male plays");
        assertThat(d.chart()).isNull();

        assertThat(d.discoveries().artists()).extracting(TimeframeDetailService.Discovery::name).containsExactly("Karol G");
        assertThat(d.discoveries().artists().get(0).first()).isEqualTo("23:50");
        assertThat(d.discoveries().songs()).hasSize(8);
        assertThat(d.comebacks()).extracting(TimeframeDetailService.Comeback::name).containsExactly("Bad Bunny");
        assertThat(d.comebackNote()).isEqualTo("back after 90+ days away");
        assertThat(d.sameDate()).extracting(TimeframeDetailService.DateBar::label).containsExactly("'24", "'25");
        assertThat(d.sameDate().get(0).self()).isTrue();
        assertThat(d.contains()).extracting(TimeframeDetailService.NavLink::label).containsExactly("01/03/2025");
        assertThat(d.partOf()).extracting(TimeframeDetailService.NavLink::label)
                .containsExactly("2020s", "2024", "Spring 2024", "March 2024", "Week 9, 2024");
    }

    @Test
    void inProgressPeriodsProjectAndHoldBackFarewells() {
        Detail d = build("years", "2024", LocalDate.of(2024, 3, 3)).orElseThrow();
        assertThat(d.inProgress()).isTrue();
        assertThat(d.eyebrow()).isEqualTo("Year · in progress");
        assertThat(d.next()).isNull();
        assertThat(d.tiles()).extracting(Tile::label).doesNotContain("Projected", "Per day");
        assertThat(d.farewells()).isEmpty();
        assertThat(d.discoveries().showSince()).isFalse();
        assertThat(d.sub()).isEqualTo("01/01/2024 – today 03/03/2024 · day 63 of 366");
        assertThat(d.standing().get(0).label()).isEqualTo("of 3 years by plays so far");

        assertThat(build("years", "2030", TODAY)).isEmpty();
        assertThat(build("weeks", "nope", TODAY)).isEmpty();
    }

    @Test
    void pagesRenderTheAgreedSectionOrder() {
        Document year = DetailTemplateSupport.render("timeframes/detail", ".tf-page", Map.of("d", build("years", "2024", TODAY).orElseThrow()));
        assertThat(year.select(".tf-page").hasClass("gender-male")).isTrue();
        assertThat(year.select(".cd-hero h1").text()).isEqualTo("2024");
        assertThat(year.select(".tf-tiles > *")).hasSize(8);
        assertThat(year.select(".cd-standing .cd-stand b").eachText()).containsExactly("#1", "#2");
        assertThat(year.select(".cd-standing .cd-stand.is-top")).hasSize(2);
        assertThat(year.select(".tf-top a.tf-chart").attr("href")).isEqualTo("/charts/yearly/2024");
        assertThat(year.select(".tf-page > *").eachAttr("class")).containsSubsequence("cd-row", "cd-mixes",
                "dt-card cd-card tf-won", "dt-card cd-card tf-across", "tf-two", "dt-card cd-card tf-top",
                "dt-card cd-card tf-disc", "tf-two", "dt-card cd-card cd-peers", "dt-card cd-card cd-when");
        assertThat(year.select(".cd-mix")).hasSize(3);
        assertThat(year.select("[data-tf-stream]")).hasSize(4);
        assertThat(year.select("[data-tf-pills=months] .cd-pill")).hasSize(12);
        assertThat(year.select("[data-tf-pills=weeks]").hasAttr("hidden")).isTrue();
        assertThat(year.select("[data-tf-list=all] .tf-tr")).hasSize(4 + 2 + 11);
        assertThat(year.select(".tf-tb.is-f")).isNotEmpty();
        assertThat(year.select(".tf-run").attr("href")).isEqualTo("/days/2024-03-01");
        assertThat(year.select(".tf-bye .tf-cb")).hasSize(2);
        assertThat(year.select(".cd-when .dp-wk")).hasSize(1);
        assertThat(year.select(".cd-when .dp-moy:not(.dp-wk)")).isEmpty();
        // Hours and weekdays share one row when there is no month panel
        assertThat(year.select(".cd-when-grid.is-pair")).hasSize(1);

        Document week = DetailTemplateSupport.render("timeframes/detail", ".tf-page", Map.of("d", build("weeks", "2024-W09", TODAY).orElseThrow()));
        assertThat(week.select(".cd-when .dp-hours")).hasSize(1);
        assertThat(week.select(".cd-when .dp-moy")).isEmpty();
        assertThat(week.select(".cd-when-grid.is-pair")).isEmpty();

        Document day = DetailTemplateSupport.render("timeframes/detail", ".tf-page", Map.of("d", build("days", "2024-03-01", TODAY).orElseThrow()));
        assertThat(day.select(".tf-sameday .tf-otd a")).hasSize(2);
        assertThat(day.select(".tf-strip span")).hasSize(2);
        assertThat(day.select(".tf-sessions .tf-ss")).hasSize(2);
        assertThat(day.select(".cd-when, .cd-peers")).isEmpty();
        assertThat(day.select(".tf-bye")).isEmpty();
    }
}
