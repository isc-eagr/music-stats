package library;

import library.service.CatalogDetailService;
import library.service.CatalogDetailService.Detail;
import library.service.CatalogDetailService.Dimension;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogDetailServiceTest {
    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private CatalogDetailService service;

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

        jdbc.execute("INSERT INTO Gender VALUES (1, 'Female'), (2, 'Male')");
        jdbc.execute("INSERT INTO Genre VALUES (1, 'Reggaeton'), (2, 'Pop')");
        jdbc.execute("INSERT INTO SubGenre VALUES (10, 'Trap Latino', 1), (11, 'Perreo', 1)");
        jdbc.execute("INSERT INTO Language VALUES (1, 'Spanish'), (2, 'English')");
        jdbc.execute("INSERT INTO Ethnicity VALUES (1, 'Latino')");
        jdbc.execute("INSERT INTO Artist VALUES (1, 'Bad Bunny', 2, 'Puerto Rico', 1, 1, 10, 1),"
                + " (2, 'Karol G', 1, 'Colombia', 1, 1, 11, 1), (3, 'Selena', 1, 'United States', 1, 2, NULL, 1)");
        // Selena's album is overridden to Reggaeton, one Bad Bunny song is overridden to Pop
        jdbc.execute("INSERT INTO Album VALUES (10, 1, 'Un Verano', NULL, NULL, NULL), (20, 2, 'Bichota', NULL, NULL, NULL),"
                + " (30, 3, 'Remixes', 1, NULL, NULL)");
        jdbc.execute("INSERT INTO Song VALUES (100, 1, 10, 'Titi', 200, NULL, NULL, NULL, NULL, NULL),"
                + " (101, 1, 10, 'Pop Song', 100, 2, NULL, NULL, NULL, NULL),"
                + " (200, 2, 20, 'Provenza', 180, NULL, NULL, NULL, NULL, NULL),"
                + " (300, 3, 30, 'Como La Flor Remix', 150, NULL, NULL, NULL, NULL, NULL),"
                + " (301, 3, NULL, 'Bidi Bidi', 120, NULL, NULL, NULL, NULL, NULL)");
        for (int i = 0; i < 3; i++) play("2023-01-02 10:0" + i, 100);   // Monday, January
        play("2005-02-13 12:00", 100);                                  // initial Last.fm load, left out of the rhythm
        play("2024-06-08 22:00", 200);                                  // Saturday, June
        play("2024-06-08 22:10", 200);
        play("2024-06-09 15:00", 300);                                  // Sunday, June
        for (int i = 0; i < 4; i++) play("2024-03-01 09:0" + i, 101);   // Pop
        play("2023-05-05 09:00", 301);                                  // Pop
        service = new CatalogDetailService(jdbc);
    }

    @AfterEach
    void cleanup() {
        dataSource.destroy();
    }

    private void play(String date, int songId) {
        jdbc.update("INSERT INTO Play (play_date, song_id, account) VALUES (?, ?, 'vatito')", date, songId);
    }

    @Test
    void genreFollowsTheOverrideCascadeAndSplitsByGender() {
        Detail d = service.build(Dimension.GENRE, "1").orElseThrow();
        assertThat(d.name()).isEqualTo("Reggaeton");
        assertThat(d.typeLabel()).isEqualTo("Genre · 2 subgenres");
        assertThat(d.plays()).isEqualTo(7);
        assertThat(d.stats().get(0).value()).isEqualTo("7");
        assertThat(d.rankLabel()).isEqualTo("#1 of 2 genres");
        assertThat(d.shareLabel()).isEqualTo("58% of all your plays");
        assertThat(d.firstPlay()).isEqualTo("13/02/2005");
        assertThat(d.lastPlay()).isEqualTo("09/06/2024");
        assertThat(d.theme()).isEqualTo("gender-male");
        // Genre artists come from the artist's own genre, albums from the album cascade, songs from the song cascade
        assertThat(d.links()).extracting(CatalogDetailService.ListLink::value).containsExactly("2", "3", "3");
        assertThat(d.links().get(0).href()).isEqualTo("/artists?genre=1&genreMode=includes&view=table");
        var plays = d.gender().get(3);
        assertThat(plays.label()).isEqualTo("Plays");
        assertThat(plays.male()).isEqualTo("4");
        assertThat(plays.female()).isEqualTo("3");
        assertThat(plays.malePct()).isEqualTo(57);
        assertThat(d.gender().get(0).maleHref()).isEqualTo("/artists?genre=1&genreMode=includes&view=table&genderMode=includes&gender=2");
        assertThat(d.gender().get(0).femaleHref()).endsWith("&gender=1");
        assertThat(d.gender().get(4).maleHref()).isNull();
    }

    @Test
    void breakdownAndMixesSkipThePagesOwnCatalogAndKeepUnsetValues() {
        Detail d = service.build(Dimension.GENRE, "1").orElseThrow();
        assertThat(d.breakdownTitle()).isEqualTo("Subgenres");
        assertThat(d.breakdown()).extracting(CatalogDetailService.BreakdownRow::name).containsExactly("Trap Latino", "Perreo", "Not set");
        assertThat(d.breakdown().get(0).href())
                .isEqualTo("/songs?genre=1&genreMode=includes&subgenre=10&subgenreMode=includes&view=table");
        assertThat(d.breakdown().get(0).unset()).isFalse();
        assertThat(d.breakdown().get(2).href()).isEqualTo("/songs?genre=1&genreMode=includes&subgenreMode=isnull&view=table");
        assertThat(d.breakdown().get(2).unset()).isTrue();
        assertThat(d.mixes()).extracting(CatalogDetailService.MixCard::title).containsExactly("Languages", "Countries", "Ethnicities");
        assertThat(d.mixes().get(1).rows()).extracting(CatalogDetailService.MixRow::name)
                .containsExactly("Puerto Rico", "Colombia", "United States");
        assertThat(d.mixes().get(1).rows().get(0).href())
                .isEqualTo("/songs?genre=1&genreMode=includes&country=Puerto%20Rico&countryMode=includes&view=table");
        assertThat(d.mixes().get(1).rows().get(0).share()).isEqualTo("57");

        Detail language = service.build(Dimension.LANGUAGE, "1").orElseThrow();
        assertThat(language.breakdownTitle()).isEqualTo("Genres");
        assertThat(language.mixes()).extracting(CatalogDetailService.MixCard::title).containsExactly("Countries", "Ethnicities", "Subgenres");
    }

    @Test
    void yearPillValuesStayShort() {
        assertThat(CatalogDetailService.compactCount(999)).isEqualTo("999");
        assertThat(CatalogDetailService.compactCount(1000)).isEqualTo("1k");
        assertThat(CatalogDetailService.compactCount(1250)).isEqualTo("1.3k");
        assertThat(CatalogDetailService.compactCount(12400)).isEqualTo("12k");
        assertThat(CatalogDetailService.compactTime(1800)).isEqualTo("30m");
        assertThat(CatalogDetailService.compactTime(5 * 3600 + 1200)).isEqualTo("5h");
        assertThat(CatalogDetailService.compactTime(1500L * 3600)).isEqualTo("63d");
    }

    @Test
    void yearsCarryTheRankAmongPeersAndPeersAreRankedTwoWays() {
        Detail d = service.build(Dimension.GENRE, "1").orElseThrow();
        assertThat(d.years()).hasSize(20);
        assertThat(d.years().get(0).rank()).isEqualTo(1);
        assertThat(d.years().get(1).rank()).isNull();
        assertThat(d.years().get(18).year()).isEqualTo(2023);
        assertThat(d.years().get(18).rank()).isEqualTo(1);
        assertThat(d.years().get(19).rank()).isEqualTo(2);
        // Time has its own yearly rank and value, so the Time toggle changes the pills too
        assertThat(d.years().get(18).plays()).isEqualTo("3");
        assertThat(d.years().get(18).playsTitle()).endsWith("· 3 plays");
        assertThat(d.years().get(18).timeRank()).isNotNull();
        assertThat(d.years().get(18).timeTitle()).endsWith("listened");
        assertThat(d.years().get(1).timeRank()).isNull();
        assertThat(d.playsStream().male()).startsWith("M").endsWith("Z");
        assertThat(d.playsStream().note()).isEqualTo("Peak year 2023 with 3 plays");
        assertThat(d.byPlays()).extracting(CatalogDetailService.PeerRow::name).containsExactly("Reggaeton", "Pop");
        assertThat(d.byPlays().get(0).self()).isTrue();
        // Pop is 4 of 5 male plays (80%) against Reggaeton's 57%
        assertThat(d.byMale()).extracting(CatalogDetailService.PeerRow::name).containsExactly("Pop", "Reggaeton");
        assertThat(d.byMale().get(1).rank()).isEqualTo(2);
    }

    @Test
    void globalRhythmCountsEveryPlayIncludingUnmatchedOnes() {
        jdbc.update("INSERT INTO Play (play_date, song_id, account) VALUES ('2024-06-10 09:30', NULL, 'vatito')"); // Monday, unmatched
        var r = service.globalRhythm();
        assertThat(r.hourNote()).isEqualTo("Peak hour 09:00 · 50.0% of plays");
        assertThat(r.weekBands()).extracting(CatalogDetailService.Band::share).containsExactly(75, 25);
        assertThat(r.weekNote()).isEqualTo("Peak day Friday · 41.7% of plays");
        assertThat(r.monthNote()).isEqualTo("Peak month March · 33.3% of plays");
        assertThat(r.excludedNote()).contains("Leaves out 1 plays from 13/02/2005");
    }

    @Test
    void rhythmSplitsWeekdaysFromTheWeekendAndLeavesOutTheInitialLoad() {
        var r = service.build(Dimension.GENRE, "1").orElseThrow().rhythm();
        assertThat(r.hourBands()).extracting(CatalogDetailService.Band::share).containsExactly(0, 50, 17, 33);
        assertThat(r.hours().get(10).best()).isTrue();
        assertThat(r.hourNote()).isEqualTo("Peak hour 10:00 · 50.0% of plays");
        assertThat(r.weekBands()).extracting(CatalogDetailService.Band::name).containsExactly("Weekdays", "Weekend");
        assertThat(r.weekBands()).extracting(CatalogDetailService.Band::share).containsExactly(50, 50);
        assertThat(r.week()).extracting(CatalogDetailService.Bar::label).containsExactly("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");
        assertThat(r.week().get(0).best()).isTrue();
        assertThat(r.weekNote()).isEqualTo("Peak day Monday · 50.0% of plays");
        assertThat(r.monthBands()).extracting(CatalogDetailService.Band::name).containsExactly("Winter", "Spring", "Summer", "Fall", null);
        assertThat(r.monthBands().get(0).share()).isEqualTo(50);
        assertThat(r.monthNote()).isEqualTo("Peak month January · 50.0% of plays");
        assertThat(r.excludedNote()).contains("Leaves out 1 plays from 13/02/2005");
    }

    @Test
    void subgenreCountryAndUnknownKeys() {
        Detail sub = service.build(Dimension.SUBGENRE, "10").orElseThrow();
        assertThat(sub.parentName()).isEqualTo("Reggaeton");
        assertThat(sub.parentHref()).isEqualTo("/genres/1");
        assertThat(sub.breakdownTitle()).isEqualTo("Countries");
        Document subPage = DetailTemplateSupport.render("catalog/detail", ".cd-page", Map.of("d", sub));
        assertThat(subPage.select(".cd-eyebrow").text()).isEqualTo("Subgenre of Reggaeton");
        assertThat(subPage.select(".cd-eyebrow a").attr("href")).isEqualTo("/genres/1");

        Detail country = service.build(Dimension.COUNTRY, "Puerto Rico").orElseThrow();
        assertThat(country.plays()).isEqualTo(8);
        assertThat(country.links().get(0).href()).isEqualTo("/artists?country=Puerto%20Rico&countryMode=includes&view=table");
        assertThat(country.byPlays().get(0).href()).isEqualTo("/countries/Puerto%20Rico");

        assertThat(service.build(Dimension.COUNTRY, "Atlantis")).isEmpty();
        assertThat(service.build(Dimension.GENRE, "99")).isEmpty();
        assertThat(service.build(Dimension.GENRE, "abc")).isEmpty();
    }

    @Test
    void pageRendersEverySection() throws Exception {
        Detail d = service.build(Dimension.GENRE, "1").orElseThrow();
        Document doc = DetailTemplateSupport.render("catalog/detail", ".cd-page", Map.of("d", d));
        assertThat(doc.select(".cd-page").hasClass("gender-male")).isTrue();
        assertThat(doc.select(".cd-hero h1").text()).isEqualTo("Reggaeton");
        // Rank and share are the standing numbers, the play dates sit under the name
        assertThat(doc.select(".cd-stand b").eachText()).containsExactly("#1", "58%");
        assertThat(doc.select(".cd-stand-l").eachText()).containsExactly("of 2 genres by plays", "of all your plays");
        assertThat(doc.select(".cd-stand.is-top")).hasSize(1);
        assertThat(doc.select(".cd-hero-sub").text()).isEqualTo("First play 13/02/2005 · last play 09/06/2024");
        assertThat(doc.select(".cd-tile")).hasSize(6);
        assertThat(doc.select("a.cd-tile").eachAttr("href")).containsExactly(
                "/artists?genre=1&genreMode=includes&view=table", "/albums?genre=1&genreMode=includes&view=table",
                "/songs?genre=1&genreMode=includes&view=table");
        assertThat(doc.select(".cd-gcol")).hasSize(5);
        assertThat(doc.select(".cd-gcol a.cd-gv")).hasSize(6);
        assertThat(doc.select(".cd-break a.cd-brow")).hasSize(3);
        assertThat(doc.select(".cd-break a.cd-brow.is-unset").text()).contains("Not set");
        assertThat(doc.select(".cd-break a.cd-brow").first().attr("href"))
                .isEqualTo("/songs?genre=1&genreMode=includes&subgenre=10&subgenreMode=includes&view=table");
        assertThat(doc.select("[data-cd-stream]")).hasSize(2);
        assertThat(doc.select(".cd-pill")).hasSize(20);
        assertThat(doc.select(".cd-pill [data-cd-pill=plays] b.is-one")).hasSize(2);
        assertThat(doc.select(".cd-pill [data-cd-pill=time]")).hasSize(20).allMatch(e -> e.hasAttr("hidden"));
        assertThat(doc.select(".cd-pill [data-cd-pill=plays]")).noneMatch(e -> e.hasAttr("hidden"));
        assertThat(doc.select(".cd-peer.is-self")).hasSize(2);
        assertThat(doc.select(".dp-hours > i")).hasSize(24);
        assertThat(doc.select(".dp-wk .dp-sband")).hasSize(2);
        assertThat(doc.select(".dp-wk > div")).hasSize(7);
        assertThat(doc.select(".dp-moy:not(.dp-wk) > div")).hasSize(12);
        assertThat(doc.select(".cd-mix")).hasSize(3);
        // The mixes sit right under the gender / breakdown row, before the peers and the rhythm panels
        assertThat(doc.select(".cd-page > *").eachAttr("class")).containsSubsequence(
                "cd-row", "cd-mixes", "dt-card cd-card cd-peers", "dt-card cd-card cd-when");
        // No tabs: everything lives on one page
        assertThat(doc.select("[role=tablist], .detail-tabs")).isEmpty();
    }
}
