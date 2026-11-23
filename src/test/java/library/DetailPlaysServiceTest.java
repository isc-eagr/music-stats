package library;

import library.service.DetailPlaysService;
import library.service.DetailPlaysService.Kind;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.thymeleaf.context.Context;
import org.thymeleaf.context.IExpressionContext;
import org.thymeleaf.linkbuilder.StandardLinkBuilder;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.StringTemplateResolver;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class DetailPlaysServiceTest {
    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private DetailPlaysService service;

    @BeforeEach
    void setup() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE Artist (id INTEGER PRIMARY KEY, name TEXT, gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Album (id INTEGER PRIMARY KEY, artist_id INTEGER, name TEXT, release_date DATE)");
        jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER, album_id INTEGER, name TEXT, "
                + "override_gender_id INTEGER, release_date DATE)");
        jdbc.execute("CREATE TABLE SongFeaturedArtist (song_id INTEGER, artist_id INTEGER)");
        jdbc.execute("CREATE TABLE Play (id INTEGER PRIMARY KEY AUTOINCREMENT, play_date TEXT, song_id INTEGER, account TEXT)");
        jdbc.execute("INSERT INTO Artist VALUES (1, 'Selena', 1), (2, 'Bad Bunny', 2), (3, 'Grupo', 2)");
        jdbc.execute("INSERT INTO Album VALUES (10, 1, 'Amor Prohibido', '2023-03-13'), (20, 2, 'Un Verano Sin Ti', '2022-05-06')");
        jdbc.execute("INSERT INTO Song VALUES (100, 1, 10, 'Como La Flor', NULL, NULL), (101, 1, 10, 'Bidi Bidi', NULL, NULL), "
                + "(200, 2, 20, 'Titi', 1, NULL), (300, 3, NULL, 'Collab', NULL, NULL)");
        jdbc.execute("INSERT INTO SongFeaturedArtist VALUES (300, 1)");
        service = new DetailPlaysService(jdbc);
    }

    @AfterEach
    void cleanup() {
        dataSource.destroy();
    }

    @Test
    void milestoneStepsFollowEachCatalogsJumps() throws Exception {
        Method steps = DetailPlaysService.class.getDeclaredMethod("milestoneSteps", Kind.class, long.class);
        steps.setAccessible(true);
        assertThat(steps.invoke(null, Kind.ARTIST, 3200L)).isEqualTo(List.of(50, 100, 200, 500, 1000, 2000, 3000));
        assertThat(steps.invoke(null, Kind.ALBUM, 2600L))
                .isEqualTo(List.of(100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000, 2500));
        assertThat(steps.invoke(null, Kind.SONG, 260L)).isEqualTo(List.of(50, 100, 150, 200, 250));
        assertThat(steps.invoke(null, Kind.ARTIST, 49L)).isEqualTo(List.of());
    }

    @Test
    void artistTabCountsScopeYearsLegacyTopSongsAndLatestPlays() {
        play("2021-05-01 10:00", 100, "robertlover");
        play("2023-03-13 08:15", 100, "vatito");
        play("2023-03-13 21:00", 101, "vatito");
        play("2023-07-02 22:30", 100, "vatito");
        play("2024-01-05 09:00", 300, "vatito");
        play("2024-01-06 09:00", 200, "vatito");

        var tab = service.build(service.artistScope(1, List.of(), true, true), Map.of(2023, new Integer[]{1, 1}));
        assertThat(tab.total()).isEqualTo(5);
        assertThat(tab.legacy()).isEqualTo(1);
        // Every year from the first play to the last, the empty 2022 included, newest first
        assertThat(tab.years()).extracting(DetailPlaysService.YearRow::year).containsExactly(2024, 2023, 2022, 2021);
        var y2023 = tab.years().get(1);
        assertThat(y2023.plays()).isEqualTo(3);
        assertThat(y2023.overallRank()).isEqualTo(1);
        assertThat(y2023.months().get(2)).isEqualTo(2);
        assertThat(y2023.bestMonth()).isEqualTo(3);
        assertThat(y2023.topSongs()).extracting(DetailPlaysService.TopSong::name).containsExactly("Como La Flor", "Bidi Bidi");
        assertThat(tab.years().get(2).plays()).isZero();
        assertThat(tab.years().get(2).bestMonth()).isZero();
        assertThat(tab.years().get(1).change()).isNull();
        assertThat(tab.years().get(0).change()).isEqualTo(-67);

        assertThat(tab.latest()).hasSize(5);
        assertThat(tab.latest().getFirst().date()).isEqualTo("05/01/2024");
        assertThat(tab.latest().getFirst().sourceLabel()).isEqualTo("Feat. on Grupo");
        assertThat(tab.latest().get(1).time()).isEqualTo("22:30");

        @SuppressWarnings("unchecked")
        var releases = (List<Map<String, Object>>) tab.chart().get("releases");
        assertThat(releases).extracting(r -> r.get("name")).containsExactly("Amor Prohibido");
        @SuppressWarnings("unchecked")
        var hours = (Map<String, long[]>) tab.chart().get("hours");
        assertThat(hours.get("2023")[21]).isEqualTo(1);

        var mainOnly = service.build(service.artistScope(1, List.of(), true, false), Map.of());
        assertThat(mainOnly.total()).isEqualTo(4);
        var nothing = service.build(service.artistScope(1, null, false, false), Map.of());
        assertThat(nothing.total()).isZero();
        assertThat(nothing.years()).isEmpty();
    }

    @Test
    void milestonesRecordTheDayAndSongOfTheNthPlay() {
        for (int i = 1; i <= 60; i++) play("2024-02-" + String.format("%02d", Math.min(29, i)) + " 10:" + String.format("%02d", i % 60), 200, "vatito");
        var tab = service.build(service.songScope(200), Map.of());
        @SuppressWarnings("unchecked")
        var milestones = (List<Map<String, Object>>) tab.chart().get("milestones");
        assertThat(milestones).hasSize(1);
        assertThat(milestones.getFirst()).containsEntry("n", 50).containsEntry("day", "2024-02-29").containsEntry("song", "Titi");
    }

    @Test
    void yearRanksSplitOverallAndGenderUsingTheSongOverride() {
        play("2024-01-01 10:00", 100, "vatito");
        play("2024-01-02 10:00", 100, "vatito");
        play("2024-01-03 10:00", 200, "vatito");
        // Titi's override makes it female, so it ranks #2 among female songs and #2 overall
        assertThat(service.yearRanks(Kind.SONG, 200).get(2024)).containsExactly(2, 2);
        // Bad Bunny is the only male artist with plays that year
        assertThat(service.yearRanks(Kind.ARTIST, 2).get(2024)).containsExactly(2, 1);
        assertThat(service.yearRanks(Kind.ALBUM, 10).get(2024)).containsExactly(1, 1);
    }

    @Test
    void rankMovesReplayPassesAndFallsNewestFirst() {
        TreeMap<String, Map<Integer, Long>> byDay = new TreeMap<>();
        byDay.put("2024-01-01", Map.of(2, 3L));
        byDay.put("2024-01-02", Map.of(1, 2L));
        byDay.put("2024-01-03", Map.of(1, 2L));
        byDay.put("2024-01-04", Map.of(3, 9L));
        byDay.put("2024-01-05", Map.of(2, 1L));
        Map<Integer, Object[]> info = new HashMap<>();
        info.put(1, new Object[]{"Selena", 1});
        info.put(2, new Object[]{"Bad Bunny", 2});
        info.put(3, new Object[]{"Grupo", 2});

        var result = callReplay(1, byDay, info);
        assertThat(result.rank()).isEqualTo(2);
        assertThat(result.moves()).hasSize(2);
        var fall = result.moves().get(0);
        assertThat(fall.up()).isFalse();
        assertThat(fall.otherName()).isEqualTo("Grupo");
        assertThat(fall.date()).isEqualTo("04/01/2024");
        assertThat(fall.from()).isEqualTo(1);
        assertThat(fall.to()).isEqualTo(2);
        assertThat(fall.otherGender()).isEqualTo("male");
        var pass = result.moves().get(1);
        assertThat(pass.up()).isTrue();
        assertThat(pass.otherName()).isEqualTo("Bad Bunny");
        assertThat(pass.from()).isEqualTo(2);
        assertThat(pass.to()).isEqualTo(1);
        // Bad Bunny tying again on 05/01 keeps Selena ahead, so no extra move
        assertThat(result.ups()).isEqualTo(1);
        assertThat(result.downs()).isEqualTo(1);
    }

    @Test
    void rankMovesQueryWorksForEveryCatalog() {
        // Selena ties Bad Bunny on her first day (no move on a debut), then passes him
        play("2024-01-01 10:00", 200, "vatito");
        play("2024-01-02 10:00", 100, "vatito");
        play("2024-01-03 11:00", 100, "vatito");
        assertThat(service.rankMoves(Kind.ARTIST, 1).moves()).extracting(DetailPlaysService.RankMove::otherName).containsExactly("Bad Bunny");
        assertThat(service.rankMoves(Kind.ALBUM, 10).rank()).isEqualTo(1);
        assertThat(service.rankMoves(Kind.SONG, 200).moves().getFirst().up()).isFalse();
        assertThat(service.rankMoves(Kind.SONG, 101).rank()).isNull();
    }

    @Test
    void fragmentRendersYearsLatestPlaysAndChartData() throws Exception {
        play("2023-03-13 08:15", 100, "vatito");
        play("2024-01-05 09:00", 101, "robertlover");
        var tab = service.build(service.artistScope(1, List.of(), true, false), Map.of(2024, new Integer[]{3, 120}));
        String fragment = Files.readString(Path.of("src/main/resources/templates/fragments/detail-plays-tab.html"));
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(new StringTemplateResolver());
        engine.setLinkBuilder(new StandardLinkBuilder() {
            @Override
            protected String computeContextPath(IExpressionContext context, String base, Map<String, Object> parameters) {
                return "";
            }
        });
        var context = new Context(Locale.ENGLISH, Map.of("tab", tab, "kind", "artist", "genderLabel", "Female",
                "rankMovesUrl", "/artists/1/rank-moves"));
        var document = Jsoup.parse(engine.process(fragment, context));
        assertThat(document.select(".dp").attr("data-rank-moves-url")).isEqualTo("/artists/1/rank-moves");
        assertThat(document.select(".dp-lr[data-dp-year]")).hasSize(2);
        assertThat(document.select(".dp-lr-head").text()).contains("Female rk", "Top song");
        assertThat(document.select(".dp-lr[data-dp-year=2024] .dt-rank").eachText()).containsExactly("#120", "#3");
        assertThat(document.select(".dp-lr[data-dp-year=2024] .dt-rank.is-far")).hasSize(1);
        // Every year starts closed
        assertThat(document.select(".dp-panel[data-dp-panel]")).hasSize(2).allMatch(panel -> panel.hasAttr("hidden"));
        assertThat(document.select(".dp-lr.is-open")).isEmpty();
        // Year pickers are drawn by the script into these slots, not chip rows
        assertThat(document.select("[data-dp-cal-years], [data-dp-rh-scopes]")).hasSize(2);
        assertThat(document.select(".dp-chips")).isEmpty();
        assertThat(document.select(".dp-lp")).hasSize(2);
        assertThat(document.select(".dp-lp a").first().attr("href")).isEqualTo("/songs/101");
        assertThat(document.select("script").html()).contains("window.detailPlaysData", "\"days\"", "2023-03-13");
    }

    @Test
    void playCoversUseTheAlbumThenFallBackToTheSongCover() throws Exception {
        play("2024-01-05 09:00", 300, "vatito");
        play("2024-01-06 09:00", 100, "vatito");
        var tab = service.build(service.artistScope(1, List.of(), true, true), Map.of());
        String fragment = Files.readString(Path.of("src/main/resources/templates/fragments/detail-plays-tab.html"));
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(new StringTemplateResolver());
        engine.setLinkBuilder(new StandardLinkBuilder() {
            @Override
            protected String computeContextPath(IExpressionContext context, String base, Map<String, Object> parameters) {
                return "";
            }
        });
        var context = new Context(Locale.ENGLISH, Map.of("tab", tab, "kind", "artist", "genderLabel", "Female",
                "rankMovesUrl", "/artists/1/rank-moves"));
        var latest = Jsoup.parse(engine.process(fragment, context)).select(".dp-lp .dp-cov img");
        assertThat(latest).hasSize(2);
        // Song on an album: album cover, song cover if the album has no art
        assertThat(latest.get(0).attr("src")).isEqualTo("/albums/10/image?thumbnail=true");
        assertThat(latest.get(0).attr("data-fallback")).isEqualTo("/songs/100/image?thumbnail=true");
        // Song without an album: straight to the song cover, nothing left to fall back to
        assertThat(latest.get(1).attr("src")).isEqualTo("/songs/300/image?thumbnail=true");
        assertThat(latest.get(1).hasAttr("data-fallback")).isFalse();

        var noAlbum = playDto("2024-01-05 09:00", 300, "Collab", "vatito");
        noAlbum.setAlbumId(null);
        var history = renderHistory(List.of(playDto("2024-01-06 09:00", 100, "Como La Flor", "vatito"), noAlbum),
                2, 0, 1, "artist", true).select(".dp-hist .dp-cov img");
        assertThat(history).extracting(img -> img.attr("src"))
                .containsExactly("/albums/10/image?thumbnail=true", "/songs/300/image?thumbnail=true");
        assertThat(history).extracting(img -> img.attr("data-fallback")).containsExactly("/songs/100/image?thumbnail=true", "");
    }

    @Test
    void fullHistoryRendersPagesWithDisplayDatesAndAccounts() throws Exception {
        var older = playDto("2024-01-05 09:00", 101, "Bidi Bidi", "robertlover");
        var sameDay = playDto("2024-01-05 08:30", 100, "Como La Flor", "vatito");
        sameDay.setFromFeatured(true);
        sameDay.setArtistName("Grupo");
        var document = renderHistory(List.of(older, sameDay), 2500, 7, 25, "artist", true);
        var section = document.selectFirst("#dpFullHistory");
        assertThat(section.hasAttr("hidden")).isFalse();
        assertThat(section.select(".dp-hr:not(.dp-hr-head)")).hasSize(2);
        assertThat(section.select(".dp-lp-d").eachText()).containsExactly("05/01/2024", "05/01/2024");
        assertThat(section.select(".dp-lp-d.is-same")).hasSize(1);
        assertThat(section.select(".dp-lp-t").eachText()).containsExactly("09:00", "08:30");
        assertThat(section.select(".dp-acc").eachText()).containsExactly("Legacy", "Primary");
        assertThat(section.select(".dp-lp-tx em").text()).contains("Feat. on Grupo");
        // Pages around the current one, the first and last, and the arrows
        assertThat(section.select(".dp-pg.is-on").text()).isEqualTo("8");
        assertThat(section.select(".dp-pg").eachText()).contains("1", "6", "7", "8", "9", "10", "25");
        assertThat(section.select(".dp-pg.is-on").attr("href")).isEqualTo("/artists/1?tab=plays&playsPage=7#dpFullHistory");

        var closedSong = renderHistory(List.of(older), 1, 0, 1, "song", false).selectFirst("#dpFullHistory");
        assertThat(closedSong.hasAttr("hidden")).isTrue();
        assertThat(closedSong.select(".dp-hist.is-song .dp-lp-nm")).isEmpty();
        assertThat(closedSong.select(".dp-pager")).isEmpty();
    }

    @Test
    void detailPagesUseTheSharedTabAndHighlightTheirTableLinks() throws Exception {
        for (String type : List.of("artists", "albums", "songs")) {
            String template = Files.readString(Path.of("src/main/resources/templates", type, "detail.html"));
            String self = type.substring(0, type.length() - 1);
            assertThat(template).contains("fragments/detail-plays-tab :: tab(${playsTab}")
                    .contains("fragments/detail-plays-history :: history(${plays}")
                    .contains("detail-plays.css(v=3)").contains("detail-plays-tab.js(v=3)")
                    .contains("view='table',highlight=${" + self + ".id}")
                    .doesNotContain("view='table')}")
                    .doesNotContain("playsByYearChart").doesNotContain("chartjs-plugin-annotation")
                    .doesNotContain("class=\"top-table\" id=\"playsTable\"");
            // Thymeleaf refuses request parameters inside fragment includes, so the page reads them in a th:with first
            assertThat(template.lines().filter(line -> line.contains("th:replace") || line.contains("th:insert")))
                    .noneMatch(line -> line.contains("param."));
        }
        String graphs = Files.readString(Path.of("src/main/resources/static/js/graphs.js"));
        assertThat(graphs).contains("data-id=\"${artist.id}\"", "data-id=\"${album.id}\"", "data-id=\"${song.id}\"")
                .contains("revealHighlightedRow(type)").contains("params.delete('highlight')");
        assertThat(Files.readString(Path.of("src/main/resources/static/css/catalog-table.css"))).contains("@keyframes ct-row-glow");
    }

    private org.jsoup.nodes.Document renderHistory(List<library.dto.PlayDTO> plays, long total, int page, int pages,
                                                   String kind, boolean open) throws Exception {
        String fragment = Files.readString(Path.of("src/main/resources/templates/fragments/detail-plays-history.html"));
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(new StringTemplateResolver());
        engine.setLinkBuilder(new StandardLinkBuilder() {
            @Override
            protected String computeContextPath(IExpressionContext context, String base, Map<String, Object> parameters) {
                return "";
            }
        });
        var context = new Context(Locale.ENGLISH, Map.of("plays", plays, "total", total, "page", page, "pages", pages,
                "pageUrl", "/artists/1?tab=plays&playsPage=", "kind", kind, "open", open));
        return Jsoup.parse(engine.process(fragment, context));
    }

    private static library.dto.PlayDTO playDto(String date, int songId, String song, String account) {
        var dto = new library.dto.PlayDTO();
        dto.setPlayDate(date);
        dto.setSongId(songId);
        dto.setSongName(song);
        dto.setAccount(account);
        dto.setAlbumId(10);
        dto.setAlbumName("Amor Prohibido");
        return dto;
    }

    private DetailPlaysService.RankMoves callReplay(int id, TreeMap<String, Map<Integer, Long>> byDay, Map<Integer, Object[]> info) {
        try {
            Method replay = DetailPlaysService.class.getDeclaredMethod("replay", int.class, TreeMap.class, Map.class);
            replay.setAccessible(true);
            return (DetailPlaysService.RankMoves) replay.invoke(null, id, byDay, info);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private void play(String date, int songId, String account) {
        jdbc.update("INSERT INTO Play (play_date, song_id, account) VALUES (?, ?, ?)", date, songId, account);
    }
}
