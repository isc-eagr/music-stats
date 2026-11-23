package library;

import library.dto.ReignPageDTO;
import library.dto.ReignPageDTO.BoardRow;
import library.dto.TopPlayedSnapshotDTO;
import library.dto.TopPlayedSnapshotItemDTO;
import library.service.ReignPageBuilder;
import library.service.ReignPageBuilder.GenreShare;
import library.service.TopPlayedTimelineService;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReignPageTest {

    private static final LocalDate TODAY = LocalDate.of(2024, 1, 31);
    private static final java.time.format.DateTimeFormatter DMY = java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy");

    /** A (male) holds #1, B (female) takes it for 10 days, A takes it back; C (mixed) joins at #3. */
    private static List<TopPlayedSnapshotDTO> timeline() {
        return List.of(
                snap(1, "01/01/2024", "11/01/2024", 10, false, item(1, "A", 2, 1, 10, 20, "NEW"), item(2, "B", 1, 2, 5, 18, "NEW")),
                snap(2, "11/01/2024", "21/01/2024", 10, false, item(2, "B", 1, 1, 21, 30, "CLIMBED"), item(1, "A", 2, 2, 20, 29, "DROPPED"), item(3, "C", 3, 3, 2, 5, "NEW")),
                snap(3, "21/01/2024", "Present", 10, true, item(1, "A", 2, 1, 31, 50, "CLIMBED"), item(2, "B", 1, 2, 30, 40, "DROPPED"), item(3, "C", 3, 3, 6, 10, null)));
    }

    @Test
    void reignsHeroAndTop3HistoryFollowTheSnapshots() {
        ReignPageDTO page = ReignPageBuilder.build("artist", "Artist", timeline(), Map.of(), TODAY);

        assertThat(page.theme()).isEqualTo("gender-male");
        assertThat(page.stats().crownChanges()).isEqualTo(2);
        assertThat(page.stats().holders()).isEqualTo(2);
        assertThat(page.stats().entrants()).isEqualTo(3);
        assertThat(page.stats().maleAt1Pct()).isEqualTo(67);

        var current = page.current();
        assertThat(current.first().name()).isEqualTo("A");
        assertThat(current.reignOrdinal()).isEqualTo("2nd reign");
        assertThat(current.reignDays()).isEqualTo(10);
        assertThat(current.daysAt1Total()).isEqualTo(20);
        assertThat(current.gapToSecond()).isEqualTo(10);
        assertThat(current.gapToThird()).isEqualTo(30);

        assertThat(page.board()).extracting(BoardRow::name).containsExactly("A", "B", "C");
        BoardRow a = page.board().getFirst();
        assertThat(List.of(a.daysAt1(), a.daysAt2(), a.daysAt3(), a.reignsAt1(), a.longestAt1())).containsExactly(20, 10, 0, 2, 10);
        BoardRow c = page.board().getLast();
        assertThat(c.tone()).isEqualTo("other");
        assertThat(c.best()).isEqualTo(3);
        assertThat(c.inTop3Now()).isTrue();
        assertThat(c.last()).isEqualTo("Present");
        assertThat(c.lastSort()).isEqualTo("31/01/2024");

        // #1 lane changes hands twice, #3 lane starts with the second snapshot
        assertThat(page.lanes().getFirst().timeSegments()).extracting(ReignPageDTO.Segment::name).containsExactly("A", "B", "A");
        assertThat(page.lanes().get(2).timeSegments()).hasSize(1);
        assertThat(page.lanes().get(2).timeSegments().getFirst().left()).isGreaterThan(30);
        // Every change: three lineups, a third of the lane each
        assertThat(page.lanes().getFirst().changeSegments()).extracting(ReignPageDTO.Segment::left).containsExactly(0.0, 33.33, 66.67);

        assertThat(page.snapshots().getFirst().current()).isTrue();
        assertThat(page.snapshots().getFirst().slots().getFirst().movement()).isEqualTo("climbed");
    }

    @Test
    void quickSwapsFoldIntoOneBlockOnRealTimeButNotOnEveryChange() {
        // C holds #3 for 1000 days, D and C swap it three times in three days, then C holds it again for 1000 days
        LocalDate start = LocalDate.of(2020, 1, 1);
        LocalDate today = start.plusDays(2003);
        String[] thirds = {"C", "D", "C", "D", "C"};
        int[] days = {1000, 1, 1, 1, 1000};
        List<TopPlayedSnapshotDTO> snaps = new ArrayList<>();
        LocalDate at = start;
        for (int i = 0; i < thirds.length; i++) {
            LocalDate end = at.plusDays(days[i]);
            boolean last = i == thirds.length - 1;
            int thirdId = thirds[i].equals("C") ? 3 : 4;
            snaps.add(snap(i + 1, at.format(DMY), last ? "Present" : end.format(DMY), days[i], last,
                    item(1, "A", 2, 1, 100, 100, null), item(2, "B", 1, 2, 90, 90, null), item(thirdId, thirds[i], 2, 3, 10, 10, null)));
            at = end;
        }
        ReignPageDTO page = ReignPageBuilder.build("artist", "Artist", snaps, Map.of(), today);

        List<ReignPageDTO.Segment> real = page.lanes().get(2).timeSegments();
        assertThat(real).extracting(ReignPageDTO.Segment::shuffle).containsExactly(false, true, false);
        ReignPageDTO.Segment swaps = real.get(1);
        assertThat(swaps.count()).isEqualTo(3);
        assertThat(swaps.names()).isEqualTo("D, C");
        assertThat(swaps.tone()).isEqualTo("other");
        // the widened swaps block never overlaps the block after it
        assertThat(swaps.left() + swaps.width()).isLessThanOrEqualTo(real.get(2).left() + 0.001);

        assertThat(page.lanes().get(2).changeSegments()).extracting(ReignPageDTO.Segment::name).containsExactly("C", "D", "C", "D", "C");
        assertThat(page.lanes().get(2).changeSegments()).noneMatch(ReignPageDTO.Segment::shuffle);
    }

    @Test
    void genresTakeTheToneOfTheGenderWithMorePlays() {
        Map<Integer, GenreShare> shares = Map.of(1, new GenreShare(10, 80, 100), 2, new GenreShare(90, 5, 100));
        ReignPageDTO page = ReignPageBuilder.build("genre", "Genre", timeline(), shares, TODAY);

        BoardRow first = page.board().getFirst();
        assertThat(first.tone()).isEqualTo("female");
        assertThat(first.malePct()).isEqualTo(10);
        assertThat(page.board().get(1).tone()).isEqualTo("male");
        assertThat(page.board().get(2).tone()).isEqualTo("other");
        assertThat(page.board().get(2).malePct()).isNull();
        // #1 days weighted by plays split: 20 days at 10% male + 10 days at 90% male = 11 of 30 days
        assertThat(page.genderSplits().getFirst().maleDays()).isEqualTo(11);
    }

    @Test
    void emptyTimelineRendersNoHero() {
        ReignPageDTO page = ReignPageBuilder.build("song", "Song", List.of(), Map.of(), TODAY);
        assertThat(page.current()).isNull();
        assertThat(page.board()).isEmpty();
    }

    @Test
    void genreSharesCoverEveryGenreInTheTop3() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            TopPlayedTimelineService service = new TopPlayedTimelineService(db.jdbcTemplate);
            Map<Integer, GenreShare> shares = service.getGenreGenderShares();
            for (TopPlayedSnapshotItemDTO item : service.getGenreTimeline().getLast().getItems()) {
                GenreShare share = shares.get(item.getItemId());
                assertThat(share).as(item.getItemName()).isNotNull();
                assertThat(share.total()).isGreaterThanOrEqualTo(item.getPlaysCount());
                assertThat(share.male() + share.female()).isLessThanOrEqualTo(share.total());
            }
        }
    }

    @Test
    void pageRendersHeroLanesAndSortableBoard() {
        ReignPageDTO page = ReignPageBuilder.build("artist", "Artist", timeline(), Map.of(), TODAY);
        Document doc = DetailTemplateSupport.render("fragments/reigns-view", "div.rg-page", Map.of("r", page));

        assertThat(doc.select(".rg-hero .rg-name").text()).isEqualTo("A");
        assertThat(doc.select(".rg-hero .rg-name a").attr("href")).isEqualTo("/artists/1");
        assertThat(doc.select(".rg-runner")).hasSize(2);
        // Both layouts render, Every change starts hidden
        assertThat(doc.select("[data-lanes=time] .rg-lane")).hasSize(3);
        assertThat(doc.select("[data-lanes=changes] .rg-lane")).hasSize(3);
        assertThat(doc.select("[data-lanes=changes]").hasAttr("hidden")).isTrue();
        assertThat(doc.select("[data-lanes=time]").hasAttr("hidden")).isFalse();
        assertThat(doc.select("[data-lanes-mode]")).extracting(e -> e.text()).containsExactly("Real time", "Every change");
        assertThat(doc.select(".rg-snaps tbody tr")).hasSize(3);
        assertThat(doc.select("#rgBoard tr[data-sort-item]")).hasSize(3);
        assertThat(doc.select("#rgBoard [data-sort-key]")).extracting(e -> e.attr("data-sort-key"))
                .containsExactly("name", "best", "d1", "d2", "d3", "top3", "reigns", "longest", "first", "last");
        assertThat(doc.select(".rg-tabs a.is-on").text()).isEqualTo("Artist");

        // Trophy photo of the #1, thumbnails of #2 and #3, the reign length on the ribbon
        assertThat(doc.select(".rg-trophy img").attr("src")).isEqualTo("/artists/1/image");
        assertThat(doc.select(".rg-trophy-ribbon").text()).isEqualTo("10 days on top");
        assertThat(doc.select(".rg-thumb img")).extracting(e -> e.attr("src"))
                .containsExactly("/artists/2/image?thumbnail=true", "/artists/3/image?thumbnail=true");
        assertThat(doc.select(".rg-crown")).isEmpty();

        // Scoreboard instead of chips, and no breadcrumb: the switch says which page this is
        assertThat(doc.select(".rg-score dt")).extracting(e -> e.text())
                .containsExactly("Plays", "Lead over #2", "Current reign", "Total at #1", "Reigns at #1");
        assertThat(doc.select(".rg-score dd")).extracting(e -> e.text())
                .containsExactly("50", "+10", "10d", "20d", "2");
        assertThat(doc.select(".cd-crumb")).isEmpty();
        assertThat(doc.select(".rg-tabs a.is-on").attr("aria-current")).isEqualTo("page");
        assertThat(doc.select(".rg-tabs a")).extracting(e -> e.attr("href")).containsExactly(
                "/playground?view=reigns&reign=artists", "/playground?view=reigns&reign=songs", "/playground?view=reigns&reign=genres");
    }

    @Test
    void reignsLiveInPlaygroundAndOldLinksRedirect() throws Exception {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            var controller = new library.controller.PlaygroundController(
                    new library.service.PlaygroundService(db.jdbcTemplate), new TopPlayedTimelineService(db.jdbcTemplate));

            var model = new org.springframework.ui.ExtendedModelMap();
            assertThat(controller.reigns("songs", model)).isEqualTo("playground");
            assertThat(model.get("view")).isEqualTo("reigns");
            assertThat(((ReignPageDTO) model.get("r")).type()).isEqualTo("song");

            var genres = new org.springframework.ui.ExtendedModelMap();
            controller.reigns("genres", genres);
            assertThat(((ReignPageDTO) genres.get("r")).current().first().name()).isEqualTo("Pop");

            assertThat(controller.oldReignPage("artists")).isEqualTo("redirect:/playground?view=reigns&reign=artists");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> controller.oldReignPage("albums"))
                    .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> controller.reigns("albums", new org.springframework.ui.ExtendedModelMap()))
                    .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        }
    }

    @Test
    void playgroundShowsTheReignsTabAndTheMainMenuNoLongerDoes() throws Exception {
        ReignPageDTO page = ReignPageBuilder.build("artist", "Artist", timeline(), Map.of(), TODAY);
        Document doc = DetailTemplateSupport.render("playground", ".playground-page",
                Map.of("view", "reigns", "r", page, "currentSection", "playground"));

        assertThat(doc.select(".playground-tabs a.active").text()).isEqualTo("Reigns");
        assertThat(doc.select("#reigns-panel .rg-page .rg-name").text()).isEqualTo("A");

        String nav = java.nio.file.Files.readString(java.nio.file.Path.of("src/main/resources/templates/fragments/navigation.html"));
        assertThat(nav).doesNotContain("/reign/").doesNotContain(">Reigns<");
    }

    @Test
    void genrePageKeepsTheBadgeInsteadOfAPhoto() {
        ReignPageDTO page = ReignPageBuilder.build("genre", "Genre", timeline(), Map.of(), TODAY);
        Document doc = DetailTemplateSupport.render("fragments/reigns-view", "div.rg-page", Map.of("r", page));

        assertThat(doc.select(".rg-trophy")).isEmpty();
        assertThat(doc.select(".rg-thumb")).isEmpty();
        assertThat(doc.select(".rg-crown").text()).isEqualTo("#1");
        assertThat(doc.select(".rg-hero .rg-name a").attr("href")).isEqualTo("/genres/1");
    }

    private static TopPlayedSnapshotDTO snap(int rank, String start, String end, int days, boolean current, TopPlayedSnapshotItemDTO... items) {
        TopPlayedSnapshotDTO s = new TopPlayedSnapshotDTO();
        s.setRank(rank);
        s.setStartDate(start);
        s.setEndDate(end);
        s.setDaysInConfig(days);
        s.setCurrent(current);
        s.setItems(new ArrayList<>(List.of(items)));
        return s;
    }

    private static TopPlayedSnapshotItemDTO item(int id, String name, int genderId, int position, int playsAtStart, int playsNow, String movement) {
        TopPlayedSnapshotItemDTO it = new TopPlayedSnapshotItemDTO();
        it.setItemId(id);
        it.setItemName(name);
        it.setGenderId(genderId);
        it.setPosition(position);
        it.setPlaysAtSnapshotStart(playsAtStart);
        it.setPlaysCount(playsNow);
        it.setMovement(movement);
        return it;
    }
}
