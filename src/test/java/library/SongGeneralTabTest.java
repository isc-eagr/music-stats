package library;

import library.dto.ConsistencyPeriodDTO;
import library.dto.FeaturedArtistDTO;
import library.dto.LinkedSongDTO;
import library.dto.WeeklyChartStatsDTO;
import library.entity.Song;
import library.service.SongService;
import library.util.ArtistGeneralSummaries.PlaysShareSlice;
import library.util.SongGeneralSummaries;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Song detail General tab: profile sources, #1 weeks, linked version split, and the rendered sidebar / stats card. */
class SongGeneralTabTest {

    @Test
    void attributeSourcesTellOverridesFromAlbumAndArtistValues() {
        Song song = song();
        Map<String, String> sources = SongGeneralSummaries.attributeSources(song);

        assertThat(sources)
                .containsEntry("genre", SongGeneralSummaries.OVERRIDE)
                .containsEntry("language", SongGeneralSummaries.ALBUM)
                .containsEntry("gender", SongGeneralSummaries.ARTIST)
                .doesNotContainKeys("subgenre", "ethnicity");

        // Without the song override the genre falls back to the artist, never to the missing album value
        song.setOverrideGenreId(null);
        assertThat(SongGeneralSummaries.attributeSources(song)).containsEntry("genre", SongGeneralSummaries.ARTIST);
        song.setOverrideGenderId(2);
        assertThat(SongGeneralSummaries.attributeSources(song)).containsEntry("gender", SongGeneralSummaries.OVERRIDE);
        assertThat(SongGeneralSummaries.attributeSources(null)).isEmpty();
    }

    @Test
    void releaseDateIsAnOverrideOnlyWhenTheAlbumHasADateToo() {
        assertThat(SongGeneralSummaries.releaseDateSource(true, true)).isEqualTo(SongGeneralSummaries.OVERRIDE);
        assertThat(SongGeneralSummaries.releaseDateSource(true, false)).isNull();
        assertThat(SongGeneralSummaries.releaseDateSource(false, true)).isEqualTo(SongGeneralSummaries.ALBUM);
        assertThat(SongGeneralSummaries.releaseDateSource(false, false)).isNull();
    }

    @Test
    void linkedPlaysShareSplitsPlaysAcrossVersionsWithPlays() {
        List<LinkedSongDTO> linked = List.of(
                linked(7, "Ojitos Lindos", "Un Verano Sin Ti", 30),
                linked(8, "Ojitos Lindos (Remix)", null, 90),
                linked(9, "Ojitos Lindos (Live)", "Live", 0));

        List<PlaysShareSlice> slices = SongGeneralSummaries.linkedPlaysShare(linked, 7);

        assertThat(slices).extracting(PlaysShareSlice::name)
                .containsExactly("Ojitos Lindos (Remix)", "Ojitos Lindos · Un Verano Sin Ti");
        assertThat(slices).extracting(PlaysShareSlice::kind).containsExactly("linked", "current");
        assertThat(slices).extracting(PlaysShareSlice::percent).containsExactly(75.0, 25.0);
        assertThat(slices.get(0).shade()).isGreaterThan(slices.get(1).shade());

        // A single version with plays is not a split
        assertThat(SongGeneralSummaries.linkedPlaysShare(List.of(linked.get(0), linked.get(2)), 7)).isEmpty();
        assertThat(SongGeneralSummaries.linkedPlaysShare(null, 7)).isEmpty();
    }

    @Test
    void songCountCountsTheWholeCatalog() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            SongService service = new SongService(db.songRepository, null, null, db.jdbcTemplate, null, null, null);
            Integer expected = db.jdbcTemplate.queryForObject("SELECT COUNT(*) FROM Song", Integer.class);
            assertThat(service.getSongCount()).isEqualTo(expected).isPositive();
        }
    }

    @Test
    void genderRankUsesTheSongsGenderOverride() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            // Ojitos Lindos is by a gender-1 artist but overridden to gender 2, so it ranks among gender-2 songs:
            // behind Bidi Bidi (3 plays) and Standalone Jam (2 plays, first played earlier), ahead of No Me Queda Mas (1)
            db.jdbcTemplate.update("INSERT INTO Play (artist, album, song, play_date, song_id, account) VALUES "
                    + "('Bad Bunny', 'Un Verano Sin Ti', 'Ojitos Lindos', '2030-01-01 10:00:00', 4, 'vatito'), "
                    + "('Bad Bunny', 'Un Verano Sin Ti', 'Ojitos Lindos', '2030-01-02 10:00:00', 4, 'vatito')");
            SongService service = new SongService(db.songRepository, null, null, db.jdbcTemplate, null, null, null);
            assertThat(service.getAllSongRankings(4)).containsEntry("gender", 3);
            // Without an override the artist's gender still applies
            assertThat(service.getAllSongRankings(3)).containsEntry("gender", 1);
        }
    }

    @Test
    void profileShowsRanksWithValueSourcesAndTheOverflowMenu() {
        Document doc = render(".ad-profile-view", baseModel());
        Element profile = doc.selectFirst(".ad-profile-view");
        assertThat(profile).isNotNull();

        // The overall standing moved to the General box header
        assertThat(profile.select(".ad-standing")).isEmpty();

        Element genre = row(profile, "Genre");
        assertThat(genre.select(".ad-ovr").text()).isEqualTo("Override");
        assertThat(genre.select(".ad-rank.is-gold").text()).isEqualTo("#1");
        assertThat(genre.attr("href")).isEqualTo("/songs?genre=5&genreMode=includes&view=table&highlight=7");
        assertThat(genre.attr("title")).isEqualTo("Genre: #1 among Reggaeton songs");

        Element language = row(profile, "Language");
        assertThat(language.select(".ad-from").text()).isEqualTo("from album");
        assertThat(language.select(".ad-ovr")).isEmpty();

        Element gender = row(profile, "Gender");
        assertThat(gender.select(".ad-from").text()).isEqualTo("from artist");
        assertThat(gender.select(".ad-dot")).hasSize(1);
        assertThat(gender.select(".ad-rank").text()).isEqualTo("#3");

        assertThat(row(profile, "Subgenre").select(".ad-row-v.is-empty").text()).isEqualTo("Not set");
        assertThat(row(profile, "Artist").attr("href")).isEqualTo("/artists/2");
        assertThat(row(profile, "Album").attr("href")).isEqualTo("/albums/2");
        assertThat(row(profile, "Released in").select(".ad-rank").text()).isEqualTo("#5");

        assertThat(profile.select(".ad-facts .ad-fact").eachText()).containsExactly("Single", "Not organized");
        assertThat(profile.select(".ad-tags a.ad-tag").eachAttr("href")).containsExactly("/artists/4");
        assertThat(profile.select(".ad-tags span.ad-tag").eachText()).containsExactly("Perreo");

        Element delete = profile.selectFirst("#deleteBtn");
        assertThat(delete.hasAttr("disabled")).isTrue();
        assertThat(delete.text()).contains("Locked while the song has 150 plays");
        assertThat(profile.select("#deletePlaysBtn")).hasSize(1);
        assertThat(profile.select("#btnViewSamples")).hasSize(1);
        assertThat(profile.select(".winning-chip")).isEmpty();
    }

    @Test
    void sidebarShowsArtistAlbumLinksAndLengthWhileThePlaysLeadTheHeader() {
        Document doc = render(".sd-photo-sub", baseModel());
        assertThat(doc.select(".sd-photo-sub a").eachAttr("href")).containsExactly("/artists/2", "/albums/2");

        // Length sits on the cover; the old quick-fact cells are gone
        assertThat(render(".ad-photo-len", baseModel()).select(".ad-photo-len").text()).isEqualTo("4:18");
        assertThat(render(".ad-side-cells", baseModel()).select(".ad-side-cells, .ad-cells")).isEmpty();

        // Plays sit in the General box header: total, split bar, legend and the overall standing badge
        Element plays = render(".ad-hero", baseModel()).selectFirst(".ad-hero .ad-plays.ad-hero-plays");
        assertThat(plays.select(".ad-plays-total").text()).isEqualTo("150");
        assertThat(plays.select(".ad-bar > span").eachAttr("style")).containsExactly("flex: 120 1 0", "flex: 30 1 0");
        assertThat(plays.select(".ad-legend > span").eachText()).containsExactly("Primary 120 80%", "Legacy 30 20%");
        assertThat(plays.select(".ad-plays-top > a.ad-standing .ad-standing-n").text()).isEqualTo("#12");
        assertThat(plays.select(".ad-plays-foot > .ad-plays-time").text()).isEqualTo("10m 45s listened");
    }

    @Test
    void legacyFiguresDropOutWhenTheSongCannotHaveLegacyPlays() {
        Map<String, Object> model = baseModel();
        model.put("songPlayCount", 120);
        model.put("songRobertloverPlayCount", 0);
        model.put("showLegacy", false);

        Element plays = render(".ad-plays", model).selectFirst(".ad-plays");
        // Total alone tells the story: no Primary / Legacy legend, no Primary or Legacy tile
        assertThat(plays.select(".ad-plays-total").text()).isEqualTo("120");
        assertThat(plays.select(".ad-legend")).isEmpty();
        Element stats = render("#generalSectionContent", model).selectFirst("section.ad-stats");
        assertThat(stats.select(".ad-tile > span").eachText()).doesNotContain("Legacy", "Primary");
    }

    @Test
    void listsDropLegacyWhenTheListenedFilterStartsAfter2009() {
        assertThat(library.util.LegacyPlays.worthShowingFrom("2016-01-01")).isFalse();
        assertThat(library.util.LegacyPlays.worthShowingFrom("01/03/2010")).isFalse();
        assertThat(library.util.LegacyPlays.worthShowingFrom("2009-06-01")).isTrue();
        assertThat(library.util.LegacyPlays.worthShowingFrom(null)).isTrue();
        assertThat(library.util.LegacyPlays.worthShowingFrom("garbage")).isTrue();
    }

    @Test
    void legacyPlaysAreShownUnlessTheContextStartsAfterTheLegacyAccountStopped() {
        assertThat(library.util.LegacyPlays.worthShowing(0, java.time.LocalDate.of(2009, 12, 31))).isTrue();
        assertThat(library.util.LegacyPlays.worthShowing(0, java.time.LocalDate.of(2010, 1, 1))).isFalse();
        // Any known start after 2009 is enough (release date or first play)
        assertThat(library.util.LegacyPlays.worthShowing(0, java.time.LocalDate.of(2004, 1, 1),
                java.time.LocalDate.of(2015, 3, 1))).isFalse();
        // Legacy plays that exist are never hidden, and unknown dates keep the figures
        assertThat(library.util.LegacyPlays.worthShowing(4, java.time.LocalDate.of(2015, 3, 1))).isTrue();
        assertThat(library.util.LegacyPlays.worthShowing(0, (java.time.LocalDate) null)).isTrue();
        assertThat(library.util.LegacyPlays.worthShowing(null, 2016)).isFalse();
    }

    @Test
    void statsCardCarriesEveryOldStatisticAndTheChartRuns() {
        Document doc = render("#generalSectionContent", baseModel());
        Element stats = doc.selectFirst("#generalSectionContent section.ad-card.ad-stats");
        assertThat(stats).isNotNull();

        // Header: overall standing on the left, the listening span as First / Last / Over rows on the right
        Element hero = stats.child(0);
        assertThat(hero.hasClass("ad-hero")).isTrue();
        Element standing = hero.selectFirst(".ad-hero-plays a.ad-standing");
        assertThat(standing.attr("href")).isEqualTo("/songs?view=table&highlight=7");
        assertThat(standing.attr("title")).isEqualTo("Rank #12 among all songs by total plays");
        assertThat(standing.select(".ad-standing-n").text()).isEqualTo("#12");
        assertThat(standing.select(".ad-standing-k").text()).isEqualTo("Overall");
        assertThat(standing.select(".ad-standing-sub").text()).isEqualTo("of 4,000 songs");
        assertThat(hero.select(".ad-hero-span .ad-run-rows dt").eachText()).containsExactly("First play", "Last play", "Over");
        assertThat(hero.select(".ad-hero-span .ad-run-rows dd").eachText()).containsExactly("01-Jun-2022", "02-Jul-2025", "3 years");
        assertThat(stats.select(".ad-tl")).isEmpty();

        // Only the Consistency ledger: plays, length, release and library facts already show in the sidebar
        assertThat(stats.select(".ad-tile")).isEmpty();
        Element ledger = stats.selectFirst("section.cons");
        assertThat(ledger.hasClass("cons-compact")).isFalse();
        assertThat(ledger.attr("data-cons-url")).isEqualTo("/songs/7/consistency");
        assertThat(ledger.select(".cons-title").text()).isEqualTo("Consistency");
        assertThat(ledger.select("tbody th").eachText()).containsExactly("Days", "Weeks", "Months", "Years");
        assertThat(ledger.select(".cons-count").eachText()).containsExactly("40 / 1,200", "20 / 170", "9 / 40", "3 / 4");
        assertThat(ledger.select(".cons-share").eachText()).containsExactly("3.3%", "12%", "23%", "75%");
        assertThat(ledger.select(".cons-bar > span").first().attr("style")).isEqualTo("width: 3.3%");
        // Ranks and gender ranks wait for the background request
        assertThat(ledger.select("[data-cons-rank]").eachText()).containsOnly("...");
        assertThat(ledger.select("[data-cons-grank]")).hasSize(4);

        assertThat(stats.select(".ad-year").eachAttr("class")).containsExactly(
                "ad-year is-medal is-gold", "ad-year is-medal is-silver", "ad-year is-dim");
        assertThat(stats.select(".ad-year").first().attr("href"))
                .isEqualTo("/songs?listenedDateFrom=2022-01-01&listenedDateTo=2022-12-31&view=table&highlight=7");

        // Weeks at #1 live on the Chart History tab now, not as a General tab crown
        assertThat(stats.select(".ad-crown, .ad-crowns")).isEmpty();

        assertThat(stats.select(".ad-share-row")).hasSize(2);
        assertThat(stats.select(".ad-share-row.is-extra .ad-share-name").text()).isEqualTo("Ojitos Lindos (Remix)");
        assertThat(stats.select(".ad-stats-top .ad-cap").first().text()).isEqualTo("Where the 150 plays come from");

        Element charts = stats.selectFirst(".sd-charts");
        assertThat(charts.select(".ad-row-k").eachText()).containsExactly("Weekly chart", "Billboard Hot 100");
        assertThat(charts.select(".ad-row .ad-rank").eachText()).containsExactly("#1", "#14");
        assertThat(charts.select(".sd-chart-tags a").eachAttr("href")).containsExactly("/charts/seasonal/2022-Summer");
        assertThat(doc.select(".ds-row, .detail-stats-panel")).isEmpty();
    }

    @Test
    void linkedSongsSectionKeepsItsFormAndListsOtherVersions() {
        Document doc = render("#songLinkedSongs", baseModel());
        Element section = doc.getElementById("songLinkedSongs");
        assertThat(section).isNotNull();
        assertThat(section.classNames()).contains("detail-content-section", "general-tab-section", "ad-card", "ad-section");
        assertThat(section.select(".ad-sec-head .ad-count").text()).isEqualTo("1");

        Element form = section.selectFirst("form.linked-song-form");
        assertThat(form.attr("action")).isEqualTo("/songs/7/linked-songs");
        assertThat(form.select("input[name=linkedSongIds]").eachAttr("value")).containsExactly("8");
        assertThat(form.select(".linked-song-row .linked-song-plays b.ad-gold").text()).isEqualTo("120");
        assertThat(form.select(".linked-remove-btn").attr("onclick")).isEqualTo("this.closest('.linked-song-row').remove()");
        assertThat(form.select("#linkedSongList, #linkedSongSearch, #linkedSongResults")).hasSize(3);
        assertThat(form.selectFirst("#linkedSongsSaveButton").hasAttr("disabled")).isTrue();
        assertThat(form.select(".linked-empty")).isEmpty();
    }

    @Test
    void templateTurnsTheProfileIntoFieldBoxesAndDropsTheOldCards() throws IOException {
        String html = Files.readString(Path.of("src/main/resources/templates/songs/detail.html"), StandardCharsets.UTF_8);
        Document doc = Jsoup.parse(html);

        Element container = doc.selectFirst(".container-wide.song-detail.ad-page");
        assertThat(container).isNotNull();
        Element sidebar = container.selectFirst(".detail-grid > aside.detail-sidebar.ad-card#songSidebar");
        assertThat(sidebar).isNotNull();
        Element main = container.selectFirst(".detail-grid > .ad-main");
        assertThat(main.children().stream().map(child -> child.id().isEmpty() ? child.className() : child.id()).toList())
                .containsExactly("detail-tabs", "generalSectionContent", "songLinkedSongs",
                        "playsTabContent", "featuredTabContent", "chartHistoryTabContent");

        Element form = sidebar.selectFirst("form#songForm.ad-profile-edit");
        assertThat(form.attr("method")).isEqualTo("post");
        assertThat(form.attr("th:action")).isEqualTo("@{'/songs/' + ${song.id}}");
        assertThat(form.select(".ad-fb[data-field]").eachAttr("data-field")).containsExactly(
                "name", "albumId", "lengthFormatted", "trackNumber", "releaseDate",
                "overrideGenderId", "overrideEthnicityId", "overrideGenreId", "overrideSubgenreId", "overrideLanguageId",
                "isSingle", "organized", "featuredArtists", "tags");
        for (String name : List.of("name", "albumId", "lengthSeconds", "trackNumber", "releaseDate", "overrideGenderId",
                "overrideEthnicityId", "overrideGenreId", "overrideSubgenreId", "overrideLanguageId", "isSingle", "organized", "artistId")) {
            assertThat(form.select("[name=" + name + "]")).describedAs(name).hasSize(1);
        }
        assertThat(form.select(".ad-switch input[type=checkbox]")).hasSize(2);
        assertThat(form.select("select option[value='']").eachAttr("th:text")).hasSize(5)
                .allSatisfy(text -> assertThat(text).startsWith("'Inherit ("));
        assertThat(form.select(".ad-edit-foot #btnFetchItunes, .ad-edit-foot #btnFetchExternalMeta")).hasSize(2);
        assertThat(form.select(".ad-edit-foot #profileDirtyText")).hasSize(1);

        assertThat(doc.select(".winning-chip, .rank-chip, .chip-group-separator, .detail-stats-panel, .attribute-text, #formActions"))
                .isEmpty();
        assertThat(html)
                .contains("function snapshotProfileValues()", "function updateProfileDirtyState()", "function toggleProfileMenu(event)")
                .contains("dateFormat: 'd/m/Y'")
                .doesNotContain("🎵").doesNotContain("🔹").doesNotContain("⏳");

        List<String> stylesheets = doc.select("link[rel=stylesheet]").eachAttr("th:href");
        // Shared detail styles load last, after the song-only sheet (version numbers are cache busters only)
        assertThat(stylesheets.get(stylesheets.size() - 1)).startsWith("@{/css/detail-general.css");
        assertThat(stylesheets.get(stylesheets.size() - 2)).startsWith("@{/css/song-detail.css");
    }

    private static Element row(Element profile, String label) {
        return profile.select(".ad-row").stream()
                .filter(row -> {
                    Element key = row.selectFirst(".ad-row-k");
                    return key != null && key.ownText().equals(label);
                })
                .findFirst()
                .orElseThrow(() -> new AssertionError("No profile row " + label));
    }

    private static Song song() {
        Song song = new Song();
        song.setId(7);
        song.setName("Ojitos Lindos");
        song.setArtistId(2);
        song.setAlbumId(2);
        song.setOverrideGenreId(5);
        song.setArtistGenreId(6);
        song.setAlbumLanguageId(3);
        song.setArtistGenderId(1);
        song.setIsSingle(true);
        song.setOrganized(false);
        song.setTrackNumber(4);
        song.setLengthSeconds(258);
        song.setReleaseDate(Date.valueOf("2022-05-29"));
        return song;
    }

    private static Map<String, Object> baseModel() {
        Song song = song();
        Map<String, Object> model = new HashMap<>();
        model.put("activeTab", "general");
        model.put("song", song);
        model.put("artistName", "Bad Bunny");
        model.put("albumName", "Un Verano Sin Ti");
        model.put("songPlayCount", 150);
        model.put("songVatitoPlayCount", 120);
        model.put("songRobertloverPlayCount", 30);
        model.put("showLegacy", true);
        model.put("totalListeningTime", "10m 45s");
        model.put("overallPosition", 12);
        model.put("totalSongCount", 4000);
        model.put("rankByArtist", 1);
        model.put("rankByAlbum", 2);
        model.put("rankByGender", 3);
        model.put("rankByGenre", 1);
        model.put("rankByLanguage", 8);
        model.put("rankByReleaseYear", 5);
        model.put("releaseYear", 2022);
        model.put("effectiveGenderName", "Male");
        model.put("effectiveGenreName", "Reggaeton");
        model.put("effectiveLanguageName", "Spanish");
        model.put("valueSources", SongGeneralSummaries.attributeSources(song));
        model.put("featuredArtists", List.of(new FeaturedArtistDTO(4, "Guest Singer")));
        model.put("assignedTags", List.of(Map.of("id", 1, "name", "Perreo")));

        model.put("weeklyChartStats", new WeeklyChartStatsDTO(12, 1, 4));
        model.put("billboardHot100Stats", Map.of("weeksOnChart", 20, "peakPosition", 14, "weeksAtPeak", 2));
        model.put("seasonalChartHistory", List.of(Map.of("displayName", "#3 in Summer 2022", "periodKey", "2022-Summer")));
        model.put("yearlyChartHistory", List.of());
        Map<Integer, Integer> ranksByYear = new LinkedHashMap<>();
        ranksByYear.put(2022, 1);
        ranksByYear.put(2023, 2);
        ranksByYear.put(2024, 150);
        model.put("ranksByYear", ranksByYear);
        model.put("firstListenedDate", "01-Jun-2022");
        model.put("lastListenedDate", "02-Jul-2025");
        model.put("listeningSpan", "3 years");
        model.put("consistencyPeriods", List.of(
                new ConsistencyPeriodDTO("days", "Days", 40, 1200),
                new ConsistencyPeriodDTO("weeks", "Weeks", 20, 170),
                new ConsistencyPeriodDTO("months", "Months", 9, 40),
                new ConsistencyPeriodDTO("years", "Years", 3, 4)));
        model.put("consistencyRankUrl", "/songs/7/consistency");

        List<LinkedSongDTO> linkedSongs = List.of(
                linked(7, "Ojitos Lindos", "Un Verano Sin Ti", 30),
                linked(8, "Ojitos Lindos (Remix)", null, 120));
        model.put("linkedSongs", linkedSongs);
        model.put("linkedPlaysShare", SongGeneralSummaries.linkedPlaysShare(linkedSongs, 7));
        return model;
    }

    private static LinkedSongDTO linked(int id, String name, String albumName, int plays) {
        LinkedSongDTO dto = new LinkedSongDTO();
        dto.setId(id);
        dto.setName(name);
        dto.setArtistName("Bad Bunny");
        dto.setAlbumName(albumName);
        dto.setPlayCount(plays);
        return dto;
    }

    private static Document render(String selector, Map<String, Object> model) {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new MockHttpServletRequest(servletContext), new MockHttpServletResponse());
        return Jsoup.parse(engine.process("songs/detail", Set.of(selector), new WebContext(exchange, Locale.ENGLISH, model)));
    }
}
