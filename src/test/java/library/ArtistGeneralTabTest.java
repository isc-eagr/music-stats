package library;

import library.dto.ArtistAlbumDTO;
import library.dto.ArtistSongDTO;
import library.util.ArtistGeneralSummaries;
import library.util.ArtistGeneralSummaries.NoAlbumSummary;
import library.util.ArtistGeneralSummaries.PlaysShareSlice;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ArtistGeneralTabTest {

    @Test
    void noAlbumSummaryTotalsOnlyOwnSongsWithoutAnAlbum() {
        List<ArtistSongDTO> songs = List.of(
                song(1, 10, 8, 2, "01-Feb-2020", "05-Mar-2024", false),
                song(2, null, 5, 1, "15-Jan-2019", "01-Jan-2023", false),
                song(3, null, 3, 0, "10-Jan-2018", "20-Jun-2025", false),
                song(4, null, 40, 0, "01-Jan-2010", "01-Jan-2026", true));

        NoAlbumSummary summary = ArtistGeneralSummaries.noAlbumSummary(songs);

        assertThat(summary.songCount()).isEqualTo(2);
        assertThat(summary.primaryPlays()).isEqualTo(8);
        assertThat(summary.legacyPlays()).isEqualTo(1);
        assertThat(summary.totalPlays()).isEqualTo(9);
        assertThat(summary.firstListened()).isEqualTo("10-Jan-2018");
        assertThat(summary.lastListened()).isEqualTo("20-Jun-2025");
        assertThat(ArtistGeneralSummaries.noAlbumSummary(List.of(songs.get(0)))).isNull();
    }

    @Test
    void releasesCountOwnAlbumsWithFirstAndLatestRelease() {
        ArtistAlbumDTO debut = album("Afrodisíaco", 10);
        debut.setReleaseDate("13-Nov-2020");
        ArtistAlbumDTO latest = album("Cosa Nuestra", 10);
        latest.setReleaseDate("15-Nov-2024");
        ArtistAlbumDTO undated = album("Undated", 10);
        ArtistAlbumDTO someoneElses = album("Guest spot", 10);
        someoneElses.setReleaseDate("01-Jan-2010");
        someoneElses.setFromCrossArtist(true);

        ArtistGeneralSummaries.ReleasesSummary releases =
                ArtistGeneralSummaries.releases(List.of(latest, undated, debut, someoneElses));
        // The album the artist only appears on is left out, the undated one still counts
        assertThat(releases.count()).isEqualTo(3);
        assertThat(releases.first().getName()).isEqualTo("Afrodisíaco");
        assertThat(releases.latest().getName()).isEqualTo("Cosa Nuestra");
        assertThat(releases.span()).isEqualTo("4 years, 2 days");
        assertThat(ArtistGeneralSummaries.releases(List.of(someoneElses))).isNull();
        assertThat(ArtistGeneralSummaries.releases(List.of(undated)).first()).isNull();
    }

    @Test
    void playsShareListsEveryPlayedAlbumPlusLooseAndFeaturedSongs() {
        List<ArtistAlbumDTO> albums = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
            ArtistAlbumDTO album = album("Album " + i, 100 - i);
            album.setId(i);
            albums.add(album);
        }
        albums.add(album("Never played", 0));
        List<ArtistSongDTO> songs = List.of(
                song(1, null, 50, 0, null, null, false),
                song(2, null, 20, 0, null, null, true));

        List<PlaysShareSlice> slices = ArtistGeneralSummaries.playsShare(albums, songs);

        assertThat(slices).hasSize(14);
        assertThat(slices).extracting(PlaysShareSlice::name)
                .startsWith("Album 1")
                .contains("Album 12", "No album songs", "Featured on other artists")
                .doesNotContain("Never played")
                .noneMatch(name -> name.contains("other albums"));
        assertThat(slices).extracting(PlaysShareSlice::plays)
                .isSortedAccordingTo((a, b) -> Integer.compare(b, a));
        assertThat(slices.stream().mapToDouble(PlaysShareSlice::percent).sum()).isCloseTo(100.0,
                org.assertj.core.data.Offset.offset(0.001));
        assertThat(slices.get(0).shade()).isGreaterThan(slices.get(slices.size() - 1).shade());
        // Album slices carry the album id so the legend links to the album; the other slices stay plain
        assertThat(slices.get(0).id()).isEqualTo(1);
        assertThat(slices).filteredOn(slice -> !"album".equals(slice.kind()))
                .extracting(PlaysShareSlice::id).containsOnlyNulls();
        assertThat(ArtistGeneralSummaries.playsShare(List.of(), List.of())).isEmpty();
    }

    @Test
    void generalTabFragmentsRenderRanksAndSortControls() {
        var resolver = new org.thymeleaf.templateresolver.ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new org.thymeleaf.spring6.SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new org.springframework.mock.web.MockServletContext();
        var exchange = org.thymeleaf.web.servlet.JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new org.springframework.mock.web.MockHttpServletRequest(servletContext),
                        new org.springframework.mock.web.MockHttpServletResponse());

        List<ArtistSongDTO> songs = List.of(song(1, null, 30, 2, "01-Jan-2020", "02-Jan-2024", false));
        java.util.Map<String, Object> model = new java.util.HashMap<>();
        model.put("slices", ArtistGeneralSummaries.playsShare(List.of(album("Debut", 70)), songs));
        model.put("noAlbum", ArtistGeneralSummaries.noAlbumSummary(songs));

        var html = engine.process("test-detail-general",
                new org.thymeleaf.context.WebContext(exchange, java.util.Locale.ENGLISH, model));
        var doc = org.jsoup.Jsoup.parse(html);

        // Rank rows: linked value with #rank (gold for #1), flag when given, plain "Not set" row when empty
        assertThat(doc.select("#ranks a.ad-row")).hasSize(2);
        assertThat(doc.select("#ranks a.ad-row").get(0).select(".ad-rank.is-gold").text()).isEqualTo("#1");
        assertThat(doc.select("#ranks a.ad-row").get(0).attr("href")).isEqualTo("/artists?genre=1&view=table");
        assertThat(doc.select("#ranks a.ad-row").get(1).select("img").attr("src")).isEqualTo("https://flagcdn.com/w40/mx.png");
        assertThat(doc.select("#ranks div.ad-row .is-empty").text()).isEqualTo("Not set");
        // Hover text names what is ranked; the source shows an Override badge or "from X"
        assertThat(doc.select("#ranks a.ad-row").get(0).attr("title")).isEqualTo("Genre: #1 among Pop artists");
        assertThat(doc.select("#ranks a.ad-row").get(0).select(".ad-ovr").text()).isEqualTo("Override");
        assertThat(doc.select("#ranks a.ad-row").get(1).select(".ad-from").text()).isEqualTo("from Selena");
        assertThat(doc.select("#ranks div.ad-row .ad-ovr, #ranks div.ad-row .ad-from")).isEmpty();

        // One chip and one header per sortable field
        assertThat(doc.select(".ad-sortbar [data-sort-key]")).extracting(e -> e.attr("data-sort-key"))
                .containsExactly("totalplays", "primaryplays", "legacyplays", "length", "releasedate",
                        "firstlistened", "lastlistened", "timelistened", "name");
        assertThat(doc.select("thead [data-sort-key]")).hasSize(9);

        assertThat(doc.select("#shareTotal").text()).isEqualTo("102");
        assertThat(doc.select("#shareFirst").text()).isEqualTo("Debut|album|68.6");
        assertThat(doc.select("#noAlbum").text()).isEqualTo("1|32|01-Jan-2020");
    }

    private static ArtistAlbumDTO album(String name, int plays) {
        ArtistAlbumDTO album = new ArtistAlbumDTO();
        album.setName(name);
        album.setTotalPlays(plays);
        return album;
    }

    private static ArtistSongDTO song(int id, Integer albumId, int primary, int legacy,
                                      String first, String last, boolean featuredOn) {
        ArtistSongDTO song = new ArtistSongDTO();
        song.setId(id);
        song.setAlbumId(albumId);
        song.setVatitoPlays(primary);
        song.setRobertloverPlays(legacy);
        song.setTotalPlays(primary + legacy);
        song.setTotalListeningTimeSeconds((primary + legacy) * 200);
        song.setFirstListenedDate(first);
        song.setLastListenedDate(last);
        song.setFeaturedOn(featuredOn);
        return song;
    }
}
