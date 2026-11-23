package library;

import library.service.DetailAssociationsService;
import library.service.DetailAssociationsService.AlbumGuest;
import library.service.DetailAssociationsService.AlbumTab;
import library.service.DetailAssociationsService.ArtistRef;
import library.service.DetailAssociationsService.ArtistTab;
import library.service.DetailAssociationsService.Credit;
import library.service.DetailAssociationsService.LinkCount;
import library.service.DetailAssociationsService.MapNode;
import library.service.DetailAssociationsService.MapRing;
import library.service.DetailAssociationsService.Partner;
import library.service.DetailAssociationsService.Split;
import library.service.DetailAssociationsService.SongTab;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class DetailAssociationsServiceTest {

    /**
     * On top of the seed (Selena's "No Me Queda Mas" features Guest Singer):
     * Guest Singer's "Duet Night" features Selena, and Bad Bunny's "Trio Track" (on Un Verano Sin Ti) features
     * Selena and Legacy Legend. Mystery Artist has an unplayed "Cameo" on Un Verano Sin Ti.
     */
    private static void seedCollabs(TestDatabaseSupport db) {
        db.jdbcTemplate.update("INSERT INTO Song (id, artist_id, album_id, name, release_date, track_number) VALUES "
                + "(9, 4, NULL, 'Duet Night', '2001-05-05', NULL), (10, 2, 2, 'Trio Track', NULL, 3), (11, 5, 2, 'Cameo', NULL, 4)");
        db.jdbcTemplate.update("INSERT INTO SongFeaturedArtist (song_id, artist_id) VALUES (9, 1), (10, 1), (10, 6)");
        db.jdbcTemplate.update("INSERT INTO Play (id, song_id, play_date, account) VALUES "
                + "(101, 9, '2024-06-01 10:00:00', 'vatito'), (102, 9, '2024-06-02 10:00:00', 'vatito'), "
                + "(103, 9, '2024-06-03 10:00:00', 'robertlover'), (104, 10, '2024-06-04 10:00:00', 'vatito'), "
                + "(105, 10, '2024-06-05 10:00:00', 'vatito'), (106, 10, '2024-06-06 10:00:00', 'vatito'), "
                + "(107, 10, '2024-06-07 10:00:00', 'vatito')");
    }

    @Test
    void artistTabSharesPlaysPerPartnerWithLinkTypesAndSplits() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            seedCollabs(db);
            ArtistTab tab = new DetailAssociationsService(db.jdbcTemplate).artistTab(1);

            assertThat(tab.self().name()).isEqualTo("Selena");
            assertThat(tab.self().gender()).isEqualTo("f");
            assertThat(tab.partners()).extracting(p -> p.artist().name())
                    .containsExactly("Guest Singer", "Bad Bunny", "Legacy Legend");
            // Guest on her song and host of "Duet Night": one entry per type, with its songs and plays
            Partner guest = tab.partners().get(0);
            assertThat(guest.songs()).isEqualTo(2);
            assertThat(guest.plays()).isEqualTo(4);
            assertThat(guest.primary()).isEqualTo(3);
            assertThat(guest.legacy()).isEqualTo(1);
            assertThat(guest.links()).extracting(LinkCount::label).containsExactly("Guest", "Host");
            assertThat(guest.links()).extracting(LinkCount::songs).containsExactly(1, 1);
            assertThat(guest.links()).extracting(LinkCount::plays).containsExactly(1L, 3L);
            assertThat(guest.typeTip()).isEqualTo("Guest 1 · Host 1 (2 songs)");
            assertThat(guest.topSongName()).isEqualTo("Duet Night");
            assertThat(guest.topSongPlays()).isEqualTo(3);
            assertThat(guest.songList()).isEqualTo("Duet Night, No Me Queda Mas");

            Partner bunny = tab.partners().get(1);
            assertThat(bunny.links()).extracting(LinkCount::label).containsExactly("Host");
            assertThat(bunny.mainLink()).isEqualTo("Host");
            assertThat(bunny.artist().gender()).isEqualTo("m");
            Partner legend = tab.partners().get(2);
            assertThat(legend.links()).extracting(LinkCount::label).containsExactly("Co-guest");

            assertThat(tab.creditedSongs()).isEqualTo(5);
            assertThat(tab.songsTogether()).isEqualTo(3);
            assertThat(tab.creditedPlays()).isEqualTo(13);
            assertThat(tab.sharedPlays()).isEqualTo(8);
            assertThat(tab.sharedPct()).isCloseTo(61.54, within(0.01));
            assertThat(tab.count("m")).isEqualTo(2);
            assertThat(tab.count("f")).isEqualTo(1);

            assertThat(tab.byGender()).extracting(Split::key).containsExactly("m", "f");
            assertThat(tab.byGender().get(0).plays()).isEqualTo(8);
            assertThat(tab.byGender().get(0).pct()).isCloseTo(66.67, within(0.01));
            // By type, the parts add up to every pair's shared plays (4 + 4 + 4)
            assertThat(tab.byLink()).extracting(Split::key).containsExactly("guest", "host", "co");
            assertThat(tab.byLink()).extracting(Split::plays).containsExactly(1L, 7L, 4L);
            assertThat(tab.byLink()).extracting(Split::artists).containsExactly(1, 2, 1);

            assertThat(tab.map()).hasSize(3);
            assertThat(tab.rings()).extracting(MapRing::label).containsExactly("#1-3");
        }
    }

    /** Selena and Mystery Artist join The Static Hearts, next to the seeded member Guest Singer. */
    private static void joinStaticHearts(TestDatabaseSupport db) {
        db.jdbcTemplate.update("INSERT INTO ArtistMember (group_artist_id, member_artist_id) VALUES (3, 1), (3, 5)");
    }

    @Test
    void familiesListEveryMemberOfEachGroupWithTheArtistFirst() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            seedCollabs(db);
            joinStaticHearts(db);
            DetailAssociationsService service = new DetailAssociationsService(db.jdbcTemplate);

            // A member sees the group and all its members, themselves first, then by plays of their own songs
            List<DetailAssociationsService.Family> selena = service.artistTab(1).families();
            assertThat(selena).hasSize(1);
            DetailAssociationsService.Family hearts = selena.getFirst();
            assertThat(hearts.group().name()).isEqualTo("The Static Hearts");
            assertThat(hearts.self()).isFalse();
            assertThat(hearts.songs()).isEqualTo(1);
            assertThat(hearts.plays()).isZero();
            assertThat(hearts.members()).extracting(m -> m.artist().name())
                    .containsExactly("Selena", "Guest Singer", "Mystery Artist");
            assertThat(hearts.members()).extracting(DetailAssociationsService.Member::self).containsExactly(true, false, false);
            assertThat(hearts.members()).extracting(DetailAssociationsService.Member::plays).containsExactly(6L, 3L, 0L);
            // Mystery Artist owns "Unknown Silence" and the "Cameo" seeded above
            assertThat(hearts.members()).extracting(DetailAssociationsService.Member::songs).containsExactly(3, 1, 2);

            // The group's own page shows itself as the group, members by plays
            List<DetailAssociationsService.Family> group = service.artistTab(3).families();
            assertThat(group).hasSize(1);
            assertThat(group.getFirst().self()).isTrue();
            assertThat(group.getFirst().members()).extracting(m -> m.artist().name())
                    .containsExactly("Selena", "Guest Singer", "Mystery Artist");

            assertThat(service.artistTab(2).families()).isEmpty();
        }
    }

    @Test
    void bandmatesShareTheirGroupsSongsAndCountInEveryBreakdown() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            seedCollabs(db);
            joinStaticHearts(db);
            // Two plays of The Static Hearts' "Quiet Track", which features Bad Bunny and bandmate Guest Singer
            db.jdbcTemplate.update("INSERT INTO Play (id, song_id, play_date, account) VALUES "
                    + "(201, 6, '2024-07-01 10:00:00', 'vatito'), (202, 6, '2024-07-02 10:00:00', 'vatito')");
            db.jdbcTemplate.update("INSERT INTO SongFeaturedArtist (song_id, artist_id) VALUES (6, 2), (6, 4)");
            ArtistTab tab = new DetailAssociationsService(db.jdbcTemplate).artistTab(1);

            assertThat(tab.partners()).extracting(p -> p.artist().name())
                    .containsExactly("Guest Singer", "Bad Bunny", "Legacy Legend", "Mystery Artist");
            // A bandmate credited on the group's song stays a bandmate for it
            Partner guest = tab.partners().get(0);
            assertThat(guest.links()).extracting(LinkCount::label).containsExactly("Guest", "Host", "Bandmate");
            assertThat(guest.links()).extracting(LinkCount::plays).containsExactly(1L, 3L, 2L);
            assertThat(guest.songs()).isEqualTo(3);
            assertThat(guest.plays()).isEqualTo(6);
            // A guest on the group's song links through the group
            Partner bunny = tab.partners().get(1);
            assertThat(bunny.links()).extracting(LinkCount::label).containsExactly("Host", "Via group");
            assertThat(bunny.links()).extracting(LinkCount::plays).containsExactly(4L, 2L);
            assertThat(bunny.plays()).isEqualTo(6);
            // The group itself is never a collaborator of its member
            assertThat(tab.partners()).noneMatch(p -> p.artist().id() == 3);
            // Mystery Artist is only linked through the band
            Partner mystery = tab.partners().get(3);
            assertThat(mystery.links()).extracting(LinkCount::label).containsExactly("Bandmate");
            assertThat(mystery.plays()).isEqualTo(2);
            assertThat(mystery.artist().gender()).isEqualTo("o");

            // The group's songs join the artist's own (5 credited + 1 group song)
            assertThat(tab.creditedSongs()).isEqualTo(6);
            assertThat(tab.songsTogether()).isEqualTo(4);
            assertThat(tab.creditedPlays()).isEqualTo(15);
            assertThat(tab.sharedPlays()).isEqualTo(10);
            assertThat(tab.byLink()).extracting(Split::key).containsExactly("guest", "host", "co", "group", "band");
            assertThat(tab.byLink()).extracting(Split::label).containsExactly("Guests", "Hosts", "Co-guests", "Via group", "Bandmates");
            assertThat(tab.byLink()).extracting(Split::plays).containsExactly(1L, 7L, 4L, 2L, 4L);
            assertThat(tab.byGender()).extracting(Split::key).containsExactly("m", "f", "o");
            assertThat(tab.map()).hasSize(4);
        }
    }

    @Test
    void artistWithoutCollaborationsGetsAnEmptyTab() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ArtistTab tab = new DetailAssociationsService(db.jdbcTemplate).artistTab(6);
            assertThat(tab.partners()).isEmpty();
            assertThat(tab.map()).isEmpty();
            assertThat(tab.creditedSongs()).isEqualTo(1);
            assertThat(tab.sharedPlays()).isZero();
        }
    }

    @Test
    void mapDrawsTheTopTwentyOnThreeRingsBiggestFirst() {
        List<Partner> partners = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            partners.add(partner(i, "Artist Number " + i + " With A Long Name", 1000 - i * 30));
        }
        List<MapNode> nodes = new ArrayList<>();
        List<MapRing> rings = new ArrayList<>();
        DetailAssociationsService.layoutMap(partners, nodes, rings);

        assertThat(nodes).hasSize(20);
        assertThat(rings).extracting(MapRing::label).containsExactly("#1-5", "#6-12", "#13-20");
        assertThat(rings).extracting(MapRing::r).containsExactly(150, 218, 278);
        assertThat(nodes.getFirst().d()).isEqualTo(70);
        assertThat(nodes.getLast().d()).isLessThan(nodes.getFirst().d());
        // Every bubble stays inside the 600 x 680 canvas, labels included
        for (MapNode n : nodes) {
            assertThat(n.x() - n.d() / 2).isGreaterThanOrEqualTo(0);
            assertThat(n.x() + n.d() / 2).isLessThanOrEqualTo(600);
            assertThat(n.y() - n.d() / 2).isGreaterThanOrEqualTo(0);
            assertThat(n.labelY() + 13).isLessThanOrEqualTo(680);
        }
        assertThat(nodes.getFirst().label()).hasSize(18).endsWith("…");
    }

    @Test
    void albumTabColorsGuestTracksAndListsEveryGuest() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            seedCollabs(db);
            AlbumTab tab = new DetailAssociationsService(db.jdbcTemplate).albumTab(2);

            assertThat(tab.tracks()).extracting(DetailAssociationsService.Track::name)
                    .containsExactly("Titi Me Pregunto", "Ojitos Lindos", "Trio Track", "Cameo");
            DetailAssociationsService.Track trio = tab.tracks().get(2);
            assertThat(trio.guests()).extracting(ArtistRef::name).containsExactly("Legacy Legend", "Selena");
            assertThat(trio.gender()).isEqualTo("m");
            assertThat(trio.tip()).isEqualTo("3. Trio Track (with Legacy Legend, Selena) · 4 plays");
            assertThat(tab.tracks().get(0).hasGuests()).isFalse();
            assertThat(tab.tracks().get(0).barPx()).isEqualTo(200);
            assertThat(tab.tracks().get(1).barPx()).isEqualTo(2);

            assertThat(tab.guests()).extracting(g -> g.artist().name())
                    .containsExactly("Legacy Legend", "Selena", "Mystery Artist");
            AlbumGuest selena = tab.guests().get(1);
            assertThat(selena.role()).isEqualTo("Featured");
            assertThat(selena.plays()).isEqualTo(4);
            assertThat(selena.trackNos()).isEqualTo("3");
            assertThat(selena.albumPct()).isCloseTo(44.44, within(0.01));
            assertThat(selena.theirPlays()).isEqualTo(13);
            AlbumGuest mystery = tab.guests().get(2);
            assertThat(mystery.role()).isEqualTo("Track artist");
            assertThat(mystery.artist().gender()).isEqualTo("o");
            assertThat(mystery.artist().genderLabel()).isEqualTo("Unknown");

            assertThat(tab.total()).isEqualTo(9);
            assertThat(tab.guestPlays()).isEqualTo(4);
            assertThat(tab.soloPlays()).isEqualTo(5);
            assertThat(tab.guestTracks()).isEqualTo(2);
            assertThat(tab.guestTrackPct()).isCloseTo(50.0, within(0.01));
        }
    }

    @Test
    void songTabListsEachCreditedArtistAndThePairs() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            seedCollabs(db);
            SongTab tab = new DetailAssociationsService(db.jdbcTemplate).songTab(2);

            assertThat(tab.plays()).isEqualTo(1);
            assertThat(tab.hasFeatures()).isTrue();
            assertThat(tab.credits()).extracting(c -> c.artist().name()).containsExactly("Selena", "Guest Singer");
            Credit selena = tab.credits().get(0);
            assertThat(selena.main()).isTrue();
            assertThat(selena.theirSongs()).isEqualTo(5);
            assertThat(selena.theirPlays()).isEqualTo(13);
            // Own-song plays: Bad Bunny 9, Selena 6, Guest Singer 3, Legacy Legend 1
            assertThat(selena.overallRank()).isEqualTo(2);
            Credit guest = tab.credits().get(1);
            assertThat(guest.main()).isFalse();
            assertThat(guest.theirSongs()).isEqualTo(2);
            assertThat(guest.theirPlays()).isEqualTo(4);
            assertThat(guest.overallRank()).isEqualTo(3);

            assertThat(tab.pairs()).hasSize(1);
            assertThat(tab.pairs().getFirst().songs()).isEqualTo(2);
            assertThat(tab.pairs().getFirst().plays()).isEqualTo(4);
            assertThat(tab.pairs().getFirst().others()).extracting(DetailAssociationsService.PairSong::name)
                    .containsExactly("Duet Night");
            assertThat(tab.hasOtherSongs()).isTrue();

            SongTab solo = new DetailAssociationsService(db.jdbcTemplate).songTab(1);
            assertThat(solo.hasFeatures()).isFalse();
            assertThat(solo.pairs()).isEmpty();
        }
    }

    @Test
    void fragmentsRenderTheMapLedgerStripAndCredits() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            seedCollabs(db);
            DetailAssociationsService service = new DetailAssociationsService(db.jdbcTemplate);

            Document artist = render("artist", service.artistTab(1));
            // The map sits in the Collaborators card; bubbles and rows share the artist id and jump to each other
            assertThat(artist.select("#assocPartners #assocMap .as-node")).hasSize(3);
            assertThat(artist.select(".as-rl, .as-rank-list")).isEmpty();
            assertThat(artist.select("#assocMap .as-node").eachAttr("href"))
                    .containsExactly("#assocRow4", "#assocRow2", "#assocRow6");
            assertThat(artist.select("#assocPartners tbody tr").eachAttr("id"))
                    .containsExactly("assocRow4", "assocRow2", "assocRow6");
            assertThat(artist.select("#assocPartners tbody tr").first().attr("data-plays")).isEqualTo("4");
            // One chip per association type with its song count (no combined "Both")
            assertThat(artist.select("#assocPartners .as-link").eachText()).containsExactly("Guest 1", "Host 1", "Host 1", "Co-guest 1");
            assertThat(artist.select("#assocPartners tbody tr").eachAttr("data-link")).containsExactly("Guest", "Host", "Co-guest");
            // Type bar: guest + host segments for Guest Singer, one co-guest segment for Legacy Legend
            assertThat(artist.select("#assocRow4 .as-types i").eachAttr("class")).containsExactly("is-link-guest", "is-link-host");
            assertThat(artist.select("#assocRow6 .as-types i").eachAttr("class")).containsExactly("is-link-co");
            assertThat(artist.select("#assocPartners thead [data-sort-key]").eachAttr("data-sort-key")).containsExactly(
                    "name", "link", "songs", "plays", "primary", "legacy", "top");
            assertThat(artist.select("#assocPartners tbody .dt-mb")).isEmpty();
            // Shared / Primary / Legacy live in one column: total, the split beside it, one bar scaled to the top partner
            assertThat(artist.select("#assocPartners thead th")).hasSize(6);
            assertThat(artist.select("#assocRow4 .as-pl-top b").text()).isEqualTo("4");
            assertThat(artist.select("#assocRow4 .as-pl-top small").text()).isEqualTo("3 1");
            assertThat(artist.select("#assocRow4 .as-pl-bar i").eachAttr("style")).containsExactly("width: 75.0%", "width: 25.0%");
            assertThat(artist.select("#assocRow2 .as-pl-bar i").eachAttr("style")).containsExactly("width: 100.0%");
            assertThat(artist.getElementById("assocFamily")).isNull();
            assertThat(artist.select("#assocGlance .dt-tile > b").eachText()).containsExactly("3", "8");
            // Readable breakdowns with a bar: share of songs and of plays
            assertThat(artist.select("#assocGlance .as-tile small").eachText())
                    .containsExactly("60.0% of 5 songs", "61.5% of 13 plays");
            // Collaborators and their gender split are one block: count, plays bar, artists and plays per gender
            assertThat(artist.select("#assocCollaborators .dt-count").text()).isEqualTo("3");
            assertThat(artist.select("#assocCollaborators .as-split i").eachAttr("class")).containsExactly("is-m", "is-f");
            assertThat(artist.select("#assocCollaborators .as-split-legend > div").eachText())
                    .containsExactly("Male 2 artists 8 plays · 66.7%", "Female 1 artist 4 plays · 33.3%");
            List<String> fills = artist.select("#assocGlance .as-tile-bar i").eachAttr("style");
            assertThat(fills).hasSize(2);
            assertThat(fills.get(0)).isEqualTo("width: 60.0%");
            assertThat(fills.get(1)).startsWith("width: 61.5");
            assertThat(artist.select("#assocGlance .as-split").size()).isEqualTo(2);

            Document album = render("album", service.albumTab(2));
            assertThat(album.select("#assocTracks .as-tk")).hasSize(4);
            assertThat(album.select("#assocTracks .as-tk-bar.is-m")).hasSize(1);
            assertThat(album.select("#assocTracks .as-tk-bar.is-o")).hasSize(1);
            assertThat(album.select("#assocGuests tbody tr")).hasSize(3);

            Document song = render("song", service.songTab(2));
            assertThat(song.select("#assocCredits .as-credit")).hasSize(2);
            assertThat(song.select("#assocCredits .as-credit-name").eachText()).containsExactly("Selena", "Guest Singer");
            // Just the basics: plays, songs and overall rank
            assertThat(song.select("#assocCredits .as-credit-stats").eachText())
                    .containsExactly("13 plays 5 songs #2 overall", "4 plays 2 songs #3 overall");
            assertThat(song.select("#assocPairs .as-also").eachText()).containsExactly("Duet Night 3");

            // Mystery Artist's only song sits on someone else's album, which is not a credit
            Document empty = render("artist", service.artistTab(5));
            assertThat(empty.select(".as-empty").text()).isEqualTo("No collaborations yet");
            assertThat(empty.getElementById("assocMap")).isNull();
            assertThat(empty.getElementById("assocPartners")).isNull();

            // In a band: the family tree shows up and bandmates join the table with their own chip
            joinStaticHearts(db);
            Document band = render("artist", service.artistTab(1));
            assertThat(band.select("#assocFamily .as-tree")).hasSize(1);
            assertThat(band.select("#assocFamily .as-grp b").first().text()).isEqualTo("The Static Hearts");
            assertThat(band.select("#assocFamily .as-grp .as-chip.is-group").text()).isEqualTo("Is part of");
            assertThat(band.select("#assocFamily .as-mc b").eachText()).containsExactly("Selena", "Guest Singer", "Mystery Artist");
            assertThat(band.select("#assocFamily .as-mc").get(0).hasClass("is-self")).isTrue();
            assertThat(band.select("#assocFamily .as-mc.is-dim b").eachText()).containsExactly("Mystery Artist");
            assertThat(band.select("#assocFamily .as-mc-n").eachText()).containsExactly("6 plays", "3 plays", "No plays");
            assertThat(band.select("#assocRow4 .as-link").eachText()).containsExactly("Guest 1", "Host 1", "Bandmate 1");
            assertThat(band.select("#assocRow5 .as-link").eachText()).containsExactly("Bandmate 1");
            assertThat(band.select("#assocRow5 .as-types i").eachAttr("class")).containsExactly("is-link-band");
        }
    }

    private static Document render(String kind, Object tab) {
        Map<String, Object> model = new HashMap<>();
        model.put("kind", kind);
        model.put("tab", tab);
        model.put("showLegacy", true);
        var resolver = new org.thymeleaf.templateresolver.ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new org.thymeleaf.spring6.SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new org.springframework.mock.web.MockServletContext();
        var exchange = org.thymeleaf.web.servlet.JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new org.springframework.mock.web.MockHttpServletRequest(servletContext),
                        new org.springframework.mock.web.MockHttpServletResponse());
        return Jsoup.parse(engine.process("associations-harness",
                new org.thymeleaf.context.WebContext(exchange, Locale.ENGLISH, model)));
    }

    private static Partner partner(int id, String name, long plays) {
        return new Partner(new ArtistRef(id, name, id % 2 == 0 ? "m" : "f", "Male", null),
                List.of(new LinkCount("guest", "Guest", 1, plays)), 1, plays, plays, 0, id, "Song " + id, plays, "Song " + id);
    }
}
