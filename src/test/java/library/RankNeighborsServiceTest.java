package library;

import library.service.AlbumService;
import library.service.RankNeighborsService;
import library.service.RankNeighborsService.Neighbor;
import library.service.RankNeighborsService.RankNeighbors;
import library.service.SongService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Detail sidebar rank tooltips: the items ranked around the current one, in the same order as the rank badges. */
class RankNeighborsServiceTest {

    @Test
    void artistNeighborsAreRankedByPlaysWithinTheAttribute() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            RankNeighbors male = new RankNeighborsService(db.jdbcTemplate).forArtist(2, "gender");
            assertThat(male.total()).isEqualTo(3);
            assertThat(male.items()).extracting(Neighbor::name)
                    .containsExactly("Bad Bunny", "Legacy Legend", "The Static Hearts");
            assertThat(male.items()).extracting(Neighbor::plays).containsExactly(5L, 1L, 0L);
            assertThat(male.items()).extracting(Neighbor::current).containsExactly(true, false, false);
            assertThat(male.items()).extracting(Neighbor::gender).containsOnly("male");

            RankNeighbors mexico = new RankNeighborsService(db.jdbcTemplate).forArtist(6, "country");
            assertThat(mexico.items()).extracting(Neighbor::rank, Neighbor::name)
                    .containsExactly(org.assertj.core.groups.Tuple.tuple(1, "Selena"),
                            org.assertj.core.groups.Tuple.tuple(2, "Legacy Legend"));
            assertThat(mexico.items().get(0).gender()).isEqualTo("female");
        }
    }

    @Test
    void onlyFiveItemsAboveAndBelowAreReturned() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            // 14 artists without plays tie on plays and first play, so the id breaks the tie
            for (int id = 100; id < 114; id++) {
                db.jdbcTemplate.update("INSERT INTO Artist (id, name, gender_id, country) VALUES (?, ?, 1, 'Testland')",
                        id, "Tester " + id);
            }
            RankNeighbors result = new RankNeighborsService(db.jdbcTemplate).forArtist(107, "country");
            assertThat(result.total()).isEqualTo(14);
            assertThat(result.items()).extracting(Neighbor::rank).containsExactly(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
            assertThat(result.items()).filteredOn(Neighbor::current).extracting(Neighbor::rank).containsExactly(8);

            // Near the top there is nothing more above
            assertThat(new RankNeighborsService(db.jdbcTemplate).forArtist(100, "country").items())
                    .extracting(Neighbor::rank).containsExactly(1, 2, 3, 4, 5, 6);
        }
    }

    @Test
    void songNeighborsFollowOverridesAndMatchTheRankBadges() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            RankNeighborsService service = new RankNeighborsService(db.jdbcTemplate);
            SongService songService = new SongService(db.songRepository, null, null, db.jdbcTemplate, null, null, null);

            // Ojitos Lindos is overridden to gender 2, so it sits among the gender-2 songs with no plays yet
            RankNeighbors female = service.forSong(4, "gender");
            assertThat(female.items()).extracting(Neighbor::name)
                    .containsExactly("Bidi Bidi Bom Bom", "Standalone Jam", "No Me Queda Mas", "Ojitos Lindos");
            assertThat(female.items().get(3).artistName()).isEqualTo("Bad Bunny");
            assertThat(female.items().get(3).gender()).isEqualTo("female");
            assertThat(currentRank(female)).isEqualTo(songService.getAllSongRankings(4).get("gender"));

            // Bidi Bidi takes its genre from the album override (Rock), not from Selena (Pop)
            RankNeighbors rock = service.forSong(1, "genre");
            assertThat(rock.items()).extracting(Neighbor::name).containsExactly("Bidi Bidi Bom Bom", "Old Hit", "Quiet Track");
            assertThat(currentRank(rock)).isEqualTo(songService.getAllSongRankings(1).get("genre"));

            RankNeighbors selena = service.forSong(5, "artist");
            assertThat(selena.items()).extracting(Neighbor::name)
                    .containsExactly("Bidi Bidi Bom Bom", "Standalone Jam", "No Me Queda Mas");
            assertThat(currentRank(selena)).isEqualTo(songService.getSongRankByArtist(5));
        }
    }

    @Test
    void albumNeighborsMatchTheRankBadges() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            RankNeighborsService service = new RankNeighborsService(db.jdbcTemplate);
            AlbumService albumService = new AlbumService(db.albumRepository, null, null, db.jdbcTemplate, null);

            // Amor Prohibido is Rock through its override, next to the Rock albums by Rock artists
            RankNeighbors rock = service.forAlbum(1, "genre");
            assertThat(rock.items()).extracting(Neighbor::name)
                    .containsExactly("Amor Prohibido", "Legacy Collection", "Silent Record");
            assertThat(currentRank(rock)).isEqualTo(albumService.getAllAlbumRankings(1).get("genre"));
            assertThat(rock.items().get(0).artistName()).isEqualTo("Selena");
        }
    }

    @Test
    void overallNeighborsRankEveryItemAndMatchTheOverallPosition() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            RankNeighborsService service = new RankNeighborsService(db.jdbcTemplate);
            SongService songService = new SongService(db.songRepository, null, null, db.jdbcTemplate, null, null, null);
            AlbumService albumService = new AlbumService(db.albumRepository, null, null, db.jdbcTemplate, null);

            // Every artist counts, played or not: Selena 6, Bad Bunny 5, Legacy Legend 1, then the rest by id
            RankNeighbors artists = service.forArtist(6, RankNeighborsService.OVERALL);
            assertThat(artists.total()).isEqualTo(6);
            assertThat(artists.items()).extracting(Neighbor::name).containsExactly(
                    "Selena", "Bad Bunny", "Legacy Legend", "The Static Hearts", "Guest Singer", "Mystery Artist");
            assertThat(currentRank(artists)).isEqualTo(3);

            RankNeighbors songs = service.forSong(2, RankNeighborsService.OVERALL);
            assertThat(songs.total()).isEqualTo(8);
            assertThat(currentRank(songs)).isEqualTo(songService.getSongOverallPosition(2));

            RankNeighbors albums = service.forAlbum(3, RankNeighborsService.OVERALL);
            assertThat(currentRank(albums)).isEqualTo(albumService.getAlbumOverallPosition(3));
        }
    }

    @Test
    void unknownAttributesAndItemsWithoutTheValueGiveNoRows() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            RankNeighborsService service = new RankNeighborsService(db.jdbcTemplate);
            assertThat(service.forArtist(1, "album")).isNull();
            assertThat(service.forSong(1, "bogus")).isNull();
            // Mystery Artist has no genre
            assertThat(service.forArtist(5, "genre").items()).isEmpty();
        }
    }

    private static Integer currentRank(RankNeighbors neighbors) {
        return neighbors.items().stream().filter(Neighbor::current).map(Neighbor::rank).findFirst().orElse(null);
    }
}
