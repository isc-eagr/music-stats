package library;

import library.dto.ConsistencyPeriodDTO;
import library.service.ConsistencyRankService;
import library.service.ConsistencyRankService.Catalog;
import library.service.ConsistencyRankService.ConsistencyRanks;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

/** Consistency ledger: periods since the first play, and ranks by periods played with share then plays as tie-breaks. */
class ConsistencyRankServiceTest {

    @Test
    void songRanksBreakTiesByShareThenPlays() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ConsistencyRankService service = new ConsistencyRankService(db.jdbcTemplate);
            addOverrideGenderPlay(db);

            // Days: 5, 3, 2, then three songs with 1 day where the later first play (higher share) wins
            assertThat(rank(service, Catalog.SONGS, 3).periods().get("days").rank()).isEqualTo(1);
            assertThat(rank(service, Catalog.SONGS, 1).periods().get("days").rank()).isEqualTo(2);
            assertThat(rank(service, Catalog.SONGS, 4).periods().get("days").rank()).isEqualTo(4);
            assertThat(rank(service, Catalog.SONGS, 8).periods().get("days").rank()).isEqualTo(5);
            assertThat(rank(service, Catalog.SONGS, 2).periods().get("days").rank()).isEqualTo(6);

            // Months: songs 3 and 1 both have 2 months from January, so more plays wins
            assertThat(rank(service, Catalog.SONGS, 3).periods().get("months").rank()).isEqualTo(1);
            assertThat(rank(service, Catalog.SONGS, 1).periods().get("months").rank()).isEqualTo(2);

            // Years: every song has 1 year starting 2024, so plays decide
            assertThat(rank(service, Catalog.SONGS, 1).periods().get("years").rank()).isEqualTo(2);
        }
    }

    @Test
    void genderRanksFollowTheSongOverride() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ConsistencyRankService service = new ConsistencyRankService(db.jdbcTemplate);
            addOverrideGenderPlay(db);

            // Ojitos Lindos is by a male artist but overridden to Female: ranked after Selena's songs 1 and 5
            ConsistencyRanks overridden = rank(service, Catalog.SONGS, 4);
            assertThat(overridden.genderName()).isEqualTo("Female");
            assertThat(overridden.periods().get("days").genderRank()).isEqualTo(3);

            ConsistencyRanks male = rank(service, Catalog.SONGS, 8);
            assertThat(male.genderName()).isEqualTo("Male");
            assertThat(male.periods().get("days").genderRank()).isEqualTo(2);
            assertThat(male.periods().keySet()).containsExactly("days", "weeks", "months", "years");
        }
    }

    @Test
    void albumAndArtistRanksCountEveryOwnSong() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ConsistencyRankService service = new ConsistencyRankService(db.jdbcTemplate);
            addOverrideGenderPlay(db);

            assertThat(rank(service, Catalog.ALBUMS, 2).periods().get("days").rank()).isEqualTo(1);
            ConsistencyRanks selenaAlbum = rank(service, Catalog.ALBUMS, 1);
            assertThat(selenaAlbum.periods().get("days").rank()).isEqualTo(2);
            assertThat(selenaAlbum.periods().get("days").genderRank()).isEqualTo(1);

            // Selena and Bad Bunny both have 6 days; Bad Bunny started later, so a higher share
            assertThat(rank(service, Catalog.ARTISTS, 2).periods().get("days").rank()).isEqualTo(1);
            assertThat(rank(service, Catalog.ARTISTS, 1).periods().get("days").rank()).isEqualTo(2);
        }
    }

    @Test
    void itemsWithoutPlaysHaveNoRanks() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ConsistencyRankService service = new ConsistencyRankService(db.jdbcTemplate);
            assertThat(service.findRanks(Catalog.SONGS, 6)).isEmpty();
            assertThat(Catalog.fromPath("albums")).contains(Catalog.ALBUMS);
            assertThat(Catalog.fromPath("genres")).isEmpty();
        }
    }

    @Test
    void periodsCountCalendarUnitsSinceTheFirstPlay() {
        var periods = ConsistencyPeriodDTO.of(40, 20, 9, 3, LocalDate.of(2023, 1, 15), LocalDate.of(2025, 3, 10));
        assertThat(periods).extracting(ConsistencyPeriodDTO::total).containsExactly(786L,
                periods.get(1).total(), 27L, 3L);
        assertThat(periods.get(0).shareLabel()).isEqualTo("5.1%");
        assertThat(periods.get(3).shareLabel()).isEqualTo("100%");
        assertThat(periods.get(0).barWidth()).isEqualTo("5.1");
        // List card value when sorted by Consistency
        assertThat(periods.get(1).summary()).endsWith(" weeks (" + periods.get(1).shareLabel() + ")");
        assertThat(ConsistencyPeriodDTO.of(19, 0, 0, 1_000, LocalDate.of(2005, 2, 13), LocalDate.of(2025, 3, 10))
                .get(3).summary()).isEqualTo("1,000 / 21 years (100%)");
        assertThat(ConsistencyPeriodDTO.forCard(null, null, null, 0, null).get(3).summary()).isEqualTo("0 years");

        var unplayed = ConsistencyPeriodDTO.forCard(null, null, null, null, null);
        assertThat(unplayed).extracting(ConsistencyPeriodDTO::played).containsOnly(0L);
        assertThat(unplayed.get(0).shareLabel()).isEqualTo("-");
        assertThat(ConsistencyPeriodDTO.forCard(1, 1, 1, 1, "01-Mar-2025").get(3).total())
                .isEqualTo(LocalDate.now().getYear() - 2025 + 1L);
    }

    private static void addOverrideGenderPlay(TestDatabaseSupport db) {
        db.jdbcTemplate.update("""
                INSERT INTO Play (id, artist, album, song, play_date, song_id, account)
                VALUES (100, 'Bad Bunny', 'Un Verano Sin Ti', 'Ojitos Lindos', '2024-06-01 10:00:00', 4, 'vatito')
                """);
    }

    private static ConsistencyRanks rank(ConsistencyRankService service, Catalog catalog, int id) {
        return service.findRanks(catalog, id).orElseThrow();
    }
}
