package library;

import library.dto.TopPlayedSnapshotDTO;
import library.dto.TopPlayedSnapshotItemDTO;
import library.service.TopPlayedTimelineService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TopPlayedTimelineTest {

    @Test
    void artistReignsFollowCumulativePlaysWithIncumbentsKeepingTies() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            db.jdbcTemplate.update("INSERT INTO Play (id, play_date, song_id, account) VALUES (50, NULL, 3, 'vatito'), (51, '2024-01-06 10:00:00', 999, 'vatito')");
            List<TopPlayedSnapshotDTO> reigns = new TopPlayedTimelineService(db.jdbcTemplate).getArtistTimeline();

            assertThat(reigns).extracting(TopPlayedSnapshotDTO::getStartDate, TopPlayedSnapshotDTO::getEndDate)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("01/01/2024", "03/01/2024"),
                            org.assertj.core.groups.Tuple.tuple("03/01/2024", "03/03/2024"),
                            org.assertj.core.groups.Tuple.tuple("03/03/2024", "02/04/2024"),
                            org.assertj.core.groups.Tuple.tuple("02/04/2024", "01/05/2024"),
                            org.assertj.core.groups.Tuple.tuple("01/05/2024", "Present"));
            assertThat(reigns).extracting(reign -> reign.getItems().stream().map(TopPlayedSnapshotItemDTO::getItemName).toList())
                    .containsExactly(
                            List.of("Selena"),
                            List.of("Selena", "Bad Bunny"),
                            List.of("Bad Bunny", "Selena"),
                            List.of("Selena", "Bad Bunny"),
                            List.of("Selena", "Bad Bunny", "Legacy Legend"));

            TopPlayedSnapshotItemDTO overtaken = reigns.get(2).getItems().get(1);
            assertThat(overtaken.getItemName()).isEqualTo("Selena");
            assertThat(overtaken.getPlaysCount()).isEqualTo(5); // count just before Selena retook #1
            assertThat(reigns.getLast().isCurrent()).isTrue();
            assertThat(reigns.getLast().getItems().getFirst().getGenderName()).isEqualTo("Female");
        }
    }

    @Test
    void songAndGenreReignsUseTheirOwnItems() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            TopPlayedTimelineService service = new TopPlayedTimelineService(db.jdbcTemplate);

            TopPlayedSnapshotDTO currentSongs = service.getSongTimeline().getLast();
            assertThat(currentSongs.getItems()).extracting(TopPlayedSnapshotItemDTO::getItemName)
                    .containsExactly("Titi Me Pregunto", "Bidi Bidi Bom Bom", "Standalone Jam");
            assertThat(currentSongs.getItems().getFirst().getSecondaryName()).isEqualTo("Bad Bunny");

            // Genre resolves song override, then album override, then artist genre.
            TopPlayedSnapshotDTO currentGenres = service.getGenreTimeline().getLast();
            assertThat(currentGenres.getItems()).extracting(TopPlayedSnapshotItemDTO::getItemName)
                    .containsExactly("Pop", "Rock", "Dance");
        }
    }
}
