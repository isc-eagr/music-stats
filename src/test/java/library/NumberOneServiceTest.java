package library;

import library.dto.ChartOverviewRows.SongRow;
import library.service.ChartOverviewService;
import library.service.ChartOverviewService.Chart;
import library.service.NumberOneService;
import library.service.NumberOneService.Run;
import library.service.NumberOneService.TopEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NumberOneServiceTest {

    @Test
    void runsGroupConsecutivePeriodsAndCountRunNumberCumulativeAndTotal() {
        List<Run> runs = NumberOneService.buildRuns(List.of(
                new TopEntry("2024-01-01", "2024-01-07", "a"),
                new TopEntry("2024-01-08", "2024-01-14", "a"),
                new TopEntry("2024-01-15", "2024-01-21", "b"),
                new TopEntry("2024-01-22", "2024-01-28", "a"),
                new TopEntry("2024-01-22", "2024-01-28", "b"),
                new TopEntry("2024-01-29", "2024-02-04", "c")));

        // Most recent first; the second #1 on 22/01 is ignored
        assertThat(runs).extracting(Run::key).containsExactly("c", "a", "b", "a");
        assertThat(runs).extracting(Run::length).containsExactly(1, 1, 1, 2);
        assertThat(runs).extracting(Run::runNumber).containsExactly(1, 2, 1, 1);
        assertThat(runs).extracting(Run::cumulative).containsExactly(1, 3, 1, 2);
        assertThat(runs).extracting(Run::total).containsExactly(1, 3, 1, 3);
        assertThat(runs.get(3).startDate()).isEqualTo("2024-01-01");
        assertThat(runs.get(3).endDate()).isEqualTo("2024-01-14");
    }

    @Test
    void weeklySongAndAlbumRunsUseTheDisplayDateFormatAndLinks() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            NumberOneService service = new NumberOneService(db.jdbcTemplate, mock(ChartOverviewService.class));

            List<Run> songs = service.runs(Chart.WEEKLY, "song");
            assertThat(songs).extracting(run -> run.display().title()).containsExactly("Bidi Bidi Bom Bom", "Titi Me Pregunto");
            assertThat(songs.get(0).startDisplay()).isEqualTo("08/01/2024");
            assertThat(songs.get(0).endDisplay()).isEqualTo("14/01/2024");
            assertThat(songs.get(0).year()).isEqualTo("2024");
            assertThat(songs.get(0).display().link()).isEqualTo("/songs/1");
            assertThat(songs.get(1).display().artistName()).isEqualTo("Bad Bunny");

            List<Run> albums = service.runs(Chart.WEEKLY, "album");
            assertThat(albums).hasSize(1);
            assertThat(albums.get(0).display().title()).isEqualTo("Amor Prohibido");
            assertThat(albums.get(0).display().link()).isEqualTo("/albums/1");
        }
    }

    @Test
    void externalChartRunsTakeNamesAndGenderFromTheOverviewRowsAndKeepUnlinkedEntries() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            db.jdbcTemplate.update("""
                    INSERT INTO billboard_hot100_entry (id, chart_date, position, artist_name, song_title, peak_position, weeks_on_chart, song_id)
                    VALUES (101, '2024-04-06', 1, 'SELENA', 'Bidi Bidi Bom Bom', 1, 1, 1),
                           (102, '2024-04-13', 1, 'Selena', 'Bidi Bidi Bom Bom', 1, 2, 1),
                           (103, '2024-04-20', 1, 'Someone New', 'Unlinked Hit', 1, 1, NULL),
                           (104, '2024-04-27', 1, 'someone new', 'unlinked hit', 1, 2, NULL),
                           (105, '2024-05-04', 1, 'Selena', 'Bidi Bidi Bom Bom', 1, 3, 1)
                    """);
            ChartOverviewService overview = mock(ChartOverviewService.class);
            when(overview.songRows(eq(Chart.HOT100), any())).thenReturn(List.of(
                    songRow("1", 1, "Bidi Bidi Bom Bom", "Selena", "female", "/albums/1/image?thumbnail=true")));
            NumberOneService service = new NumberOneService(db.jdbcTemplate, overview);

            List<Run> runs = service.runs(Chart.HOT100, "song");

            // The seeded catalog adds an earlier #1 (Titi Me Pregunto) after these three
            assertThat(runs).extracting(run -> run.display().title())
                    .startsWith("Bidi Bidi Bom Bom", "Unlinked Hit", "Bidi Bidi Bom Bom");
            assertThat(runs).extracting(Run::length).startsWith(1, 2, 2);
            assertThat(runs.get(0).runNumber()).isEqualTo(2);
            assertThat(runs.get(0).total()).isEqualTo(3);
            assertThat(runs.get(0).display().gender()).isEqualTo("female");
            assertThat(runs.get(0).display().link()).isEqualTo("/songs/1");
            assertThat(runs.get(0).display().coverUrl()).isEqualTo("/albums/1/image?thumbnail=true");
            assertThat(runs.get(1).display().link()).isNull();
            assertThat(runs.get(1).display().gender()).isNull();
            assertThat(runs.get(1).startDisplay()).isEqualTo("20/04/2024");
            assertThat(runs.get(1).endDisplay()).isEqualTo("27/04/2024");
        }
    }

    private static SongRow songRow(String runKey, Integer songId, String title, String artist, String gender, String cover) {
        return new SongRow(runKey, songId, null, 1, title, artist, null, gender, songId != null, cover, null,
                1, null, 1, 1, 1, null, null, null, null, null, null, new int[]{1, 0, 0}, false, 0, null);
    }
}
