package library;

import library.service.ChartService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChartMissingWeeksTest {

    // The pre-optimization query: one DISTINCT pass over every play.
    private static final String FULL_SCAN_SQL = """
            SELECT DISTINCT strftime('%Y-W%W', p.play_date) as period_key
            FROM Play p
            WHERE p.play_date IS NOT NULL
              AND p.song_id IS NOT NULL
              AND strftime('%Y-W%W', p.play_date) NOT IN (
                  SELECT period_key FROM Chart WHERE chart_type = 'song'
              )
            ORDER BY period_key ASC
            """;

    @Test
    void listsCompletedWeeksWithPlaysButNoSongChart() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ChartService chartService = new ChartService(null, null, db.jdbcTemplate, null, null, null);

            // Seeded plays fall in 2024-W01 (charted), W05, W09, W14 and W18.
            assertThat(chartService.getWeeksWithoutCharts())
                    .containsExactly("2024-W05", "2024-W09", "2024-W14", "2024-W18");
        }
    }

    @Test
    void matchesTheFullScanWithAndWithoutTheWeekExpressionIndex() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            db.jdbcTemplate.update("""
                    INSERT INTO Play (id, play_date, song_id, account)
                    VALUES
                        (100, '2023-12-31 23:00:00', 1, 'vatito'),
                        (101, '2024-06-10 09:00:00', NULL, 'vatito'),
                        (102, 'not-a-date', 1, 'vatito'),
                        (103, NULL, 3, 'vatito'),
                        (104, '2024-06-17 09:00:00', 3, 'robertlover'),
                        (105, '2024-06-17 18:00:00', 4, 'vatito'),
                        (106, '2024-08-05 00:00:00', 8, 'vatito')
                    """);
            ChartService chartService = new ChartService(null, null, db.jdbcTemplate, null, null, null);

            List<String> expected = db.jdbcTemplate.queryForList(FULL_SCAN_SQL, String.class).stream()
                    .filter(week -> !week.endsWith("-W00"))
                    .filter(chartService::isWeekComplete)
                    .toList();
            assertThat(expected).contains("2024-W25", "2024-W32").doesNotContain("2024-W24");

            assertThat(chartService.getWeeksWithoutCharts()).isEqualTo(expected);

            db.jdbcTemplate.execute("""
                    CREATE INDEX idx_play_period_week_song
                        ON Play(strftime('%Y-W%W', play_date), play_date, song_id)
                        WHERE play_date IS NOT NULL
                    """);
            assertThat(chartService.getWeeksWithoutCharts()).isEqualTo(expected);
        }
    }
}
