package library;

import library.service.AppConfigService;
import library.service.ChartService;
import library.service.SongLinkService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ChartOverviewPerformanceTest {
    @ParameterizedTest
    @ValueSource(strings = {"weekly", "seasonal", "yearly"})
    void overviewBatchesTooltipWorkAndReadsFreshChartData(String period) {
        try (var db = TestDatabaseSupport.create()) {
            var config = mock(AppConfigService.class);
            when(config.isCombineLinkedSongsEnabled()).thenReturn(true);
            var links = mock(SongLinkService.class);
            db.jdbcTemplate.execute("CREATE TABLE song_link_group_member (group_id INTEGER, song_id INTEGER)");
            db.jdbcTemplate.update("INSERT INTO song_link_group_member VALUES (1, 1), (1, 2)");
            String first = switch (period) {
                case "weekly" -> "2025-W01";
                case "seasonal" -> "2025-Winter";
                default -> "2025";
            };
            String last = switch (period) {
                case "weekly" -> "2026-W01";
                case "seasonal" -> "2026-Winter";
                default -> "2026";
            };
            db.jdbcTemplate.update("DELETE FROM ChartEntry");
            db.jdbcTemplate.update("DELETE FROM Chart");
            db.jdbcTemplate.update("INSERT INTO Chart (id, chart_type, period_type, period_key, period_start_date, is_finalized) VALUES (1, 'song', ?, ?, '2025-01-01', 1), (2, 'song', ?, ?, '2026-01-01', 1)", period, first, period, last);
            db.jdbcTemplate.update("INSERT INTO ChartEntry (id, chart_id, position, song_id) VALUES (1, 1, 5, 1), (2, 2, 1, 1), (3, 1, 2, 2)");
            var jdbc = spy(db.jdbcTemplate);
            var service = new ChartService(null, null, jdbc, null, config, links);
            var rows = service.getChartOverviewSongRows(period);
            var song = rows.stream().filter(row -> row.getSongId() == 1).findFirst().orElseThrow();
            assertThat(song.getTotalChartSpan()).isEqualTo(2);
            assertThat(song.getDebutPosition()).isEqualTo(5);
            assertThat(song.getPeakPosition()).isEqualTo(1);
            assertThat(song.getSpanAtPeak()).isEqualTo(1);
            assertThat(song.getSpanAtTopThresholds()[1]).isEqualTo(1);
            assertThat(song.getSpanAtTopThresholds()[5]).isEqualTo(2);
            assertThat(song.getFirstAppearanceKey()).isEqualTo(first);
            assertThat(song.getPeakAppearanceKey()).isEqualTo(last);
            assertThat(song.getLinkedSongTitles()).hasSize(2);
            verify(config, times(1)).isCombineLinkedSongsEnabled();
            verifyNoInteractions(links);
            var queries = mockingDetails(jdbc).getInvocations().stream()
                    .filter(call -> call.getMethod().getName().equals("query"))
                    .filter(call -> call.getArguments().length > 0 && call.getArguments()[0] instanceof String)
                    .map(call -> (String) call.getArguments()[0]).distinct().toList();
            assertThat(queries).hasSize(2).allSatisfy(sql -> assertThat(sql).doesNotContain("JOIN Play"));

            db.jdbcTemplate.update("UPDATE ChartEntry SET position = 3 WHERE id = 2");
            assertThat(service.getChartOverviewSongRows(period).stream()
                    .filter(row -> row.getSongId() == 1).findFirst().orElseThrow().getPeakPosition()).isEqualTo(3);
        }
    }
}
