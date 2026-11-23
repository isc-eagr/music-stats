package library.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Data behind the Import Plays page: next run label, latest sync check and the Run history day/time helpers. */
class ImportPageStateTest {

    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;

    @BeforeEach
    void openDatabase() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
    }

    @AfterEach
    void closeDatabase() {
        dataSource.destroy();
    }

    private static AppConfigService.AutomationConfig config(boolean enabled, int intervalSeconds, int startHour, int endHour) {
        return new AppConfigService.AutomationConfig(enabled, "vatito", "key", intervalSeconds, 20, startHour, endHour);
    }

    private static long millis(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    @Test
    void nextRunFollowsTheIntervalInsideTheWindow() {
        LocalDateTime now = LocalDateTime.of(2026, 11, 22, 21, 40, 0);
        AppConfigService.AutomationConfig tenMinutes = config(true, 600, 7, 23);

        assertThat(PlayAutomationStateService.describeNextRun(tenMinutes, millis(now.minusMinutes(4)), now)).isEqualTo("21:46");
        // Already overdue, or never run: the scheduler picks it up on its next tick
        assertThat(PlayAutomationStateService.describeNextRun(tenMinutes, millis(now.minusMinutes(30)), now)).isEqualTo("Any moment");
        assertThat(PlayAutomationStateService.describeNextRun(tenMinutes, 0L, now)).isEqualTo("Any moment");
    }

    @Test
    void nextRunWaitsForTheWindowAndIsHiddenWhenAutomationIsOff() {
        AppConfigService.AutomationConfig window = config(true, 600, 7, 22);

        LocalDateTime lateNight = LocalDateTime.of(2026, 11, 22, 23, 15, 0);
        assertThat(PlayAutomationStateService.describeNextRun(window, millis(lateNight.minusHours(1)), lateNight)).isEqualTo("Tomorrow 07:00");

        LocalDateTime earlyMorning = LocalDateTime.of(2026, 11, 22, 5, 30, 0);
        assertThat(PlayAutomationStateService.describeNextRun(window, millis(earlyMorning.minusHours(7)), earlyMorning)).isEqualTo("07:00");

        assertThat(PlayAutomationStateService.describeNextRun(config(false, 600, 7, 22), 0L, earlyMorning)).isNull();
    }

    @Test
    void latestSyncCheckComesFromTheNewestRunThatComparedCounts() {
        AppConfigService configService = new AppConfigService(jdbc, false, "vatito", "", 600, 20, 0, 23);
        configService.initialize();
        PlayAutomationStateService stateService = new PlayAutomationStateService(jdbc, configService);
        stateService.initializeStateTable();

        assertThat(stateService.getLatestSyncCheck()).isNull();

        String insert = "INSERT INTO play_import_automation_log (run_started_at, run_finished_at, status, message, interval_minutes, imported_count, unmatched_count, lastfm_playcount, local_playcount) VALUES (?, ?, ?, ?, 10, ?, 0, ?, ?)";
        jdbc.update(insert, "22/11/2026 19:10:00", "22/11/2026 19:10:06", "WARNING", "Out of sync", 14, 1002, 1000);
        jdbc.update(insert, "22/11/2026 21:40:00", "22/11/2026 21:40:04", "ERROR", "Last.fm down", 0, null, null);

        // The error did not compare counts, so the warning before it is still the latest check (warnings are not in the Run history list)
        PlayAutomationStateService.SyncCheck check = stateService.getLatestSyncCheck();
        assertThat(check.lastfmPlaycount()).isEqualTo(1002);
        assertThat(check.localPlaycount()).isEqualTo(1000);
        assertThat(check.isInSync()).isFalse();
        assertThat(check.getDifference()).isEqualTo(2);
        assertThat(check.checkedAt()).isEqualTo("22/11/2026 19:10:06");
        assertThat(stateService.getImportPageState().getLatestSyncCheck()).isEqualTo(check);
    }

    @Test
    void successesAndErrorsEachGetTheirOwnLatestRows() {
        AppConfigService configService = new AppConfigService(jdbc, false, "vatito", "", 600, 20, 0, 23);
        configService.initialize();
        PlayAutomationStateService stateService = new PlayAutomationStateService(jdbc, configService);
        stateService.initializeStateTable();

        String insert = "INSERT INTO play_import_automation_log (run_started_at, run_finished_at, status, message, interval_minutes, imported_count, unmatched_count) VALUES ('22/11/2026 10:00:00', '22/11/2026 10:00:05', ?, ?, 10, ?, 0)";
        jdbc.update(insert, "ERROR", "old error", 0);
        jdbc.update(insert, "WARNING", "still out of sync", 3);
        for (int i = 1; i <= 25; i++) {
            jdbc.update(insert, "SUCCESS", "imported " + i, i);
        }
        jdbc.update(insert, "SUCCESS", "nothing new", 0);

        // 25 newer successes no longer squeeze the older error and warning out of view
        assertThat(stateService.getRecentSuccessLogs(20))
                .hasSize(20)
                .allSatisfy(entry -> assertThat(entry.getImportedCount()).isPositive())
                .first().satisfies(entry -> assertThat(entry.getMessage()).isEqualTo("imported 25"));
        assertThat(stateService.getRecentErrorLogs(20))
                .extracting(PlayAutomationStateService.AutomationRunLogEntry::getStatus)
                .containsExactly("WARNING", "ERROR");

        PlayAutomationStateService.ImportPageState page = stateService.getImportPageState();
        assertThat(page.getSuccessLogs()).hasSize(20);
        assertThat(page.getErrorLogs()).hasSize(2);
    }

    @Test
    void runLogEntriesSplitIntoDayAndTimeForTheRunHistory() {
        PlayAutomationStateService.AutomationRunLogEntry entry = new PlayAutomationStateService.AutomationRunLogEntry(
                1L, "22/11/2026 21:40:12", "22/11/2026 21:40:16", "SUCCESS", "ok", 600, 6, 0, 1006, 1006);

        assertThat(entry.getRunDay()).isEqualTo("22/11/2026");
        assertThat(entry.getRunTime()).isEqualTo("21:40");
        assertThat(entry.getSyncDifference()).isZero();

        LocalDate today = LocalDate.of(2026, 11, 22);
        assertThat(PlayAutomationStateService.AutomationRunLogEntry.dayLabel("22/11/2026", today)).isEqualTo("Today · 22/11/2026");
        assertThat(PlayAutomationStateService.AutomationRunLogEntry.dayLabel("21/11/2026", today)).isEqualTo("Yesterday · 21/11/2026");
        assertThat(PlayAutomationStateService.AutomationRunLogEntry.dayLabel("19/11/2026", today)).isEqualTo("19/11/2026");

        PlayAutomationStateService.AutomationRunLogEntry error = new PlayAutomationStateService.AutomationRunLogEntry(
                2L, null, null, "ERROR", "failed", 600, 0, 0, null, null);
        assertThat(error.getRunDay()).isEmpty();
        assertThat(error.getRunTime()).isEmpty();
        assertThat(error.getSyncDifference()).isNull();
    }
}
