package library.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AutomationIntervalSecondsTest {

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

    private AppConfigService newConfigService() {
        AppConfigService service = new AppConfigService(jdbc, false, "vatito", "", 600, 20, 0, 23);
        service.initialize();
        return service;
    }

    @Test
    void legacyMinutesSettingBecomesSecondsAndIsRemoved() {
        jdbc.execute("CREATE TABLE app_config (config_key TEXT PRIMARY KEY, config_value TEXT NOT NULL, updated_at TEXT NOT NULL)");
        jdbc.update("INSERT INTO app_config VALUES ('automation.intervalMinutes', '3', '19/11/2026 10:00:00')");

        AppConfigService service = newConfigService();

        assertThat(service.getAutomationConfig().intervalSeconds()).isEqualTo(180);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM app_config WHERE config_key = 'automation.intervalMinutes'", Integer.class)).isZero();
    }

    @Test
    void freshDatabaseUsesDefaultSecondsAndSavedSecondsRoundTrip() {
        AppConfigService service = newConfigService();
        assertThat(service.getAutomationConfig().intervalSeconds()).isEqualTo(600);

        AppConfigService.AutomationConfig current = service.getAutomationConfig();
        service.updateAutomationConfig(new AppConfigService.AutomationConfig(
                current.enabled(), current.account(), current.apiKey(),
                AppConfigService.toIntervalSeconds(45, "sec"),
                current.importLogLimit(), current.startHour(), current.endHour()));

        AppConfigService.AutomationConfig saved = service.getAutomationConfig();
        assertThat(saved.intervalSeconds()).isEqualTo(45);
        assertThat(saved.intervalInMinutes()).isFalse();
        assertThat(saved.intervalDisplayValue()).isEqualTo(45);
    }

    @Test
    void unitConversionClampsAndLabelsRead() {
        assertThat(AppConfigService.toIntervalSeconds(10, "min")).isEqualTo(600);
        assertThat(AppConfigService.toIntervalSeconds(2, "sec")).isEqualTo(AppConfigService.MIN_AUTOMATION_INTERVAL_SECONDS);
        assertThat(AppConfigService.toIntervalSeconds(999, "min")).isEqualTo(AppConfigService.MAX_AUTOMATION_INTERVAL_SECONDS);
        assertThat(AppConfigService.formatInterval(600)).isEqualTo("10 min");
        assertThat(AppConfigService.formatInterval(90)).isEqualTo("1 min 30 sec");
        assertThat(AppConfigService.formatInterval(30)).isEqualTo("30 sec");
    }

    @Test
    void scheduledImportWaitsForTheConfiguredSeconds() {
        PlayService playService = mock(PlayService.class);
        PlayAutomationStateService stateService = mock(PlayAutomationStateService.class);
        AppConfigService configService = mock(AppConfigService.class);
        when(configService.getAutomationConfig()).thenReturn(
                new AppConfigService.AutomationConfig(true, "vatito", "", 30, 20, 0, 23));
        AutomatedPlayImportService importService = new AutomatedPlayImportService(playService, stateService, configService);

        when(stateService.getLastAttemptEpochMillis()).thenReturn(System.currentTimeMillis() - 20_000L);
        importService.runScheduledImport();
        verify(stateService, never()).recordAttempt();

        when(stateService.getLastAttemptEpochMillis()).thenReturn(System.currentTimeMillis() - 31_000L);
        importService.runScheduledImport();
        verify(stateService).recordAttempt();
        // Blank API key: the run logs the problem instead of calling Last.fm
        verify(stateService).activateSyncIssue(any());
    }
}
