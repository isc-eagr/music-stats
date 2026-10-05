package library;

import library.controller.ConfigController;
import library.service.AppConfigService;
import library.service.ChartService;
import library.service.DatabaseBackupService;
import library.service.PlayAutomationStateService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.ui.ExtendedModelMap;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.flash;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ConfigControllerTest {

    private final AppConfigService configService = mock(AppConfigService.class);
    private final ChartService chartService = mock(ChartService.class);
    private final DatabaseBackupService backupService = mock(DatabaseBackupService.class);
    private final ConfigController controller = new ConfigController(configService,
            mock(PlayAutomationStateService.class), chartService, backupService);

    @Test
    void backupPostRedirectsWithSavedPathWithoutChangingConfiguration() throws Exception {
        Path backup = Path.of("C:/Music Stats DB/music-stats-backup-04-10-2026_12-00-00-123.db");
        when(backupService.backupDatabase()).thenReturn(backup);

        MockMvcBuilders.standaloneSetup(controller).build().perform(post("/config/backup"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/config"))
                .andExpect(flash().attribute("backupSuccess", "Database backup saved to " + backup))
                .andExpect(flash().attributeCount(1));

        verifyNoInteractions(configService, chartService);
    }

    @ParameterizedTest
    @MethodSource("backupFailures")
    void backupFailureRedirectsWithErrorInsteadOfSuccess(Exception failure) throws Exception {
        when(backupService.backupDatabase()).thenThrow(failure);
        String message = failure instanceof IllegalStateException ? failure.getMessage()
                : "Database backup failed. Check available disk space and folder write permissions, then try again. See the server log for details.";

        MockMvcBuilders.standaloneSetup(controller).build().perform(post("/config/backup"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/config"))
                .andExpect(flash().attribute("backupError", message))
                .andExpect(flash().attributeCount(1));
    }

    @Test
    void configDisplaysTheActiveDatabaseLocation() throws Exception {
        Path database = Path.of("C:/Music Stats DB/music-stats.db");
        when(backupService.getDatabasePath()).thenReturn(database);
        var model = new ExtendedModelMap();

        assertThat(controller.showConfigPage(false, model)).isEqualTo("config/index");
        assertThat(model).containsEntry("databasePath", database.toString()).doesNotContainKey("backupUnavailable");
    }

    @Test
    void unavailableBackupDoesNotPreventConfigPageFromOpening() throws Exception {
        when(backupService.getDatabasePath()).thenThrow(new SQLException("No file-backed database"));
        var model = new ExtendedModelMap();

        assertThat(controller.showConfigPage(false, model)).isEqualTo("config/index");
        assertThat(model).containsKey("backupUnavailable").doesNotContainKey("databasePath");
    }

    private static Stream<Exception> backupFailures() {
        return Stream.of(new SQLException("Database locked"), new IOException("Disk full"),
                new IllegalStateException("A database backup is already in progress. Please wait for it to finish."));
    }
}
