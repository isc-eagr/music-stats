package library.controller;

import library.service.AppConfigService;
import library.service.ChartService;
import library.service.DatabaseBackupService;
import library.service.PlayAutomationStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.IOException;
import java.sql.SQLException;

@Controller
public class ConfigController {

    private static final Logger logger = LoggerFactory.getLogger(ConfigController.class);

    private final AppConfigService appConfigService;
    private final PlayAutomationStateService automationStateService;
    private final ChartService chartService;
    private final DatabaseBackupService databaseBackupService;

    public ConfigController(AppConfigService appConfigService, PlayAutomationStateService automationStateService,
                            ChartService chartService, DatabaseBackupService databaseBackupService) {
        this.appConfigService = appConfigService;
        this.automationStateService = automationStateService;
        this.chartService = chartService;
        this.databaseBackupService = databaseBackupService;
    }

    @GetMapping("/config")
    public String showConfigPage(@RequestParam(required = false, defaultValue = "false") boolean saved, Model model) {
        model.addAttribute("currentSection", "config");
        model.addAttribute("saved", saved);
        model.addAttribute("automationConfig", appConfigService.getAutomationConfig());
        model.addAttribute("fullListenConfig", appConfigService.getAlbumFullListenConfig());
        model.addAttribute("pageSizeConfig", appConfigService.getPageSizeConfig());
        model.addAttribute("combineLinkedSongs", appConfigService.isCombineLinkedSongsEnabled());
        model.addAttribute("automationImportState", automationStateService.getImportPageState());
        try {
            model.addAttribute("databasePath", databaseBackupService.getDatabasePath().toString());
        } catch (SQLException exception) {
            logger.error("Unable to determine the database backup location", exception);
            model.addAttribute("backupUnavailable", "Unable to determine the database location. Backups require a database stored on disk.");
        }
        return "config/index";
    }

    @PostMapping("/config/backup")
    public String backupDatabase(RedirectAttributes redirectAttributes) {
        try {
            redirectAttributes.addFlashAttribute("backupSuccess", "Database backup saved to " + databaseBackupService.backupDatabase());
        } catch (IllegalStateException exception) {
            redirectAttributes.addFlashAttribute("backupError", exception.getMessage());
        } catch (SQLException | IOException exception) {
            logger.error("Unable to back up the database", exception);
            redirectAttributes.addFlashAttribute("backupError",
                    "Database backup failed. Check available disk space and folder write permissions, then try again. See the server log for details.");
        }
        return "redirect:/config";
    }

    @PostMapping("/config")
    public String saveConfig(
            @RequestParam(defaultValue = "false") boolean automationEnabled,
            @RequestParam(defaultValue = "") String automationAccount,
            @RequestParam(defaultValue = "") String automationApiKey,
            @RequestParam int automationIntervalMinutes,
            @RequestParam int automationImportLogLimit,
            @RequestParam int automationStartHour,
            @RequestParam int automationEndHour,
            @RequestParam(defaultValue = "false") boolean combineLinkedSongs,
            @RequestParam int allowedMissingUpTo6Tracks,
            @RequestParam int allowedMissingUpTo10Tracks,
            @RequestParam int allowedMissingUpTo15Tracks,
            @RequestParam int allowedMissingUpTo20Tracks,
            @RequestParam int allowedMissingOver20Tracks,
            @RequestParam int allowedInterruptingSongs,
            @RequestParam int artistsListPageSize,
            @RequestParam int albumsListPageSize,
            @RequestParam int songsListPageSize,
            @RequestParam int timeframesListPageSize,
            @RequestParam int artistDetailPlaysPageSize,
            @RequestParam int albumDetailPlaysPageSize,
            @RequestParam int songDetailPlaysPageSize,
            @RequestParam int weeklyOverviewPageSize,
            @RequestParam int seasonalOverviewPageSize,
            @RequestParam int pcOverviewPageSize,
            @RequestParam int trlOverviewPageSize,
            @RequestParam int billboardOverviewPageSize) {
        appConfigService.updateAutomationConfig(new AppConfigService.AutomationConfig(
                automationEnabled,
                automationAccount,
                automationApiKey,
                automationIntervalMinutes,
                automationImportLogLimit,
                automationStartHour,
                automationEndHour
        ));

        appConfigService.updateAlbumFullListenConfig(new AppConfigService.AlbumFullListenConfig(
                allowedMissingUpTo6Tracks,
                allowedMissingUpTo10Tracks,
                allowedMissingUpTo15Tracks,
                allowedMissingUpTo20Tracks,
                allowedMissingOver20Tracks,
                allowedInterruptingSongs
        ));

        appConfigService.updatePageSizeConfig(new AppConfigService.PageSizeConfig(
                artistsListPageSize,
                albumsListPageSize,
                songsListPageSize,
                timeframesListPageSize,
                artistDetailPlaysPageSize,
                albumDetailPlaysPageSize,
                songDetailPlaysPageSize,
                weeklyOverviewPageSize,
                seasonalOverviewPageSize,
                pcOverviewPageSize,
                trlOverviewPageSize,
                billboardOverviewPageSize
        ));

        if (appConfigService.updateCombineLinkedSongs(combineLinkedSongs)) {
            chartService.regenerateAffectedWeeklySongChartsForLinkedSongs();
        }

        return "redirect:/config?saved=true";
    }
}
