package library.controller;

import library.service.AppConfigService;
import library.service.ChartService;
import library.service.PlayAutomationStateService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class ConfigController {

    private final AppConfigService appConfigService;
    private final PlayAutomationStateService automationStateService;
    private final ChartService chartService;

    public ConfigController(AppConfigService appConfigService, PlayAutomationStateService automationStateService, ChartService chartService) {
        this.appConfigService = appConfigService;
        this.automationStateService = automationStateService;
        this.chartService = chartService;
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
        return "config/index";
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
            @RequestParam int allowedMissingUpTo20Tracks,
            @RequestParam int allowedMissingOver20Tracks,
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
                allowedMissingUpTo20Tracks,
                allowedMissingOver20Tracks
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
