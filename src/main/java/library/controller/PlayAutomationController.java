package library.controller;

import library.service.AppConfigService;
import library.service.PlayAutomationStateService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/plays/api/automation")
public class PlayAutomationController {

    private final PlayAutomationStateService automationStateService;

    public PlayAutomationController(PlayAutomationStateService automationStateService) {
        this.automationStateService = automationStateService;
    }

    @PostMapping("/dismiss-imported-banner")
    public ResponseEntity<Map<String, Object>> dismissImportedBanner() {
        automationStateService.clearImportedSinceSeen();
        return ResponseEntity.ok(Map.of("success", true));
    }

    @PostMapping("/dismiss-sync-issue-banner")
    public ResponseEntity<Map<String, Object>> dismissSyncIssueBanner() {
        automationStateService.clearSyncIssue();
        return ResponseEntity.ok(Map.of("success", true));
    }

    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> updateAutomationConfig(@RequestBody Map<String, Object> request) {
        Integer runIntervalMinutes = request.get("runIntervalMinutes") instanceof Number number
                ? number.intValue()
                : null;

        if (runIntervalMinutes == null
            || runIntervalMinutes < AppConfigService.MIN_AUTOMATION_INTERVAL_MINUTES
            || runIntervalMinutes > AppConfigService.MAX_AUTOMATION_INTERVAL_MINUTES) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "Run interval must be between 1 and 180 minutes."
            ));
        }

        automationStateService.updateRunIntervalMinutes(runIntervalMinutes);
        return ResponseEntity.ok(Map.of(
                "success", true,
                "runIntervalMinutes", runIntervalMinutes
        ));
    }
}