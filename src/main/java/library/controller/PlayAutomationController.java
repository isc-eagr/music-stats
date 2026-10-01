package library.controller;

import library.service.PlayAutomationStateService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
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

    @GetMapping("/unmatched-banner")
    public ResponseEntity<Map<String, Object>> getUnmatchedBanner() {
        PlayAutomationStateService.BannerState bannerState = automationStateService.getBannerState();
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("unmatchedBanner", bannerState.getUnmatchedBanner());
        return ResponseEntity.ok(response);
    }
}
