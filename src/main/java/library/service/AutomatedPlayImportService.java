package library.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class AutomatedPlayImportService {

    private static final Logger logger = LoggerFactory.getLogger(AutomatedPlayImportService.class);

    private final PlayService playService;
    private final PlayAutomationStateService automationStateService;
    private final AppConfigService appConfigService;
    private final AtomicBoolean runInProgress = new AtomicBoolean(false);

    public AutomatedPlayImportService(
            PlayService playService,
            PlayAutomationStateService automationStateService,
            AppConfigService appConfigService) {
        this.playService = playService;
        this.automationStateService = automationStateService;
        this.appConfigService = appConfigService;
    }

    @Scheduled(cron = "0 * * * * *")
    public void runScheduledImport() {
        AppConfigService.AutomationConfig automationConfig = appConfigService.getAutomationConfig();
        if (!automationConfig.enabled()) {
            return;
        }
        if (!isWithinAutomationWindow(automationConfig)) {
            return;
        }
        if (!runInProgress.compareAndSet(false, true)) {
            return;
        }

        try {
            if (!shouldRunNow(automationConfig)) {
                return;
            }
            executeImportCycle(automationConfig);
        } finally {
            runInProgress.set(false);
        }
    }

    private boolean shouldRunNow(AppConfigService.AutomationConfig automationConfig) {
        long lastAttemptEpochMillis = automationStateService.getLastAttemptEpochMillis();
        if (lastAttemptEpochMillis <= 0L) {
            return true;
        }

        long intervalMillis = automationConfig.intervalMinutes() * 60_000L;
        long elapsedMillis = System.currentTimeMillis() - lastAttemptEpochMillis;
        return elapsedMillis >= intervalMillis;
    }

    private boolean isWithinAutomationWindow(AppConfigService.AutomationConfig automationConfig) {
        int startHour = Math.min(automationConfig.startHour(), automationConfig.endHour());
        int endHour = Math.max(automationConfig.startHour(), automationConfig.endHour());
        int currentHour = LocalTime.now().getHour();
        return currentHour >= startHour && currentHour <= endHour;
    }

    private void executeImportCycle(AppConfigService.AutomationConfig automationConfig) {
        automationStateService.recordAttempt();

        String automationAccount = automationConfig.account();
        String automationApiKey = automationConfig.apiKey();

        if (automationApiKey == null || automationApiKey.isBlank()) {
            String message = "Automated play import is enabled, but no Last.fm API key is configured.";
            automationStateService.activateSyncIssue(message);
            automationStateService.appendRunLog("ERROR", message, 0, playService.getUnmatchedPlayCountByAccount(automationAccount), null);
            logger.error("Automated play import for {} skipped because the Last.fm API key is missing.", automationAccount);
            return;
        }

        int startingPlayCount = playService.getPlayCountByAccount(automationAccount);
        ImportCycleResult cycleResult = importWithRecovery(automationConfig);
        if (cycleResult.failureMessage() != null) {
            String message = "Automated play import failed after 4 attempts: " + cycleResult.failureMessage();
            automationStateService.activateSyncIssue(message);
            int unmatchedCount = playService.getUnmatchedPlayCountByAccount(automationAccount);
            automationStateService.refreshUnmatchedBanner();
            automationStateService.appendRunLog("ERROR", message, 0, unmatchedCount, null);
            logger.error("Automated play import for {} failed: {}", automationAccount, cycleResult.failureMessage());
            return;
        }

        int endingPlayCount = playService.getPlayCountByAccount(automationAccount);
        int importedSinceLastVisit = Math.max(0, endingPlayCount - startingPlayCount);
        automationStateService.recordSuccess(importedSinceLastVisit);

        int unmatchedCount = playService.getUnmatchedPlayCountByAccount(automationAccount);
        AttemptOutcome outcome = cycleResult.outcome();
        if (outcome.validation.matches) {
            automationStateService.clearSyncIssue();
            int totalUnmatchedFromImport = outcome.result.stats.getOrDefault("totalUnmatched", 0);
            if (totalUnmatchedFromImport > 0) {
                automationStateService.activateUnmatchedBanner();
            } else {
                automationStateService.refreshUnmatchedBanner();
            }

            String message = successMessageForStage(cycleResult.stage());
            automationStateService.appendRunLog("SUCCESS", message, importedSinceLastVisit, unmatchedCount, outcome.validation);
            logger.info("Automated play import for {} completed successfully. stage={}, imported={}, unmatched={}", automationAccount, cycleResult.stage(), importedSinceLastVisit, unmatchedCount);
            return;
        }

        String warningMessage = String.format(
            "%s play import is still out of sync after automatic 5-day and 10-day recovery. Last.fm: %,d. Local: %,d. Check /plays/upload.",
            automationAccount != null && !automationAccount.isBlank() ? automationAccount : "Configured",
            outcome.validation.lastfmPlaycount,
            outcome.validation.localPlayCount
        );
        automationStateService.activateSyncIssue(warningMessage);
        automationStateService.refreshUnmatchedBanner();
        automationStateService.appendRunLog("WARNING", warningMessage, importedSinceLastVisit, unmatchedCount, outcome.validation);
        logger.warn("Automated play import for {} finished out of sync after recovery. imported={}, lastfmPlaycount={}, localPlayCount={}", automationAccount, importedSinceLastVisit, outcome.validation.lastfmPlaycount, outcome.validation.localPlayCount);
    }

    private ImportCycleResult importWithRecovery(AppConfigService.AutomationConfig automationConfig) {
        String automationAccount = automationConfig.account();
        try {
            AttemptOutcome initialAttempt = fetchWithRetries(automationConfig);
            if (initialAttempt.validation.matches) {
                return ImportCycleResult.completed(initialAttempt, "initial");
            }

            playService.deleteRecentPlays(automationAccount, 5);
            AttemptOutcome fiveDayAttempt = fetchWithRetries(automationConfig);
            if (fiveDayAttempt.validation.matches) {
                return ImportCycleResult.completed(fiveDayAttempt, "after-5-day-recovery");
            }

            playService.deleteRecentPlays(automationAccount, 10);
            AttemptOutcome tenDayAttempt = fetchWithRetries(automationConfig);
            return ImportCycleResult.completed(tenDayAttempt, "after-10-day-recovery");
        } catch (Exception ex) {
            String message = ex.getMessage() != null && !ex.getMessage().isBlank()
                    ? ex.getMessage()
                    : "Unknown Last.fm error";
            return ImportCycleResult.failed(message);
        }
    }

    private AttemptOutcome fetchWithRetries(AppConfigService.AutomationConfig automationConfig) throws Exception {
        String automationAccount = automationConfig.account();
        String automationApiKey = automationConfig.apiKey();
        Exception lastFailure = null;
        for (int attempt = 1; attempt <= 4; attempt++) {
            try {
                PlayService.ImportResult result = playService.fetchAndImportPlaysFromLastfm(automationAccount, automationApiKey);
                PlayService.ValidationResult validation = result.validation != null
                        ? result.validation
                        : playService.validatePlayCount(automationAccount, automationApiKey);
                return new AttemptOutcome(result, validation);
            } catch (Exception ex) {
                lastFailure = ex;
                logger.warn("Automated play import attempt {} failed for {}: {}", attempt, automationAccount, ex.getMessage());
            }
        }

        if (lastFailure != null) {
            throw lastFailure;
        }
        throw new IllegalStateException("Unknown Last.fm error");
    }

    private String successMessageForStage(String stage) {
        return switch (stage) {
            case "after-5-day-recovery" -> "Automated play import synced successfully after the 5-day recovery delete.";
            case "after-10-day-recovery" -> "Automated play import synced successfully after the 10-day recovery delete.";
            default -> "Automated play import synced successfully on the first pass.";
        };
    }

    private record AttemptOutcome(PlayService.ImportResult result, PlayService.ValidationResult validation) {
    }

    private record ImportCycleResult(AttemptOutcome outcome, String stage, String failureMessage) {
        private static ImportCycleResult completed(AttemptOutcome outcome, String stage) {
            return new ImportCycleResult(outcome, stage, null);
        }

        private static ImportCycleResult failed(String failureMessage) {
            return new ImportCycleResult(null, null, failureMessage);
        }
    }
}