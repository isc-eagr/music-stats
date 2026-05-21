package library.service;

import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service
public class PlayAutomationStateService {

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    private final JdbcTemplate jdbcTemplate;
    private final AppConfigService appConfigService;

    public PlayAutomationStateService(
            JdbcTemplate jdbcTemplate,
            AppConfigService appConfigService) {
        this.jdbcTemplate = jdbcTemplate;
        this.appConfigService = appConfigService;
    }

    @PostConstruct
    public void initializeStateTable() {
        int configuredIntervalMinutes = appConfigService.getAutomationConfig().intervalMinutes();
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS play_import_automation_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    unmatched_banner_active INTEGER NOT NULL DEFAULT 0,
                    unmatched_count INTEGER NOT NULL DEFAULT 0,
                    imported_since_seen INTEGER NOT NULL DEFAULT 0,
                    sync_issue_active INTEGER NOT NULL DEFAULT 0,
                    sync_issue_message TEXT,
                    last_attempt_at TEXT,
                    last_success_at TEXT,
                    last_issue_at TEXT,
                    run_interval_minutes INTEGER NOT NULL DEFAULT 10,
                    last_attempt_epoch_millis INTEGER NOT NULL DEFAULT 0
                )
                """);
        ensureColumnExists("play_import_automation_state", "run_interval_minutes", "run_interval_minutes INTEGER NOT NULL DEFAULT 10");
        ensureColumnExists("play_import_automation_state", "last_attempt_epoch_millis", "last_attempt_epoch_millis INTEGER NOT NULL DEFAULT 0");
            jdbcTemplate.update("INSERT OR IGNORE INTO play_import_automation_state (id, run_interval_minutes) VALUES (1, ?)", configuredIntervalMinutes);
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET run_interval_minutes = ? WHERE id = 1 AND (run_interval_minutes IS NULL OR run_interval_minutes < 1)",
                configuredIntervalMinutes
        );

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS play_import_automation_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_started_at TEXT NOT NULL,
                    run_finished_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    interval_minutes INTEGER NOT NULL,
                    imported_count INTEGER NOT NULL DEFAULT 0,
                    unmatched_count INTEGER NOT NULL DEFAULT 0,
                    lastfm_playcount INTEGER,
                    local_playcount INTEGER
                )
                """);
    }

    public void recordAttempt() {
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET last_attempt_at = ?, last_attempt_epoch_millis = ? WHERE id = 1",
                currentTimestampText(),
                Instant.now().toEpochMilli()
        );
    }

    public void recordSuccess(int importedCount) {
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET imported_since_seen = imported_since_seen + ?, last_success_at = ? WHERE id = 1",
                Math.max(0, importedCount),
                currentTimestampText()
        );
    }

    public void clearImportedSinceSeen() {
        jdbcTemplate.update("UPDATE play_import_automation_state SET imported_since_seen = 0 WHERE id = 1");
    }

    public void activateUnmatchedBanner() {
        int unmatchedCount = getCurrentUnmatchedCount();
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET unmatched_banner_active = 1, unmatched_count = ? WHERE id = 1",
                unmatchedCount
        );
    }

    public void refreshUnmatchedBanner() {
        StateRow row = loadStateRow();
        if (!row.unmatchedBannerActive()) {
            return;
        }

        int unmatchedCount = getCurrentUnmatchedCount();
        if (unmatchedCount <= 0) {
            clearUnmatchedBanner();
            return;
        }

        if (row.unmatchedCount() != unmatchedCount) {
            jdbcTemplate.update(
                    "UPDATE play_import_automation_state SET unmatched_count = ? WHERE id = 1",
                    unmatchedCount
            );
        }
    }

    public void clearUnmatchedBanner() {
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET unmatched_banner_active = 0, unmatched_count = 0 WHERE id = 1"
        );
    }

    public void activateSyncIssue(String message) {
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET sync_issue_active = 1, sync_issue_message = ?, last_issue_at = ? WHERE id = 1",
                truncateMessage(message),
                currentTimestampText()
        );
    }

    public void clearSyncIssue() {
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET sync_issue_active = 0, sync_issue_message = NULL WHERE id = 1"
        );
    }

    public int getRunIntervalMinutes() {
        return appConfigService.getAutomationConfig().intervalMinutes();
    }

    public long getLastAttemptEpochMillis() {
        long stored = loadStateRow().lastAttemptEpochMillis();
        return Math.max(0L, stored);
    }

    public void updateRunIntervalMinutes(int runIntervalMinutes) {
        AppConfigService.AutomationConfig automationConfig = appConfigService.getAutomationConfig();
        int safeRunIntervalMinutes = Math.max(AppConfigService.MIN_AUTOMATION_INTERVAL_MINUTES,
            Math.min(AppConfigService.MAX_AUTOMATION_INTERVAL_MINUTES, runIntervalMinutes));
        appConfigService.updateAutomationConfig(new AppConfigService.AutomationConfig(
            automationConfig.enabled(),
            automationConfig.account(),
            automationConfig.apiKey(),
            safeRunIntervalMinutes,
            automationConfig.importLogLimit(),
            automationConfig.startHour(),
            automationConfig.endHour()
        ));
        jdbcTemplate.update(
                "UPDATE play_import_automation_state SET run_interval_minutes = ? WHERE id = 1",
            safeRunIntervalMinutes
        );
    }

    public void appendRunLog(String status, String message, int importedCount, int unmatchedCount, PlayService.ValidationResult validation) {
        StateRow row = loadStateRow();
        jdbcTemplate.update(
                "INSERT INTO play_import_automation_log (run_started_at, run_finished_at, status, message, interval_minutes, imported_count, unmatched_count, lastfm_playcount, local_playcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row.lastAttemptAt() != null && !row.lastAttemptAt().isBlank() ? row.lastAttemptAt() : currentTimestampText(),
                currentTimestampText(),
                status,
                truncateMessage(message),
                getRunIntervalMinutes(),
                Math.max(0, importedCount),
                Math.max(0, unmatchedCount),
                validation != null ? validation.lastfmPlaycount : null,
                validation != null ? validation.localPlayCount : null
        );
    }

    public BannerState getBannerState() {
        refreshUnmatchedBanner();

        StateRow row = loadStateRow();
        String automationAccount = appConfigService.getAutomationConfig().account();
        UnmatchedBanner unmatchedBanner = row.unmatchedBannerActive() && row.unmatchedCount() > 0
                ? new UnmatchedBanner(row.unmatchedCount(), "/plays/unmatched?account=" + automationAccount)
                : null;
        ImportedBanner importedBanner = row.importedSinceSeen() > 0
                ? new ImportedBanner(row.importedSinceSeen())
                : null;
        SyncIssueBanner syncIssueBanner = row.syncIssueActive() && row.syncIssueMessage() != null && !row.syncIssueMessage().isBlank()
                ? new SyncIssueBanner(row.syncIssueMessage())
                : null;
        return new BannerState(unmatchedBanner, importedBanner, syncIssueBanner);
    }

    public ImportPageState getImportPageState() {
        StateRow row = loadStateRow();
        int importLogLimit = appConfigService.getAutomationConfig().importLogLimit();
        return new ImportPageState(
                getRunIntervalMinutes(),
                row.lastAttemptAt(),
                row.lastSuccessAt(),
                row.lastIssueAt(),
            importLogLimit,
            getRecentRunLogs(importLogLimit)
        );
    }

    public List<AutomationRunLogEntry> getRecentRunLogs(int limit) {
        int safeLimit = Math.max(AppConfigService.MIN_AUTOMATION_IMPORT_LOG_LIMIT, Math.min(limit, AppConfigService.MAX_AUTOMATION_IMPORT_LOG_LIMIT));

        List<AutomationRunLogEntry> successLogs = queryRecentRunLogs(
            "WHERE status = 'SUCCESS' AND imported_count > 0",
            safeLimit
        );
        int remainingLimit = Math.max(0, safeLimit - successLogs.size());
        List<AutomationRunLogEntry> errorLogs = remainingLimit > 0
            ? queryRecentRunLogs("WHERE status = 'ERROR'", remainingLimit)
            : List.of();

        List<AutomationRunLogEntry> combined = new ArrayList<>(successLogs.size() + errorLogs.size());
        combined.addAll(successLogs);
        combined.addAll(errorLogs);
        combined.sort(Comparator.comparingLong(AutomationRunLogEntry::getLogId).reversed());
        return combined;
    }

        private List<AutomationRunLogEntry> queryRecentRunLogs(String whereClause, int limit) {
        int safeLimit = Math.max(1, limit);
        return jdbcTemplate.query(
            "SELECT id, run_started_at, run_finished_at, status, message, interval_minutes, imported_count, unmatched_count, lastfm_playcount, local_playcount FROM play_import_automation_log " + whereClause + " ORDER BY id DESC LIMIT " + safeLimit,
            (rs, rowNum) -> new AutomationRunLogEntry(
                rs.getLong("id"),
                rs.getString("run_started_at"),
                rs.getString("run_finished_at"),
                rs.getString("status"),
                rs.getString("message"),
                rs.getInt("interval_minutes"),
                rs.getInt("imported_count"),
                rs.getInt("unmatched_count"),
                getNullableInteger(rs.getObject("lastfm_playcount")),
                getNullableInteger(rs.getObject("local_playcount"))
            )
        );
        }

    private StateRow loadStateRow() {
        return jdbcTemplate.queryForObject(
                "SELECT unmatched_banner_active, unmatched_count, imported_since_seen, sync_issue_active, sync_issue_message, last_attempt_at, last_success_at, last_issue_at, run_interval_minutes, last_attempt_epoch_millis FROM play_import_automation_state WHERE id = 1",
                (rs, rowNum) -> new StateRow(
                        rs.getInt("unmatched_banner_active") == 1,
                        rs.getInt("unmatched_count"),
                        rs.getInt("imported_since_seen"),
                        rs.getInt("sync_issue_active") == 1,
                        rs.getString("sync_issue_message"),
                        rs.getString("last_attempt_at"),
                        rs.getString("last_success_at"),
                        rs.getString("last_issue_at"),
                        rs.getInt("run_interval_minutes"),
                        rs.getLong("last_attempt_epoch_millis")
                )
        );
    }

    private int getCurrentUnmatchedCount() {
        String automationAccount = appConfigService.getAutomationConfig().account();
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM play WHERE song_id IS NULL AND COALESCE(account, '') = ?",
                Integer.class,
                automationAccount
        );
        return count != null ? count : 0;
    }

    private void ensureColumnExists(String tableName, String columnName, String columnDefinition) {
        List<Map<String, Object>> columns = jdbcTemplate.queryForList("PRAGMA table_info(" + tableName + ")");
        boolean columnExists = columns.stream()
                .map(column -> column.get("name"))
                .filter(name -> name != null)
                .map(Object::toString)
                .anyMatch(existingName -> existingName.equalsIgnoreCase(columnName));
        if (!columnExists) {
            jdbcTemplate.execute("ALTER TABLE " + tableName + " ADD COLUMN " + columnDefinition);
        }
    }

    private Integer getNullableInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private String currentTimestampText() {
        return LocalDateTime.now(ZoneId.systemDefault()).format(TIMESTAMP_FORMAT);
    }

    private String truncateMessage(String message) {
        if (message == null) {
            return "Automated play import failed.";
        }
        String trimmed = message.trim();
        if (trimmed.length() <= 400) {
            return trimmed;
        }
        return trimmed.substring(0, 397) + "...";
    }

    private record StateRow(
            boolean unmatchedBannerActive,
            int unmatchedCount,
            int importedSinceSeen,
            boolean syncIssueActive,
            String syncIssueMessage,
            String lastAttemptAt,
            String lastSuccessAt,
            String lastIssueAt,
            int runIntervalMinutes,
            long lastAttemptEpochMillis) {
    }

    public static final class ImportPageState {
        private final int runIntervalMinutes;
        private final String lastAttemptAt;
        private final String lastSuccessAt;
        private final String lastIssueAt;
        private final int importLogLimit;
        private final List<AutomationRunLogEntry> recentLogs;

        public ImportPageState(int runIntervalMinutes, String lastAttemptAt, String lastSuccessAt, String lastIssueAt, int importLogLimit, List<AutomationRunLogEntry> recentLogs) {
            this.runIntervalMinutes = runIntervalMinutes;
            this.lastAttemptAt = lastAttemptAt;
            this.lastSuccessAt = lastSuccessAt;
            this.lastIssueAt = lastIssueAt;
            this.importLogLimit = importLogLimit;
            this.recentLogs = recentLogs;
        }

        public int getRunIntervalMinutes() {
            return runIntervalMinutes;
        }

        public String getLastAttemptAt() {
            return lastAttemptAt;
        }

        public String getLastSuccessAt() {
            return lastSuccessAt;
        }

        public String getLastIssueAt() {
            return lastIssueAt;
        }

        public int getImportLogLimit() {
            return importLogLimit;
        }

        public List<AutomationRunLogEntry> getRecentLogs() {
            return recentLogs;
        }
    }

    public static final class AutomationRunLogEntry {
        private final long logId;
        private final String runStartedAt;
        private final String runFinishedAt;
        private final String status;
        private final String message;
        private final int intervalMinutes;
        private final int importedCount;
        private final int unmatchedCount;
        private final Integer lastfmPlaycount;
        private final Integer localPlaycount;

        public AutomationRunLogEntry(long logId, String runStartedAt, String runFinishedAt, String status, String message, int intervalMinutes, int importedCount, int unmatchedCount, Integer lastfmPlaycount, Integer localPlaycount) {
            this.logId = logId;
            this.runStartedAt = runStartedAt;
            this.runFinishedAt = runFinishedAt;
            this.status = status;
            this.message = message;
            this.intervalMinutes = intervalMinutes;
            this.importedCount = importedCount;
            this.unmatchedCount = unmatchedCount;
            this.lastfmPlaycount = lastfmPlaycount;
            this.localPlaycount = localPlaycount;
        }

        public long getLogId() {
            return logId;
        }

        public String getRunStartedAt() {
            return runStartedAt;
        }

        public String getRunFinishedAt() {
            return runFinishedAt;
        }

        public String getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public int getIntervalMinutes() {
            return intervalMinutes;
        }

        public int getImportedCount() {
            return importedCount;
        }

        public int getUnmatchedCount() {
            return unmatchedCount;
        }

        public Integer getLastfmPlaycount() {
            return lastfmPlaycount;
        }

        public Integer getLocalPlaycount() {
            return localPlaycount;
        }
    }

    public static final class BannerState {
        private final UnmatchedBanner unmatchedBanner;
        private final ImportedBanner importedBanner;
        private final SyncIssueBanner syncIssueBanner;

        public BannerState(UnmatchedBanner unmatchedBanner, ImportedBanner importedBanner, SyncIssueBanner syncIssueBanner) {
            this.unmatchedBanner = unmatchedBanner;
            this.importedBanner = importedBanner;
            this.syncIssueBanner = syncIssueBanner;
        }

        public UnmatchedBanner getUnmatchedBanner() {
            return unmatchedBanner;
        }

        public ImportedBanner getImportedBanner() {
            return importedBanner;
        }

        public SyncIssueBanner getSyncIssueBanner() {
            return syncIssueBanner;
        }
    }

    public static final class UnmatchedBanner {
        private final int count;
        private final String href;

        public UnmatchedBanner(int count, String href) {
            this.count = count;
            this.href = href;
        }

        public int getCount() {
            return count;
        }

        public String getHref() {
            return href;
        }
    }

    public static final class ImportedBanner {
        private final int count;

        public ImportedBanner(int count) {
            this.count = count;
        }

        public int getCount() {
            return count;
        }
    }

    public static final class SyncIssueBanner {
        private final String message;

        public SyncIssueBanner(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }
}