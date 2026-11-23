package library.service;

import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
        // run_interval_minutes is legacy: the interval lives in app_config (automation.intervalSeconds)
        jdbcTemplate.update("INSERT OR IGNORE INTO play_import_automation_state (id) VALUES (1)");

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
                    local_playcount INTEGER,
                    interval_seconds INTEGER
                )
                """);
        ensureColumnExists("play_import_automation_log", "interval_seconds", "interval_seconds INTEGER");
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
        refreshUnmatchedBanner(loadStateRow());
    }

    private void refreshUnmatchedBanner(StateRow row) {
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

    public int getRunIntervalSeconds() {
        return appConfigService.getAutomationConfig().intervalSeconds();
    }

    public long getLastAttemptEpochMillis() {
        long stored = loadStateRow().lastAttemptEpochMillis();
        return Math.max(0L, stored);
    }

    public void appendRunLog(String status, String message, int importedCount, int unmatchedCount, PlayService.ValidationResult validation) {
        StateRow row = loadStateRow();
        int intervalSeconds = getRunIntervalSeconds();
        jdbcTemplate.update(
                "INSERT INTO play_import_automation_log (run_started_at, run_finished_at, status, message, interval_minutes, interval_seconds, imported_count, unmatched_count, lastfm_playcount, local_playcount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row.lastAttemptAt() != null && !row.lastAttemptAt().isBlank() ? row.lastAttemptAt() : currentTimestampText(),
                currentTimestampText(),
                status,
                truncateMessage(message),
                intervalSeconds / 60,
                intervalSeconds,
                Math.max(0, importedCount),
                Math.max(0, unmatchedCount),
                validation != null ? validation.lastfmPlaycount : null,
                validation != null ? validation.localPlayCount : null
        );
    }

    public BannerState getBannerState() {
        StateRow row = loadStateRow();
        refreshUnmatchedBanner(row);
        // Reload only if refreshUnmatchedBanner may have mutated the counts
        if (row.unmatchedBannerActive()) {
            row = loadStateRow();
        }
        UnmatchedBanner unmatchedBanner = row.unmatchedBannerActive() && row.unmatchedCount() > 0
                ? new UnmatchedBanner(row.unmatchedCount(), "/plays/unmatched?account=" + appConfigService.getAutomationConfig().account())
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
        AppConfigService.AutomationConfig config = appConfigService.getAutomationConfig();
        int importLogLimit = config.importLogLimit();
        return new ImportPageState(
                getRunIntervalSeconds(),
                row.lastAttemptAt(),
                row.lastSuccessAt(),
                row.lastIssueAt(),
            importLogLimit,
            getRecentSuccessLogs(importLogLimit),
            getRecentErrorLogs(importLogLimit),
            getLatestSyncCheck(),
            describeNextRun(config, Math.max(0L, row.lastAttemptEpochMillis()), LocalDateTime.now(ZoneId.systemDefault()))
        );
    }

    /**
     * Most recent Last.fm vs local play count comparison from any logged run (success, warning or error),
     * or null when no run has recorded one yet.
     */
    public SyncCheck getLatestSyncCheck() {
        List<SyncCheck> rows = jdbcTemplate.query(
                "SELECT lastfm_playcount, local_playcount, run_finished_at FROM play_import_automation_log "
                        + "WHERE lastfm_playcount IS NOT NULL AND local_playcount IS NOT NULL ORDER BY id DESC LIMIT 1",
                (rs, rowNum) -> new SyncCheck(rs.getInt("lastfm_playcount"), rs.getInt("local_playcount"), rs.getString("run_finished_at")));
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * When the scheduler will next import, mirroring AutomatedPlayImportService: a run fires once the interval has
     * passed since the last attempt, but only while the current hour is inside the start..end window (inclusive).
     * Returns null when automation is off; "Any moment" when a run is already due inside the window.
     */
    static String describeNextRun(AppConfigService.AutomationConfig config, long lastAttemptEpochMillis, LocalDateTime now) {
        if (config == null || !config.enabled()) {
            return null;
        }
        int startHour = Math.min(config.startHour(), config.endHour());
        int endHour = Math.max(config.startHour(), config.endHour());

        LocalDateTime due = lastAttemptEpochMillis > 0L
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(lastAttemptEpochMillis), ZoneId.systemDefault()).plusSeconds(config.intervalSeconds())
                : now;
        if (due.isBefore(now)) {
            due = now;
        }

        LocalDateTime next;
        if (due.getHour() < startHour) {
            next = due.toLocalDate().atTime(startHour, 0);
        } else if (due.getHour() > endHour) {
            next = due.toLocalDate().plusDays(1).atTime(startHour, 0);
        } else {
            next = due;
        }

        if (!next.isAfter(now)) {
            return "Any moment";
        }
        String time = next.format(DateTimeFormatter.ofPattern("HH:mm"));
        if (next.toLocalDate().equals(now.toLocalDate())) {
            return time;
        }
        if (next.toLocalDate().equals(now.toLocalDate().plusDays(1))) {
            return "Tomorrow " + time;
        }
        return next.format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"));
    }

    /** Latest successful runs that imported at least one play, newest first (Run history > Successes). */
    public List<AutomationRunLogEntry> getRecentSuccessLogs(int limit) {
        return queryRecentRunLogs("WHERE status = 'SUCCESS' AND imported_count > 0", clampLogLimit(limit));
    }

    /** Latest failed runs plus runs still out of sync after recovery, newest first (Run history > Errors). */
    public List<AutomationRunLogEntry> getRecentErrorLogs(int limit) {
        return queryRecentRunLogs("WHERE status IN ('ERROR', 'WARNING')", clampLogLimit(limit));
    }

    private static int clampLogLimit(int limit) {
        return Math.max(AppConfigService.MIN_AUTOMATION_IMPORT_LOG_LIMIT, Math.min(limit, AppConfigService.MAX_AUTOMATION_IMPORT_LOG_LIMIT));
    }

        private List<AutomationRunLogEntry> queryRecentRunLogs(String whereClause, int limit) {
        int safeLimit = Math.max(1, limit);
        return jdbcTemplate.query(
            "SELECT id, run_started_at, run_finished_at, status, message, COALESCE(interval_seconds, interval_minutes * 60) AS interval_seconds, imported_count, unmatched_count, lastfm_playcount, local_playcount FROM play_import_automation_log " + whereClause + " ORDER BY id DESC LIMIT " + safeLimit,
            (rs, rowNum) -> new AutomationRunLogEntry(
                rs.getLong("id"),
                rs.getString("run_started_at"),
                rs.getString("run_finished_at"),
                rs.getString("status"),
                rs.getString("message"),
                rs.getInt("interval_seconds"),
                rs.getInt("imported_count"),
                rs.getInt("unmatched_count"),
                getNullableInteger(rs.getObject("lastfm_playcount")),
                getNullableInteger(rs.getObject("local_playcount"))
            )
        );
        }

    private StateRow loadStateRow() {
        return jdbcTemplate.queryForObject(
                "SELECT unmatched_banner_active, unmatched_count, imported_since_seen, sync_issue_active, sync_issue_message, last_attempt_at, last_success_at, last_issue_at, last_attempt_epoch_millis FROM play_import_automation_state WHERE id = 1",
                (rs, rowNum) -> new StateRow(
                        rs.getInt("unmatched_banner_active") == 1,
                        rs.getInt("unmatched_count"),
                        rs.getInt("imported_since_seen"),
                        rs.getInt("sync_issue_active") == 1,
                        rs.getString("sync_issue_message"),
                        rs.getString("last_attempt_at"),
                        rs.getString("last_success_at"),
                        rs.getString("last_issue_at"),
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
            long lastAttemptEpochMillis) {
    }

    public static final class ImportPageState {
        private final int runIntervalSeconds;
        private final String lastAttemptAt;
        private final String lastSuccessAt;
        private final String lastIssueAt;
        private final int importLogLimit;
        private final List<AutomationRunLogEntry> successLogs;
        private final List<AutomationRunLogEntry> errorLogs;
        private final SyncCheck latestSyncCheck;
        private final String nextRunLabel;

        public ImportPageState(int runIntervalSeconds, String lastAttemptAt, String lastSuccessAt, String lastIssueAt, int importLogLimit,
                               List<AutomationRunLogEntry> successLogs, List<AutomationRunLogEntry> errorLogs,
                               SyncCheck latestSyncCheck, String nextRunLabel) {
            this.runIntervalSeconds = runIntervalSeconds;
            this.lastAttemptAt = lastAttemptAt;
            this.lastSuccessAt = lastSuccessAt;
            this.lastIssueAt = lastIssueAt;
            this.importLogLimit = importLogLimit;
            this.successLogs = successLogs;
            this.errorLogs = errorLogs;
            this.latestSyncCheck = latestSyncCheck;
            this.nextRunLabel = nextRunLabel;
        }

        public SyncCheck getLatestSyncCheck() {
            return latestSyncCheck;
        }

        public String getNextRunLabel() {
            return nextRunLabel;
        }

        public String getRunIntervalLabel() {
            return AppConfigService.formatInterval(runIntervalSeconds);
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

        public List<AutomationRunLogEntry> getSuccessLogs() {
            return successLogs;
        }

        public List<AutomationRunLogEntry> getErrorLogs() {
            return errorLogs;
        }
    }

    public static final class AutomationRunLogEntry {
        private final long logId;
        private final String runStartedAt;
        private final String runFinishedAt;
        private final String status;
        private final String message;
        private final int intervalSeconds;
        private final int importedCount;
        private final int unmatchedCount;
        private final Integer lastfmPlaycount;
        private final Integer localPlaycount;

        public AutomationRunLogEntry(long logId, String runStartedAt, String runFinishedAt, String status, String message, int intervalSeconds, int importedCount, int unmatchedCount, Integer lastfmPlaycount, Integer localPlaycount) {
            this.logId = logId;
            this.runStartedAt = runStartedAt;
            this.runFinishedAt = runFinishedAt;
            this.status = status;
            this.message = message;
            this.intervalSeconds = intervalSeconds;
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

        public String getIntervalLabel() {
            return AppConfigService.formatInterval(intervalSeconds);
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

        /** Run date (dd/MM/yyyy) from the stored "dd/MM/yyyy HH:mm:ss" start time, used to group the Run history by day. */
        public String getRunDay() {
            return runStartedAt != null && runStartedAt.length() >= 10 ? runStartedAt.substring(0, 10) : "";
        }

        /** Run start time as HH:mm. */
        public String getRunTime() {
            return runStartedAt != null && runStartedAt.length() >= 16 ? runStartedAt.substring(11, 16) : "";
        }

        /** "Today · 22/11/2026", "Yesterday · 21/11/2026" or just the date. */
        public String getRunDayLabel() {
            return dayLabel(getRunDay(), LocalDate.now(ZoneId.systemDefault()));
        }

        static String dayLabel(String day, LocalDate today) {
            if (day == null || day.isEmpty()) {
                return "";
            }
            DateTimeFormatter dayFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            if (day.equals(today.format(dayFormat))) {
                return "Today · " + day;
            }
            if (day.equals(today.minusDays(1).format(dayFormat))) {
                return "Yesterday · " + day;
            }
            return day;
        }

        /** Last.fm minus local play count for this run, or null when the run did not compare them. */
        public Integer getSyncDifference() {
            return lastfmPlaycount != null && localPlaycount != null ? lastfmPlaycount - localPlaycount : null;
        }
    }

    /** One Last.fm vs local play count comparison from the run log. */
    public record SyncCheck(int lastfmPlaycount, int localPlaycount, String checkedAt) {
        public boolean isInSync() {
            return lastfmPlaycount == localPlaycount;
        }

        public int getDifference() {
            return lastfmPlaycount - localPlaycount;
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