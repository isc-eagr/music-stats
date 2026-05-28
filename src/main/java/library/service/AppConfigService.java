package library.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class AppConfigService {

    public static final int MIN_PAGE_SIZE = 10;
    public static final int MAX_PAGE_SIZE = 500;
    public static final int MIN_AUTOMATION_INTERVAL_MINUTES = 1;
    public static final int MAX_AUTOMATION_INTERVAL_MINUTES = 180;
    public static final int MIN_AUTOMATION_IMPORT_LOG_LIMIT = 10;
    public static final int MAX_AUTOMATION_IMPORT_LOG_LIMIT = 100;

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    private static final String KEY_AUTOMATION_ENABLED = "automation.enabled";
    private static final String KEY_AUTOMATION_ACCOUNT = "automation.account";
    private static final String KEY_AUTOMATION_API_KEY = "automation.apiKey";
    private static final String KEY_AUTOMATION_INTERVAL_MINUTES = "automation.intervalMinutes";
    private static final String KEY_AUTOMATION_IMPORT_LOG_LIMIT = "automation.importLogLimit";
    private static final String KEY_AUTOMATION_START_HOUR = "automation.startHour";
    private static final String KEY_AUTOMATION_END_HOUR = "automation.endHour";

    private static final String KEY_FULL_LISTEN_UP_TO_6 = "albumFullListen.allowedMissing.upTo6";
    private static final String KEY_FULL_LISTEN_UP_TO_10 = "albumFullListen.allowedMissing.upTo10";
    private static final String KEY_FULL_LISTEN_UP_TO_20 = "albumFullListen.allowedMissing.upTo20";
    private static final String KEY_FULL_LISTEN_OVER_20 = "albumFullListen.allowedMissing.over20";

    private static final String KEY_PAGE_SIZE_ARTISTS_LIST = "pageSize.artists.list";
    private static final String KEY_PAGE_SIZE_ALBUMS_LIST = "pageSize.albums.list";
    private static final String KEY_PAGE_SIZE_SONGS_LIST = "pageSize.songs.list";
    private static final String KEY_PAGE_SIZE_TIMEFRAMES_LIST = "pageSize.timeframes.list";
    private static final String KEY_PAGE_SIZE_ARTIST_DETAIL_PLAYS = "pageSize.artistDetail.plays";
    private static final String KEY_PAGE_SIZE_ALBUM_DETAIL_PLAYS = "pageSize.albumDetail.plays";
    private static final String KEY_PAGE_SIZE_SONG_DETAIL_PLAYS = "pageSize.songDetail.plays";
    private static final String KEY_PAGE_SIZE_WEEKLY_OVERVIEW = "pageSize.charts.weeklyOverview";
    private static final String KEY_PAGE_SIZE_SEASONAL_OVERVIEW = "pageSize.charts.seasonalOverview";
    private static final String KEY_PAGE_SIZE_PC_OVERVIEW = "pageSize.misc.pcOverview";
    private static final String KEY_PAGE_SIZE_TRL_OVERVIEW = "pageSize.misc.trlOverview";
    private static final String KEY_PAGE_SIZE_BILLBOARD_OVERVIEW = "pageSize.misc.billboardHot100Overview";
    private static final String KEY_COMBINE_LINKED_SONGS = "songs.combineLinkedSongs";

    private final JdbcTemplate jdbcTemplate;
    private final boolean defaultAutomationEnabled;
    private final String defaultAutomationAccount;
    private final String defaultAutomationApiKey;
    private final int defaultAutomationIntervalMinutes;
    private final int defaultAutomationImportLogLimit;
    private final int defaultAutomationStartHour;
    private final int defaultAutomationEndHour;

    public AppConfigService(
            JdbcTemplate jdbcTemplate,
            @Value("${musicstats.play-import.automation.enabled:true}") boolean defaultAutomationEnabled,
            @Value("${musicstats.play-import.automation.account:vatito}") String defaultAutomationAccount,
            @Value("${musicstats.play-import.automation.api-key:}") String defaultAutomationApiKey,
            @Value("${musicstats.play-import.automation.interval-minutes:10}") int defaultAutomationIntervalMinutes,
            @Value("${musicstats.play-import.automation.import-log-limit:20}") int defaultAutomationImportLogLimit,
            @Value("${musicstats.play-import.automation.start-hour:7}") int defaultAutomationStartHour,
            @Value("${musicstats.play-import.automation.end-hour:23}") int defaultAutomationEndHour) {
        this.jdbcTemplate = jdbcTemplate;
        this.defaultAutomationEnabled = defaultAutomationEnabled;
        this.defaultAutomationAccount = defaultAutomationAccount;
        this.defaultAutomationApiKey = defaultAutomationApiKey;
        this.defaultAutomationIntervalMinutes = clamp(defaultAutomationIntervalMinutes, MIN_AUTOMATION_INTERVAL_MINUTES, MAX_AUTOMATION_INTERVAL_MINUTES);
        this.defaultAutomationImportLogLimit = clamp(defaultAutomationImportLogLimit, MIN_AUTOMATION_IMPORT_LOG_LIMIT, MAX_AUTOMATION_IMPORT_LOG_LIMIT);
        this.defaultAutomationStartHour = clamp(defaultAutomationStartHour, 0, 23);
        this.defaultAutomationEndHour = clamp(defaultAutomationEndHour, 0, 23);
    }

    @PostConstruct
    public void initialize() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS app_config (
                    config_key TEXT PRIMARY KEY,
                    config_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """);

        putDefault(KEY_AUTOMATION_ENABLED, Boolean.toString(defaultAutomationEnabled));
        putDefault(KEY_AUTOMATION_ACCOUNT, defaultAutomationAccount != null ? defaultAutomationAccount.trim() : "vatito");
        putDefault(KEY_AUTOMATION_API_KEY, defaultAutomationApiKey != null ? defaultAutomationApiKey.trim() : "");
        putDefault(KEY_AUTOMATION_INTERVAL_MINUTES, Integer.toString(resolveLegacyAutomationIntervalMinutes()));
        putDefault(KEY_AUTOMATION_IMPORT_LOG_LIMIT, Integer.toString(defaultAutomationImportLogLimit));
        putDefault(KEY_AUTOMATION_START_HOUR, Integer.toString(defaultAutomationStartHour));
        putDefault(KEY_AUTOMATION_END_HOUR, Integer.toString(defaultAutomationEndHour));

        putDefault(KEY_FULL_LISTEN_UP_TO_6, "0");
        putDefault(KEY_FULL_LISTEN_UP_TO_10, "2");
        putDefault(KEY_FULL_LISTEN_UP_TO_20, "3");
        putDefault(KEY_FULL_LISTEN_OVER_20, "4");

        putDefault(KEY_PAGE_SIZE_ARTISTS_LIST, "100");
        putDefault(KEY_PAGE_SIZE_ALBUMS_LIST, "100");
        putDefault(KEY_PAGE_SIZE_SONGS_LIST, "100");
        putDefault(KEY_PAGE_SIZE_TIMEFRAMES_LIST, "50");
        putDefault(KEY_PAGE_SIZE_ARTIST_DETAIL_PLAYS, "100");
        putDefault(KEY_PAGE_SIZE_ALBUM_DETAIL_PLAYS, "100");
        putDefault(KEY_PAGE_SIZE_SONG_DETAIL_PLAYS, "100");
        putDefault(KEY_PAGE_SIZE_WEEKLY_OVERVIEW, "100");
        putDefault(KEY_PAGE_SIZE_SEASONAL_OVERVIEW, "100");
        putDefault(KEY_PAGE_SIZE_PC_OVERVIEW, "100");
        putDefault(KEY_PAGE_SIZE_TRL_OVERVIEW, "100");
        putDefault(KEY_PAGE_SIZE_BILLBOARD_OVERVIEW, "250");
        putDefault(KEY_COMBINE_LINKED_SONGS, "false");
    }

    public AutomationConfig getAutomationConfig() {
        return new AutomationConfig(
                getBoolean(KEY_AUTOMATION_ENABLED, defaultAutomationEnabled),
                getString(KEY_AUTOMATION_ACCOUNT, defaultAutomationAccount),
                getString(KEY_AUTOMATION_API_KEY, defaultAutomationApiKey),
                getInt(KEY_AUTOMATION_INTERVAL_MINUTES, defaultAutomationIntervalMinutes, MIN_AUTOMATION_INTERVAL_MINUTES, MAX_AUTOMATION_INTERVAL_MINUTES),
                getInt(KEY_AUTOMATION_IMPORT_LOG_LIMIT, defaultAutomationImportLogLimit, MIN_AUTOMATION_IMPORT_LOG_LIMIT, MAX_AUTOMATION_IMPORT_LOG_LIMIT),
                getInt(KEY_AUTOMATION_START_HOUR, defaultAutomationStartHour, 0, 23),
                getInt(KEY_AUTOMATION_END_HOUR, defaultAutomationEndHour, 0, 23)
        );
    }

    public AlbumFullListenConfig getAlbumFullListenConfig() {
        return new AlbumFullListenConfig(
                getInt(KEY_FULL_LISTEN_UP_TO_6, 0, 0, 6),
                getInt(KEY_FULL_LISTEN_UP_TO_10, 2, 0, 10),
                getInt(KEY_FULL_LISTEN_UP_TO_20, 3, 0, 20),
                getInt(KEY_FULL_LISTEN_OVER_20, 4, 0, 50)
        );
    }

    public PageSizeConfig getPageSizeConfig() {
        return new PageSizeConfig(
                getInt(KEY_PAGE_SIZE_ARTISTS_LIST, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_ALBUMS_LIST, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_SONGS_LIST, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_TIMEFRAMES_LIST, 50, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_ARTIST_DETAIL_PLAYS, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_ALBUM_DETAIL_PLAYS, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_SONG_DETAIL_PLAYS, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_WEEKLY_OVERVIEW, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_SEASONAL_OVERVIEW, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_PC_OVERVIEW, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_TRL_OVERVIEW, 100, MIN_PAGE_SIZE, MAX_PAGE_SIZE),
                getInt(KEY_PAGE_SIZE_BILLBOARD_OVERVIEW, 250, MIN_PAGE_SIZE, MAX_PAGE_SIZE)
        );
    }

    public int getArtistsListPageSize() {
        return getPageSizeConfig().artistsListPageSize();
    }

    public int getAlbumsListPageSize() {
        return getPageSizeConfig().albumsListPageSize();
    }

    public int getSongsListPageSize() {
        return getPageSizeConfig().songsListPageSize();
    }

    public int getTimeframesListPageSize() {
        return getPageSizeConfig().timeframesListPageSize();
    }

    public int getArtistDetailPlaysPageSize() {
        return getPageSizeConfig().artistDetailPlaysPageSize();
    }

    public int getAlbumDetailPlaysPageSize() {
        return getPageSizeConfig().albumDetailPlaysPageSize();
    }

    public int getSongDetailPlaysPageSize() {
        return getPageSizeConfig().songDetailPlaysPageSize();
    }

    public int getWeeklyOverviewPageSize() {
        return getPageSizeConfig().weeklyOverviewPageSize();
    }

    public int getSeasonalOverviewPageSize() {
        return getPageSizeConfig().seasonalOverviewPageSize();
    }

    public int getPcOverviewPageSize() {
        return getPageSizeConfig().pcOverviewPageSize();
    }

    public int getTrlOverviewPageSize() {
        return getPageSizeConfig().trlOverviewPageSize();
    }

    public int getBillboardOverviewPageSize() {
        return getPageSizeConfig().billboardOverviewPageSize();
    }

    public boolean isCombineLinkedSongsEnabled() {
        return getBoolean(KEY_COMBINE_LINKED_SONGS, false);
    }

    @Transactional
    public boolean updateCombineLinkedSongs(boolean combineLinkedSongs) {
        boolean previousValue = isCombineLinkedSongsEnabled();
        putValue(KEY_COMBINE_LINKED_SONGS, Boolean.toString(combineLinkedSongs));
        return previousValue != combineLinkedSongs;
    }

    public int normalizePageSize(Integer requestedPageSize, int configuredDefaultPageSize) {
        return normalizePageSize(requestedPageSize, configuredDefaultPageSize, MIN_PAGE_SIZE, MAX_PAGE_SIZE);
    }

    public int normalizePageSize(Integer requestedPageSize, int configuredDefaultPageSize, int minPageSize, int maxPageSize) {
        int fallbackPageSize = clamp(configuredDefaultPageSize, minPageSize, maxPageSize);
        int requestedOrDefault = requestedPageSize == null ? fallbackPageSize : requestedPageSize;
        return clamp(requestedOrDefault, minPageSize, maxPageSize);
    }

    @Transactional
    public void updateAutomationConfig(AutomationConfig automationConfig) {
        int safeStartHour = clamp(automationConfig.startHour(), 0, 23);
        int safeEndHour = clamp(automationConfig.endHour(), 0, 23);
        putValue(KEY_AUTOMATION_ENABLED, Boolean.toString(automationConfig.enabled()));
        putValue(KEY_AUTOMATION_ACCOUNT, sanitizeText(automationConfig.account()));
        putValue(KEY_AUTOMATION_API_KEY, sanitizeText(automationConfig.apiKey()));
        putValue(KEY_AUTOMATION_INTERVAL_MINUTES, Integer.toString(clamp(automationConfig.intervalMinutes(), MIN_AUTOMATION_INTERVAL_MINUTES, MAX_AUTOMATION_INTERVAL_MINUTES)));
        putValue(KEY_AUTOMATION_IMPORT_LOG_LIMIT, Integer.toString(clamp(automationConfig.importLogLimit(), MIN_AUTOMATION_IMPORT_LOG_LIMIT, MAX_AUTOMATION_IMPORT_LOG_LIMIT)));
        putValue(KEY_AUTOMATION_START_HOUR, Integer.toString(Math.min(safeStartHour, safeEndHour)));
        putValue(KEY_AUTOMATION_END_HOUR, Integer.toString(Math.max(safeStartHour, safeEndHour)));
    }

    @Transactional
    public void updateAlbumFullListenConfig(AlbumFullListenConfig config) {
        putValue(KEY_FULL_LISTEN_UP_TO_6, Integer.toString(clamp(config.allowedMissingUpTo6Tracks(), 0, 6)));
        putValue(KEY_FULL_LISTEN_UP_TO_10, Integer.toString(clamp(config.allowedMissingUpTo10Tracks(), 0, 10)));
        putValue(KEY_FULL_LISTEN_UP_TO_20, Integer.toString(clamp(config.allowedMissingUpTo20Tracks(), 0, 20)));
        putValue(KEY_FULL_LISTEN_OVER_20, Integer.toString(clamp(config.allowedMissingOver20Tracks(), 0, 50)));
    }

    @Transactional
    public void updatePageSizeConfig(PageSizeConfig config) {
        putValue(KEY_PAGE_SIZE_ARTISTS_LIST, Integer.toString(clamp(config.artistsListPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_ALBUMS_LIST, Integer.toString(clamp(config.albumsListPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_SONGS_LIST, Integer.toString(clamp(config.songsListPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_TIMEFRAMES_LIST, Integer.toString(clamp(config.timeframesListPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_ARTIST_DETAIL_PLAYS, Integer.toString(clamp(config.artistDetailPlaysPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_ALBUM_DETAIL_PLAYS, Integer.toString(clamp(config.albumDetailPlaysPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_SONG_DETAIL_PLAYS, Integer.toString(clamp(config.songDetailPlaysPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_WEEKLY_OVERVIEW, Integer.toString(clamp(config.weeklyOverviewPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_SEASONAL_OVERVIEW, Integer.toString(clamp(config.seasonalOverviewPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_PC_OVERVIEW, Integer.toString(clamp(config.pcOverviewPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_TRL_OVERVIEW, Integer.toString(clamp(config.trlOverviewPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
        putValue(KEY_PAGE_SIZE_BILLBOARD_OVERVIEW, Integer.toString(clamp(config.billboardOverviewPageSize(), MIN_PAGE_SIZE, MAX_PAGE_SIZE)));
    }

    public int getRequiredSongsForFullListen(int totalTracks) {
        return getAlbumFullListenConfig().requiredSongsFor(totalTracks);
    }

    private void putDefault(String key, String value) {
        jdbcTemplate.update(
                "INSERT OR IGNORE INTO app_config (config_key, config_value, updated_at) VALUES (?, ?, ?)",
                key,
                value,
                currentTimestampText()
        );
    }

    private void putValue(String key, String value) {
        int updated = jdbcTemplate.update(
                "UPDATE app_config SET config_value = ?, updated_at = ? WHERE config_key = ?",
                value,
                currentTimestampText(),
                key
        );
        if (updated == 0) {
            jdbcTemplate.update(
                    "INSERT INTO app_config (config_key, config_value, updated_at) VALUES (?, ?, ?)",
                    key,
                    value,
                    currentTimestampText()
            );
        }
    }

    private String getRawValue(String key) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT config_value FROM app_config WHERE config_key = ?",
                    String.class,
                    key
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private String getString(String key, String defaultValue) {
        String value = getRawValue(key);
        if (value == null) {
            return defaultValue;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? defaultValue : trimmed;
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        String value = getRawValue(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }

    private int getInt(String key, int defaultValue, int minValue, int maxValue) {
        String value = getRawValue(key);
        if (value == null || value.isBlank()) {
            return clamp(defaultValue, minValue, maxValue);
        }
        try {
            return clamp(Integer.parseInt(value.trim()), minValue, maxValue);
        } catch (NumberFormatException ignored) {
            return clamp(defaultValue, minValue, maxValue);
        }
    }

    private int resolveLegacyAutomationIntervalMinutes() {
        try {
            Integer tableExists = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'play_import_automation_state'",
                    Integer.class
            );
            if (tableExists == null || tableExists == 0) {
                return defaultAutomationIntervalMinutes;
            }
            Integer storedValue = jdbcTemplate.queryForObject(
                    "SELECT run_interval_minutes FROM play_import_automation_state WHERE id = 1",
                    Integer.class
            );
            if (storedValue == null) {
                return defaultAutomationIntervalMinutes;
            }
            return clamp(storedValue, MIN_AUTOMATION_INTERVAL_MINUTES, MAX_AUTOMATION_INTERVAL_MINUTES);
        } catch (Exception ignored) {
            return defaultAutomationIntervalMinutes;
        }
    }

    private String sanitizeText(String value) {
        return value == null ? "" : value.trim();
    }

    private String currentTimestampText() {
        return LocalDateTime.now().format(TIMESTAMP_FORMAT);
    }

    private int clamp(int value, int minValue, int maxValue) {
        return Math.max(minValue, Math.min(maxValue, value));
    }

    public record AutomationConfig(
            boolean enabled,
            String account,
            String apiKey,
            int intervalMinutes,
            int importLogLimit,
            int startHour,
            int endHour) {
    }

    public record AlbumFullListenConfig(
            int allowedMissingUpTo6Tracks,
            int allowedMissingUpTo10Tracks,
            int allowedMissingUpTo20Tracks,
            int allowedMissingOver20Tracks) {

        public int requiredSongsFor(int totalTracks) {
            int safeTotalTracks = Math.max(1, totalTracks);
            int allowedMissing = safeTotalTracks <= 6
                    ? allowedMissingUpTo6Tracks
                    : safeTotalTracks <= 10
                    ? allowedMissingUpTo10Tracks
                    : safeTotalTracks <= 20
                    ? allowedMissingUpTo20Tracks
                    : allowedMissingOver20Tracks;
            return Math.max(1, safeTotalTracks - Math.max(0, allowedMissing));
        }
    }

    public record PageSizeConfig(
            int artistsListPageSize,
            int albumsListPageSize,
            int songsListPageSize,
            int timeframesListPageSize,
            int artistDetailPlaysPageSize,
            int albumDetailPlaysPageSize,
            int songDetailPlaysPageSize,
            int weeklyOverviewPageSize,
            int seasonalOverviewPageSize,
            int pcOverviewPageSize,
            int trlOverviewPageSize,
            int billboardOverviewPageSize) {
    }
}
