package library.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class BillboardHot100ImportSupport {

    public static final String ALL_CHARTS_URL = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json";
    private static final String USER_AGENT = "MusicStatsApp/1.0 (billboard-hot100-import)";
    private static final int BATCH_SIZE = 1000;
    private static final List<Pattern> FEATURE_PATTERNS = List.of(
        Pattern.compile("\\((?:feat\\.?|featuring|ft\\.?|with)\\s+([^)]+)\\)", Pattern.CASE_INSENSITIVE),
        Pattern.compile("\\[(?:feat\\.?|featuring|ft\\.?|with)\\s+([^]]+)\\]", Pattern.CASE_INSENSITIVE),
        Pattern.compile("(?:feat\\.?|featuring|ft\\.?|with)\\s+([^()\\[\\]]+)$", Pattern.CASE_INSENSITIVE)
    );

    private BillboardHot100ImportSupport() {
    }

    public record ImportReport(int chartCount, int entryCount, int preservedLinks) {
    }

    public record NameIssueSample(
            String billboardArtistName,
            String billboardSongTitle,
            int weeksOnChart,
            int peakPosition,
            Integer songId,
            String libraryArtistName,
            String librarySongTitle
    ) {
    }

    public record NameIssueReport(
            int sourceVariantGroupCount,
            int normalizedOnlyCandidateCount,
            List<NameIssueSample> samples
    ) {
    }

    public static void ensureSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                "CREATE TABLE IF NOT EXISTS billboard_hot100_entry (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "chart_date TEXT NOT NULL, " +
                "position INTEGER NOT NULL, " +
                "artist_name TEXT NOT NULL, " +
                "song_title TEXT NOT NULL, " +
                "peak_position INTEGER NOT NULL, " +
                "weeks_on_chart INTEGER NOT NULL, " +
                "song_id INTEGER, " +
                "FOREIGN KEY (song_id) REFERENCES Song(id))"
            );
            statement.executeUpdate(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_pos " +
                "ON billboard_hot100_entry(chart_date, position)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_song_id " +
                "ON billboard_hot100_entry(song_id)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_date " +
                "ON billboard_hot100_entry(chart_date)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_artist_song " +
                "ON billboard_hot100_entry(artist_name, song_title)"
            );
        }
    }

    public static ImportReport importAllCharts(Connection connection) throws SQLException, IOException {
        ensureSchema(connection);
        Map<String, Integer> existingLinks = loadExistingLinks(connection);

        boolean originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        try (Statement deleteStatement = connection.createStatement()) {
            deleteStatement.executeUpdate("DELETE FROM billboard_hot100_entry");

            ImportReport report = streamAndInsertCharts(connection, existingLinks);
            connection.commit();
            return report;
        } catch (Exception e) {
            connection.rollback();
            if (e instanceof SQLException sqlException) {
                throw sqlException;
            }
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw new SQLException("Billboard Hot 100 import failed", e);
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    public static ImportReport importNewCharts(Connection connection) throws SQLException, IOException {
        ensureSchema(connection);
        Map<String, Integer> existingLinks = loadExistingLinks(connection);
        String latestExistingChartDate = loadLatestChartDate(connection);
        Set<String> existingChartDates = loadExistingChartDates(connection);

        boolean originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        try {
            ImportReport report = streamAndInsertCharts(connection, existingLinks, latestExistingChartDate, existingChartDates);
            connection.commit();
            return report;
        } catch (Exception e) {
            connection.rollback();
            if (e instanceof SQLException sqlException) {
                throw sqlException;
            }
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw new SQLException("Billboard Hot 100 incremental import failed", e);
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    public static int autoLinkExactMatches(Connection connection) throws SQLException {
        ensureSchema(connection);
        int linkedRows = runExactAutoLink(connection);
        linkedRows += runContributorAwareFuzzyAutoLink(connection);
        return linkedRows;
    }

    private static int runExactAutoLink(Connection connection) throws SQLException {
        String sql =
            "UPDATE billboard_hot100_entry " +
            "SET song_id = ( " +
            "    SELECT MIN(s.id) " +
            "    FROM Song s " +
            "    JOIN Artist a ON a.id = s.artist_id " +
            "    WHERE LOWER(TRIM(s.name)) = LOWER(TRIM(billboard_hot100_entry.song_title)) " +
            "      AND LOWER(TRIM(a.name)) = LOWER(TRIM(billboard_hot100_entry.artist_name)) " +
            ") " +
            "WHERE song_id IS NULL " +
            "  AND 1 = ( " +
            "    SELECT COUNT(*) " +
            "    FROM Song s " +
            "    JOIN Artist a ON a.id = s.artist_id " +
            "    WHERE LOWER(TRIM(s.name)) = LOWER(TRIM(billboard_hot100_entry.song_title)) " +
            "      AND LOWER(TRIM(a.name)) = LOWER(TRIM(billboard_hot100_entry.artist_name)) " +
            ")";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            return statement.executeUpdate();
        }
    }

    private static int runContributorAwareFuzzyAutoLink(Connection connection) throws SQLException {
        List<LibrarySongCandidate> librarySongs = loadLibrarySongs(connection);
        Map<String, Integer> uniqueLookup = new HashMap<>();
        Set<String> ambiguousKeys = new HashSet<>();

        for (LibrarySongCandidate librarySong : librarySongs) {
            registerUniqueCandidate(
                contributorAwareKey(librarySong.artistName(), librarySong.songTitle()),
                librarySong.songId(),
                uniqueLookup,
                ambiguousKeys
            );
        }

        int updatedRows = 0;
        for (DistinctEntryGroup group : loadDistinctUnmatchedGroups(connection)) {
            String fuzzyKey = contributorAwareKey(group.artistName(), group.songTitle());
            if (fuzzyKey == null || ambiguousKeys.contains(fuzzyKey)) {
                continue;
            }

            Integer songId = uniqueLookup.get(fuzzyKey);
            if (songId == null) {
                continue;
            }

            try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE billboard_hot100_entry SET song_id = ? " +
                "WHERE song_id IS NULL " +
                "  AND LOWER(TRIM(artist_name)) = LOWER(TRIM(?)) " +
                "  AND LOWER(TRIM(song_title)) = LOWER(TRIM(?))")) {
                statement.setInt(1, songId);
                statement.setString(2, group.artistName());
                statement.setString(3, group.songTitle());
                updatedRows += statement.executeUpdate();
            }
        }

        return updatedRows;
    }

    public static NameIssueReport analyzePotentialNameIssues(Connection connection, int sampleLimit) throws SQLException {
        ensureSchema(connection);

        List<DistinctEntryGroup> sourceGroups = loadDistinctSourceGroups(connection);
        Map<String, Set<String>> sourceVariantsByNormalizedKey = new HashMap<>();
        for (DistinctEntryGroup group : sourceGroups) {
            String normalizedKey = normalizedSearchKey(group.artistName(), group.songTitle());
            if (normalizedKey == null) {
                continue;
            }
            sourceVariantsByNormalizedKey
                .computeIfAbsent(normalizedKey, ignored -> new HashSet<>())
                .add(exactKey(group.artistName(), group.songTitle()));
        }

        int sourceVariantGroupCount = 0;
        for (Set<String> variants : sourceVariantsByNormalizedKey.values()) {
            if (variants.size() > 1) {
                sourceVariantGroupCount++;
            }
        }

        List<LibrarySongCandidate> librarySongs = loadLibrarySongs(connection);
        Set<String> exactLibraryKeys = new HashSet<>();
        Map<String, List<LibrarySongCandidate>> normalizedLibraryCandidates = new HashMap<>();
        for (LibrarySongCandidate librarySong : librarySongs) {
            exactLibraryKeys.add(exactKey(librarySong.artistName(), librarySong.songTitle()));
            String normalizedKey = normalizedSearchKey(librarySong.artistName(), librarySong.songTitle());
            if (normalizedKey == null) {
                continue;
            }
            normalizedLibraryCandidates
                .computeIfAbsent(normalizedKey, ignored -> new ArrayList<>())
                .add(librarySong);
        }

        List<DistinctEntryGroup> unmatchedGroups = loadDistinctUnmatchedGroups(connection);
        List<NameIssueSample> samples = new ArrayList<>();
        int normalizedOnlyCandidateCount = 0;

        for (DistinctEntryGroup group : unmatchedGroups) {
            String exactKey = exactKey(group.artistName(), group.songTitle());
            if (exactLibraryKeys.contains(exactKey)) {
                continue;
            }

            String normalizedKey = normalizedSearchKey(group.artistName(), group.songTitle());
            if (normalizedKey == null) {
                continue;
            }

            List<LibrarySongCandidate> candidates = normalizedLibraryCandidates.get(normalizedKey);
            if (candidates == null || candidates.isEmpty()) {
                continue;
            }

            normalizedOnlyCandidateCount++;
            if (samples.size() >= sampleLimit) {
                continue;
            }

            LibrarySongCandidate firstCandidate = candidates.get(0);
            samples.add(new NameIssueSample(
                group.artistName(),
                group.songTitle(),
                group.weeksOnChart(),
                group.peakPosition(),
                firstCandidate.songId(),
                firstCandidate.artistName(),
                firstCandidate.songTitle()
            ));
        }

        return new NameIssueReport(sourceVariantGroupCount, normalizedOnlyCandidateCount, samples);
    }

    private static ImportReport streamAndInsertCharts(Connection connection, Map<String, Integer> existingLinks)
            throws SQLException, IOException {
        return streamAndInsertCharts(connection, existingLinks, null, Set.of());
        }

        private static ImportReport streamAndInsertCharts(
            Connection connection,
            Map<String, Integer> existingLinks,
            String latestExistingChartDate,
            Set<String> existingChartDates)
            throws SQLException, IOException {
        String insertSql =
            "INSERT INTO billboard_hot100_entry (chart_date, position, artist_name, song_title, peak_position, weeks_on_chart, song_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";

        int chartCount = 0;
        int entryCount = 0;
        int preservedLinks = 0;
        ObjectMapper objectMapper = new ObjectMapper();

        try (PreparedStatement insert = connection.prepareStatement(insertSql);
             InputStream inputStream = openAllChartsStream();
             JsonParser parser = new JsonFactory().createParser(inputStream)) {

            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IOException("Unexpected Billboard Hot 100 payload format");
            }

            while (parser.nextToken() != JsonToken.END_ARRAY) {
                JsonNode chartNode = objectMapper.readTree(parser);
                if (chartNode == null || chartNode.isMissingNode()) {
                    continue;
                }

                String chartDate = chartNode.path("date").asText(null);
                JsonNode dataNode = chartNode.path("data");
                if (chartDate == null || !dataNode.isArray()) {
                    continue;
                }

                if (shouldSkipChart(chartDate, latestExistingChartDate, existingChartDates)) {
                    continue;
                }

                chartCount++;
                for (JsonNode entryNode : dataNode) {
                    String artistName = entryNode.path("artist").asText(null);
                    String songTitle = entryNode.path("song").asText(null);
                    int position = entryNode.path("this_week").asInt(0);
                    int peakPosition = entryNode.path("peak_position").asInt(0);
                    int weeksOnChart = entryNode.path("weeks_on_chart").asInt(0);

                    if (artistName == null || songTitle == null || position <= 0 || peakPosition <= 0 || weeksOnChart <= 0) {
                        continue;
                    }

                    Integer linkedSongId = existingLinks.get(exactKey(artistName, songTitle));
                    if (linkedSongId != null) {
                        preservedLinks++;
                    }

                    insert.setString(1, chartDate);
                    insert.setInt(2, position);
                    insert.setString(3, artistName);
                    insert.setString(4, songTitle);
                    insert.setInt(5, peakPosition);
                    insert.setInt(6, weeksOnChart);
                    if (linkedSongId != null) {
                        insert.setInt(7, linkedSongId);
                    } else {
                        insert.setNull(7, java.sql.Types.INTEGER);
                    }
                    insert.addBatch();

                    entryCount++;
                    if (entryCount % BATCH_SIZE == 0) {
                        insert.executeBatch();
                    }
                }
            }

            insert.executeBatch();
        }

        return new ImportReport(chartCount, entryCount, preservedLinks);
    }

    private static boolean shouldSkipChart(String chartDate, String latestExistingChartDate, Set<String> existingChartDates) {
        if (chartDate == null) {
            return true;
        }
        if (existingChartDates.contains(chartDate)) {
            return true;
        }
        return latestExistingChartDate != null && chartDate.compareTo(latestExistingChartDate) <= 0;
    }

    private static InputStream openAllChartsStream() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(ALL_CHARTS_URL).toURL().openConnection();
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setRequestProperty("Accept", "application/json");
        connection.setConnectTimeout(20_000);
        connection.setReadTimeout(120_000);
        return connection.getInputStream();
    }

    private static Map<String, Integer> loadExistingLinks(Connection connection) throws SQLException {
        Map<String, Integer> existingLinks = new HashMap<>();
        String sql =
            "SELECT artist_name, song_title, song_id " +
            "FROM billboard_hot100_entry " +
            "WHERE song_id IS NOT NULL " +
            "GROUP BY LOWER(TRIM(artist_name)), LOWER(TRIM(song_title)), artist_name, song_title, song_id";

        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                existingLinks.put(
                    exactKey(rs.getString("artist_name"), rs.getString("song_title")),
                    rs.getInt("song_id")
                );
            }
        }
        return existingLinks;
    }

    private static String loadLatestChartDate(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT MAX(chart_date) AS latest_chart_date FROM billboard_hot100_entry");
             ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                return rs.getString("latest_chart_date");
            }
            return null;
        }
    }

    private static Set<String> loadExistingChartDates(Connection connection) throws SQLException {
        Set<String> chartDates = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT DISTINCT chart_date FROM billboard_hot100_entry");
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String chartDate = rs.getString("chart_date");
                if (chartDate != null && !chartDate.isBlank()) {
                    chartDates.add(chartDate);
                }
            }
        }
        return chartDates;
    }

    private static List<DistinctEntryGroup> loadDistinctSourceGroups(Connection connection) throws SQLException {
        List<DistinctEntryGroup> groups = new ArrayList<>();
        String sql =
            "SELECT artist_name, song_title, COUNT(*) AS weeks_on_chart, MIN(position) AS peak_position " +
            "FROM billboard_hot100_entry " +
            "GROUP BY artist_name, song_title";
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                groups.add(new DistinctEntryGroup(
                    rs.getString("artist_name"),
                    rs.getString("song_title"),
                    rs.getInt("weeks_on_chart"),
                    rs.getInt("peak_position")
                ));
            }
        }
        return groups;
    }

    private static List<DistinctEntryGroup> loadDistinctUnmatchedGroups(Connection connection) throws SQLException {
        List<DistinctEntryGroup> groups = new ArrayList<>();
        String sql =
            "SELECT artist_name, song_title, COUNT(*) AS weeks_on_chart, MIN(position) AS peak_position " +
            "FROM billboard_hot100_entry " +
            "WHERE song_id IS NULL " +
            "GROUP BY artist_name, song_title " +
            "ORDER BY weeks_on_chart DESC, peak_position ASC, artist_name ASC, song_title ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                groups.add(new DistinctEntryGroup(
                    rs.getString("artist_name"),
                    rs.getString("song_title"),
                    rs.getInt("weeks_on_chart"),
                    rs.getInt("peak_position")
                ));
            }
        }
        return groups;
    }

    private static List<LibrarySongCandidate> loadLibrarySongs(Connection connection) throws SQLException {
        List<LibrarySongCandidate> librarySongs = new ArrayList<>();
        String sql =
            "SELECT s.id, s.name AS song_title, a.name AS artist_name " +
            "FROM Song s " +
            "JOIN Artist a ON a.id = s.artist_id";
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                librarySongs.add(new LibrarySongCandidate(
                    rs.getInt("id"),
                    rs.getString("artist_name"),
                    rs.getString("song_title")
                ));
            }
        }
        return librarySongs;
    }

    private static String exactKey(String artistName, String songTitle) {
        if (artistName == null || songTitle == null) {
            return null;
        }
        return artistName.trim().toLowerCase() + "||" + songTitle.trim().toLowerCase();
    }

    private static String normalizedSearchKey(String artistName, String songTitle) {
        String normalizedArtist = StringNormalizer.normalizeForSearch(artistName);
        String normalizedSong = StringNormalizer.normalizeForSearch(songTitle);
        if (normalizedArtist == null || normalizedArtist.isBlank() || normalizedSong == null || normalizedSong.isBlank()) {
            return null;
        }
        return normalizedArtist + "||" + normalizedSong;
    }

    private static void registerUniqueCandidate(String key, Integer songId, Map<String, Integer> uniqueLookup, Set<String> ambiguousKeys) {
        if (key == null || key.isBlank() || songId == null) {
            return;
        }
        if (ambiguousKeys.contains(key)) {
            return;
        }

        Integer existing = uniqueLookup.get(key);
        if (existing == null) {
            uniqueLookup.put(key, songId);
            return;
        }

        if (!existing.equals(songId)) {
            uniqueLookup.remove(key);
            ambiguousKeys.add(key);
        }
    }

    private static String contributorAwareKey(String artistName, String songTitle) {
        String normalizedSong = StringNormalizer.normalizeForSearch(songTitle);
        if (normalizedSong == null || normalizedSong.isBlank()) {
            return null;
        }

        LinkedHashSet<String> contributorTokens = new LinkedHashSet<>();
        addContributorTokens(contributorTokens, artistName);
        for (String featuredContributor : extractFeaturedContributors(songTitle)) {
            addContributorTokens(contributorTokens, featuredContributor);
        }

        if (contributorTokens.isEmpty()) {
            return null;
        }

        List<String> sortedTokens = new ArrayList<>(contributorTokens);
        Collections.sort(sortedTokens);
        return String.join(" ", sortedTokens) + "||" + normalizedSong;
    }

    private static void addContributorTokens(Set<String> tokens, String value) {
        String normalized = StringNormalizer.normalizeForSearch(value);
        if (normalized == null || normalized.isBlank()) {
            return;
        }
        for (String token : normalized.split("\\s+")) {
            if (!token.isBlank()) {
                tokens.add(token);
            }
        }
    }

    private static List<String> extractFeaturedContributors(String songTitle) {
        if (songTitle == null || songTitle.isBlank()) {
            return List.of();
        }

        List<String> contributors = new ArrayList<>();
        for (Pattern pattern : FEATURE_PATTERNS) {
            Matcher matcher = pattern.matcher(songTitle);
            while (matcher.find()) {
                String contributor = matcher.group(1);
                if (contributor != null && !contributor.isBlank()) {
                    contributors.add(contributor.trim());
                }
            }
        }
        return contributors;
    }

    private record DistinctEntryGroup(String artistName, String songTitle, int weeksOnChart, int peakPosition) {
    }

    private record LibrarySongCandidate(int songId, String artistName, String songTitle) {
    }
}