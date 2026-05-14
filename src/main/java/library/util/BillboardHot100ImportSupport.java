package library.util;

import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;

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
import java.util.Locale;
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

        private record ParsedChart(String chartDate, List<ParsedEntry> entries) {
        }

        private record ParsedEntry(String artistName, String songTitle, int position, int peakPosition, int weeksOnChart) {
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
            statement.executeUpdate(
                "CREATE TABLE IF NOT EXISTS billboard_hot100_debut (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "identity_key TEXT NOT NULL, " +
                "matched INTEGER NOT NULL, " +
                "song_id INTEGER, " +
                "resolved_artist_id INTEGER, " +
                "artist_name TEXT NOT NULL, " +
                "song_title TEXT NOT NULL, " +
                "gender_name TEXT, " +
                "debut_week TEXT, " +
                "last_week TEXT, " +
                "peak_week TEXT, " +
                "debut_position INTEGER, " +
                "peak_position INTEGER, " +
                "weeks_on_chart INTEGER NOT NULL, " +
                "weeks_at_peak INTEGER NOT NULL, " +
                "weeks_at_top1 INTEGER NOT NULL, " +
                "weeks_at_top5 INTEGER NOT NULL, " +
                "weeks_at_top10 INTEGER NOT NULL, " +
                "weeks_at_top20 INTEGER NOT NULL, " +
                "weeks_at_top50 INTEGER NOT NULL, " +
                "weeks_at_top100 INTEGER NOT NULL, " +
                "FOREIGN KEY (song_id) REFERENCES Song(id), " +
                "FOREIGN KEY (resolved_artist_id) REFERENCES Artist(id))"
            );
            statement.executeUpdate(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_billboard_hot100_debut_identity " +
                "ON billboard_hot100_debut(identity_key)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_song_id " +
                "ON billboard_hot100_debut(song_id)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_artist_song " +
                "ON billboard_hot100_debut(artist_name, song_title)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_debut_week " +
                "ON billboard_hot100_debut(debut_week)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_last_week " +
                "ON billboard_hot100_debut(last_week)"
            );
            statement.executeUpdate(
                "CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_peak_position " +
                "ON billboard_hot100_debut(peak_position)"
            );
        }
    }

    public static void rebuildDebutTable(Connection connection) throws SQLException {
        ensureSchema(connection);

        boolean originalAutoCommit = connection.getAutoCommit();
        if (originalAutoCommit) {
            connection.setAutoCommit(false);
        }

        String sourceSql =
            "SELECT e.song_id, a.id AS resolved_artist_id, a.name AS canonical_artist_name, s.name AS canonical_song_title, " +
            "       LOWER(g.name) AS gender_name, e.artist_name AS raw_artist_name, e.song_title AS raw_song_title, " +
            "       e.chart_date, e.position " +
            "FROM billboard_hot100_entry e " +
            "LEFT JOIN Song s ON s.id = e.song_id " +
            "LEFT JOIN Artist a ON a.id = s.artist_id " +
            "LEFT JOIN Gender g ON g.id = a.gender_id";
        String insertSql =
            "INSERT INTO billboard_hot100_debut (" +
            "identity_key, matched, song_id, resolved_artist_id, artist_name, song_title, gender_name, " +
            "debut_week, last_week, peak_week, debut_position, peak_position, weeks_on_chart, weeks_at_peak, " +
            "weeks_at_top1, weeks_at_top5, weeks_at_top10, weeks_at_top20, weeks_at_top50, weeks_at_top100) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Statement deleteStatement = connection.createStatement();
             PreparedStatement sourceStatement = connection.prepareStatement(sourceSql);
             ResultSet rs = sourceStatement.executeQuery();
             PreparedStatement insertStatement = connection.prepareStatement(insertSql)) {
            deleteStatement.executeUpdate("DELETE FROM billboard_hot100_debut");

            Map<String, BillboardDebutAggregate> aggregates = new LinkedHashMap<>();
            while (rs.next()) {
                Integer songId = null;
                int songIdValue = rs.getInt("song_id");
                if (!rs.wasNull()) {
                    songId = songIdValue;
                }

                String rawArtistName = rs.getString("raw_artist_name");
                String rawSongTitle = rs.getString("raw_song_title");
                String identityKey = buildBillboardIdentityKey(songId, rawArtistName, rawSongTitle);
                boolean matchedEntry = songId != null;
                BillboardDebutAggregate aggregate = aggregates.computeIfAbsent(identityKey, key -> new BillboardDebutAggregate(identityKey, matchedEntry));

                Integer resolvedArtistId = null;
                int resolvedArtistIdValue = rs.getInt("resolved_artist_id");
                if (!rs.wasNull()) {
                    resolvedArtistId = resolvedArtistIdValue;
                }

                aggregate.accept(
                    songId,
                    resolvedArtistId,
                    rs.getString("canonical_artist_name"),
                    rs.getString("canonical_song_title"),
                    rs.getString("gender_name"),
                    rawArtistName,
                    rawSongTitle,
                    rs.getString("chart_date"),
                    rs.getInt("position")
                );
            }

            int batchSize = 0;
            for (BillboardDebutAggregate aggregate : aggregates.values()) {
                insertStatement.setString(1, aggregate.identityKey);
                insertStatement.setInt(2, aggregate.matched ? 1 : 0);
                if (aggregate.songId != null) {
                    insertStatement.setInt(3, aggregate.songId);
                } else {
                    insertStatement.setNull(3, java.sql.Types.INTEGER);
                }
                if (aggregate.resolvedArtistId != null) {
                    insertStatement.setInt(4, aggregate.resolvedArtistId);
                } else {
                    insertStatement.setNull(4, java.sql.Types.INTEGER);
                }
                insertStatement.setString(5, aggregate.artistName);
                insertStatement.setString(6, aggregate.songTitle);
                insertStatement.setString(7, aggregate.genderName);
                insertStatement.setString(8, aggregate.debutWeek);
                insertStatement.setString(9, aggregate.lastWeek);
                insertStatement.setString(10, aggregate.peakWeek);
                if (aggregate.debutPosition != null) {
                    insertStatement.setInt(11, aggregate.debutPosition);
                } else {
                    insertStatement.setNull(11, java.sql.Types.INTEGER);
                }
                if (aggregate.peakPosition != null) {
                    insertStatement.setInt(12, aggregate.peakPosition);
                } else {
                    insertStatement.setNull(12, java.sql.Types.INTEGER);
                }
                insertStatement.setInt(13, aggregate.weeksOnChart);
                insertStatement.setInt(14, aggregate.weeksAtPeak);
                insertStatement.setInt(15, aggregate.weeksAtTop1);
                insertStatement.setInt(16, aggregate.weeksAtTop5);
                insertStatement.setInt(17, aggregate.weeksAtTop10);
                insertStatement.setInt(18, aggregate.weeksAtTop20);
                insertStatement.setInt(19, aggregate.weeksAtTop50);
                insertStatement.setInt(20, aggregate.weeksAtTop100);
                insertStatement.addBatch();

                batchSize++;
                if (batchSize % BATCH_SIZE == 0) {
                    insertStatement.executeBatch();
                }
            }
            insertStatement.executeBatch();

            if (originalAutoCommit) {
                connection.commit();
            }
        } catch (SQLException e) {
            if (originalAutoCommit) {
                connection.rollback();
            }
            throw e;
        } finally {
            if (originalAutoCommit) {
                connection.setAutoCommit(true);
            }
        }
    }

    private static String buildBillboardIdentityKey(Integer songId, String artistName, String songTitle) {
        if (songId != null) {
            return "song:" + songId;
        }
        return "raw:" + normalizeBillboardIdentityPart(artistName) + "||" + normalizeBillboardIdentityPart(songTitle);
    }

    private static String normalizeBillboardIdentityPart(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static String pickPreferredDisplayValue(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank()) {
            return candidateValue;
        }

        int ignoreCase = candidateValue.compareToIgnoreCase(currentValue);
        if (ignoreCase < 0) {
            return candidateValue;
        }
        if (ignoreCase == 0 && candidateValue.compareTo(currentValue) < 0) {
            return candidateValue;
        }
        return currentValue;
    }

    private static final class BillboardDebutAggregate {
        private final String identityKey;
        private final boolean matched;
        private Integer songId;
        private Integer resolvedArtistId;
        private String artistName;
        private String songTitle;
        private String genderName;
        private String debutWeek;
        private String lastWeek;
        private String peakWeek;
        private Integer debutPosition;
        private Integer peakPosition;
        private int weeksOnChart;
        private int weeksAtPeak;
        private int weeksAtTop1;
        private int weeksAtTop5;
        private int weeksAtTop10;
        private int weeksAtTop20;
        private int weeksAtTop50;
        private int weeksAtTop100;

        private BillboardDebutAggregate(String identityKey, boolean matched) {
            this.identityKey = identityKey;
            this.matched = matched;
        }

        private void accept(
                Integer songId,
                Integer resolvedArtistId,
                String canonicalArtistName,
                String canonicalSongTitle,
                String genderName,
                String rawArtistName,
                String rawSongTitle,
                String chartDate,
                int position) {
            if (songId != null) {
                this.songId = songId;
            }
            if (resolvedArtistId != null) {
                this.resolvedArtistId = resolvedArtistId;
            }
            if (genderName != null && !genderName.isBlank()) {
                this.genderName = genderName;
            }

            if (matched) {
                if (canonicalArtistName != null && !canonicalArtistName.isBlank()) {
                    this.artistName = canonicalArtistName;
                }
                if (canonicalSongTitle != null && !canonicalSongTitle.isBlank()) {
                    this.songTitle = canonicalSongTitle;
                }
            } else {
                this.artistName = pickPreferredDisplayValue(this.artistName, rawArtistName);
                this.songTitle = pickPreferredDisplayValue(this.songTitle, rawSongTitle);
            }

            weeksOnChart++;
            if (position <= 1) weeksAtTop1++;
            if (position <= 5) weeksAtTop5++;
            if (position <= 10) weeksAtTop10++;
            if (position <= 20) weeksAtTop20++;
            if (position <= 50) weeksAtTop50++;
            if (position <= 100) weeksAtTop100++;

            if (debutWeek == null || chartDate.compareTo(debutWeek) < 0) {
                debutWeek = chartDate;
                debutPosition = position;
            } else if (chartDate.equals(debutWeek) && (debutPosition == null || position < debutPosition)) {
                debutPosition = position;
            }

            if (lastWeek == null || chartDate.compareTo(lastWeek) > 0) {
                lastWeek = chartDate;
            }

            if (peakPosition == null || position < peakPosition) {
                peakPosition = position;
                peakWeek = chartDate;
                weeksAtPeak = 1;
            } else if (position == peakPosition) {
                weeksAtPeak++;
                if (peakWeek == null || chartDate.compareTo(peakWeek) < 0) {
                    peakWeek = chartDate;
                }
            }
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
        try (PreparedStatement insert = connection.prepareStatement(insertSql);
             InputStream inputStream = openAllChartsStream();
             JsonParser parser = new JsonFactory().createParser(inputStream)) {

            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IOException("Unexpected Billboard Hot 100 payload format");
            }

            while (parser.nextToken() != JsonToken.END_ARRAY) {
                ParsedChart chart = parseChart(parser);
                if (chart == null) {
                    continue;
                }

                String chartDate = chart.chartDate();
                if (chartDate == null) {
                    continue;
                }

                if (shouldSkipChart(chartDate, latestExistingChartDate, existingChartDates)) {
                    continue;
                }

                chartCount++;
                for (ParsedEntry entry : chart.entries()) {
                    String artistName = entry.artistName();
                    String songTitle = entry.songTitle();
                    int position = entry.position();
                    int peakPosition = entry.peakPosition();
                    int weeksOnChart = entry.weeksOnChart();

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

    private static ParsedChart parseChart(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            parser.skipChildren();
            return null;
        }

        String chartDate = null;
        List<ParsedEntry> entries = new ArrayList<>();

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            JsonToken valueToken = parser.nextToken();
            if (fieldName == null) {
                parser.skipChildren();
                continue;
            }

            switch (fieldName) {
                case "date" -> chartDate = parser.getValueAsString();
                case "data" -> {
                    if (valueToken != JsonToken.START_ARRAY) {
                        parser.skipChildren();
                        break;
                    }
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        ParsedEntry entry = parseEntry(parser);
                        if (entry != null) {
                            entries.add(entry);
                        }
                    }
                }
                default -> parser.skipChildren();
            }
        }

        return new ParsedChart(chartDate, entries);
    }

    private static ParsedEntry parseEntry(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            parser.skipChildren();
            return null;
        }

        String artistName = null;
        String songTitle = null;
        int position = 0;
        int peakPosition = 0;
        int weeksOnChart = 0;

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (fieldName == null) {
                parser.skipChildren();
                continue;
            }

            switch (fieldName) {
                case "artist" -> artistName = parser.getValueAsString();
                case "song" -> songTitle = parser.getValueAsString();
                case "this_week" -> position = parser.getValueAsInt(0);
                case "peak_position" -> peakPosition = parser.getValueAsInt(0);
                case "weeks_on_chart" -> weeksOnChart = parser.getValueAsInt(0);
                default -> parser.skipChildren();
            }
        }

        return new ParsedEntry(artistName, songTitle, position, peakPosition, weeksOnChart);
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