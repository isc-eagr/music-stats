package library;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.*;

/**
 * Standalone script to populate Song.track_number using two sources:
 * 
 * 1. LOCAL iTunes XML (primary) - Parses the local iTunes library for track numbers.
 *    This is fast, free, and covers the majority of songs.
 * 
 * 2. MusicBrainz API (fallback) - For songs NOT in iTunes, queries MusicBrainz
 *    to look up the release tracklist by artist + album name, then matches songs
 *    to get their track position.
 * 
 * Usage: Run main() method directly from IDE or via:
 *   mvnw exec:java -Dexec.mainClass="library.TrackNumberPopulator"
 * 
 * Options:
 *   --dry-run         Don't save changes to database
 *   --force           Re-populate even if track_number already exists
 *   --itunes-only     Only use iTunes XML, skip MusicBrainz fallback
 *   --musicbrainz-only Only use MusicBrainz, skip iTunes
 */
public class TrackNumberPopulator {

    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    private static final String ITUNES_XML_PATH = "C:/Users/ing_e/Music/iTunes/iTunes Music Library.xml";

    // MusicBrainz API Configuration
    private static final String MB_API = "https://musicbrainz.org/ws/2";
    private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";

    // MusicBrainz rate limit: 1 request per second
    private static final long MB_DELAY_MS = 1100;

    private JdbcTemplate jdbcTemplate;
    private ObjectMapper objectMapper;
    private long lastMbRequest = 0;

    // iTunes data: key = normalized(artist||album||song) -> trackNumber
    private Map<String, Integer> itunesTrackNumbers = new HashMap<>();

    // Statistics
    private int songsProcessed = 0;
    private int songsAlreadyHaveTrackNumber = 0;
    private int songsUpdatedFromItunes = 0;
    private int songsUpdatedFromMusicBrainz = 0;
    private int songsNotFound = 0;
    private int singlesSkipped = 0;
    private int apiErrors = 0;

    // Configuration flags
    private boolean dryRun = false;
    private boolean force = false;
    private boolean itunesOnly = false;
    private boolean musicBrainzOnly = false;

    // MusicBrainz cache: normalized(artist||album) -> Map<normalizedSongName, trackNumber>
    private Map<String, Map<String, Integer>> mbAlbumCache = new HashMap<>();

    public TrackNumberPopulator() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) {
        TrackNumberPopulator populator = new TrackNumberPopulator();

        for (String arg : args) {
            switch (arg.toLowerCase()) {
                case "--dry-run":
                    populator.dryRun = true;
                    break;
                case "--force":
                    populator.force = true;
                    break;
                case "--itunes-only":
                    populator.itunesOnly = true;
                    break;
                case "--musicbrainz-only":
                    populator.musicBrainzOnly = true;
                    break;
            }
        }

        populator.run();
    }

    public void run() {
        System.out.println("========================================");
        System.out.println("Track Number Populator");
        System.out.println("========================================");
        System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
        System.out.println("Force re-populate: " + force);
        System.out.println("Sources: " + (itunesOnly ? "iTunes only" : (musicBrainzOnly ? "MusicBrainz only" : "iTunes + MusicBrainz fallback")));
        System.out.println();

        try {
            // Step 1: Load iTunes track numbers into memory
            if (!musicBrainzOnly) {
                loadItunesTrackNumbers();
            }

            // Step 2: Process all songs in the database
            populateTrackNumbers();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }

        printSummary();
    }

    // ============ iTunes XML Parsing ============

    /**
     * Parse the local iTunes XML and build a lookup map of track numbers.
     */
    private void loadItunesTrackNumbers() {
        File file = new File(ITUNES_XML_PATH);
        if (!file.exists()) {
            System.out.println("iTunes library not found at: " + ITUNES_XML_PATH);
            System.out.println("Will use MusicBrainz only for all songs.");
            System.out.println();
            return;
        }

        System.out.println("Loading iTunes library from: " + ITUNES_XML_PATH);

        try (InputStream is = new FileInputStream(file)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(is);

            Element plist = (Element) doc.getElementsByTagName("plist").item(0);
            Element mainDict = getFirstChildElement(plist, "dict");
            Element tracksDict = findTracksDict(mainDict);

            if (tracksDict == null) {
                System.out.println("Could not find Tracks dict in iTunes XML.");
                return;
            }

            int tracksLoaded = 0;
            NodeList trackNodes = tracksDict.getChildNodes();

            for (int i = 0; i < trackNodes.getLength(); i++) {
                Node node = trackNodes.item(i);
                if (node.getNodeType() != Node.ELEMENT_NODE) continue;

                Element elem = (Element) node;
                if ("dict".equals(elem.getTagName())) {
                    Map<String, String> fields = parseDictFields(elem);

                    String kind = fields.get("Kind");
                    if (kind != null && (kind.toLowerCase().contains("video") || kind.toLowerCase().contains("podcast"))) {
                        continue;
                    }

                    String artist = fields.get("Artist");
                    String album = fields.get("Album");
                    String name = fields.get("Name");
                    String trackNumStr = fields.get("Track Number");

                    if (artist != null && album != null && name != null && trackNumStr != null) {
                        try {
                            int trackNum = Integer.parseInt(trackNumStr);
                            String key = createLookupKey(artist, album, name);
                            itunesTrackNumbers.put(key, trackNum);
                            tracksLoaded++;
                        } catch (NumberFormatException e) {
                            // Skip invalid track numbers
                        }
                    }
                }
            }

            System.out.println("Loaded " + tracksLoaded + " tracks with track numbers from iTunes.");
            System.out.println();

        } catch (Exception e) {
            System.err.println("Error parsing iTunes XML: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Parse all key-value pairs from a dict element into a flat map.
     */
    private Map<String, String> parseDictFields(Element dictElem) {
        Map<String, String> fields = new HashMap<>();
        NodeList children = dictElem.getChildNodes();
        String currentKey = null;

        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;

            Element elem = (Element) node;
            String tagName = elem.getTagName();

            if ("key".equals(tagName)) {
                currentKey = elem.getTextContent();
            } else if (currentKey != null) {
                fields.put(currentKey, elem.getTextContent());
                currentKey = null;
            }
        }
        return fields;
    }

    /**
     * Navigate to the Tracks dict inside the main plist dict.
     */
    private Element findTracksDict(Element mainDict) {
        NodeList children = mainDict.getChildNodes();
        boolean foundTracksKey = false;

        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;

            Element elem = (Element) node;
            if ("key".equals(elem.getTagName()) && "Tracks".equals(elem.getTextContent())) {
                foundTracksKey = true;
                continue;
            }
            if (foundTracksKey && "dict".equals(elem.getTagName())) {
                return elem;
            }
        }
        return null;
    }

    private Element getFirstChildElement(Element parent, String tagName) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE && tagName.equals(node.getNodeName())) {
                return (Element) node;
            }
        }
        return null;
    }

    // ============ Main Population Logic ============

    /**
     * Query all songs from the DB and attempt to populate track numbers.
     */
    private void populateTrackNumbers() {
        String whereClause = force ? "" : "WHERE s.track_number IS NULL ";
        String sql = "SELECT s.id, s.name, s.album_id, s.is_single, " +
                "ar.name as artist_name, al.name as album_name " +
                "FROM Song s " +
                "INNER JOIN Artist ar ON s.artist_id = ar.id " +
                "LEFT JOIN Album al ON s.album_id = al.id " +
                whereClause +
                "ORDER BY ar.name, al.name, s.id";

        List<SongData> songs = jdbcTemplate.query(sql, (rs, rowNum) -> {
            SongData song = new SongData();
            song.id = rs.getInt("id");
            song.name = rs.getString("name");
            song.albumId = rs.getObject("album_id") != null ? rs.getInt("album_id") : null;
            song.isSingle = rs.getObject("is_single") != null && rs.getBoolean("is_single");
            song.artistName = rs.getString("artist_name");
            song.albumName = rs.getString("album_name");
            return song;
        });

        System.out.println("Found " + songs.size() + " songs to process");
        System.out.println();

        // Group songs by album for efficient MusicBrainz batch lookups
        Map<String, List<SongData>> songsByAlbum = new LinkedHashMap<>();
        List<SongData> singles = new ArrayList<>();

        for (SongData song : songs) {
            if (song.albumName == null || song.albumName.isBlank()) {
                singles.add(song);
            } else {
                String albumKey = song.artistName + "||" + song.albumName;
                songsByAlbum.computeIfAbsent(albumKey, k -> new ArrayList<>()).add(song);
            }
        }

        // Skip songs with no album (track number not applicable)
        singlesSkipped = singles.size();
        if (singlesSkipped > 0) {
            System.out.println("Skipping " + singlesSkipped + " songs with no album (track number not applicable)");
            System.out.println();
        }

        // Process each album group
        for (Map.Entry<String, List<SongData>> entry : songsByAlbum.entrySet()) {
            List<SongData> albumSongs = entry.getValue();
            String artistName = albumSongs.get(0).artistName;
            String albumName = albumSongs.get(0).albumName;

            System.out.println("--- " + artistName + " - " + albumName + " (" + albumSongs.size() + " songs) ---");

            // Phase 1: Try iTunes for each song
            List<SongData> needsMusicBrainz = new ArrayList<>();

            for (SongData song : albumSongs) {
                songsProcessed++;

                if (!musicBrainzOnly) {
                    String key = createLookupKey(song.artistName, song.albumName, song.name);
                    Integer itunesTrackNum = itunesTrackNumbers.get(key);

                    if (itunesTrackNum != null) {
                        System.out.println("  [iTunes] #" + itunesTrackNum + " " + song.name);
                        if (!dryRun) {
                            updateTrackNumber(song.id, itunesTrackNum);
                        }
                        songsUpdatedFromItunes++;
                        continue;
                    }
                }

                needsMusicBrainz.add(song);
            }

            // Phase 2: MusicBrainz fallback for songs not found in iTunes
            if (!needsMusicBrainz.isEmpty() && !itunesOnly) {
                Map<String, Integer> mbTracks = getMusicBrainzTrackNumbers(artistName, albumName);

                for (SongData song : needsMusicBrainz) {
                    if (!musicBrainzOnly) {
                        // Already counted in songsProcessed above
                    } else {
                        songsProcessed++;
                    }

                    String normalizedName = normalizeName(song.name);
                    Integer mbTrackNum = mbTracks.get(normalizedName);

                    if (mbTrackNum != null) {
                        System.out.println("  [MusicBrainz] #" + mbTrackNum + " " + song.name);
                        if (!dryRun) {
                            updateTrackNumber(song.id, mbTrackNum);
                        }
                        songsUpdatedFromMusicBrainz++;
                    } else {
                        System.out.println("  [NOT FOUND] " + song.name);
                        songsNotFound++;
                    }
                }
            } else if (!needsMusicBrainz.isEmpty() && itunesOnly) {
                for (SongData song : needsMusicBrainz) {
                    System.out.println("  [NOT IN iTunes] " + song.name);
                    songsNotFound++;
                }
            }

            System.out.println();
        }
    }

    // ============ MusicBrainz API ============

    /**
     * Get track numbers for all songs in an album from MusicBrainz.
     * Uses a cache to avoid re-querying the same album.
     * Returns a map of normalizedSongName -> trackNumber.
     */
    private Map<String, Integer> getMusicBrainzTrackNumbers(String artistName, String albumName) {
        String cacheKey = normalizeName(artistName) + "||" + normalizeName(albumName);

        if (mbAlbumCache.containsKey(cacheKey)) {
            return mbAlbumCache.get(cacheKey);
        }

        Map<String, Integer> trackMap = new HashMap<>();

        try {
            // Step 1: Search for the release
            String releaseId = searchRelease(artistName, albumName);

            if (releaseId != null) {
                // Step 2: Look up the release with recordings+media to get track positions
                trackMap = fetchTrackList(releaseId);
            }
        } catch (Exception e) {
            System.err.println("  [MusicBrainz Error] " + e.getMessage());
            apiErrors++;
        }

        mbAlbumCache.put(cacheKey, trackMap);
        return trackMap;
    }

    /**
     * Search MusicBrainz for a release matching artist + album name.
     * Returns the release MBID or null if not found.
     */
    private String searchRelease(String artistName, String albumName) throws IOException, InterruptedException {
        respectMbRateLimit();

        String query = URLEncoder.encode(
                "release:\"" + albumName + "\" AND artist:\"" + artistName + "\"",
                StandardCharsets.UTF_8);
        String url = MB_API + "/release/?query=" + query + "&fmt=json&limit=5";

        String response = makeHttpRequest(url);
        JsonNode root = objectMapper.readTree(response);

        JsonNode releases = root.get("releases");
        if (releases == null || !releases.isArray() || releases.isEmpty()) {
            return null;
        }

        // Try to find the best match: official release with matching artist name
        String normalizedAlbum = normalizeName(albumName);
        String normalizedArtist = normalizeName(artistName);

        for (JsonNode release : releases) {
            String title = release.has("title") ? release.get("title").asText() : "";
            String status = release.has("status") ? release.get("status").asText() : "";

            // Check artist credit matches
            JsonNode artistCredit = release.get("artist-credit");
            if (artistCredit != null && artistCredit.isArray()) {
                for (JsonNode credit : artistCredit) {
                    String creditName = credit.has("name") ? credit.get("name").asText() : "";
                    JsonNode artistNode = credit.get("artist");
                    String mbArtistName = (artistNode != null && artistNode.has("name"))
                            ? artistNode.get("name").asText() : creditName;

                    if (normalizeName(mbArtistName).equals(normalizedArtist)
                            && normalizeName(title).equals(normalizedAlbum)) {
                        // Prefer official releases
                        if ("Official".equalsIgnoreCase(status)) {
                            return release.get("id").asText();
                        }
                    }
                }
            }
        }

        // Fallback: take the first result if the title matches reasonably
        JsonNode first = releases.get(0);
        String firstTitle = first.has("title") ? first.get("title").asText() : "";
        if (normalizeName(firstTitle).equals(normalizedAlbum)) {
            return first.get("id").asText();
        }

        return null;
    }

    /**
     * Fetch the full tracklist for a release from MusicBrainz.
     * Returns a map of normalizedSongTitle -> trackPosition.
     */
    private Map<String, Integer> fetchTrackList(String releaseId) throws IOException, InterruptedException {
        respectMbRateLimit();

        String url = MB_API + "/release/" + releaseId + "?inc=recordings+media&fmt=json";
        String response = makeHttpRequest(url);
        JsonNode root = objectMapper.readTree(response);

        Map<String, Integer> trackMap = new HashMap<>();

        JsonNode media = root.get("media");
        if (media != null && media.isArray()) {
            for (JsonNode medium : media) {
                JsonNode tracks = medium.get("tracks");
                if (tracks != null && tracks.isArray()) {
                    for (JsonNode track : tracks) {
                        int position = track.has("position") ? track.get("position").asInt() : 0;
                        String title = track.has("title") ? track.get("title").asText() : "";

                        if (position > 0 && !title.isEmpty()) {
                            trackMap.put(normalizeName(title), position);
                        }

                        // Also try the recording title as fallback
                        JsonNode recording = track.get("recording");
                        if (recording != null && recording.has("title")) {
                            String recTitle = recording.get("title").asText();
                            if (!recTitle.isEmpty()) {
                                trackMap.putIfAbsent(normalizeName(recTitle), position);
                            }
                        }
                    }
                }
            }
        }

        return trackMap;
    }

    // ============ HTTP & Rate Limiting ============

    private String makeHttpRequest(String urlString) throws IOException {
        URI uri = URI.create(urlString);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setRequestProperty("Accept", "application/json");

        int responseCode = conn.getResponseCode();
        if (responseCode == 404) {
            return "{\"releases\":[]}";
        } else if (responseCode == 503) {
            // Rate limited - wait and retry once
            System.out.println("  [Rate limited] Waiting 2 seconds...");
            try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            conn.disconnect();
            return makeHttpRequest(urlString);
        } else if (responseCode != 200) {
            throw new IOException("HTTP " + responseCode + ": " + conn.getResponseMessage());
        }

        try (InputStream is = conn.getInputStream()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void respectMbRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long timeSinceLastRequest = now - lastMbRequest;

        if (timeSinceLastRequest < MB_DELAY_MS) {
            long sleepTime = MB_DELAY_MS - timeSinceLastRequest;
            Thread.sleep(sleepTime);
        }

        lastMbRequest = System.currentTimeMillis();
    }

    // ============ Database ============

    private void updateTrackNumber(int songId, int trackNumber) {
        String sql = "UPDATE Song SET track_number = ? WHERE id = ?";
        jdbcTemplate.update(sql, trackNumber, songId);
    }

    // ============ Normalization ============

    /**
     * Create a normalized lookup key for matching iTunes songs to DB songs.
     * Uses the same normalization approach as the existing ItunesService.
     */
    private String createLookupKey(String artist, String album, String song) {
        return normalizeName(artist) + "||" + normalizeName(album) + "||" + normalizeName(song);
    }

    /**
     * Normalize name for comparison: lowercase, strip accents, remove special chars.
     */
    private String normalizeName(String name) {
        if (name == null) return "";

        // Handle special non-decomposable characters
        String result = name.toLowerCase().trim()
                .replace('\u00F1', 'n').replace('\u00D1', 'N')
                .replace('\u00E7', 'c').replace('\u00C7', 'C');

        // Unicode NFD normalization to strip accents
        String normalized = Normalizer.normalize(result, Normalizer.Form.NFD);
        result = normalized.replaceAll("\\p{M}", "");

        // Remove parenthetical/bracketed suffixes
        result = result.replaceAll("\\s*[\\(\\[].*?[\\)\\]]", "");

        // Remove special characters
        result = result.replaceAll("[^a-z0-9\\s]", "");

        // Collapse whitespace
        result = result.replaceAll("\\s+", " ").trim();

        return result;
    }

    // ============ Summary ============

    private void printSummary() {
        System.out.println("========================================");
        System.out.println("Summary");
        System.out.println("========================================");
        System.out.println("Songs processed:           " + songsProcessed);
        System.out.println("Singles skipped:            " + singlesSkipped);
        System.out.println("Already had track number:  " + songsAlreadyHaveTrackNumber);
        System.out.println("Updated from iTunes:       " + songsUpdatedFromItunes);
        System.out.println("Updated from MusicBrainz:  " + songsUpdatedFromMusicBrainz);
        System.out.println("Not found anywhere:        " + songsNotFound);
        System.out.println("API errors:                " + apiErrors);
        System.out.println();

        int totalUpdated = songsUpdatedFromItunes + songsUpdatedFromMusicBrainz;
        System.out.println("Total updated: " + totalUpdated);

        if (dryRun) {
            System.out.println();
            System.out.println("*** DRY RUN - No changes were saved to database ***");
        } else {
            System.out.println();
            System.out.println("Done! Changes saved to database.");
        }
    }

    // ============ Data Classes ============

    private static class SongData {
        int id;
        String name;
        Integer albumId;
        boolean isSingle;
        String artistName;
        String albumName;
    }
}
