package library;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Standalone script to populate album AND song (single) covers using Apple Music/iTunes Search API.
 * 
 * Features:
 * - Populates Album.image AND Song.image from Apple Music
 * - Fetches HIGHEST quality images (up to 3000x3000px)
 * - Works for both full albums AND singles/individual songs
 * - Can update existing images if Apple has higher quality version
 * - No API key needed - completely FREE, ese
 * - No strict rate limits (reasonable requests are fine)
 * 
 * Apple Music/iTunes Search API:
 * - No API key required (gratis, vato!)
 * - Search endpoint: https://itunes.apple.com/search
 * - entity=album for albums
 * - entity=song for singles/songs
 * - Returns artworkUrl100 which can be modified for higher resolution
 * - URL trick: Change 100x100bb.jpg to 3000x3000bb.jpg for max quality
 * 
 * Usage: Run main() method directly from IDE or via:
 * mvnw exec:java -Dexec.mainClass="library.AppleMusicCoverPopulator"
 * 
 * Options:
 * --dry-run           Don't save changes to database
 * --force             Replace ALL existing images (albums + songs)
 * --albums-only       Only process albums (skip songs)
 * --songs-only        Only process songs (skip albums)
 * --upgrade-only      Only upgrade existing images if Apple has higher quality
 * --missing-only      Only populate items without images (default)
 * --max-resolution=N  Max image size (600, 1200, 3000) - default: 3000
 * --min-upgrade-kb=N  Minimum KB difference to consider an upgrade (default: 50)
 */
public class AppleMusicCoverPopulator {

    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";

    // Apple Music/iTunes Search API Configuration
    private static final String APPLE_MUSIC_API = "https://itunes.apple.com/search";
    private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
    
    // Rate limiting: Apple doesn't publish official limits, but around 100 requests then throttles
    // Using 2 second delay to be extra safe and avoid 429 errors
    private static final long APPLE_DELAY_MS = 2000;

    // Max image resolution: 600, 1200, or 3000 (highest quality)
    private int maxResolution = 3000;

    private JdbcTemplate jdbcTemplate;
    private ObjectMapper objectMapper;
    private long lastAppleRequest = 0;
    private PrintWriter logWriter;

    // Statistics
    private int albumsProcessed = 0;
    private int albumsFound = 0;
    private int albumImagesUpdated = 0;
    private int albumImagesUpgraded = 0;
    private int albumImagesSkippedSameQuality = 0;
    
    private int songsProcessed = 0;
    private int songsFound = 0;
    private int songImagesUpdated = 0;
    private int songImagesUpgraded = 0;
    private int songImagesSkippedSameQuality = 0;
    
    private int apiErrors = 0;
    private int rateLimitErrors = 0;

    // Configuration flags
    private boolean dryRun = false;
    private boolean missingOnly = false;  // Default: only items without images
    private boolean upgradeOnly = false; // Only upgrade existing images
    private boolean albumsOnly = false;
    private boolean songsOnly = true;
    private int minUpgradeKb = 1;       // Minimum KB difference for upgrade

    public AppleMusicCoverPopulator() {
        // Setup database connection
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) {
        AppleMusicCoverPopulator populator = new AppleMusicCoverPopulator();

        // Parse command line arguments
        for (String arg : args) {
            String lowerArg = arg.toLowerCase();
            
            if (lowerArg.equals("--dry-run")) {
                populator.dryRun = true;
            } else if (lowerArg.equals("--force")) {
                populator.missingOnly = false;
                populator.upgradeOnly = false;
            } else if (lowerArg.equals("--upgrade-only")) {
                populator.upgradeOnly = true;
                populator.missingOnly = false;
            } else if (lowerArg.equals("--missing-only")) {
                populator.missingOnly = true;
                populator.upgradeOnly = false;
            } else if (lowerArg.equals("--albums-only")) {
                populator.albumsOnly = true;
                populator.songsOnly = false;
            } else if (lowerArg.equals("--songs-only")) {
                populator.songsOnly = true;
                populator.albumsOnly = false;
            } else if (lowerArg.startsWith("--max-resolution=")) {
                try {
                    int res = Integer.parseInt(arg.substring("--max-resolution=".length()));
                    if (res == 600 || res == 1200 || res == 3000) {
                        populator.maxResolution = res;
                    } else {
                        System.err.println("Invalid resolution, using 3000. Valid values: 600, 1200, 3000");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid value for --max-resolution, using default: 3000");
                }
            } else if (lowerArg.startsWith("--min-upgrade-kb=")) {
                try {
                    populator.minUpgradeKb = Integer.parseInt(arg.substring("--min-upgrade-kb=".length()));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid value for --min-upgrade-kb, using default: 50");
                }
            }
        }

        populator.run();
    }

    public void run() {
        // Create log file
        try {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String logFileName = "apple_music_cover_populator_" + timestamp + ".txt";
            logWriter = new PrintWriter(new FileWriter(logFileName), true);
            log("Log file: " + logFileName);
        } catch (IOException e) {
            logError("Could not create log file: " + e.getMessage());
        }
        
        log("========================================");
        log("Apple Music Cover Populator");
        log("========================================");
        log("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
        
        String modeDesc;
        if (upgradeOnly) {
            modeDesc = "Upgrade only (update existing with higher quality)";
        } else if (missingOnly) {
            modeDesc = "Missing only (items without images)";
        } else {
            modeDesc = "Force (ALL items, replace everything - ¡aguas cabrón!)";
        }
        log("Processing: " + modeDesc);
        
        String scopeDesc;
        if (albumsOnly) {
            scopeDesc = "Albums only";
        } else if (songsOnly) {
            scopeDesc = "Songs only";
        } else {
            scopeDesc = "Both albums AND songs";
        }
        log("Scope: " + scopeDesc);
        log("Max resolution: " + maxResolution + "x" + maxResolution + " (highest quality, papi)");
        
        if (upgradeOnly || !missingOnly) {
            log("Min upgrade threshold: " + minUpgradeKb + " KB");
        }
        log("");

        try {
            if (!songsOnly) {
                populateAlbumCovers();
                log("");
            }
            
            if (!albumsOnly) {
                populateSongCovers();
                log("");
            }
        } catch (Exception e) {
            logError("Error populating covers: " + e.getMessage());
            e.printStackTrace();
        }

        printSummary();
        
        // Close log file
        if (logWriter != null) {
            logWriter.close();
            log("\nLog saved to file.");
        }
    }

    /**
     * Populate album covers from Apple Music
     */
    private void populateAlbumCovers() {
        log("========================================");
        log("Processing Albums");
        log("========================================");
        log("");

        String sql;
        
        if (upgradeOnly) {
            sql = """
                SELECT a.id, a.name, ar.name as artist_name, ar.country, LENGTH(a.image) as current_size
                FROM Album a
                INNER JOIN Artist ar ON a.artist_id = ar.id
                WHERE a.image IS NOT NULL
                ORDER BY a.id
                """;
        } else if (missingOnly) {
            sql = """
                SELECT a.id, a.name, ar.name as artist_name, ar.country, 0 as current_size
                FROM Album a
                INNER JOIN Artist ar ON a.artist_id = ar.id
                WHERE a.image IS NULL
                ORDER BY a.id
                """;
        } else {
            sql = """
                SELECT a.id, a.name, ar.name as artist_name, ar.country, COALESCE(LENGTH(a.image), 0) as current_size
                FROM Album a
                INNER JOIN Artist ar ON a.artist_id = ar.id
                ORDER BY a.id
                """;
        }

        List<ItemData> albums = jdbcTemplate.query(sql, (rs, rowNum) -> {
            ItemData album = new ItemData();
            album.id = rs.getInt("id");
            album.name = rs.getString("name");
            album.artistName = rs.getString("artist_name");
            album.artistCountry = rs.getString("country");
            album.currentImageSize = rs.getInt("current_size");
            return album;
        });

        log("Found " + albums.size() + " albums to process, ese");
        log("");

        for (ItemData album : albums) {
            albumsProcessed++;

            try {
                log("[" + albumsProcessed + "/" + albums.size() + "] Processing: " 
                        + album.artistName + " - " + album.name);
                
                if (album.currentImageSize > 0) {
                    log("  Current image: " + formatBytes(album.currentImageSize));
                }

                String imageUrl = searchAppleMusicCover(album.artistName, album.name, true, album.artistCountry);

                if (imageUrl != null && !imageUrl.isEmpty()) {
                    albumsFound++;
                    
                    String highResUrl = upgradeImageUrl(imageUrl, maxResolution);
                    log("  ✓ Found cover: " + highResUrl);

                    byte[] imageData = downloadImage(highResUrl);

                    if (imageData != null && imageData.length > 0) {
                        log("  ✓ Downloaded: " + formatBytes(imageData.length));

                        boolean shouldUpdate = shouldUpdateImage(album.currentImageSize, imageData.length);
                        
                        if (shouldUpdate) {
                            if (!dryRun) {
                                updateAlbumImage(album.id, imageData);
                            }
                            
                            if (album.currentImageSize > 0) {
                                albumImagesUpgraded++;
                            } else {
                                albumImagesUpdated++;
                            }
                        } else {
                            albumImagesSkippedSameQuality++;
                        }
                    } else {
                        log("  ⚠ Could not download image");
                    }
                } else {
                    log("  ✗ Not found in Apple Music");
                    if (album.currentImageSize > 0) {
                        log("  → Keeping existing cover (not replacing)");
                    }
                }

            } catch (Exception e) {
                String errorMsg = e.getMessage();
                if (errorMsg != null && (errorMsg.contains("429") || errorMsg.contains("Rate limited"))) {
                    logError("  ⚠ Rate limited! Waiting 30 seconds before continuing...");
                    rateLimitErrors++;
                    try {
                        Thread.sleep(30000); // Wait 30 seconds
                        log("  → Resuming after cooldown, retrying this item...");
                        
                        // Retry this same album after waiting
                        albumsProcessed--; // Decrement so it doesn't count as processed yet
                        String imageUrl = searchAppleMusicCover(album.artistName, album.name, true, album.artistCountry);
                        
                        if (imageUrl != null && !imageUrl.isEmpty()) {
                            albumsFound++;
                            String highResUrl = upgradeImageUrl(imageUrl, maxResolution);
                            log("  ✓ Found cover: " + highResUrl);
                            
                            byte[] imageData = downloadImage(highResUrl);
                            if (imageData != null && imageData.length > 0) {
                                log("  ✓ Downloaded: " + formatBytes(imageData.length));
                                boolean shouldUpdate = shouldUpdateImage(album.currentImageSize, imageData.length);
                                
                                if (shouldUpdate) {
                                    if (!dryRun) {
                                        updateAlbumImage(album.id, imageData);
                                    }
                                    if (album.currentImageSize > 0) {
                                        albumImagesUpgraded++;
                                    } else {
                                        albumImagesUpdated++;
                                    }
                                } else {
                                    albumImagesSkippedSameQuality++;
                                }
                            } else {
                                log("  ⚠ Could not download image");
                            }
                        } else {
                            log("  ✗ Not found in Apple Music");
                        }
                        albumsProcessed++; // Now count it as processed
                    } catch (Exception retryEx) {
                        logError("  ✗ Retry failed: " + retryEx.getMessage());
                        apiErrors++;
                        albumsProcessed++; // Count it anyway
                    }
                } else {
                    logError("  ✗ Error: " + errorMsg);
                    apiErrors++;
                }
            }

            log("");
        }
    }

    /**
     * Populate song covers from Apple Music (for singles, vato)
     */
    private void populateSongCovers() {
        log("========================================");
        log("Processing Songs/Singles");
        log("========================================");
        log("");

        String sql;
        
        if (upgradeOnly) {
            sql = """
                SELECT s.id, s.name, ar.name as artist_name, ar.country,
                       LENGTH(s.single_cover) as current_size
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                WHERE s.single_cover IS NOT NULL
                ORDER BY s.id
                """;
        } else if (missingOnly) {
            sql = """
                SELECT s.id, s.name, ar.name as artist_name, ar.country,
                       0 as current_size
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                WHERE s.single_cover IS NULL
                ORDER BY s.id
                """;
        } else {
            sql = """
                SELECT s.id, s.name, ar.name as artist_name, ar.country,
                       COALESCE(LENGTH(s.single_cover), 0) as current_size
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                ORDER BY s.id
                """;
        }

        List<ItemData> songs = jdbcTemplate.query(sql, (rs, rowNum) -> {
            ItemData song = new ItemData();
            song.id = rs.getInt("id");
            song.name = rs.getString("name");
            song.artistName = rs.getString("artist_name");
            song.artistCountry = rs.getString("country");
            song.currentImageSize = rs.getInt("current_size");
            return song;
        });

        log("Found " + songs.size() + " songs to process, mijo");
        log("");

        for (ItemData song : songs) {
            songsProcessed++;

            try {
                log("[" + songsProcessed + "/" + songs.size() + "] Processing: " 
                        + song.artistName + " - " + song.name);
                
                if (song.currentImageSize > 0) {
                    log("  Current image: " + formatBytes(song.currentImageSize));
                }

                // Search Apple Music for the song/single
                String imageUrl = searchAppleMusicCover(song.artistName, song.name, false, song.artistCountry);

                if (imageUrl != null && !imageUrl.isEmpty()) {
                    songsFound++;
                    
                    String highResUrl = upgradeImageUrl(imageUrl, maxResolution);
                    log("  ✓ Found cover: " + highResUrl);

                    byte[] imageData = downloadImage(highResUrl);

                    if (imageData != null && imageData.length > 0) {
                        log("  ✓ Downloaded: " + formatBytes(imageData.length));

                        boolean shouldUpdate = shouldUpdateImage(song.currentImageSize, imageData.length);
                        
                        if (shouldUpdate) {
                            if (!dryRun) {
                                updateSongImage(song.id, imageData);
                            }
                            
                            if (song.currentImageSize > 0) {
                                songImagesUpgraded++;
                            } else {
                                songImagesUpdated++;
                            }
                        } else {
                            songImagesSkippedSameQuality++;
                        }
                    } else {
                        log("  ⚠ Could not download image");
                    }
                } else {
                    log("  ✗ Not found in Apple Music");
                    if (song.currentImageSize > 0) {
                        log("  → Keeping existing cover (not replacing)");
                    }
                }

            } catch (Exception e) {
                String errorMsg = e.getMessage();
                if (errorMsg != null && (errorMsg.contains("429") || errorMsg.contains("Rate limited"))) {
                    logError("  ⚠ Rate limited! Waiting 30 seconds before continuing...");
                    rateLimitErrors++;
                    try {
                        Thread.sleep(30000); // Wait 30 seconds
                        log("  → Resuming after cooldown, retrying this item...");
                        
                        // Retry this same song after waiting
                        songsProcessed--; // Decrement so it doesn't count as processed yet
                        String imageUrl = searchAppleMusicCover(song.artistName, song.name, false, song.artistCountry);
                        
                        if (imageUrl != null && !imageUrl.isEmpty()) {
                            songsFound++;
                            String highResUrl = upgradeImageUrl(imageUrl, maxResolution);
                            log("  ✓ Found cover: " + highResUrl);
                            
                            byte[] imageData = downloadImage(highResUrl);
                            if (imageData != null && imageData.length > 0) {
                                log("  ✓ Downloaded: " + formatBytes(imageData.length));
                                boolean shouldUpdate = shouldUpdateImage(song.currentImageSize, imageData.length);
                                
                                if (shouldUpdate) {
                                    if (!dryRun) {
                                        updateSongImage(song.id, imageData);
                                    }
                                    if (song.currentImageSize > 0) {
                                        songImagesUpgraded++;
                                    } else {
                                        songImagesUpdated++;
                                    }
                                } else {
                                    songImagesSkippedSameQuality++;
                                }
                            } else {
                                log("  ⚠ Could not download image");
                            }
                        } else {
                            log("  ✗ Not found in Apple Music");
                        }
                        songsProcessed++; // Now count it as processed
                    } catch (Exception retryEx) {
                        logError("  ✗ Retry failed: " + retryEx.getMessage());
                        apiErrors++;
                        songsProcessed++; // Count it anyway
                    }
                } else {
                    logError("  ✗ Error: " + errorMsg);
                    apiErrors++;
                }
            }

            log("");
        }
    }

    /**
     * Search Apple Music for cover art
     * Returns the artwork URL or null if not found
     */
    private String searchAppleMusicCover(String artistName, String itemName, boolean isAlbum, String artistCountry) 
            throws IOException, InterruptedException {
        respectRateLimit();

        String encodedArtist = URLEncoder.encode(artistName, StandardCharsets.UTF_8);
        String entity = isAlbum ? "album" : "song";
        
        // For SONGS: Always look for collectionName = song + " - Single" OR song + " - EP"
        // For ALBUMS: Look for collectionName match with variations allowed
        String normalizedSongWithSingle = normalizeForComparison(itemName + " - Single");
        String normalizedSongWithEP = normalizeForComparison(itemName + " - EP");
        
        // First try: entity=song (for songs) or entity=album (for albums), search term = artist + item
        String searchTerm = URLEncoder.encode(artistName + " " + itemName, StandardCharsets.UTF_8);
        String url = APPLE_MUSIC_API + "?term=" + searchTerm + "&entity=" + entity + "&limit=50";
        
        String response = makeHttpRequest(url);
        JsonNode root = objectMapper.readTree(response);

        JsonNode results = root.get("results");
        if (results != null && results.isArray() && results.size() > 0) {
            String normalizedSearch = normalizeForComparison(itemName);
            String fallbackUrl = null; // For album variations (Deluxe, Remaster, etc.)
            
            for (JsonNode result : results) {
                // Verify artist match first (check both artistName and collectionArtistName)
                boolean artistMatches = false;
                JsonNode artistNameNode = result.get("artistName");
                if (artistNameNode != null) {
                    String apiArtist = artistNameNode.asText();
                    if (normalizeForComparison(apiArtist).equals(normalizeForComparison(artistName))) {
                        artistMatches = true;
                    }
                }
                if (!artistMatches) {
                    JsonNode collectionArtistNode = result.get("collectionArtistName");
                    if (collectionArtistNode != null) {
                        String collectionArtist = collectionArtistNode.asText();
                        if (normalizeForComparison(collectionArtist).equals(normalizeForComparison(artistName))) {
                            artistMatches = true;
                        }
                    }
                }
                if (!artistMatches) {
                    continue; // Skip if neither artist field matches
                }
                
                if (!isAlbum) {
                    // For songs: Check if collectionName = song + " - Single" OR song + " - EP"
                    JsonNode collectionNameNode = result.get("collectionName");
                    if (collectionNameNode != null && !collectionNameNode.isNull()) {
                        String collectionName = collectionNameNode.asText();
                        String normalizedCollection = normalizeForComparison(collectionName);
                        if (normalizedCollection.equals(normalizedSongWithSingle) || 
                            normalizedCollection.equals(normalizedSongWithEP)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                return artworkNode.asText();
                            }
                        }
                    }
                } else {
                    // For albums: Check collectionName match
                    JsonNode nameNode = result.get("collectionName");
                    if (nameNode != null) {
                        String apiName = nameNode.asText();
                        String normalizedApi = normalizeForComparison(apiName);
                        
                        // Check for EXACT match first (highest priority)
                        if (normalizedApi.equals(normalizedSearch)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                return artworkNode.asText(); // Return immediately for exact match
                            }
                        }
                        
                        // Check if it's an allowed variation (fallback)
                        if (fallbackUrl == null && isAlbumNameMatch(itemName, apiName)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                fallbackUrl = artworkNode.asText(); // Save as fallback, keep looking for exact
                            }
                        }
                    }
                }
            }
            
            // If we found a variation match but no exact match, use it
            if (fallbackUrl != null) {
                return fallbackUrl;
            }
        }

        // Second try: For SONGS, search with entity=album (search term = artist + song)
        // For ALBUMS, search for songs that belong to this album
        if (!isAlbum) {
            log("  → Trying entity=album search (artist + song)...");
            respectRateLimit();
            
            url = APPLE_MUSIC_API + "?term=" + searchTerm + "&entity=album&limit=50";
            response = makeHttpRequest(url);
            root = objectMapper.readTree(response);
            
            results = root.get("results");
            if (results != null && results.isArray() && results.size() > 0) {
                for (JsonNode result : results) {
                    // Verify artist match
                    boolean artistMatches = false;
                    JsonNode artistNameNode = result.get("artistName");
                    if (artistNameNode != null) {
                        String apiArtist = artistNameNode.asText();
                        if (normalizeForComparison(apiArtist).equals(normalizeForComparison(artistName))) {
                            artistMatches = true;
                        }
                    }
                    if (!artistMatches) {
                        JsonNode collectionArtistNode = result.get("collectionArtistName");
                        if (collectionArtistNode != null) {
                            String collectionArtist = collectionArtistNode.asText();
                            if (normalizeForComparison(collectionArtist).equals(normalizeForComparison(artistName))) {
                                artistMatches = true;
                            }
                        }
                    }
                    if (!artistMatches) {
                        continue;
                    }
                    
                    // Check if collectionName = song + " - Single" OR song + " - EP"
                    JsonNode collectionNameNode = result.get("collectionName");
                    if (collectionNameNode != null && !collectionNameNode.isNull()) {
                        String collectionName = collectionNameNode.asText();
                        String normalizedCollection = normalizeForComparison(collectionName);
                        if (normalizedCollection.equals(normalizedSongWithSingle) || 
                            normalizedCollection.equals(normalizedSongWithEP)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                log("  ✓ Found via album search");
                                return artworkNode.asText();
                            }
                        }
                    }
                }
            }
        } else {
            // For albums: Try searching for songs that belong to this album
            // Sometimes the album entity isn't available but the songs are, and they contain the album artwork
            log("  → Album not found directly, searching for songs from this album...");
            respectRateLimit();
            
            url = APPLE_MUSIC_API + "?term=" + searchTerm + "&entity=song&limit=50";
            response = makeHttpRequest(url);
            root = objectMapper.readTree(response);
            
            results = root.get("results");
            if (results != null && results.isArray() && results.size() > 0) {
                String normalizedSearch = normalizeForComparison(itemName);
                
                for (JsonNode result : results) {
                    // Verify artist match (check both artistName and collectionArtistName)
                    boolean artistMatches = false;
                    JsonNode artistNameNode = result.get("artistName");
                    if (artistNameNode != null) {
                        String apiArtist = artistNameNode.asText();
                        if (normalizeForComparison(apiArtist).equals(normalizeForComparison(artistName))) {
                            artistMatches = true;
                        }
                    }
                    if (!artistMatches) {
                        JsonNode collectionArtistNode = result.get("collectionArtistName");
                        if (collectionArtistNode != null) {
                            String collectionArtist = collectionArtistNode.asText();
                            if (normalizeForComparison(collectionArtist).equals(normalizeForComparison(artistName))) {
                                artistMatches = true;
                            }
                        }
                    }
                    if (!artistMatches) {
                        continue; // Skip if neither artist field matches
                    }
                    
                    // Check if the song's collectionName matches our album name
                    JsonNode collectionNameNode = result.get("collectionName");
                    if (collectionNameNode != null && !collectionNameNode.isNull()) {
                        String collectionName = collectionNameNode.asText();
                        String normalizedCollection = normalizeForComparison(collectionName);
                        
                        // EXACT match first (highest priority)
                        if (normalizedCollection.equals(normalizedSearch)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                log("  ✓ Found via song search (exact collectionName match)");
                                return artworkNode.asText();
                            }
                        }
                        
                        // Also allow variations (Deluxe, Anniversary, etc.)
                        if (isAlbumNameMatch(itemName, collectionName)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                log("  ✓ Found via song search (collectionName variation match)");
                                return artworkNode.asText();
                            }
                        }
                    }
                }
            }
        }

        // Third try: For SONGS, search with entity=song (search term = just song name)
        // For ALBUMS, search by artist only with entity=album
        if (!isAlbum) {
            log("  → Trying entity=song search (just song name)...");
            respectRateLimit();
            
            String songOnlyTerm = URLEncoder.encode(itemName, StandardCharsets.UTF_8);
            url = APPLE_MUSIC_API + "?term=" + songOnlyTerm + "&entity=song&limit=50";
            response = makeHttpRequest(url);
            root = objectMapper.readTree(response);
            
            results = root.get("results");
            if (results != null && results.isArray() && results.size() > 0) {
                for (JsonNode result : results) {
                    // Verify artist match
                    boolean artistMatches = false;
                    JsonNode artistNameNode = result.get("artistName");
                    if (artistNameNode != null) {
                        String apiArtist = artistNameNode.asText();
                        if (normalizeForComparison(apiArtist).equals(normalizeForComparison(artistName))) {
                            artistMatches = true;
                        }
                    }
                    if (!artistMatches) {
                        JsonNode collectionArtistNode = result.get("collectionArtistName");
                        if (collectionArtistNode != null) {
                            String collectionArtist = collectionArtistNode.asText();
                            if (normalizeForComparison(collectionArtist).equals(normalizeForComparison(artistName))) {
                                artistMatches = true;
                            }
                        }
                    }
                    if (!artistMatches) {
                        continue;
                    }
                    
                    // Check if collectionName = song + " - Single" OR song + " - EP"
                    JsonNode collectionNameNode = result.get("collectionName");
                    if (collectionNameNode != null && !collectionNameNode.isNull()) {
                        String collectionName = collectionNameNode.asText();
                        String normalizedCollection = normalizeForComparison(collectionName);
                        if (normalizedCollection.equals(normalizedSongWithSingle) || 
                            normalizedCollection.equals(normalizedSongWithEP)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                log("  ✓ Found via song-only search");
                                return artworkNode.asText();
                            }
                        }
                    }
                }
            }
        } else {
            // For albums: Search by artist only and iterate through all their albums
            // Use limit=200 (Apple Music's max) to get as many as possible
            log("  → Trying broader search (artist only)...");
            respectRateLimit();
            
            url = APPLE_MUSIC_API + "?term=" + encodedArtist + "&entity=album&limit=200";
            response = makeHttpRequest(url);
            root = objectMapper.readTree(response);

            results = root.get("results");
            if (results != null && results.isArray()) {
                String normalizedSearch = normalizeForComparison(itemName);
                String fallbackUrl = null; // For album variations (Deluxe, Remaster, etc.)
                
                for (JsonNode result : results) {
                    JsonNode nameNode = result.get("collectionName");
                    if (nameNode != null) {
                        String apiName = nameNode.asText();
                        String normalizedApi = normalizeForComparison(apiName);
                        
                        // Check for EXACT match first (highest priority)
                        if (normalizedApi.equals(normalizedSearch)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                return artworkNode.asText(); // Return immediately for exact match
                            }
                        }
                        
                        // Check if it's an allowed variation (fallback)
                        if (fallbackUrl == null && isAlbumNameMatch(itemName, apiName)) {
                            JsonNode artworkNode = result.get("artworkUrl100");
                            if (artworkNode != null && !artworkNode.isNull()) {
                                fallbackUrl = artworkNode.asText(); // Save as fallback, keep looking for exact
                            }
                        }
                    }
                }
                
                // If we found a variation match but no exact match, use it
                if (fallbackUrl != null) {
                    return fallbackUrl;
                }
            }
        }

        return null;
    }

    /**
     * Upgrade Apple Music image URL to higher resolution
     * Example: https://.../100x100bb.jpg -> https://.../3000x3000bb.jpg
     */
    private String upgradeImageUrl(String originalUrl, int resolution) {
        if (originalUrl == null || originalUrl.isEmpty()) {
            return originalUrl;
        }
        
        // Replace any size pattern (e.g., 100x100bb, 600x600bb) with desired resolution
        String upgraded = originalUrl.replaceAll("\\d+x\\d+bb", resolution + "x" + resolution + "bb");
        
        // Also handle cases without 'bb' suffix
        if (upgraded.equals(originalUrl)) {
            upgraded = originalUrl.replaceAll("\\d+x\\d+", resolution + "x" + resolution);
        }
        
        return upgraded;
    }

    /**
     * Determine if image should be updated based on size comparison
     */
    private boolean shouldUpdateImage(int currentSize, int newSize) {
        if (currentSize == 0) {
            log("  → Will save (no existing image)");
            return true;
        }
        
        // Force mode (when both missingOnly and upgradeOnly are false)
        // Replace everything EXCEPT when sizes are exactly the same
        boolean isForceMode = !missingOnly && !upgradeOnly;
        
        if (isForceMode) {
            if (currentSize == newSize) {
                log("  ⚠ Skipping (Apple image is same exact size)");
                return false;
            } else {
                int sizeDifferenceKb = (newSize - currentSize) / 1024;
                if (sizeDifferenceKb > 0) {
                    log("  → Will upgrade (+" + sizeDifferenceKb + " KB improvement) [FORCE MODE]");
                } else {
                    log("  → Will replace (" + sizeDifferenceKb + " KB difference) [FORCE MODE - Apple is source of truth]");
                }
                return true;
            }
        }
        
        // Normal mode: check threshold
        int sizeDifferenceKb = (newSize - currentSize) / 1024;
        
        if (sizeDifferenceKb >= minUpgradeKb) {
            log("  → Will upgrade (+" + sizeDifferenceKb + " KB improvement)");
            return true;
        } else if (sizeDifferenceKb > 0) {
            log("  ⚠ Skipping (only +" + sizeDifferenceKb + " KB, below threshold)");
            return false;
        } else {
            log("  ⚠ Skipping (Apple image is same or smaller quality)");
            return false;
        }
    }

    /**
     * Normalize string for fuzzy comparison
     */
    private String normalizeForComparison(String s) {
        if (s == null) return "";
        return s.toLowerCase()
                .replaceAll("[^a-z0-9]", "")
                .trim();
    }

    /**
     * Check if two album names match, allowing for common variations like Deluxe, Remaster, etc.
     * Returns true if they're an exact match OR if the difference only contains allowed keywords.
     * Returns false if the difference contains "single" or "ep" (different release types)
     */
    private boolean isAlbumNameMatch(String searchName, String apiName) {
        String normalizedSearch = normalizeForComparison(searchName);
        String normalizedApi = normalizeForComparison(apiName);
        
        // Exact match - always good
        if (normalizedSearch.equals(normalizedApi)) {
            return true;
        }
        
        // Check if one contains the other
        boolean searchContainsApi = normalizedSearch.contains(normalizedApi);
        boolean apiContainsSearch = normalizedApi.contains(normalizedSearch);
        
        if (!searchContainsApi && !apiContainsSearch) {
            return false; // Not even close
        }
        
        // Get the "difference" part - what's extra in one vs the other
        String lowerSearch = searchName.toLowerCase();
        String lowerApi = apiName.toLowerCase();
        
        // REJECT if either contains "single" or "ep" markers
        if (lowerApi.contains("single") || lowerApi.contains(" ep") || 
            lowerSearch.contains("single") || lowerSearch.contains(" ep")) {
            return false;
        }
        
        // ALLOW if the API name contains any of these permitted variation keywords
        String[] allowedKeywords = {"deluxe", "remaster", "main", "clean", "explicit", 
                                     "edition", "version", "anniversary", "expanded", "special", "revised"};
        
        for (String keyword : allowedKeywords) {
            if (lowerApi.contains(keyword)) {
                return true; // It's a permitted variation
            }
        }
        
        // If we get here: one contains the other, but no allowed keywords found
        // This means it's probably a different album (like "Savage" vs "Savage II")
        return false;
    }

    /**
     * Make HTTP GET request and return response body
     */
    private String makeHttpRequest(String urlString) throws IOException {
        URI uri = URI.create(urlString);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setRequestProperty("Accept", "application/json");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(30000);

        int responseCode = conn.getResponseCode();
        if (responseCode == 404) {
            return "{\"results\":[]}";
        } else if (responseCode == 429) {
            throw new IOException("Rate limited (429) - wait before retrying");
        } else if (responseCode != 200) {
            throw new IOException("HTTP " + responseCode + ": " + conn.getResponseMessage());
        }

        try (InputStream is = conn.getInputStream()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Download binary image data
     */
    private byte[] downloadImage(String urlString) throws IOException {
        URI uri = URI.create(urlString);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", USER_AGENT);
        conn.setInstanceFollowRedirects(true);
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(60000);

        int responseCode = conn.getResponseCode();
        if (responseCode == 307 || responseCode == 302 || responseCode == 301) {
            String newUrl = conn.getHeaderField("Location");
            conn.disconnect();
            if (newUrl != null && !newUrl.isEmpty()) {
                return downloadImage(newUrl);
            }
            return null;
        } else if (responseCode == 404) {
            return null;
        } else if (responseCode != 200) {
            throw new IOException("HTTP " + responseCode + ": " + conn.getResponseMessage());
        }

        try (InputStream is = conn.getInputStream()) {
            return is.readAllBytes();
        }
    }

    /**
     * Respect Apple Music rate limit (be nice to their servers, ese)
     */
    private void respectRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long timeSinceLastRequest = now - lastAppleRequest;

        if (timeSinceLastRequest < APPLE_DELAY_MS) {
            long sleepTime = APPLE_DELAY_MS - timeSinceLastRequest;
            Thread.sleep(sleepTime);
        }

        lastAppleRequest = System.currentTimeMillis();
    }

    /**
     * Update album image in database
     */
    private void updateAlbumImage(int albumId, byte[] imageData) {
        String sql = "UPDATE Album SET image = ? WHERE id = ?";
        jdbcTemplate.update(sql, imageData, albumId);
    }

    /**
     * Update song image in database
     */
    private void updateSongImage(int songId, byte[] imageData) {
        String sql = "UPDATE Song SET single_cover = ? WHERE id = ?";
        jdbcTemplate.update(sql, imageData, songId);
    }

    /**
     * Format bytes to human-readable format
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024)
            return bytes + " B";
        if (bytes < 1024 * 1024)
            return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }

    /**
     * Log message to both console and file
     */
    private void log(String message) {
        System.out.println(message);
        if (logWriter != null) {
            logWriter.println(message);
        }
    }

    /**
     * Log error message to both console and file
     */
    private void logError(String message) {
        System.err.println(message);
        if (logWriter != null) {
            logWriter.println(message);
        }
    }

    /**
     * Print summary statistics
     */
    private void printSummary() {
        log("========================================");
        log("Summary");
        log("========================================");
        
        if (!songsOnly) {
            log("ALBUMS:");
            log("  Processed: " + albumsProcessed);
            log("  Found in Apple Music: " + albumsFound);
            log("  New images added: " + albumImagesUpdated);
            log("  Images upgraded: " + albumImagesUpgraded);
            log("  Skipped (below threshold): " + albumImagesSkippedSameQuality);
            log("");
        }
        
        if (!albumsOnly) {
            log("SONGS/SINGLES:");
            log("  Processed: " + songsProcessed);
            log("  Found in Apple Music: " + songsFound);
            log("  New images added: " + songImagesUpdated);
            log("  Images upgraded: " + songImagesUpgraded);
            log("  Skipped (below threshold): " + songImagesSkippedSameQuality);
            log("");
        }
        
        log("API errors: " + apiErrors);
        if (rateLimitErrors > 0) {
            log("Rate limit errors (still failing): " + rateLimitErrors);
        }
        log("");

        if (dryRun) {
            log("*** DRY RUN - No changes were saved to database ***");
        } else {
            log("¡Dale! Changes saved to database, papi.");
        }
    }

    /**
     * Data class for items (albums or songs)
     */
    private static class ItemData {
        int id;
        String name;
        String artistName;
        String artistCountry;
        int currentImageSize;
    }
}
