package library;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Standalone script to populate album covers using TheAudioDB free API.
 * 
 * Features:
 * - Populates Album.image from TheAudioDB
 * - Can update existing images if API has higher quality version
 * - Supports different image sizes (small, medium, original)
 * - Respects TheAudioDB rate limit (30 requests per minute for free tier)
 * 
 * TheAudioDB Free API:
 * - API Key: 123 (free tier)
 * - Rate limit: 30 requests/minute
 * - Search endpoint: /searchalbum.php?s=artist&a=album
 * - Returns strAlbumThumb for album cover URL
 * 
 * Usage: Run main() method directly from IDE or via:
 * mvnw exec:java -Dexec.mainClass="library.AudioDbAlbumCoverPopulator"
 * 
 * Options:
 * --dry-run           Don't save changes to database
 * --force             Re-populate even if image already exists
 * --upgrade-only      Only upgrade existing images if API has higher quality
 * --missing-only      Only populate albums without images (default)
 * --image-small       Use 250px images (/small suffix)
 * --image-medium      Use 500px images (/medium suffix)
 * --image-original    Use original full-quality images (default)
 * --min-upgrade-kb=N  Minimum KB difference to consider an upgrade (default: 50)
 */
public class AudioDbAlbumCoverPopulator {

    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";

    // TheAudioDB API Configuration
    private static final String AUDIODB_API = "https://www.theaudiodb.com/api/v1/json";
    private static final String API_KEY = "123"; // Free tier API key
    private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
    
    // Rate limiting: 30 requests per minute = 1 request per 2 seconds
    private static final long AUDIODB_DELAY_MS = 2200; // 2.1 seconds to be safe

    // Image quality: "" for original, "/medium" for 500px, "/small" for 250px
    private String imageSizeSuffix = ""; // Default to original quality

    private JdbcTemplate jdbcTemplate;
    private ObjectMapper objectMapper;
    private long lastAudioDbRequest = 0;

    // Statistics
    private int albumsProcessed = 0;
    private int albumsFound = 0;
    private int albumImagesUpdated = 0;
    private int albumImagesUpgraded = 0;
    private int albumImagesSkippedSameQuality = 0;
    private int apiErrors = 0;

    // Configuration flags
    private boolean dryRun = false;
    private boolean missingOnly = true;  // Default: only albums without images
    private boolean upgradeOnly = false; // Only upgrade existing images
    private int minUpgradeKb = 50;        // Minimum KB difference for upgrade

    public AudioDbAlbumCoverPopulator() {
        // Setup database connection
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) {
        AudioDbAlbumCoverPopulator populator = new AudioDbAlbumCoverPopulator();

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
            } else if (lowerArg.equals("--image-small")) {
                populator.imageSizeSuffix = "/small";
            } else if (lowerArg.equals("--image-medium")) {
                populator.imageSizeSuffix = "/medium";
            } else if (lowerArg.equals("--image-original")) {
                populator.imageSizeSuffix = "";
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
        System.out.println("========================================");
        System.out.println("TheAudioDB Album Cover Populator");
        System.out.println("========================================");
        System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
        
        String modeDesc;
        if (upgradeOnly) {
            modeDesc = "Upgrade only (update existing with higher quality)";
        } else if (missingOnly) {
            modeDesc = "Missing only (albums without images)";
        } else {
            modeDesc = "Force (all albums, replace existing)";
        }
        System.out.println("Processing: " + modeDesc);
        
        String imageSizeDesc = switch (imageSizeSuffix) {
            case "/small" -> "Small (250px)";
            case "/medium" -> "Medium (500px)";
            default -> "Original (full quality)";
        };
        System.out.println("Image quality: " + imageSizeDesc);
        
        if (upgradeOnly || !missingOnly) {
            System.out.println("Min upgrade threshold: " + minUpgradeKb + " KB");
        }
        System.out.println();

        try {
            populateAlbumCovers();
        } catch (Exception e) {
            System.err.println("Error populating album covers: " + e.getMessage());
            e.printStackTrace();
        }

        printSummary();
    }

    /**
     * Populate album covers from TheAudioDB
     */
    private void populateAlbumCovers() {
        String sql;
        
        if (upgradeOnly) {
            // Only albums that already have an image
            sql = """
                SELECT a.id, a.name, ar.name as artist_name, LENGTH(a.image) as current_size
                FROM Album a
                INNER JOIN Artist ar ON a.artist_id = ar.id
                WHERE a.image IS NOT NULL
                ORDER BY a.id
                """;
        } else if (missingOnly) {
            // Only albums without images
            sql = """
                SELECT a.id, a.name, ar.name as artist_name, 0 as current_size
                FROM Album a
                INNER JOIN Artist ar ON a.artist_id = ar.id
                WHERE a.image IS NULL
                ORDER BY a.id
                """;
        } else {
            // All albums
            sql = """
                SELECT a.id, a.name, ar.name as artist_name, COALESCE(LENGTH(a.image), 0) as current_size
                FROM Album a
                INNER JOIN Artist ar ON a.artist_id = ar.id
                ORDER BY a.id
                """;
        }

        List<AlbumData> albums = jdbcTemplate.query(sql, (rs, rowNum) -> {
            AlbumData album = new AlbumData();
            album.id = rs.getInt("id");
            album.name = rs.getString("name");
            album.artistName = rs.getString("artist_name");
            album.currentImageSize = rs.getInt("current_size");
            return album;
        });

        System.out.println("Found " + albums.size() + " albums to process");
        System.out.println();

        for (AlbumData album : albums) {
            albumsProcessed++;

            try {
                System.out.println("[" + albumsProcessed + "/" + albums.size() + "] Processing: " 
                        + album.artistName + " - " + album.name);
                
                if (album.currentImageSize > 0) {
                    System.out.println("  Current image: " + formatBytes(album.currentImageSize));
                }

                // Search TheAudioDB for album
                String imageUrl = searchAlbumCover(album.artistName, album.name);

                if (imageUrl != null && !imageUrl.isEmpty()) {
                    albumsFound++;
                    
                    // Add size suffix if configured
                    String finalUrl = imageUrl + imageSizeSuffix;
                    System.out.println("  ✓ Found cover: " + finalUrl);

                    // Download the image
                    byte[] imageData = downloadImage(finalUrl);

                    if (imageData != null && imageData.length > 0) {
                        System.out.println("  ✓ Downloaded: " + formatBytes(imageData.length));

                        // Decide if we should update
                        boolean shouldUpdate = false;
                        
                        if (album.currentImageSize == 0) {
                            // No existing image, always update
                            shouldUpdate = true;
                            System.out.println("  → Will save (no existing image)");
                        } else {
                            // Compare sizes
                            int sizeDifferenceKb = (imageData.length - album.currentImageSize) / 1024;
                            
                            if (sizeDifferenceKb >= minUpgradeKb) {
                                shouldUpdate = true;
                                System.out.println("  → Will upgrade (+" + sizeDifferenceKb + " KB improvement)");
                            } else if (sizeDifferenceKb > 0) {
                                System.out.println("  ⚠ Skipping (only +" + sizeDifferenceKb + " KB, below threshold)");
                                albumImagesSkippedSameQuality++;
                            } else {
                                System.out.println("  ⚠ Skipping (API image is same or smaller quality)");
                                albumImagesSkippedSameQuality++;
                            }
                        }

                        if (shouldUpdate) {
                            if (!dryRun) {
                                updateAlbumImage(album.id, imageData);
                            }
                            
                            if (album.currentImageSize > 0) {
                                albumImagesUpgraded++;
                            } else {
                                albumImagesUpdated++;
                            }
                        }
                    } else {
                        System.out.println("  ⚠ Could not download image");
                    }
                } else {
                    System.out.println("  ✗ Not found in TheAudioDB");
                }

            } catch (Exception e) {
                System.err.println("  ✗ Error: " + e.getMessage());
                apiErrors++;
            }

            System.out.println();
        }
    }

    /**
     * Search TheAudioDB for an album cover
     * Returns the cover image URL or null if not found
     */
    private String searchAlbumCover(String artistName, String albumName) throws IOException, InterruptedException {
        respectRateLimit();

        // First try: search with both artist and album name
        String encodedArtist = URLEncoder.encode(artistName, StandardCharsets.UTF_8);
        String encodedAlbum = URLEncoder.encode(albumName, StandardCharsets.UTF_8);
        
        String url = AUDIODB_API + "/" + API_KEY + "/searchalbum.php?s=" + encodedArtist + "&a=" + encodedAlbum;
        
        String response = makeHttpRequest(url);
        JsonNode root = objectMapper.readTree(response);

        JsonNode albums = root.get("album");
        if (albums != null && albums.isArray() && albums.size() > 0) {
            // Find the best match
            for (JsonNode album : albums) {
                JsonNode thumbNode = album.get("strAlbumThumb");
                if (thumbNode != null && !thumbNode.isNull() && !thumbNode.asText().isEmpty()) {
                    return thumbNode.asText();
                }
                
                // Also check strAlbumThumbHQ for high quality
                JsonNode thumbHqNode = album.get("strAlbumThumbHQ");
                if (thumbHqNode != null && !thumbHqNode.isNull() && !thumbHqNode.asText().isEmpty()) {
                    return thumbHqNode.asText();
                }
            }
        }

        // Second try: search with just artist name and look for matching album
        // This can help with slight album name variations
        url = AUDIODB_API + "/" + API_KEY + "/searchalbum.php?s=" + encodedArtist;
        
        respectRateLimit();
        response = makeHttpRequest(url);
        root = objectMapper.readTree(response);

        albums = root.get("album");
        if (albums != null && albums.isArray()) {
            String normalizedSearch = normalizeForComparison(albumName);
            
            for (JsonNode album : albums) {
                JsonNode nameNode = album.get("strAlbum");
                if (nameNode != null && !nameNode.isNull()) {
                    String apiAlbumName = nameNode.asText();
                    String normalizedApi = normalizeForComparison(apiAlbumName);
                    
                    // Check if it's a close match
                    if (normalizedApi.equals(normalizedSearch) || 
                        normalizedApi.contains(normalizedSearch) || 
                        normalizedSearch.contains(normalizedApi)) {
                        
                        JsonNode thumbNode = album.get("strAlbumThumb");
                        if (thumbNode != null && !thumbNode.isNull() && !thumbNode.asText().isEmpty()) {
                            return thumbNode.asText();
                        }
                        
                        JsonNode thumbHqNode = album.get("strAlbumThumbHQ");
                        if (thumbHqNode != null && !thumbHqNode.isNull() && !thumbHqNode.asText().isEmpty()) {
                            return thumbHqNode.asText();
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Normalize string for fuzzy comparison
     */
    private String normalizeForComparison(String s) {
        if (s == null) return "";
        return s.toLowerCase()
                .replaceAll("[^a-z0-9]", "") // Remove non-alphanumeric
                .trim();
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
            return "{}"; // Not found
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
            // Follow redirect
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
     * Respect TheAudioDB rate limit (30 requests per minute for free tier)
     */
    private void respectRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long timeSinceLastRequest = now - lastAudioDbRequest;

        if (timeSinceLastRequest < AUDIODB_DELAY_MS) {
            long sleepTime = AUDIODB_DELAY_MS - timeSinceLastRequest;
            Thread.sleep(sleepTime);
        }

        lastAudioDbRequest = System.currentTimeMillis();
    }

    /**
     * Update album image in database
     */
    private void updateAlbumImage(int albumId, byte[] imageData) {
        String sql = "UPDATE Album SET image = ? WHERE id = ?";
        jdbcTemplate.update(sql, imageData, albumId);
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
     * Print summary statistics
     */
    private void printSummary() {
        System.out.println("========================================");
        System.out.println("Summary");
        System.out.println("========================================");
        System.out.println("Albums processed: " + albumsProcessed);
        System.out.println("Albums found in TheAudioDB: " + albumsFound);
        System.out.println("New album images added: " + albumImagesUpdated);
        System.out.println("Album images upgraded: " + albumImagesUpgraded);
        System.out.println("Skipped (below quality threshold): " + albumImagesSkippedSameQuality);
        System.out.println("API errors: " + apiErrors);
        System.out.println();

        if (dryRun) {
            System.out.println("*** DRY RUN - No changes were saved to database ***");
        } else {
            System.out.println("Done! Changes saved to database.");
        }
    }

    /**
     * Data class for album
     */
    private static class AlbumData {
        int id;
        String name;
        String artistName;
        int currentImageSize;
    }
}
