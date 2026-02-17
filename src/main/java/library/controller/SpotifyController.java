package library.controller;

import library.service.AlbumService;
import library.service.SongService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Controller for Spotify API integration.
 * Provides endpoints to search Spotify for album/song artwork and metadata.
 * 
 * Spotify Web API:
 * - Requires OAuth 2.0 authentication (Client Credentials flow for server-to-server)
 * - Token endpoint: https://accounts.spotify.com/api/token
 * - Search endpoint: https://api.spotify.com/v1/search
 * - Tokens expire after 3600 seconds (1 hour)
 * - Rate limits: rolling 30-second window (429 errors when exceeded)
 * 
 * To use:
 * 1. Create a Spotify Developer account at https://developer.spotify.com/
 * 2. Create an app in the dashboard to get Client ID and Client Secret
 * 3. Add credentials to application.properties:
 *    spotify.client.id=YOUR_CLIENT_ID
 *    spotify.client.secret=YOUR_CLIENT_SECRET
 */
@RestController
@RequestMapping("/api/spotify")
public class SpotifyController {

    @Autowired
    private AlbumService albumService;
    
    @Autowired
    private SongService songService;

    @Value("${spotify.client.id:}")
    private String clientId;
    
    @Value("${spotify.client.secret:}")
    private String clientSecret;

    private static final String TOKEN_URL = "https://accounts.spotify.com/api/token";
    private static final String SEARCH_URL = "https://api.spotify.com/v1/search";
    private static final String USER_AGENT = "MusicStatsApp/1.0";
    private static final Pattern MARKET_PATTERN = Pattern.compile("^[A-Z]{2}$");
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Token caching
    private String accessToken = null;
    private long tokenExpiresAt = 0;

    /**
     * Check if Spotify is configured (has credentials).
     */
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        boolean configured = clientId != null && !clientId.isEmpty() 
                          && clientSecret != null && !clientSecret.isEmpty();
        status.put("configured", configured);
        
        if (configured) {
            // Try to get a token to verify credentials work
            try {
                getAccessToken();
                status.put("authenticated", true);
            } catch (Exception e) {
                status.put("authenticated", false);
                status.put("error", e.getMessage());
            }
        }
        
        return status;
    }

    /**
     * Search Spotify for images and metadata.
     * Searches album and/or track types and returns artworks.
     * 
     * @param term Search term (e.g., "artist song name")
     * @param entity Entity type to search: "song" (track) or "album" (default: both)
     * @return List of artwork URLs with metadata
     */
    @GetMapping("/search")
    public List<Map<String, String>> searchImages(
            @RequestParam(required = false) String term,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String title,
            @RequestParam(required = false) String market,
            @RequestParam(defaultValue = "") String entity,
            @RequestParam(defaultValue = "50") int limit) {
        
        List<Map<String, String>> results = new ArrayList<>();
        Set<String> seenUrls = new HashSet<>();
        
        try {
            String token = getAccessToken();

            String normalizedMarket = normalizeMarket(market);
            String searchTerm = buildSearchTerm(term, artist, title);
            String encodedTerm = URLEncoder.encode(searchTerm, StandardCharsets.UTF_8);

            // Spotify search max limit is 50
            int safeLimit = Math.max(1, Math.min(limit, 50));
            
            // Determine which types to search based on entity parameter
            // Note: Spotify uses "track" for songs
            boolean searchTracks = entity.isEmpty() || entity.equalsIgnoreCase("song");
            boolean searchAlbums = entity.isEmpty() || entity.equalsIgnoreCase("album");
            
            // Search requested types
            if (searchAlbums) {
                searchSpotify(encodedTerm, "album", token, safeLimit, normalizedMarket, results, seenUrls);
            }
            if (searchTracks) {
                searchSpotify(encodedTerm, "track", token, safeLimit, normalizedMarket, results, seenUrls);
            }
            
        } catch (Exception e) {
            System.err.println("Spotify search error: " + e.getMessage());
            e.printStackTrace();
        }
        
        return results;
    }

    private String buildSearchTerm(String term, String artist, String title) {
        if (term != null && !term.trim().isEmpty()) {
            return term.trim();
        }

        String a = artist != null ? artist.trim() : "";
        String t = title != null ? title.trim() : "";

        if (!a.isEmpty() && !t.isEmpty()) {
            return a + " - " + t;
        }
        if (!a.isEmpty()) {
            return a;
        }
        if (!t.isEmpty()) {
            return t;
        }

        throw new IllegalArgumentException("Missing search parameters. Provide term or artist/title.");
    }

    private String normalizeMarket(String market) {
        if (market == null || market.trim().isEmpty()) {
            return "US";
        }
        String normalized = market.trim().toUpperCase(Locale.ROOT);
        if (!MARKET_PATTERN.matcher(normalized).matches()) {
            // If invalid, omit market entirely rather than erroring at Spotify
            return "";
        }
        return normalized;
    }

    /**
     * Get or refresh the access token using Client Credentials flow.
     */
    private synchronized String getAccessToken() throws Exception {
        // Check if we have a valid cached token (with 60s buffer)
        if (accessToken != null && System.currentTimeMillis() < (tokenExpiresAt - 60000)) {
            return accessToken;
        }
        
        if (clientId == null || clientId.isEmpty() || clientSecret == null || clientSecret.isEmpty()) {
            throw new RuntimeException("Spotify credentials not configured. Add spotify.client.id and spotify.client.secret to application.properties");
        }
        
        // Request new token
        URI uri = URI.create(TOKEN_URL);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        
        // Basic auth header with base64(client_id:client_secret)
        String credentials = clientId + ":" + clientSecret;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + encodedCredentials);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);
        
        // Request body
        String body = "grant_type=client_credentials";
        try (OutputStream os = connection.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        
        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            String errorBody = "";
            try (InputStream is = connection.getErrorStream()) {
                if (is != null) {
                    errorBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
            throw new RuntimeException("Failed to get Spotify token: HTTP " + responseCode + " - " + errorBody);
        }
        
        try (InputStream is = connection.getInputStream()) {
            String response = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            JsonNode root = objectMapper.readTree(response);
            
            accessToken = root.get("access_token").asText();
            int expiresIn = root.get("expires_in").asInt(); // Usually 3600 seconds
            tokenExpiresAt = System.currentTimeMillis() + (expiresIn * 1000L);
            
            System.out.println("Spotify token refreshed, expires in " + expiresIn + " seconds");
            return accessToken;
        }
    }

    private void searchSpotify(String encodedTerm, String type, String token, int limit, String market,
                               List<Map<String, String>> results, Set<String> seenUrls) throws Exception {
        try {
            String url = SEARCH_URL + "?q=" + encodedTerm + "&type=" + type + "&limit=" + limit;
            if (market != null && !market.isEmpty()) {
                url += "&market=" + market;
            }
            String response = makeAuthenticatedRequest(url, token);
            JsonNode root = objectMapper.readTree(response);
            
            // Response structure: { "albums": { "items": [...] }, "tracks": { "items": [...] } }
            String itemsKey = type.equals("album") ? "albums" : "tracks";
            JsonNode itemsContainer = root.get(itemsKey);
            
            if (itemsContainer != null) {
                JsonNode items = itemsContainer.get("items");
                if (items != null && items.isArray()) {
                    for (JsonNode item : items) {
                        addSpotifyResultToList(item, type, results, seenUrls);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error searching Spotify " + type + ": " + e.getMessage());
        }
    }

    private void addSpotifyResultToList(JsonNode item, String type, 
                                         List<Map<String, String>> results, Set<String> seenUrls) {
        // For tracks, artwork is in item.album.images
        // For albums, artwork is in item.images
        JsonNode imagesArray;
        String albumName = "";
        String releaseDate = "";
        
        if (type.equals("track")) {
            JsonNode albumNode = item.get("album");
            if (albumNode != null) {
                imagesArray = albumNode.get("images");
                albumName = getNodeText(albumNode, "name");
                releaseDate = getNodeText(albumNode, "release_date");
            } else {
                return;
            }
        } else {
            imagesArray = item.get("images");
            releaseDate = getNodeText(item, "release_date");
        }
        
        if (imagesArray == null || !imagesArray.isArray() || imagesArray.isEmpty()) {
            return;
        }
        
        // Spotify images come in sizes: 640, 300, 64 (largest first usually)
        String thumbnailUrl = null;
        String fullUrl = null;
        
        for (JsonNode img : imagesArray) {
            String imgUrl = getNodeText(img, "url");
            int height = img.has("height") && !img.get("height").isNull() ? img.get("height").asInt() : 0;
            
            if (height >= 600 || fullUrl == null) {
                fullUrl = imgUrl;
            }
            if (height >= 200 && height <= 400) {
                thumbnailUrl = imgUrl;
            }
        }
        
        // If no thumbnail found, use the first (largest) image
        if (thumbnailUrl == null && fullUrl != null) {
            thumbnailUrl = fullUrl;
        }
        
        if (fullUrl == null) {
            return;
        }
        
        // Use fullUrl as unique key
        if (seenUrls.contains(fullUrl)) {
            return;
        }
        seenUrls.add(fullUrl);
        
        Map<String, String> artwork = new HashMap<>();
        artwork.put("thumbnailUrl", thumbnailUrl);
        artwork.put("fullUrl", fullUrl);
        artwork.put("type", type.equals("track") ? "song" : "album"); // Normalize to match Apple's naming
        
        // Get artist name (array of artists)
        String artistName = "";
        JsonNode artistsArray = item.get("artists");
        if (artistsArray != null && artistsArray.isArray() && !artistsArray.isEmpty()) {
            artistName = getNodeText(artistsArray.get(0), "name");
        }
        artwork.put("artistName", artistName);
        
        // Track/Album name
        String name = getNodeText(item, "name");
        artwork.put("title", name);
        
        if (type.equals("track")) {
            artwork.put("albumName", albumName);
        }
        
        // Release date (format: YYYY-MM-DD or YYYY or YYYY-MM)
        if (!releaseDate.isEmpty()) {
            // Normalize to YYYY-MM-DD if possible
            if (releaseDate.length() == 4) {
                releaseDate = releaseDate + "-01-01"; // Just year
            } else if (releaseDate.length() == 7) {
                releaseDate = releaseDate + "-01"; // Year-month
            }
            artwork.put("releaseDate", releaseDate);
        }
        
        // Track duration (only for tracks)
        if (type.equals("track")) {
            JsonNode durationNode = item.get("duration_ms");
            if (durationNode != null && !durationNode.isNull()) {
                long millis = durationNode.asLong();
                artwork.put("trackTimeMillis", String.valueOf(millis));
                long totalSeconds = millis / 1000;
                long minutes = totalSeconds / 60;
                long seconds = totalSeconds % 60;
                artwork.put("lengthFormatted", String.format("%d:%02d", minutes, seconds));
                artwork.put("lengthSeconds", String.valueOf(totalSeconds));
            }
        }
        
        results.add(artwork);
    }

    private String getNodeText(JsonNode node, String field) {
        if (node == null) return "";
        JsonNode fieldNode = node.get(field);
        return fieldNode != null && !fieldNode.isNull() ? fieldNode.asText() : "";
    }

    private String makeAuthenticatedRequest(String urlString, String token) throws Exception {
        URI uri = URI.create(urlString);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Authorization", "Bearer " + token);
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);

        int responseCode = connection.getResponseCode();
        if (responseCode == 401) {
            // Token expired, clear it so next call refreshes
            accessToken = null;
            tokenExpiresAt = 0;
            throw new RuntimeException("Spotify token expired");
        }
        if (responseCode == 429) {
            // Rate limited - check Retry-After header
            String retryAfter = connection.getHeaderField("Retry-After");
            throw new RuntimeException("Rate limited by Spotify API. Retry after: " + retryAfter + " seconds");
        }
        if (responseCode != 200) {
            String errorBody = "";
            try (InputStream es = connection.getErrorStream()) {
                if (es != null) {
                    errorBody = new String(es.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
            if (errorBody != null && !errorBody.isBlank()) {
                throw new RuntimeException("HTTP error: " + responseCode + " - " + errorBody);
            }
            throw new RuntimeException("HTTP error: " + responseCode);
        }

        try (InputStream is = connection.getInputStream()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Download image from URL and return bytes.
     */
    private byte[] downloadImage(String imageUrl) throws Exception {
        URI uri = URI.create(imageUrl);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setConnectTimeout(15000);
        connection.setReadTimeout(15000);

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("HTTP error downloading image: " + responseCode);
        }

        try (InputStream is = connection.getInputStream()) {
            return is.readAllBytes();
        }
    }

    /**
     * DTO for save images request
     */
    public static class SaveImagesRequest {
        private List<String> imageUrls;
        private Long entityId;
        private String entityType; // "album" or "song"
        private boolean hasExistingImage;

        public List<String> getImageUrls() { return imageUrls; }
        public void setImageUrls(List<String> imageUrls) { this.imageUrls = imageUrls; }
        public Long getEntityId() { return entityId; }
        public void setEntityId(Long entityId) { this.entityId = entityId; }
        public String getEntityType() { return entityType; }
        public void setEntityType(String entityType) { this.entityType = entityType; }
        public boolean isHasExistingImage() { return hasExistingImage; }
        public void setHasExistingImage(boolean hasExistingImage) { this.hasExistingImage = hasExistingImage; }
    }

    /**
     * Save selected images from Spotify to album or song.
     * If entity has no existing image, first selected becomes default, rest are secondary.
     * If entity has existing image, all selected become secondary.
     */
    @PostMapping("/save-images")
    public Map<String, Object> saveImages(@RequestBody SaveImagesRequest request) {
        Map<String, Object> response = new HashMap<>();
        int saved = 0;
        int skippedDuplicates = 0;
        List<String> errors = new ArrayList<>();

        try {
            List<String> urls = request.getImageUrls();
            Long entityId = request.getEntityId();
            Integer id = entityId != null ? entityId.intValue() : null;
            String entityType = request.getEntityType();
            boolean hasExisting = request.isHasExistingImage();

            for (int i = 0; i < urls.size(); i++) {
                String url = urls.get(i);
                try {
                    byte[] imageBytes = downloadImage(url);
                    
                    if (entityType.equals("album")) {
                        // Check for duplicate before saving
                        if (albumService.isDuplicateImage(id, imageBytes)) {
                            skippedDuplicates++;
                            continue;
                        }
                        if (!hasExisting && i == 0) {
                            albumService.updateAlbumImage(id, imageBytes);
                            hasExisting = true;
                        } else {
                            albumService.addSecondaryImage(id, imageBytes);
                        }
                    } else if (entityType.equals("song")) {
                        // Check for duplicate before saving
                        if (songService.isDuplicateImage(id, imageBytes)) {
                            skippedDuplicates++;
                            continue;
                        }
                        if (!hasExisting && i == 0) {
                            songService.updateSongImage(id, imageBytes);
                            hasExisting = true;
                        } else {
                            songService.addSecondaryImage(id, imageBytes);
                        }
                    } else {
                        errors.add("Unknown entity type: " + entityType);
                        continue;
                    }
                    saved++;
                } catch (Exception e) {
                    errors.add("Failed to download image " + (i + 1) + ": " + e.getMessage());
                }
            }

            response.put("success", true);
            response.put("savedCount", saved);
            if (skippedDuplicates > 0) {
                response.put("skippedDuplicates", skippedDuplicates);
            }
            if (!errors.isEmpty()) {
                response.put("errors", errors);
            }
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return response;
    }
}
