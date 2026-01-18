package library.controller;

import library.service.AlbumService;
import library.service.SongService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Controller for Deezer API integration.
 * Provides endpoints to search Deezer for album/song artwork and metadata.
 * 
 * Deezer API:
 * - No authentication required for public search (completely FREE!)
 * - Search endpoint: https://api.deezer.com/search/track
 * - Returns cover_xl (1000x1000), cover_big (500x500), cover_medium (250x250)
 * - Rate limit: 50 requests per 5 seconds
 */
@RestController
@RequestMapping("/api/deezer")
public class DeezerController {

    @Autowired
    private AlbumService albumService;
    
    @Autowired
    private SongService songService;

    private static final String DEEZER_API = "https://api.deezer.com";
    private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Check if Deezer is available (always configured since it's free).
     */
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("configured", true);
        status.put("authenticated", true);
        return status;
    }

    /**
     * Search Deezer for images and metadata.
     * Searches for tracks and/or albums and returns artworks with metadata.
     * 
     * @param term Search term (e.g., "artist song name")
     * @param entity Entity type to search: "song" (track) or "album" (default: both)
     * @return List of artwork URLs with metadata (same format as Apple Music/Spotify)
     */
    @GetMapping("/search")
    public List<Map<String, String>> searchImages(
            @RequestParam String term,
            @RequestParam(defaultValue = "") String entity,
            @RequestParam(defaultValue = "50") int limit) {
        
        List<Map<String, String>> results = new ArrayList<>();
        Set<String> seenUrls = new HashSet<>();
        
        // Determine which types to search based on entity parameter
        boolean searchTracks = entity.isEmpty() || entity.equalsIgnoreCase("song");
        boolean searchAlbums = entity.isEmpty() || entity.equalsIgnoreCase("album");
        
        try {
            String encodedTerm = URLEncoder.encode(term, StandardCharsets.UTF_8);
            
            if (searchTracks) {
                searchDeezerTracks(encodedTerm, limit, results, seenUrls);
            }
            if (searchAlbums) {
                searchDeezerAlbums(encodedTerm, limit, results, seenUrls);
            }
            
        } catch (Exception e) {
            System.err.println("Deezer search error: " + e.getMessage());
            e.printStackTrace();
        }
        
        return results;
    }

    /**
     * Search Deezer for tracks.
     */
    private void searchDeezerTracks(String query, int limit, List<Map<String, String>> results, Set<String> seenUrls) {
        try {
            String url = DEEZER_API + "/search/track?q=" + query + "&limit=" + limit;
            String response = makeHttpRequest(url);
            JsonNode root = objectMapper.readTree(response);
            JsonNode data = root.get("data");
            
            if (data != null && data.isArray()) {
                for (JsonNode track : data) {
                    addTrackToResults(track, results, seenUrls);
                }
            }
        } catch (Exception e) {
            System.err.println("Error searching Deezer tracks: " + e.getMessage());
        }
    }
    
    /**
     * Search Deezer for albums.
     */
    private void searchDeezerAlbums(String query, int limit, List<Map<String, String>> results, Set<String> seenUrls) {
        try {
            String url = DEEZER_API + "/search/album?q=" + query + "&limit=" + limit;
            String response = makeHttpRequest(url);
            JsonNode root = objectMapper.readTree(response);
            JsonNode data = root.get("data");
            
            if (data != null && data.isArray()) {
                for (JsonNode album : data) {
                    addAlbumToResults(album, results, seenUrls);
                }
            }
        } catch (Exception e) {
            System.err.println("Error searching Deezer albums: " + e.getMessage());
        }
    }
    
    /**
     * Add a Deezer album to results (same format as Apple Music/Spotify).
     */
    private void addAlbumToResults(JsonNode album, List<Map<String, String>> results, Set<String> seenUrls) {
        // Get cover URLs (Deezer provides different sizes)
        String coverXl = getNodeText(album, "cover_xl");      // 1000x1000
        String coverBig = getNodeText(album, "cover_big");    // 500x500
        String coverMedium = getNodeText(album, "cover_medium"); // 250x250
        
        // Use XL as the full URL, fall back to big then medium
        String fullUrl = !coverXl.isEmpty() ? coverXl : (!coverBig.isEmpty() ? coverBig : coverMedium);
        String thumbUrl = !coverMedium.isEmpty() ? coverMedium : (!coverBig.isEmpty() ? coverBig : fullUrl);
        
        if (fullUrl.isEmpty() || seenUrls.contains(fullUrl)) {
            return; // Skip if no cover or already seen
        }
        
        seenUrls.add(fullUrl);
        
        Map<String, String> artwork = new HashMap<>();
        artwork.put("thumbnailUrl", thumbUrl);
        artwork.put("fullUrl", fullUrl);
        artwork.put("type", "album");
        
        // Artist info
        JsonNode artistNode = album.get("artist");
        if (artistNode != null) {
            artwork.put("artistName", getNodeText(artistNode, "name"));
        }
        
        // Album info
        artwork.put("title", getNodeText(album, "title"));
        artwork.put("albumName", getNodeText(album, "title"));
        
        // For release date and track count, fetch album details
        int albumId = album.has("id") ? album.get("id").asInt() : 0;
        if (albumId > 0) {
            artwork.put("albumId", String.valueOf(albumId));
            try {
                String albumData = fetchAlbumDetails(albumId);
                if (albumData != null) {
                    JsonNode albumDetails = objectMapper.readTree(albumData);
                    String releaseDate = getNodeText(albumDetails, "release_date");
                    if (!releaseDate.isEmpty()) {
                        artwork.put("releaseDate", releaseDate);
                    }
                }
            } catch (Exception e) {
                // Ignore - we'll just not have the release date
            }
        }
        
        results.add(artwork);
    }

    /**
     * Add a Deezer track to results (same format as Apple Music/Spotify).
     */
    private void addTrackToResults(JsonNode track, List<Map<String, String>> results, Set<String> seenUrls) {
        JsonNode album = track.get("album");
        if (album == null) return;
        
        // Get cover URLs (Deezer provides different sizes)
        String coverXl = getNodeText(album, "cover_xl");      // 1000x1000
        String coverBig = getNodeText(album, "cover_big");    // 500x500
        String coverMedium = getNodeText(album, "cover_medium"); // 250x250
        
        // Use XL as the full URL, fall back to big then medium
        String fullUrl = !coverXl.isEmpty() ? coverXl : (!coverBig.isEmpty() ? coverBig : coverMedium);
        String thumbUrl = !coverMedium.isEmpty() ? coverMedium : (!coverBig.isEmpty() ? coverBig : fullUrl);
        
        if (fullUrl.isEmpty() || seenUrls.contains(fullUrl)) {
            return; // Skip if no cover or already seen
        }
        
        seenUrls.add(fullUrl);
        
        Map<String, String> artwork = new HashMap<>();
        artwork.put("thumbnailUrl", thumbUrl);
        artwork.put("fullUrl", fullUrl);
        artwork.put("type", "track");
        
        // Artist info
        JsonNode artistNode = track.get("artist");
        if (artistNode != null) {
            artwork.put("artistName", getNodeText(artistNode, "name"));
        }
        
        // Track info
        artwork.put("title", getNodeText(track, "title"));
        artwork.put("albumName", getNodeText(album, "title"));
        
        // Duration in seconds - Deezer provides duration directly in seconds
        JsonNode durationNode = track.get("duration");
        if (durationNode != null && !durationNode.isNull()) {
            int seconds = durationNode.asInt();
            artwork.put("lengthSeconds", String.valueOf(seconds));
            long minutes = seconds / 60;
            long secs = seconds % 60;
            artwork.put("lengthFormatted", String.format("%d:%02d", minutes, secs));
            artwork.put("trackTimeMillis", String.valueOf(seconds * 1000L));
        }
        
        // For release date, we need to fetch album details (Deezer doesn't include it in search)
        // We'll do this lazily if needed - for now just leave it empty
        // The album endpoint /album/{id} has release_date field
        int albumId = album.has("id") ? album.get("id").asInt() : 0;
        if (albumId > 0) {
            artwork.put("albumId", String.valueOf(albumId));
            // Try to fetch release date from album
            try {
                String albumData = fetchAlbumDetails(albumId);
                if (albumData != null) {
                    JsonNode albumDetails = objectMapper.readTree(albumData);
                    String releaseDate = getNodeText(albumDetails, "release_date");
                    if (!releaseDate.isEmpty()) {
                        artwork.put("releaseDate", releaseDate);
                    }
                }
            } catch (Exception e) {
                // Ignore - release date is optional
            }
        }
        
        results.add(artwork);
    }

    /**
     * Fetch album details from Deezer to get release date.
     */
    private String fetchAlbumDetails(int albumId) {
        try {
            String url = DEEZER_API + "/album/" + albumId;
            return makeHttpRequest(url);
        } catch (Exception e) {
            return null;
        }
    }

    private String getNodeText(JsonNode node, String field) {
        if (node == null) return "";
        JsonNode fieldNode = node.get(field);
        return fieldNode != null && !fieldNode.isNull() ? fieldNode.asText() : "";
    }

    private String makeHttpRequest(String urlString) throws Exception {
        URI uri = URI.create(urlString);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);

        int responseCode = connection.getResponseCode();
        if (responseCode == 429) {
            throw new RuntimeException("Rate limited by Deezer API");
        }
        if (responseCode != 200) {
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
     * DTO for save images request (same as Apple Music/Spotify).
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
     * Save selected images from Deezer to album or song.
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
