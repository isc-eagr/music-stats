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
 * Controller for Apple Music API integration.
 * Provides endpoints to search Apple Music for album/song artwork.
 */
@RestController
@RequestMapping("/api/apple-music")
public class AppleMusicController {

    @Autowired
    private AlbumService albumService;
    
    @Autowired
    private SongService songService;

    private static final String APPLE_MUSIC_API = "https://itunes.apple.com/search";
    private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Search Apple Music for images.
     * Searches album and/or song entities and returns artworks.
     * 
     * @param term Search term (e.g., "artist song name")
     * @param entity Entity type to search: "song" or "album" (default: both)
     * @return List of artwork URLs with metadata
     */
    @GetMapping("/search")
    public List<Map<String, String>> searchImages(
            @RequestParam String term,
            @RequestParam(defaultValue = "") String entity,
            @RequestParam(defaultValue = "50") int limit) {
        
        List<Map<String, String>> results = new ArrayList<>();
        Set<String> seenUrls = new HashSet<>();
        
        String encodedTerm = URLEncoder.encode(term, StandardCharsets.UTF_8);
        
        // Determine which entities to search based on parameter
        boolean searchSongs = entity.isEmpty() || entity.equalsIgnoreCase("song");
        boolean searchAlbums = entity.isEmpty() || entity.equalsIgnoreCase("album");
        
        // Search requested entities
        if (searchAlbums) {
            searchEntity(encodedTerm, "album", limit, results, seenUrls);
        }
        if (searchSongs) {
            searchEntity(encodedTerm, "song", limit, results, seenUrls);
        }
        
        return results;
    }

    private void searchEntity(String encodedTerm, String entity, int limit,
                              List<Map<String, String>> results, Set<String> seenUrls) {
        try {
            String url = APPLE_MUSIC_API + "?term=" + encodedTerm + "&entity=" + entity + "&country=us&limit=" + limit;
            String response = makeHttpRequest(url);
            JsonNode root = objectMapper.readTree(response);
            JsonNode resultsNode = root.get("results");
            
            if (resultsNode != null && resultsNode.isArray()) {
                for (JsonNode result : resultsNode) {
                    addArtworkToResults(result, entity, results, seenUrls);
                }
            }
        } catch (Exception e) {
            System.err.println("Error searching " + entity + ": " + e.getMessage());
        }
    }

    private void addArtworkToResults(JsonNode result, String entity,
                                      List<Map<String, String>> results, Set<String> seenUrls) {
        JsonNode artworkNode = result.get("artworkUrl100");
        if (artworkNode != null && !artworkNode.isNull()) {
            String smallUrl = artworkNode.asText();
            
            // Generate URLs for different sizes
            String thumbUrl = smallUrl.replace("100x100bb", "250x250bb");
            String fullUrl = smallUrl.replace("100x100bb", "3000x3000bb");  // Max quality available from iTunes
            
            // Use the full URL as the unique key (to avoid duplicates)
            if (!seenUrls.contains(fullUrl)) {
                seenUrls.add(fullUrl);
                
                Map<String, String> artwork = new HashMap<>();
                artwork.put("thumbnailUrl", thumbUrl);
                artwork.put("fullUrl", fullUrl);
                artwork.put("type", entity);
                
                // Add metadata for display
                String artistName = getNodeText(result, "artistName");
                String collectionName = getNodeText(result, "collectionName");
                String trackName = getNodeText(result, "trackName");
                
                artwork.put("artistName", artistName);
                if (entity.equals("album")) {
                    artwork.put("title", collectionName);
                } else {
                    artwork.put("title", trackName);
                    artwork.put("albumName", collectionName);
                }
                
                // Add release date (format: YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD)
                String releaseDate = getNodeText(result, "releaseDate");
                if (releaseDate != null && !releaseDate.isEmpty()) {
                    // Extract just the date part (YYYY-MM-DD)
                    if (releaseDate.length() >= 10) {
                        artwork.put("releaseDate", releaseDate.substring(0, 10));
                    } else {
                        artwork.put("releaseDate", releaseDate);
                    }
                }
                
                // Add track duration in milliseconds (for songs)
                JsonNode trackTimeNode = result.get("trackTimeMillis");
                if (trackTimeNode != null && !trackTimeNode.isNull()) {
                    long millis = trackTimeNode.asLong();
                    artwork.put("trackTimeMillis", String.valueOf(millis));
                    // Also add formatted duration (mm:ss)
                    long totalSeconds = millis / 1000;
                    long minutes = totalSeconds / 60;
                    long seconds = totalSeconds % 60;
                    artwork.put("lengthFormatted", String.format("%d:%02d", minutes, seconds));
                    artwork.put("lengthSeconds", String.valueOf(totalSeconds));
                }
                
                results.add(artwork);
            }
        }
    }

    private String getNodeText(JsonNode node, String field) {
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
            throw new RuntimeException("Rate limited by Apple Music API");
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
     * Save selected images from Apple Music to album or song.
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
                            // First image becomes default
                            albumService.updateAlbumImage(id, imageBytes);
                            hasExisting = true; // Next ones will be secondary
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
                            // First image becomes default
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
