package library.controller;

import library.dto.SaveImagesRequest;
import library.service.ExternalImageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

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
    private ExternalImageService externalImageService;

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
            String smallUrl = artworkNode.asString();
            
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
        return fieldNode != null && !fieldNode.isNull() ? fieldNode.asString() : "";
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
     * Save selected images from Apple Music to album or song.
     */
    @PostMapping("/save-images")
    public Map<String, Object> saveImages(@RequestBody SaveImagesRequest request) {
        return externalImageService.saveImages(request);
    }
}
