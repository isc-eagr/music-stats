package library.controller;

import library.service.ArtistService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Controller for MusicBrainz API integration.
 * Provides endpoints to search MusicBrainz for artist metadata (birth/death dates, country).
 * 
 * MusicBrainz API:
 * - No authentication required (free and open!)
 * - Rate limit: 1 request per second (be respectful)
 * - Search endpoint: https://musicbrainz.org/ws/2/artist?query=...
 * - Lookup endpoint: https://musicbrainz.org/ws/2/artist/{mbid}?inc=...
 */
@RestController
@RequestMapping("/api/musicbrainz")
public class MusicBrainzController {

    @Autowired
    private ArtistService artistService;

    private static final String MUSICBRAINZ_API = "https://musicbrainz.org/ws/2";
    private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
    private static final long RATE_LIMIT_MS = 1100; // 1.1 seconds between requests
    
    private static long lastRequestTime = 0;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Check if MusicBrainz is available (always configured since it's free).
     */
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("configured", true);
        status.put("authenticated", true);
        return status;
    }

    /**
     * Search MusicBrainz for artists by name.
     * Returns multiple matches with birth/death dates and country.
     * 
     * @param term Artist name to search
     * @return List of artist matches with metadata
     */
    @GetMapping("/search-artist")
    public List<Map<String, Object>> searchArtist(@RequestParam String term) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        try {
            respectRateLimit();
            
            String encodedTerm = URLEncoder.encode(term, StandardCharsets.UTF_8);
            String url = MUSICBRAINZ_API + "/artist?query=artist:" + encodedTerm + "&fmt=json&limit=20";
            
            String response = makeHttpRequest(url);
            JsonNode root = objectMapper.readTree(response);
            JsonNode artists = root.get("artists");
            
            if (artists != null && artists.isArray()) {
                for (JsonNode artist : artists) {
                    Map<String, Object> result = new HashMap<>();
                    
                    // MBID (MusicBrainz ID)
                    String mbid = getJsonText(artist, "id");
                    result.put("mbid", mbid);
                    
                    // Name
                    result.put("name", getJsonText(artist, "name"));
                    
                    // Sort name (sometimes different from display name)
                    result.put("sortName", getJsonText(artist, "sort-name"));
                    
                    // Disambiguation (helps distinguish artists with same name)
                    result.put("disambiguation", getJsonText(artist, "disambiguation"));
                    
                    // Type (Person, Group, Orchestra, Choir, Character, Other)
                    result.put("type", getJsonText(artist, "type"));
                    
                    // Score (relevance)
                    JsonNode scoreNode = artist.get("score");
                    result.put("score", scoreNode != null ? scoreNode.asInt() : 0);
                    
                    // Life span (birth/death dates)
                    JsonNode lifeSpan = artist.get("life-span");
                    if (lifeSpan != null) {
                        String beginDate = getJsonText(lifeSpan, "begin");
                        String endDate = getJsonText(lifeSpan, "end");
                        boolean ended = lifeSpan.has("ended") && lifeSpan.get("ended").asBoolean();
                        
                        result.put("birthDate", beginDate);
                        result.put("deathDate", endDate);
                        result.put("isDeceased", ended);
                    } else {
                        result.put("birthDate", null);
                        result.put("deathDate", null);
                        result.put("isDeceased", false);
                    }
                    
                    // Country/Area
                    JsonNode area = artist.get("area");
                    if (area != null) {
                        result.put("country", getJsonText(area, "name"));
                    } else {
                        result.put("country", null);
                    }
                    
                    // Begin area (birthplace - different from country)
                    JsonNode beginArea = artist.get("begin-area");
                    if (beginArea != null) {
                        result.put("birthPlace", getJsonText(beginArea, "name"));
                    } else {
                        result.put("birthPlace", null);
                    }
                    
                    // Tags (genres) - if present
                    JsonNode tags = artist.get("tags");
                    if (tags != null && tags.isArray() && tags.size() > 0) {
                        List<String> tagList = new ArrayList<>();
                        for (JsonNode tag : tags) {
                            String tagName = getJsonText(tag, "name");
                            if (tagName != null) {
                                tagList.add(tagName);
                            }
                        }
                        result.put("tags", tagList);
                    } else {
                        result.put("tags", new ArrayList<>());
                    }
                    
                    results.add(result);
                }
            }
            
        } catch (Exception e) {
            System.err.println("MusicBrainz search error: " + e.getMessage());
            e.printStackTrace();
        }
        
        return results;
    }

    /**
     * Get detailed artist info by MusicBrainz ID.
     * Use this after user selects an artist from search results to get fuller info.
     * 
     * @param mbid MusicBrainz artist ID
     * @return Detailed artist metadata
     */
    @GetMapping("/artist/{mbid}")
    public Map<String, Object> getArtist(@PathVariable String mbid) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            respectRateLimit();
            
            String url = MUSICBRAINZ_API + "/artist/" + mbid + "?inc=tags+area-rels&fmt=json";
            
            String response = makeHttpRequest(url);
            JsonNode artist = objectMapper.readTree(response);
            
            result.put("mbid", mbid);
            result.put("name", getJsonText(artist, "name"));
            result.put("sortName", getJsonText(artist, "sort-name"));
            result.put("disambiguation", getJsonText(artist, "disambiguation"));
            result.put("type", getJsonText(artist, "type"));
            
            // Life span
            JsonNode lifeSpan = artist.get("life-span");
            if (lifeSpan != null) {
                result.put("birthDate", getJsonText(lifeSpan, "begin"));
                result.put("deathDate", getJsonText(lifeSpan, "end"));
                result.put("isDeceased", lifeSpan.has("ended") && lifeSpan.get("ended").asBoolean());
            }
            
            // Country/Area
            JsonNode area = artist.get("area");
            if (area != null) {
                result.put("country", getJsonText(area, "name"));
            }
            
            // Begin area (birthplace)
            JsonNode beginArea = artist.get("begin-area");
            if (beginArea != null) {
                result.put("birthPlace", getJsonText(beginArea, "name"));
            }
            
            // Tags (genres)
            JsonNode tags = artist.get("tags");
            if (tags != null && tags.isArray()) {
                List<Map<String, Object>> tagList = new ArrayList<>();
                for (JsonNode tag : tags) {
                    Map<String, Object> tagInfo = new HashMap<>();
                    tagInfo.put("name", getJsonText(tag, "name"));
                    tagInfo.put("count", tag.has("count") ? tag.get("count").asInt() : 0);
                    tagList.add(tagInfo);
                }
                // Sort by count descending
                tagList.sort((a, b) -> (Integer) b.get("count") - (Integer) a.get("count"));
                result.put("tags", tagList);
            }
            
        } catch (Exception e) {
            System.err.println("MusicBrainz lookup error: " + e.getMessage());
            result.put("error", e.getMessage());
        }
        
        return result;
    }

    /**
     * Apply MusicBrainz metadata to an artist.
     * Saves birth date, death date, and country to the artist.
     */
    @PostMapping("/apply/{artistId}")
    public Map<String, Object> applyMetadata(
            @PathVariable Long artistId,
            @RequestBody Map<String, String> metadata) {
        
        Map<String, Object> result = new HashMap<>();
        
        try {
            String birthDateStr = metadata.get("birthDate");
            String deathDateStr = metadata.get("deathDate");
            String country = metadata.get("country");
            Boolean isBand = null;
            
            // Determine isBand from type if provided
            String type = metadata.get("type");
            if (type != null) {
                isBand = type.equalsIgnoreCase("Group") || 
                         type.equalsIgnoreCase("Orchestra") || 
                         type.equalsIgnoreCase("Choir");
            }
            
            // Parse dates (MusicBrainz uses YYYY-MM-DD or YYYY-MM or just YYYY)
            LocalDate birthDate = parseFlexibleDate(birthDateStr);
            LocalDate deathDate = parseFlexibleDate(deathDateStr);
            
            // Update the artist using the service
            artistService.updateArtistMetadata(artistId, birthDate, deathDate, country, isBand);
            
            result.put("success", true);
            result.put("message", "Metadata applied successfully");
            result.put("birthDate", birthDate != null ? birthDate.toString() : null);
            result.put("deathDate", deathDate != null ? deathDate.toString() : null);
            result.put("country", country);
            result.put("isBand", isBand);
            
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
            System.err.println("Error applying MusicBrainz metadata: " + e.getMessage());
        }
        
        return result;
    }

    /**
     * Parse flexible date formats from MusicBrainz (YYYY, YYYY-MM, YYYY-MM-DD)
     */
    private LocalDate parseFlexibleDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        
        dateStr = dateStr.trim();
        
        try {
            // Full date: YYYY-MM-DD
            if (dateStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            }
            // Year and month: YYYY-MM -> assume first day of month
            if (dateStr.matches("\\d{4}-\\d{2}")) {
                return LocalDate.parse(dateStr + "-01", DateTimeFormatter.ISO_LOCAL_DATE);
            }
            // Year only: YYYY -> assume January 1st
            if (dateStr.matches("\\d{4}")) {
                return LocalDate.parse(dateStr + "-01-01", DateTimeFormatter.ISO_LOCAL_DATE);
            }
        } catch (Exception e) {
            System.err.println("Failed to parse date: " + dateStr + " - " + e.getMessage());
        }
        
        return null;
    }

    /**
     * Search MusicBrainz for recordings (songs) and releases (albums).
     * Returns results with cover art URLs from Cover Art Archive.
     * Format matches other services (Apple Music, Spotify, Deezer) for compatibility.
     * 
     * @param term Search term
     * @param entity "song" or "album"
     * @return List of results with artwork URLs and metadata
     */
    @GetMapping("/search")
    public List<Map<String, String>> searchImages(
            @RequestParam String term,
            @RequestParam(defaultValue = "") String entity,
            @RequestParam(defaultValue = "25") int limit) {
        
        List<Map<String, String>> results = new ArrayList<>();
        
        try {
            respectRateLimit();
            
            String encodedTerm = URLEncoder.encode(term, StandardCharsets.UTF_8);
            
            if (entity.equalsIgnoreCase("song")) {
                // Search for recordings
                String url = MUSICBRAINZ_API + "/recording?query=" + encodedTerm + "&fmt=json&limit=" + limit;
                String response = makeHttpRequest(url);
                JsonNode root = objectMapper.readTree(response);
                JsonNode recordings = root.get("recordings");
                
                if (recordings != null && recordings.isArray()) {
                    Set<String> seenReleases = new HashSet<>();
                    
                    for (JsonNode recording : recordings) {
                        String title = getJsonText(recording, "title");
                        
                        // Get artist
                        String artistName = "";
                        JsonNode artistCredit = recording.get("artist-credit");
                        if (artistCredit != null && artistCredit.isArray() && artistCredit.size() > 0) {
                            JsonNode firstArtist = artistCredit.get(0).get("artist");
                            if (firstArtist != null) {
                                artistName = getJsonText(firstArtist, "name");
                            }
                        }
                        
                        // Get releases (albums) for this recording
                        JsonNode releases = recording.get("releases");
                        if (releases != null && releases.isArray()) {
                            for (JsonNode release : releases) {
                                String releaseId = getJsonText(release, "id");
                                if (releaseId == null || seenReleases.contains(releaseId)) continue;
                                seenReleases.add(releaseId);
                                
                                String albumName = getJsonText(release, "title");
                                String releaseDate = getJsonText(release, "date");
                                
                                // Get length in seconds
                                JsonNode lengthNode = recording.get("length");
                                int lengthSeconds = 0;
                                String lengthFormatted = "";
                                if (lengthNode != null && !lengthNode.isNull()) {
                                    lengthSeconds = lengthNode.asInt() / 1000; // Convert ms to seconds
                                    int mins = lengthSeconds / 60;
                                    int secs = lengthSeconds % 60;
                                    lengthFormatted = mins + ":" + String.format("%02d", secs);
                                }
                                
                                // Build cover art URL from Cover Art Archive
                                String coverUrl = "https://coverartarchive.org/release/" + releaseId + "/front";  // Original full resolution
                                String thumbnailUrl = "https://coverartarchive.org/release/" + releaseId + "/front-250";
                                
                                Map<String, String> result = new HashMap<>();
                                result.put("title", title);
                                result.put("artistName", artistName);
                                result.put("albumName", albumName);
                                result.put("releaseDate", releaseDate != null ? releaseDate : "");
                                result.put("lengthSeconds", String.valueOf(lengthSeconds));
                                result.put("lengthFormatted", lengthFormatted);
                                result.put("thumbnailUrl", thumbnailUrl);
                                result.put("fullUrl", coverUrl);
                                result.put("type", "Song");
                                result.put("service", "musicbrainz");
                                
                                results.add(result);
                                
                                if (results.size() >= 50) break;
                            }
                        }
                        if (results.size() >= 50) break;
                    }
                }
            } else {
                // Search for releases (albums) - include release-groups for edition browsing
                String url = MUSICBRAINZ_API + "/release?query=" + encodedTerm + "&inc=release-groups&fmt=json&limit=" + limit;
                String response = makeHttpRequest(url);
                JsonNode root = objectMapper.readTree(response);
                JsonNode releases = root.get("releases");
                
                if (releases != null && releases.isArray()) {
                    for (JsonNode release : releases) {
                        String releaseId = getJsonText(release, "id");
                        String albumName = getJsonText(release, "title");
                        String releaseDate = getJsonText(release, "date");
                        
                        // Get release-group ID for browsing all editions
                        String releaseGroupId = null;
                        JsonNode releaseGroupNode = release.get("release-group");
                        if (releaseGroupNode != null) {
                            releaseGroupId = getJsonText(releaseGroupNode, "id");
                        }
                        
                        // Get artist
                        String artistName = "";
                        JsonNode artistCredit = release.get("artist-credit");
                        if (artistCredit != null && artistCredit.isArray() && artistCredit.size() > 0) {
                            JsonNode firstArtist = artistCredit.get(0).get("artist");
                            if (firstArtist != null) {
                                artistName = getJsonText(firstArtist, "name");
                            }
                        }
                        
                        // Build cover art URL from Cover Art Archive
                        String coverUrl = "https://coverartarchive.org/release/" + releaseId + "/front";  // Original full resolution
                        String thumbnailUrl = "https://coverartarchive.org/release/" + releaseId + "/front-250";
                        
                        Map<String, String> result = new HashMap<>();
                        result.put("title", albumName);
                        result.put("artistName", artistName);
                        result.put("albumName", albumName);
                        result.put("releaseDate", releaseDate != null ? releaseDate : "");
                        result.put("lengthSeconds", "");
                        result.put("lengthFormatted", "");
                        result.put("thumbnailUrl", thumbnailUrl);
                        result.put("fullUrl", coverUrl);
                        result.put("type", "Album");
                        result.put("service", "musicbrainz");
                        result.put("releaseId", releaseId);
                        result.put("releaseGroupId", releaseGroupId != null ? releaseGroupId : "");
                        
                        results.add(result);
                        
                        if (results.size() >= 50) break;
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("MusicBrainz search error: " + e.getMessage());
            e.printStackTrace();
        }
        
        return results;
    }

    /**
     * Get all releases for a release-group (album).
     * This returns all editions/versions of an album (standard, deluxe, reissue, etc.)
     * Each release has its own cover art from Cover Art Archive.
     */
    @GetMapping("/release-group/{releaseGroupId}/releases")
    public List<Map<String, String>> getReleasesForReleaseGroup(@PathVariable String releaseGroupId) {
        List<Map<String, String>> releases = new ArrayList<>();
        
        try {
            respectRateLimit();
            
            // Browse releases for this release-group
            String url = MUSICBRAINZ_API + "/release?release-group=" + releaseGroupId + "&fmt=json&limit=100";
            String response = makeHttpRequest(url);
            JsonNode root = objectMapper.readTree(response);
            JsonNode releasesNode = root.get("releases");
            
            if (releasesNode != null && releasesNode.isArray()) {
                for (JsonNode release : releasesNode) {
                    String releaseId = getJsonText(release, "id");
                    String title = getJsonText(release, "title");
                    String date = getJsonText(release, "date");
                    String country = getJsonText(release, "country");
                    String status = getJsonText(release, "status"); // Official, Promotional, Bootleg, etc.
                    
                    // Get packaging (CD, Vinyl, Digital, etc.)
                    String packaging = getJsonText(release, "packaging");
                    
                    // Get label info
                    String labelInfo = "";
                    JsonNode labelInfoNode = release.get("label-info");
                    if (labelInfoNode != null && labelInfoNode.isArray() && labelInfoNode.size() > 0) {
                        JsonNode firstLabel = labelInfoNode.get(0);
                        if (firstLabel != null) {
                            JsonNode label = firstLabel.get("label");
                            if (label != null) {
                                labelInfo = getJsonText(label, "name");
                            }
                            String catNo = getJsonText(firstLabel, "catalog-number");
                            if (catNo != null && !catNo.isEmpty()) {
                                labelInfo = labelInfo + (labelInfo.isEmpty() ? "" : " ") + "[" + catNo + "]";
                            }
                        }
                    }
                    
                    // Build description string (e.g., "2001 • US • CD • Official")
                    List<String> descParts = new ArrayList<>();
                    if (date != null && !date.isEmpty()) {
                        descParts.add(date.length() >= 4 ? date.substring(0, 4) : date);
                    }
                    if (country != null && !country.isEmpty()) descParts.add(country);
                    if (packaging != null && !packaging.isEmpty()) descParts.add(packaging);
                    if (status != null && !status.isEmpty() && !"Official".equals(status)) descParts.add(status);
                    String description = String.join(" • ", descParts);
                    
                    // Build cover art URLs
                    String coverUrl = "https://coverartarchive.org/release/" + releaseId + "/front";  // Original full resolution
                    String thumbnailUrl = "https://coverartarchive.org/release/" + releaseId + "/front-250";
                    
                    Map<String, String> result = new HashMap<>();
                    result.put("releaseId", releaseId);
                    result.put("title", title);
                    result.put("date", date != null ? date : "");
                    result.put("country", country != null ? country : "");
                    result.put("packaging", packaging != null ? packaging : "");
                    result.put("status", status != null ? status : "");
                    result.put("label", labelInfo);
                    result.put("description", description);
                    result.put("thumbnailUrl", thumbnailUrl);
                    result.put("fullUrl", coverUrl);
                    
                    releases.add(result);
                }
            }
            
            // Sort by date (newest first), with null dates at the end
            releases.sort((a, b) -> {
                String dateA = a.get("date");
                String dateB = b.get("date");
                if (dateA.isEmpty() && dateB.isEmpty()) return 0;
                if (dateA.isEmpty()) return 1;
                if (dateB.isEmpty()) return -1;
                return dateB.compareTo(dateA);
            });
            
        } catch (Exception e) {
            System.err.println("MusicBrainz release-group lookup error: " + e.getMessage());
            e.printStackTrace();
        }
        
        return releases;
    }

    /**
     * Respect MusicBrainz rate limit (1 request per second)
     */
    private synchronized void respectRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsed = now - lastRequestTime;
        if (elapsed < RATE_LIMIT_MS) {
            Thread.sleep(RATE_LIMIT_MS - elapsed);
        }
        lastRequestTime = System.currentTimeMillis();
    }

    /**
     * Make HTTP request to MusicBrainz API
     */
    private String makeHttpRequest(String urlString) throws Exception {
        URI uri = new URI(urlString);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setRequestProperty("Accept", "application/json");
        connection.setConnectTimeout(15000);
        connection.setReadTimeout(15000);
        
        int responseCode = connection.getResponseCode();
        
        if (responseCode == 200) {
            try (InputStream is = connection.getInputStream()) {
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        } else {
            throw new RuntimeException("HTTP error: " + responseCode);
        }
    }

    /**
     * Safely get text from a JSON node
     */
    private String getJsonText(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode fieldNode = node.get(field);
        if (fieldNode == null || fieldNode.isNull()) return null;
        return fieldNode.asText();
    }
}
