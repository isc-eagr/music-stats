package library.controller;

import library.service.PlayService;
import library.service.ArtistService;
import library.service.AlbumService;
import library.service.SongService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/plays")
public class PlayController {
    
    private final PlayService playService;
    private final ArtistService artistService;
    private final AlbumService albumService;
    private final SongService songService;
    
    public PlayController(PlayService playService,
                              ArtistService artistService, AlbumService albumService, SongService songService) {
        this.playService = playService;
        this.artistService = artistService;
        this.albumService = albumService;
        this.songService = songService;
    }
    
    // File upload UI
    @GetMapping("/upload")
    public String showUploadForm(Model model) {
        model.addAttribute("accounts", List.of("vatito", "robertlover"));
        model.addAttribute("maxLastfmIds", playService.getMaxLastfmIdByAccount());
        return "insertPlaysForm";
    }

    // Handle the uploaded file and stream-import
    @PostMapping("/upload")
    public String handleUpload(@RequestParam("file") MultipartFile file,
                               @RequestParam(value = "account", required = false) String account,
                               @RequestParam(value = "batchSize", required = false, defaultValue = "2000") int batchSize,
                               @RequestParam(value = "dryRun", required = false, defaultValue = "false") boolean dryRun,
                               Model model) {
        if (account == null || account.isBlank()) account = "uploaded";
        try {
            PlayService.ImportResult result = playService.importPlaysWithUnmatched(file, account, batchSize, dryRun);
            model.addAttribute("stats", result.stats);
            model.addAttribute("unmatchedList", result.unmatchedGrouped);
            model.addAttribute("dryRun", dryRun);
            model.addAttribute("account", account);
            model.addAttribute("batchSize", batchSize);
            return "insertPlaysResult";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            return "insertPlaysResult";
        }
    }

    /**
     * Page that shows unmatched plays grouped by artist/album/song with counts.
     * Supports optional account filter via ?account=<accountName>
     */
    @GetMapping("/unmatched")
    public String showUnmatchedPlays(@RequestParam(value = "account", required = false) String account,
                                         Model model) {
        // Fetch list of distinct accounts for the UI
        java.util.List<String> accounts = playService.getDistinctAccounts();

        // Interpret empty string as "no filter" (show all)
        String serviceAccount;
        if (account != null && account.equals("")) {
            serviceAccount = null; // no filter
        } else {
            serviceAccount = account; // can be null, '__BLANK__', or actual account name
        }

        // Fetch unmatched rows filtered by account (if provided)
        java.util.List<java.util.Map<String, Object>> rows = playService.getUnmatchedPlays(serviceAccount);
        
        // Calculate total unmatched plays count
        long totalUnmatchedCount = rows.stream()
            .mapToLong(row -> row.get("cnt") != null ? ((Number) row.get("cnt")).longValue() : 0)
            .sum();

        model.addAttribute("unmatched", rows);
        model.addAttribute("unmatchedGroupCount", rows.size());
        model.addAttribute("totalUnmatchedCount", totalUnmatchedCount);
        model.addAttribute("accounts", accounts);
        model.addAttribute("selectedAccount", account == null ? "" : account);
        return "unmatchedPlays";
    }
    
    /**
     * API endpoint to delete unmatched plays by their identifying fields.
     * 
     * DELETE /plays/api/unmatched
     * Body: { "account": "...", "artist": "...", "album": "...", "song": "..." }
     */
    @DeleteMapping("/api/unmatched")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> deleteUnmatchedPlays(@RequestBody Map<String, Object> request) {
        try {
            String account = (String) request.get("account");
            String artist = (String) request.get("artist");
            String album = (String) request.get("album");
            String song = (String) request.get("song");
            
            int deletedCount = playService.deleteUnmatchedPlays(account, artist, album, song);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "deletedCount", deletedCount
            ));
            
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }

    /**
     * Batch delete endpoint to remove multiple unmatched groups at once.
     * POST /plays/api/unmatched/delete-selected
     * Body: { items: [ { account, artist, album, song }, ... ] }
     */
    @PostMapping("/api/unmatched/delete-selected")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> deleteSelectedUnmatched(@RequestBody Map<String, Object> request) {
        try {
            Object rawItems = request.get("items");
            int totalDeleted = 0;
            if (rawItems instanceof java.util.List) {
                java.util.List<?> items = (java.util.List<?>) rawItems;
                for (Object o : items) {
                    if (!(o instanceof java.util.Map)) continue;
                    java.util.Map<?, ?> m = (java.util.Map<?, ?>) o;
                    String account = m.get("account") != null ? m.get("account").toString() : null;
                    String artist = m.get("artist") != null ? m.get("artist").toString() : null;
                    String album = m.get("album") != null ? m.get("album").toString() : null;
                    String song = m.get("song") != null ? m.get("song").toString() : null;
                    totalDeleted += playService.deleteUnmatchedPlays(account, artist, album, song);
                }
            }

            return ResponseEntity.ok(Map.of(
                "success", true,
                "deletedCount", totalDeleted
            ));

        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }
    
    /**
     * API endpoint to assign unmatched plays to an existing song.
     * Matches plays by account, artist, album, and song name.
     * 
     * POST /plays/api/assign
     * Body: { "account": "...", "artist": "...", "album": "...", "song": "...", "songId": 123 }
     */
    @PostMapping("/api/assign")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> assignPlaysToSong(@RequestBody Map<String, Object> request) {
        try {
            String account = (String) request.get("account");
            String artist = (String) request.get("artist");
            String album = (String) request.get("album");
            String song = (String) request.get("song");
            Integer songId = request.get("songId") != null ? ((Number) request.get("songId")).intValue() : null;
            
            if (songId == null) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "songId is required"
                ));
            }
            
            int updatedCount = playService.assignPlaysToSong(account, artist, album, song, songId);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "updatedCount", updatedCount
            ));
            
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }
    
    /**
     * API endpoint to create artist/album/song and assign plays.
     * Creates entities as needed and updates plays with canonical names.
     * 
     * POST /plays/api/create-and-assign
     * Body: { 
     *   playArtist, playAlbum, playSong, playAccount,
     *   artistId (or null), newArtistName, newArtistGenderId, newArtistLanguageId, 
     *   newArtistGenreId, newArtistSubgenreId, newArtistEthnicityId, newArtistCountry,
     *   albumId (or null), newAlbumName, newAlbumReleaseDate,
     *   songName, songReleaseDate, songLengthSeconds
     * }
     */
    @PostMapping("/api/create-and-assign")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> createAndAssign(@RequestBody Map<String, Object> request) {
        try {
            String playArtist = (String) request.get("playArtist");
            String playAlbum = (String) request.get("playAlbum");
            String playSong = (String) request.get("playSong");
            String playAccount = (String) request.get("playAccount");
            
            Integer artistId = request.get("artistId") != null ? ((Number) request.get("artistId")).intValue() : null;
            String newArtistName = (String) request.get("newArtistName");
            Integer newArtistGenderId = request.get("newArtistGenderId") != null && !request.get("newArtistGenderId").toString().isEmpty() 
                ? Integer.parseInt(request.get("newArtistGenderId").toString()) : null;
            Integer newArtistLanguageId = request.get("newArtistLanguageId") != null && !request.get("newArtistLanguageId").toString().isEmpty() 
                ? Integer.parseInt(request.get("newArtistLanguageId").toString()) : null;
            Integer newArtistGenreId = request.get("newArtistGenreId") != null && !request.get("newArtistGenreId").toString().isEmpty() 
                ? Integer.parseInt(request.get("newArtistGenreId").toString()) : null;
            Integer newArtistSubgenreId = request.get("newArtistSubgenreId") != null && !request.get("newArtistSubgenreId").toString().isEmpty() 
                ? Integer.parseInt(request.get("newArtistSubgenreId").toString()) : null;
            Integer newArtistEthnicityId = request.get("newArtistEthnicityId") != null && !request.get("newArtistEthnicityId").toString().isEmpty() 
                ? Integer.parseInt(request.get("newArtistEthnicityId").toString()) : null;
            String newArtistCountry = (String) request.get("newArtistCountry");
            String newArtistBirthDate = (String) request.get("newArtistBirthDate");
            String newArtistDeathDate = (String) request.get("newArtistDeathDate");
            Boolean newArtistIsBand = request.get("newArtistIsBand") != null ? (Boolean) request.get("newArtistIsBand") : null;
            
            Integer albumId = request.get("albumId") != null ? ((Number) request.get("albumId")).intValue() : null;
            String newAlbumName = (String) request.get("newAlbumName");
            String newAlbumReleaseDate = (String) request.get("newAlbumReleaseDate");
            String newAlbumImageUrl = (String) request.get("newAlbumImageUrl");
            
            String songName = (String) request.get("songName");
            String songReleaseDate = (String) request.get("songReleaseDate");
            Integer songLengthSeconds = request.get("songLengthSeconds") != null 
                ? ((Number) request.get("songLengthSeconds")).intValue() : null;
            String songImageUrl = (String) request.get("songImageUrl");
            
            if (songName == null || songName.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Song name is required"));
            }
            
            // 1. Get or create artist
            String canonicalArtist;
            if (artistId != null) {
                // Use existing artist
                var artist = artistService.findById(artistId);
                if (artist == null) {
                    return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Artist not found"));
                }
                canonicalArtist = artist.getName();
            } else if (newArtistName != null && !newArtistName.isBlank()) {
                // Create new artist with all fields
                Map<String, Object> artistData = new HashMap<>();
                artistData.put("name", newArtistName);
                if (newArtistGenderId != null) {
                    artistData.put("genderId", newArtistGenderId);
                }
                if (newArtistLanguageId != null) {
                    artistData.put("languageId", newArtistLanguageId);
                }
                if (newArtistGenreId != null) {
                    artistData.put("genreId", newArtistGenreId);
                }
                if (newArtistSubgenreId != null) {
                    artistData.put("subgenreId", newArtistSubgenreId);
                }
                if (newArtistEthnicityId != null) {
                    artistData.put("ethnicityId", newArtistEthnicityId);
                }
                if (newArtistCountry != null && !newArtistCountry.isBlank()) {
                    artistData.put("country", newArtistCountry);
                }
                if (newArtistBirthDate != null && !newArtistBirthDate.isBlank()) {
                    artistData.put("birthDate", newArtistBirthDate);
                }
                if (newArtistDeathDate != null && !newArtistDeathDate.isBlank()) {
                    artistData.put("deathDate", newArtistDeathDate);
                }
                if (newArtistIsBand != null) {
                    artistData.put("isBand", newArtistIsBand);
                }
                artistId = artistService.createArtist(artistData);
                canonicalArtist = newArtistName;
            } else {
                return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Artist is required"));
            }
            
            // 2. Get or create album (optional)
            String canonicalAlbum = null;
            boolean isNewAlbum = false;
            if (albumId != null) {
                // Use existing album
                var album = albumService.findById(albumId);
                if (album != null) {
                    canonicalAlbum = album.getName();
                }
            } else if (newAlbumName != null && !newAlbumName.isBlank()) {
                // Create new album with release date
                Map<String, Object> albumData = new HashMap<>();
                albumData.put("name", newAlbumName);
                albumData.put("artistId", artistId);
                if (newAlbumReleaseDate != null && !newAlbumReleaseDate.isBlank()) {
                    albumData.put("releaseDate", newAlbumReleaseDate);
                }
                albumId = albumService.createAlbum(albumData);
                canonicalAlbum = newAlbumName;
                isNewAlbum = true;
            }
            // If albumId is still null, song will be a single (no album)
            
            // 2b. Download and save album image if provided
            if (isNewAlbum && newAlbumImageUrl != null && !newAlbumImageUrl.isBlank()) {
                try {
                    byte[] imageBytes = downloadImage(newAlbumImageUrl);
                    if (imageBytes != null && imageBytes.length > 0) {
                        albumService.updateAlbumImage(albumId, imageBytes);
                    }
                } catch (Exception e) {
                    // Log but don't fail the whole operation
                    System.err.println("Failed to download album image: " + e.getMessage());
                }
            }
            
            // 3. Create song with release date and length
            Map<String, Object> songData = new HashMap<>();
            songData.put("name", songName);
            songData.put("artistId", artistId);
            if (albumId != null) {
                songData.put("albumId", albumId);
            }
            if (songReleaseDate != null && !songReleaseDate.isBlank()) {
                songData.put("releaseDate", songReleaseDate);
            }
            if (songLengthSeconds != null) {
                songData.put("lengthSeconds", songLengthSeconds);
            }
            Integer songId = songService.createSong(songData);
            
            // 3b. Download and save song image if provided
            if (songImageUrl != null && !songImageUrl.isBlank()) {
                try {
                    byte[] imageBytes = downloadImage(songImageUrl);
                    if (imageBytes != null && imageBytes.length > 0) {
                        songService.updateSongImage(songId, imageBytes);
                    }
                } catch (Exception e) {
                    // Log but don't fail the whole operation
                    System.err.println("Failed to download song image: " + e.getMessage());
                }
            }
            
            // 4. Assign plays with canonical names
            int updatedCount = playService.assignPlaysToSongWithCanonicalNames(
                playAccount, playArtist, playAlbum, playSong,
                songId, canonicalArtist, canonicalAlbum, songName
            );
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "songId", songId,
                "artistId", artistId,
                "albumId", albumId != null ? albumId : 0,
                "updatedCount", updatedCount
            ));
            
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }
    
    /**
     * API endpoint to fetch recent plays from Last.fm API.
     * Fetches plays newer than the max lastfm_id for the given account.
     * 
     * POST /plays/api/fetch-lastfm
     * Body: { "account": "vatito", "apiKey": "..." }
     */
    @PostMapping("/api/fetch-lastfm")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> fetchFromLastfm(@RequestBody Map<String, Object> request) {
        try {
            String account = (String) request.get("account");
            String apiKey = (String) request.get("apiKey");
            
            if (account == null || account.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "Account is required"
                ));
            }
            
            if (apiKey == null || apiKey.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "API key is required"
                ));
            }
            
            PlayService.ImportResult result = playService.fetchAndImportPlaysFromLastfm(account, apiKey);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("stats", result.stats);
            response.put("unmatchedList", result.unmatchedGrouped);
            
            // Include validation result if available
            if (result.validation != null) {
                Map<String, Object> validation = new HashMap<>();
                validation.put("lastfmPlaycount", result.validation.lastfmPlaycount);
                validation.put("localPlayCount", result.validation.localPlayCount);
                validation.put("matches", result.validation.matches);
                validation.put("difference", result.validation.difference);
                response.put("validation", validation);
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }
    
    /**
     * API endpoint to delete plays from the last N days.
     * 
     * POST /plays/api/delete-recent
     * Body: { "days": 10 }
     */
    @PostMapping("/api/delete-recent")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> deleteRecentPlays(@RequestBody Map<String, Object> request) {
        try {
            Integer days = request.get("days") != null ? ((Number) request.get("days")).intValue() : null;
            
            if (days == null || days < 1) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "Valid number of days is required"
                ));
            }
            
            int deletedCount = playService.deleteRecentPlays(days);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "deletedCount", deletedCount,
                "days", days
            ));
            
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }
    
    /**
     * API endpoint to delete all plays for a specific song.
     * This is a dangerous operation that removes all listening history for the song.
     * 
     * DELETE /plays/api/song/{songId}
     */
    @DeleteMapping("/api/song/{songId}")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> deletePlaysForSong(@PathVariable Long songId) {
        try {
            if (songId == null || songId < 1) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "Valid song ID is required"
                ));
            }
            
            int deletedCount = playService.deletePlaysForSong(songId);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "deletedCount", deletedCount,
                "songId", songId
            ));
            
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }
    
    /**
     * Download image from URL and return bytes.
     */
    private byte[] downloadImage(String imageUrl) throws Exception {
        java.net.URI uri = java.net.URI.create(imageUrl);
        java.net.HttpURLConnection connection = (java.net.HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", "MusicStatsApp/1.0");
        connection.setConnectTimeout(15000);
        connection.setReadTimeout(15000);

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("HTTP error downloading image: " + responseCode);
        }

        try (java.io.InputStream is = connection.getInputStream()) {
            return is.readAllBytes();
        }
    }
}
