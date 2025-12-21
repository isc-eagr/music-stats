package library.controller;

import library.service.ScrobbleService;
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
@RequestMapping("/scrobbles")
public class ScrobbleController {
    
    private final ScrobbleService scrobbleService;
    private final ArtistService artistService;
    private final AlbumService albumService;
    private final SongService songService;
    
    public ScrobbleController(ScrobbleService scrobbleService,
                              ArtistService artistService, AlbumService albumService, SongService songService) {
        this.scrobbleService = scrobbleService;
        this.artistService = artistService;
        this.albumService = albumService;
        this.songService = songService;
    }
    
    // File upload UI
    @GetMapping("/upload")
    public String showUploadForm(Model model) {
        model.addAttribute("accounts", List.of("vatito", "robertlover"));
        model.addAttribute("maxLastfmIds", scrobbleService.getMaxLastfmIdByAccount());
        return "insertScrobblesForm";
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
            ScrobbleService.ImportResult result = scrobbleService.importScrobblesWithUnmatched(file, account, batchSize, dryRun);
            model.addAttribute("stats", result.stats);
            model.addAttribute("unmatchedList", result.unmatchedGrouped);
            model.addAttribute("dryRun", dryRun);
            model.addAttribute("account", account);
            model.addAttribute("batchSize", batchSize);
            return "insertScrobblesResult";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            return "insertScrobblesResult";
        }
    }

    /**
     * Page that shows unmatched scrobbles grouped by artist/album/song with counts.
     * Supports optional account filter via ?account=<accountName>
     */
    @GetMapping("/unmatched")
    public String showUnmatchedScrobbles(@RequestParam(value = "account", required = false) String account,
                                         Model model) {
        // Fetch list of distinct accounts for the UI
        java.util.List<String> accounts = scrobbleService.getDistinctAccounts();

        // Interpret empty string as "no filter" (show all)
        String serviceAccount;
        if (account != null && account.equals("")) {
            serviceAccount = null; // no filter
        } else {
            serviceAccount = account; // can be null, '__BLANK__', or actual account name
        }

        // Fetch unmatched rows filtered by account (if provided)
        java.util.List<java.util.Map<String, Object>> rows = scrobbleService.getUnmatchedScrobbles(serviceAccount);
        
        // Calculate total unmatched scrobbles count
        long totalUnmatchedCount = rows.stream()
            .mapToLong(row -> row.get("cnt") != null ? ((Number) row.get("cnt")).longValue() : 0)
            .sum();

        model.addAttribute("unmatched", rows);
        model.addAttribute("unmatchedGroupCount", rows.size());
        model.addAttribute("totalUnmatchedCount", totalUnmatchedCount);
        model.addAttribute("accounts", accounts);
        model.addAttribute("selectedAccount", account == null ? "" : account);
        return "unmatchedScrobbles";
    }
    
    /**
     * API endpoint to delete unmatched scrobbles by their identifying fields.
     * 
     * DELETE /scrobbles/api/unmatched
     * Body: { "account": "...", "artist": "...", "album": "...", "song": "..." }
     */
    @DeleteMapping("/api/unmatched")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> deleteUnmatchedScrobbles(@RequestBody Map<String, Object> request) {
        try {
            String account = (String) request.get("account");
            String artist = (String) request.get("artist");
            String album = (String) request.get("album");
            String song = (String) request.get("song");
            
            int deletedCount = scrobbleService.deleteUnmatchedScrobbles(account, artist, album, song);
            
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
     * POST /scrobbles/api/unmatched/delete-selected
     * Body: { items: [ { account, artist, album, song }, ... ] }
     */
    @PostMapping("/api/unmatched/delete-selected")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> deleteSelectedUnmatched(@RequestBody Map<String, Object> request) {
        try {
            Object rawItems = request.get("items");
            int totalDeleted = 0;
            if (rawItems instanceof java.util.List) {
                java.util.List items = (java.util.List) rawItems;
                for (Object o : items) {
                    if (!(o instanceof java.util.Map)) continue;
                    java.util.Map m = (java.util.Map) o;
                    String account = m.get("account") != null ? m.get("account").toString() : null;
                    String artist = m.get("artist") != null ? m.get("artist").toString() : null;
                    String album = m.get("album") != null ? m.get("album").toString() : null;
                    String song = m.get("song") != null ? m.get("song").toString() : null;
                    totalDeleted += scrobbleService.deleteUnmatchedScrobbles(account, artist, album, song);
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
     * API endpoint to assign unmatched scrobbles to an existing song.
     * Matches scrobbles by account, artist, album, and song name.
     * 
     * POST /scrobbles/api/assign
     * Body: { "account": "...", "artist": "...", "album": "...", "song": "...", "songId": 123 }
     */
    @PostMapping("/api/assign")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> assignScrobblesToSong(@RequestBody Map<String, Object> request) {
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
            
            int updatedCount = scrobbleService.assignScrobblesToSong(account, artist, album, song, songId);
            
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
     * API endpoint to create artist/album/song and assign scrobbles.
     * Creates entities as needed and updates scrobbles with canonical names.
     * 
     * POST /scrobbles/api/create-and-assign
     * Body: { 
     *   scrobbleArtist, scrobbleAlbum, scrobbleSong, scrobbleAccount,
     *   artistId (or null), newArtistName, newArtistGenderId,
     *   albumId (or null), newAlbumName,
     *   songName
     * }
     */
    @PostMapping("/api/create-and-assign")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> createAndAssign(@RequestBody Map<String, Object> request) {
        try {
            String scrobbleArtist = (String) request.get("scrobbleArtist");
            String scrobbleAlbum = (String) request.get("scrobbleAlbum");
            String scrobbleSong = (String) request.get("scrobbleSong");
            String scrobbleAccount = (String) request.get("scrobbleAccount");
            
            Integer artistId = request.get("artistId") != null ? ((Number) request.get("artistId")).intValue() : null;
            String newArtistName = (String) request.get("newArtistName");
            Integer newArtistGenderId = request.get("newArtistGenderId") != null && !request.get("newArtistGenderId").toString().isEmpty() 
                ? Integer.parseInt(request.get("newArtistGenderId").toString()) : null;
            
            Integer albumId = request.get("albumId") != null ? ((Number) request.get("albumId")).intValue() : null;
            String newAlbumName = (String) request.get("newAlbumName");
            
            String songName = (String) request.get("songName");
            
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
                // Create new artist
                Map<String, Object> artistData = new HashMap<>();
                artistData.put("name", newArtistName);
                if (newArtistGenderId != null) {
                    artistData.put("genderId", newArtistGenderId);
                }
                artistId = artistService.createArtist(artistData);
                canonicalArtist = newArtistName;
            } else {
                return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Artist is required"));
            }
            
            // 2. Get or create album (optional)
            String canonicalAlbum = null;
            if (albumId != null) {
                // Use existing album
                var album = albumService.findById(albumId);
                if (album != null) {
                    canonicalAlbum = album.getName();
                }
            } else if (newAlbumName != null && !newAlbumName.isBlank()) {
                // Create new album
                Map<String, Object> albumData = new HashMap<>();
                albumData.put("name", newAlbumName);
                albumData.put("artistId", artistId);
                albumId = albumService.createAlbum(albumData);
                canonicalAlbum = newAlbumName;
            }
            // If albumId is still null, song will be a single (no album)
            
            // 3. Create song
            Map<String, Object> songData = new HashMap<>();
            songData.put("name", songName);
            songData.put("artistId", artistId);
            if (albumId != null) {
                songData.put("albumId", albumId);
            }
            Integer songId = songService.createSong(songData);
            
            // 4. Assign scrobbles with canonical names
            int updatedCount = scrobbleService.assignScrobblesToSongWithCanonicalNames(
                scrobbleAccount, scrobbleArtist, scrobbleAlbum, scrobbleSong,
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
}