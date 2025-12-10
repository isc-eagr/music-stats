package library.controller;

import library.dto.SongCardDTO;
import library.service.SongService;
import library.service.PlaylistService;
import library.service.iTunesLibraryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Controller
@RequestMapping("/playlists")
public class PlaylistController {

    @Autowired
    private SongService songService;
    
    @Autowired
    private PlaylistService playlistService;

    @Autowired
    private iTunesLibraryService iTunesLibraryService;

    /**
     * Validate songs against iTunes library (AJAX endpoint)
     * Returns a list of songs that do NOT exist in the iTunes library
     */
    @PostMapping("/validate-itunes")
    @ResponseBody
    public Map<String, Object> validateAgainstiTunes(@RequestBody List<Map<String, Object>> songs) {
        Map<String, Object> result = new HashMap<>();
        
        if (!iTunesLibraryService.libraryExists()) {
            result.put("error", "iTunes library not found");
            result.put("libraryPath", iTunesLibraryService.getDefaultLibraryPath());
            return result;
        }

        Map<String, iTunesLibraryService.iTunesTrack> library = iTunesLibraryService.loadLibrary();
        List<Map<String, Object>> matched = new ArrayList<>();
        List<Map<String, Object>> unmatched = new ArrayList<>();

        for (Map<String, Object> song : songs) {
            String name = (String) song.get("name");
            String artist = (String) song.get("artist");
            String album = (String) song.get("album");
            Integer id = song.get("id") instanceof Integer ? (Integer) song.get("id") : 
                         Integer.parseInt(song.get("id").toString());

            iTunesLibraryService.iTunesTrack track = iTunesLibraryService.findMatch(library, name, artist, album);
            
            Map<String, Object> songResult = new HashMap<>();
            songResult.put("id", id);
            songResult.put("name", name);
            songResult.put("artist", artist);
            songResult.put("album", album);
            
            if (track != null) {
                // Found exact match
                songResult.put("iTunesName", track.name);
                songResult.put("iTunesArtist", track.artist);
                songResult.put("iTunesAlbum", track.album);
                matched.add(songResult);
            } else {
                unmatched.add(songResult);
            }
        }

        result.put("matched", matched);
        result.put("unmatched", unmatched);
        result.put("totalSongs", songs.size());
        result.put("matchedCount", matched.size());
        result.put("unmatchedCount", unmatched.size());
        
        return result;
    }

    /**
     * Search for songs to add to playlist (AJAX endpoint)
     */
    @GetMapping("/search")
    @ResponseBody
    public List<SongCardDTO> searchSongs(
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int perpage) {
        
        return songService.getSongs(
                q, null, null,              // name, artistName, albumName
                null, null,                 // genreIds, genreMode
                null, null,                 // subgenreIds, subgenreMode
                null, null,                 // languageIds, languageMode
                null, null,                 // genderIds, genderMode
                null, null,                 // ethnicityIds, ethnicityMode
                null, null,                 // countries, countryMode
                null, null,                 // accounts, accountMode
                null, null, null, null,     // releaseDate, from, to, mode
                null, null, null, null,     // firstListenedDate, from, to, mode
                null, null, null, null,     // lastListenedDate, from, to, mode
                null, null,                 // listenedDateFrom, listenedDateTo
                null, null, null, null, null, // organized, hasImage, hasFeaturedArtists, isBand, isSingle
                null, null,                 // playCountMin, playCountMax
                null, null, null,           // lengthMin, lengthMax, lengthMode
                "name", "asc", page, perpage
        );
    }

    /**
     * Generate and download M3U playlist
     */
    @PostMapping("/generate")
    public ResponseEntity<byte[]> generatePlaylist(
            @RequestParam("songIds") List<Integer> songIds,
            @RequestParam(defaultValue = "playlist") String name) {
        
        String m3uContent = playlistService.generateM3U(songIds);
        
        String filename = name.replaceAll("[^a-zA-Z0-9-_]", "_") + ".m3u";
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("audio/x-mpegurl"))
                .body(m3uContent.getBytes());
    }

    /**
     * Generate M3U8 (UTF-8 encoded) playlist
     */
    @PostMapping("/generate-m3u8")
    public ResponseEntity<byte[]> generateM3U8Playlist(
            @RequestParam("songIds") List<Integer> songIds,
            @RequestParam(defaultValue = "playlist") String name) {
        
        String m3u8Content = playlistService.generateM3U8(songIds);
        
        String filename = name.replaceAll("[^a-zA-Z0-9-_]", "_") + ".m3u8";
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("audio/x-mpegurl; charset=UTF-8"))
                .body(m3u8Content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Generate PowerShell script for iTunes COM automation
     */
    @PostMapping("/generate-ps1")
    public ResponseEntity<byte[]> generatePowerShellScript(
            @RequestParam("songIds") List<Integer> songIds,
            @RequestParam(defaultValue = "playlist") String name) {
        
        String psContent = playlistService.generatePowerShellScript(songIds, name);
        
        String filename = name.replaceAll("[^a-zA-Z0-9-_]", "_") + ".ps1";
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("text/plain; charset=UTF-8"))
                .body(psContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Generate iTunes-compatible TXT file (tab-separated, importable via File > Library > Import Playlist)
     */
    @PostMapping("/generate-itunes")
    public ResponseEntity<byte[]> generateiTunesTxt(
            @RequestParam("songIds") List<Integer> songIds,
            @RequestParam(defaultValue = "playlist") String name) {
        
        String txtContent = playlistService.generateiTunesTxt(songIds);
        
        String filename = name.replaceAll("[^a-zA-Z0-9-_]", "_") + ".txt";
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("text/plain; charset=UTF-8"))
                .body(txtContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Download the folder watcher PowerShell script
     */
    @GetMapping("/download-watcher")
    public ResponseEntity<byte[]> downloadWatcherScript() {
        String watchFolder = playlistService.getDefaultWatchFolder();
        String psContent = playlistService.generateFolderWatcherScript(watchFolder);
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"iTunesPlaylistWatcher.ps1\"")
                .contentType(MediaType.parseMediaType("text/plain; charset=UTF-8"))
                .body(psContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Get the watch folder path (for display in UI)
     */
    @GetMapping("/watch-folder")
    @ResponseBody
    public Map<String, String> getWatchFolder() {
        Map<String, String> result = new HashMap<>();
        result.put("path", playlistService.getDefaultWatchFolder());
        return result;
    }

    /**
     * Export playlist directly to the watch folder (no download - instant import if watcher is running)
     */
    @PostMapping("/export-to-folder")
    @ResponseBody
    public Map<String, Object> exportToFolder(
            @RequestParam("songIds") List<Integer> songIds,
            @RequestParam(defaultValue = "playlist") String name) {
        
        Map<String, Object> result = new HashMap<>();
        
        try {
            String watchFolder = playlistService.getDefaultWatchFolder();
            java.nio.file.Path folderPath = java.nio.file.Paths.get(watchFolder);
            
            // Create folder if it doesn't exist
            if (!java.nio.file.Files.exists(folderPath)) {
                java.nio.file.Files.createDirectories(folderPath);
            }
            
            // Generate the playlist content
            String txtContent = playlistService.generateiTunesTxt(songIds);
            
            // Write to file
            String filename = name.replaceAll("[^a-zA-Z0-9-_ ]", "_") + ".txt";
            java.nio.file.Path filePath = folderPath.resolve(filename);
            java.nio.file.Files.writeString(filePath, txtContent, java.nio.charset.StandardCharsets.UTF_8);
            
            result.put("success", true);
            result.put("path", filePath.toString());
            result.put("songCount", songIds.size());
            result.put("message", "Playlist exported! If the watcher is running, it will be imported automatically.");
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        
        return result;
    }

    /**
     * Download the auto-start batch file
     */
    @GetMapping("/download-autostart")
    public ResponseEntity<byte[]> downloadAutoStartBatch() {
        String watchFolder = playlistService.getDefaultWatchFolder();
        String batContent = playlistService.generateAutoStartBatch(watchFolder);
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"StartWatcher.bat\"")
                .contentType(MediaType.parseMediaType("text/plain; charset=UTF-8"))
                .body(batContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Get auto-start instructions
     */
    @GetMapping("/autostart-instructions")
    @ResponseBody
    public Map<String, String> getAutoStartInstructions() {
        String watchFolder = playlistService.getDefaultWatchFolder();
        Map<String, String> result = new HashMap<>();
        result.put("instructions", playlistService.getAutoStartInstructions(watchFolder));
        result.put("startupFolder", "%APPDATA%\\Microsoft\\Windows\\Start Menu\\Programs\\Startup");
        result.put("watchFolder", watchFolder);
        return result;
    }
}
