package library.controller;

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

            Map<String, Object> localSong = playlistService.getSongDataForPlaylist(id);
            if (localSong != null) {
                name = (String) localSong.get("song_name");
                artist = (String) localSong.get("artist_name");
                album = (String) localSong.get("album_name");
            }

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
     * Generate iTunes-compatible TXT file (tab-separated, importable via File > Library > Import Playlist)
     */
    @PostMapping("/generate-itunes")
    public ResponseEntity<byte[]> generateiTunesTxt(
            @RequestParam("songIds") List<Integer> songIds,
            @RequestParam(defaultValue = "playlist") String name) {
        
        String txtContent = playlistService.generateiTunesTxt(songIds);
        
        String filename = name.replaceAll("[^a-zA-Z0-9\\s\\-_]", "_") + ".txt";
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("text/plain; charset=UTF-8"))
                .body(txtContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
