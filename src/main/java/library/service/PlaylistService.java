package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PlaylistService {

    private final JdbcTemplate jdbcTemplate;
    
    public PlaylistService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Generate iTunes-compatible tab-separated text file.
     * This format can be imported via File > Library > Import Playlist in iTunes.
     * Using minimal columns: Name, Artist, Album
     */
    public String generateiTunesTxt(List<Integer> songIds) {
        StringBuilder sb = new StringBuilder();
        
        // Header row (tab-separated)
        sb.append("Name\tArtist\tAlbum\n");
        
        for (Integer songId : songIds) {
            Map<String, Object> songData = getSongData(songId);
            if (songData != null) {
                String songName = (String) songData.get("song_name");
                String artistName = (String) songData.get("artist_name");
                String albumName = (String) songData.get("album_name");
                
                // Tab-separated values (escape tabs in content just in case)
                sb.append(escapeForTsv(songName)).append("\t")
                  .append(escapeForTsv(artistName != null ? artistName : "")).append("\t")
                  .append(escapeForTsv(albumName != null ? albumName : "")).append("\n");
            }
        }
        
        return sb.toString();
    }

    /**
     * Escape a string for TSV format (replace tabs and newlines)
     */
    private String escapeForTsv(String str) {
        if (str == null) return "";
        return str.replace("\t", " ").replace("\n", " ").replace("\r", "");
    }

    public Map<String, Object> getSongDataForPlaylist(Integer songId) {
        return getSongData(songId);
    }
    
    /**
     * Get song data with artist and album names using a join query.
     */
    private Map<String, Object> getSongData(Integer songId) {
        String sql = """
            SELECT 
                s.name as song_name,
                a.name as artist_name,
                al.name as album_name
            FROM Song s
            LEFT JOIN Artist a ON s.artist_id = a.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE s.id = ?
            """;
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, songId);
        return results.isEmpty() ? null : results.get(0);
    }
}
