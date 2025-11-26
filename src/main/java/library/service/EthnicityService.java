package library.service;

import library.dto.EthnicityCardDTO;
import library.entity.Ethnicity;
import library.repository.EthnicityRepository;
import library.repository.LookupRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class EthnicityService {
    
    private final EthnicityRepository ethnicityRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    
    public EthnicityService(EthnicityRepository ethnicityRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate) {
        this.ethnicityRepository = ethnicityRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<EthnicityCardDTO> getEthnicities(String name, String sortBy, String sortDir, int page, int perPage) {
        int offset = page * perPage;
        
        // Determine sort direction
        String sortColumn = "e.name";
        String sortDirection = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        
        if (sortBy != null) {
            switch (sortBy.toLowerCase()) {
                case "plays":
                    sortColumn = "play_count";
                    break;
                case "time":
                    sortColumn = "time_listened";
                    break;
                case "artists":
                    sortColumn = "artist_count";
                    break;
                case "songs":
                    sortColumn = "song_count";
                    break;
                case "albums":
                    sortColumn = "album_count";
                    break;
                default:
                    sortColumn = "e.name";
            }
        }
        
        String sql = """
            SELECT 
                e.id,
                e.name,
                CASE WHEN e.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                COALESCE(stats.play_count, 0) as play_count,
                COALESCE(stats.time_listened, 0) as time_listened,
                COALESCE(stats.artist_count, 0) as artist_count,
                COALESCE(stats.album_count, 0) as album_count,
                COALESCE(stats.song_count, 0) as song_count,
                COALESCE(stats.male_song_count, 0) as male_song_count,
                COALESCE(stats.female_song_count, 0) as female_song_count,
                COALESCE(stats.male_artist_count, 0) as male_artist_count,
                COALESCE(stats.female_artist_count, 0) as female_artist_count,
                COALESCE(stats.male_album_count, 0) as male_album_count,
                COALESCE(stats.female_album_count, 0) as female_album_count,
                COALESCE(stats.male_play_count, 0) as male_play_count,
                COALESCE(stats.female_play_count, 0) as female_play_count,
                COALESCE(stats.male_time_listened, 0) as male_time_listened,
                COALESCE(stats.female_time_listened, 0) as female_time_listened
            FROM Ethnicity e
            LEFT JOIN (
                SELECT 
                    COALESCE(s.override_ethnicity_id, ar.ethnicity_id) as effective_ethnicity_id,
                    COUNT(DISTINCT scr.id) as play_count,
                    SUM(s.length_seconds) as time_listened,
                    COUNT(DISTINCT ar.id) as artist_count,
                    COUNT(DISTINCT al.id) as album_count,
                    COUNT(DISTINCT s.id) as song_count,
                    SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_song_count,
                    SUM(CASE WHEN gn.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_song_count,
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) as male_artist_count,
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN ar.id END) as female_artist_count,
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) as male_album_count,
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN al.id END) as female_album_count,
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) as male_play_count,
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN scr.id END) as female_play_count,
                    SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as male_time_listened,
                    SUM(CASE WHEN gn.name LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as female_time_listened
                FROM Song s
                JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
                LEFT JOIN Scrobble scr ON s.id = scr.song_id
                WHERE COALESCE(s.override_ethnicity_id, ar.ethnicity_id) IS NOT NULL
                GROUP BY effective_ethnicity_id
            ) stats ON e.id = stats.effective_ethnicity_id
            WHERE (? IS NULL OR e.name LIKE '%' || ? || '%')
            ORDER BY """ + " " + sortColumn + " " + sortDirection + " LIMIT ? OFFSET ?";
        
        List<Object[]> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Object[] row = new Object[18];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getInt("has_image");
            row[3] = rs.getInt("play_count");
            row[4] = rs.getLong("time_listened");
            row[5] = rs.getInt("artist_count");
            row[6] = rs.getInt("album_count");
            row[7] = rs.getInt("song_count");
            row[8] = rs.getInt("male_song_count");
            row[9] = rs.getInt("female_song_count");
            row[10] = rs.getInt("male_artist_count");
            row[11] = rs.getInt("female_artist_count");
            row[12] = rs.getInt("male_album_count");
            row[13] = rs.getInt("female_album_count");
            row[14] = rs.getInt("male_play_count");
            row[15] = rs.getInt("female_play_count");
            row[16] = rs.getLong("male_time_listened");
            row[17] = rs.getLong("female_time_listened");
            return row;
        }, name, name, perPage, offset);
        
        List<EthnicityCardDTO> ethnicities = new ArrayList<>();
        for (Object[] row : results) {
            EthnicityCardDTO dto = new EthnicityCardDTO();
            dto.setId((Integer) row[0]);
            dto.setName((String) row[1]);
            dto.setHasImage(((Integer) row[2]) == 1);
            dto.setPlayCount((Integer) row[3]);
            dto.setTimeListened((Long) row[4]);
            dto.setTimeListenedFormatted(formatTime((Long) row[4]));
            dto.setArtistCount((Integer) row[5]);
            dto.setAlbumCount((Integer) row[6]);
            dto.setSongCount((Integer) row[7]);
            dto.setMaleCount((Integer) row[8]);
            dto.setFemaleCount((Integer) row[9]);
            dto.setMaleArtistCount((Integer) row[10]);
            dto.setFemaleArtistCount((Integer) row[11]);
            dto.setMaleAlbumCount((Integer) row[12]);
            dto.setFemaleAlbumCount((Integer) row[13]);
            dto.setMalePlayCount((Integer) row[14]);
            dto.setFemalePlayCount((Integer) row[15]);
            dto.setMaleTimeListened((Long) row[16]);
            dto.setFemaleTimeListened((Long) row[17]);
            ethnicities.add(dto);
        }
        
        return ethnicities;
    }
    
    public long countEthnicities(String name) {
        String sql = """
            SELECT COUNT(*)
            FROM Ethnicity e
            WHERE (? IS NULL OR e.name LIKE '%' || ? || '%')
            """;
        
        Long count = jdbcTemplate.queryForObject(sql, Long.class, name, name);
        return count != null ? count : 0;
    }
    
    public Optional<Ethnicity> getEthnicityById(Integer id) {
        String sql = """
            SELECT id, name, creation_date, update_date
            FROM Ethnicity
            WHERE id = ?
            """;
        
        List<Ethnicity> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Ethnicity ethnicity = new Ethnicity();
            ethnicity.setId(rs.getInt("id"));
            ethnicity.setName(rs.getString("name"));
            ethnicity.setCreationDate(rs.getTimestamp("creation_date"));
            ethnicity.setUpdateDate(rs.getTimestamp("update_date"));
            return ethnicity;
        }, id);
        
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
    
    public Ethnicity createEthnicity(Ethnicity ethnicity) {
        return ethnicityRepository.save(ethnicity);
    }
    
    public byte[] getEthnicityImage(Integer id) {
        String sql = "SELECT image FROM Ethnicity WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("image"), id);
        } catch (Exception e) {
            return null;
        }
    }
    
    public void updateEthnicityImage(Integer id, byte[] imageData) {
        ethnicityRepository.updateImage(id, imageData);
    }
    
    public Map<Integer, String> getEthnicities() {
        return lookupRepository.getAllEthnicities();
    }
    
    // Get top 50 artists for an ethnicity by play count
    public List<Map<String, Object>> getTopArtistsForEthnicity(Integer ethnicityId) {
        String sql = """
            SELECT 
                ar.id,
                ar.name,
                COUNT(scr.id) as play_count,
                CASE WHEN ar.image IS NOT NULL THEN 1 ELSE 0 END as has_image
            FROM Artist ar
            JOIN Song s ON ar.id = s.artist_id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = ?
            GROUP BY ar.id, ar.name
            ORDER BY play_count DESC
            LIMIT 50
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> artist = new java.util.HashMap<>();
            artist.put("id", rs.getInt("id"));
            artist.put("name", rs.getString("name"));
            artist.put("playCount", rs.getInt("play_count"));
            artist.put("hasImage", rs.getInt("has_image") == 1);
            return artist;
        }, ethnicityId);
    }
    
    // Get top 50 albums for an ethnicity by play count
    public List<Map<String, Object>> getTopAlbumsForEthnicity(Integer ethnicityId) {
        String sql = """
            SELECT 
                al.id,
                al.name,
                ar.name as artist_name,
                COUNT(scr.id) as play_count,
                CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END as has_image
            FROM Album al
            JOIN Artist ar ON al.artist_id = ar.id
            JOIN Song s ON al.id = s.album_id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = ?
            GROUP BY al.id, al.name, ar.name
            ORDER BY play_count DESC
            LIMIT 50
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> album = new java.util.HashMap<>();
            album.put("id", rs.getInt("id"));
            album.put("name", rs.getString("name"));
            album.put("artistName", rs.getString("artist_name"));
            album.put("playCount", rs.getInt("play_count"));
            album.put("hasImage", rs.getInt("has_image") == 1);
            return album;
        }, ethnicityId);
    }
    
    // Get top 50 songs for an ethnicity by play count
    public List<Map<String, Object>> getTopSongsForEthnicity(Integer ethnicityId) {
        String sql = """
            SELECT 
                s.id,
                s.name,
                ar.name as artist_name,
                al.name as album_name,
                COUNT(scr.id) as play_count
            FROM Song s
            JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = ?
            GROUP BY s.id, s.name, ar.name, al.name
            ORDER BY play_count DESC
            LIMIT 50
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> song = new java.util.HashMap<>();
            song.put("id", rs.getInt("id"));
            song.put("name", rs.getString("name"));
            song.put("artistName", rs.getString("artist_name"));
            song.put("albumName", rs.getString("album_name"));
            song.put("playCount", rs.getInt("play_count"));
            return song;
        }, ethnicityId);
    }
    
    public Map<String, Object> getEthnicityStats(Integer ethnicityId) {
        String sql = """
            SELECT 
                COUNT(DISTINCT scr.id) as play_count,
                COALESCE(SUM(s.length_seconds), 0) as total_length,
                COUNT(DISTINCT ar.id) as artist_count,
                COUNT(DISTINCT al.id) as album_count,
                COUNT(DISTINCT s.id) as song_count,
                MIN(scr.scrobble_date) as first_listened,
                MAX(scr.scrobble_date) as last_listened
            FROM Song s
            JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE ar.ethnicity_id = ?
            """;
        
        return jdbcTemplate.queryForMap(sql, ethnicityId);
    }
    
    // Helper method to format time in seconds to human-readable format
    private String formatTime(long totalSeconds) {
        if (totalSeconds == 0) {
            return "0m";
        }
        
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        
        if (days > 0) {
            return String.format("%dd %dh", days, hours);
        } else if (hours > 0) {
            return String.format("%dh %dm", hours, minutes);
        } else {
            return String.format("%dm", minutes);
        }
    }
}
