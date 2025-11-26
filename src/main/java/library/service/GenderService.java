package library.service;

import library.dto.GenderCardDTO;
import library.repository.LookupRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class GenderService {
    
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    
    public GenderService(LookupRepository lookupRepository, JdbcTemplate jdbcTemplate) {
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<GenderCardDTO> getGenders(String name, String sortBy, String sortDir, int page, int perPage) {
        int offset = page * perPage;
        
        // Determine sort direction
        String sortColumn = "g.name";
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
                    sortColumn = "g.name";
            }
        }
        
        String sql = """
            SELECT 
                g.id,
                g.name,
                COALESCE(stats.play_count, 0) as play_count,
                COALESCE(stats.time_listened, 0) as time_listened,
                COALESCE(stats.artist_count, 0) as artist_count,
                COALESCE(stats.album_count, 0) as album_count,
                COALESCE(stats.song_count, 0) as song_count
            FROM Gender g
            LEFT JOIN (
                SELECT 
                    COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id,
                    COUNT(DISTINCT scr.id) as play_count,
                    SUM(s.length_seconds) as time_listened,
                    COUNT(DISTINCT ar.id) as artist_count,
                    COUNT(DISTINCT al.id) as album_count,
                    COUNT(DISTINCT s.id) as song_count
                FROM Song s
                JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Scrobble scr ON s.id = scr.song_id
                WHERE COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL
                GROUP BY effective_gender_id
            ) stats ON g.id = stats.effective_gender_id
            WHERE (? IS NULL OR g.name LIKE '%' || ? || '%')
            ORDER BY """ + " " + sortColumn + " " + sortDirection + " LIMIT ? OFFSET ?";
        
        List<Object[]> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Object[] row = new Object[7];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getInt("play_count");
            row[3] = rs.getLong("time_listened");
            row[4] = rs.getInt("artist_count");
            row[5] = rs.getInt("album_count");
            row[6] = rs.getInt("song_count");
            return row;
        }, name, name, perPage, offset);
        
        List<GenderCardDTO> genders = new ArrayList<>();
        for (Object[] row : results) {
            GenderCardDTO dto = new GenderCardDTO();
            dto.setId((Integer) row[0]);
            dto.setName((String) row[1]);
            dto.setPlayCount((Integer) row[2]);
            dto.setTimeListened((Long) row[3]);
            dto.setTimeListenedFormatted(formatTime((Long) row[3]));
            dto.setArtistCount((Integer) row[4]);
            dto.setAlbumCount((Integer) row[5]);
            dto.setSongCount((Integer) row[6]);
            genders.add(dto);
        }
        
        return genders;
    }
    
    public long countGenders(String name) {
        String sql = """
            SELECT COUNT(*)
            FROM Gender g
            WHERE (? IS NULL OR g.name LIKE '%' || ? || '%')
            """;
        
        Long count = jdbcTemplate.queryForObject(sql, Long.class, name, name);
        return count != null ? count : 0;
    }
    
    public Map<Integer, String> getAllGenders() {
        return lookupRepository.getAllGenders();
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
