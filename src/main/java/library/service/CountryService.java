package library.service;

import library.dto.CountryCardDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class CountryService {
    
    private final JdbcTemplate jdbcTemplate;
    
    public CountryService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<CountryCardDTO> getCountries(String name, String sortBy, String sortDir, int page, int perPage) {
        int offset = page * perPage;
        
        // Determine sort direction
        String sortColumn = "country";
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
                    sortColumn = "country";
            }
        }
        
        String sql = """
            SELECT 
                ar.country,
                COUNT(DISTINCT scr.id) as play_count,
                COUNT(DISTINCT CASE WHEN scr.account = 'vatito' THEN scr.id END) as vatito_play_count,
                COUNT(DISTINCT CASE WHEN scr.account = 'robertlover' THEN scr.id END) as robertlover_play_count,
                COALESCE(SUM(s.length_seconds), 0) as time_listened,
                COUNT(DISTINCT ar.id) as artist_count,
                COUNT(DISTINCT al.id) as album_count,
                COUNT(DISTINCT s.id) as song_count,
                SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_song_count,
                SUM(CASE WHEN gn.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_song_count,
                SUM(CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as other_song_count,
                COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) as male_artist_count,
                COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN ar.id END) as female_artist_count,
                COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) as other_artist_count,
                COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) as male_album_count,
                COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN al.id END) as female_album_count,
                COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) as other_album_count,
                COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) as male_play_count,
                COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN scr.id END) as female_play_count,
                COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) as other_play_count,
                SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as male_time_listened,
                SUM(CASE WHEN gn.name LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as female_time_listened,
                SUM(CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as other_time_listened
            FROM Artist ar
            JOIN Song s ON ar.id = s.artist_id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE ar.country IS NOT NULL AND ar.country != ''
                AND (? IS NULL OR ar.country LIKE '%' || ? || '%')
            GROUP BY ar.country
            ORDER BY """ + " " + sortColumn + " " + sortDirection + " LIMIT ? OFFSET ?";
        
        List<Object[]> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Object[] row = new Object[23];
            row[0] = rs.getString("country");
            row[1] = rs.getInt("play_count");
            row[2] = rs.getInt("vatito_play_count");
            row[3] = rs.getInt("robertlover_play_count");
            row[4] = rs.getLong("time_listened");
            row[5] = rs.getInt("artist_count");
            row[6] = rs.getInt("album_count");
            row[7] = rs.getInt("song_count");
            row[8] = rs.getInt("male_song_count");
            row[9] = rs.getInt("female_song_count");
            row[10] = rs.getInt("other_song_count");
            row[11] = rs.getInt("male_artist_count");
            row[12] = rs.getInt("female_artist_count");
            row[13] = rs.getInt("other_artist_count");
            row[14] = rs.getInt("male_album_count");
            row[15] = rs.getInt("female_album_count");
            row[16] = rs.getInt("other_album_count");
            row[17] = rs.getInt("male_play_count");
            row[18] = rs.getInt("female_play_count");
            row[19] = rs.getInt("other_play_count");
            row[20] = rs.getLong("male_time_listened");
            row[21] = rs.getLong("female_time_listened");
            row[22] = rs.getLong("other_time_listened");
            return row;
        }, name, name, perPage, offset);
        
        List<CountryCardDTO> countries = new ArrayList<>();
        for (Object[] row : results) {
            CountryCardDTO dto = new CountryCardDTO();
            dto.setName((String) row[0]);
            dto.setPlayCount((Integer) row[1]);
            dto.setVatitoPlayCount((Integer) row[2]);
            dto.setRobertloverPlayCount((Integer) row[3]);
            dto.setTimeListened((Long) row[4]);
            dto.setTimeListenedFormatted(formatTime((Long) row[4]));
            dto.setArtistCount((Integer) row[5]);
            dto.setAlbumCount((Integer) row[6]);
            dto.setSongCount((Integer) row[7]);
            dto.setMaleCount((Integer) row[8]);
            dto.setFemaleCount((Integer) row[9]);
            dto.setOtherCount((Integer) row[10]);
            dto.setMaleArtistCount((Integer) row[11]);
            dto.setFemaleArtistCount((Integer) row[12]);
            dto.setOtherArtistCount((Integer) row[13]);
            dto.setMaleAlbumCount((Integer) row[14]);
            dto.setFemaleAlbumCount((Integer) row[15]);
            dto.setOtherAlbumCount((Integer) row[16]);
            dto.setMalePlayCount((Integer) row[17]);
            dto.setFemalePlayCount((Integer) row[18]);
            dto.setOtherPlayCount((Integer) row[19]);
            dto.setMaleTimeListened((Long) row[20]);
            dto.setFemaleTimeListened((Long) row[21]);
            dto.setOtherTimeListened((Long) row[22]);
            countries.add(dto);
        }
        
        return countries;
    }
    
    public long countCountries(String name) {
        String sql = """
            SELECT COUNT(DISTINCT ar.country)
            FROM Artist ar
            WHERE ar.country IS NOT NULL AND ar.country != ''
                AND (? IS NULL OR ar.country LIKE '%' || ? || '%')
            """;
        
        Long count = jdbcTemplate.queryForObject(sql, Long.class, name, name);
        return count != null ? count : 0;
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
