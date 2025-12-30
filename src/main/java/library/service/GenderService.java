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
                COALESCE(stats.vatito_play_count, 0) as vatito_play_count,
                COALESCE(stats.robertlover_play_count, 0) as robertlover_play_count,
                COALESCE(stats.time_listened, 0) as time_listened,
                COALESCE(stats.artist_count, 0) as artist_count,
                COALESCE(stats.album_count, 0) as album_count,
                COALESCE(stats.song_count, 0) as song_count
            FROM Gender g
            LEFT JOIN (
                SELECT 
                    COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id,
                    COUNT(DISTINCT scr.id) as play_count,
                    COUNT(DISTINCT CASE WHEN scr.account = 'vatito' THEN scr.id END) as vatito_play_count,
                    COUNT(DISTINCT CASE WHEN scr.account = 'robertlover' THEN scr.id END) as robertlover_play_count,
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
            Object[] row = new Object[9];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getInt("play_count");
            row[3] = rs.getInt("vatito_play_count");
            row[4] = rs.getInt("robertlover_play_count");
            row[5] = rs.getLong("time_listened");
            row[6] = rs.getInt("artist_count");
            row[7] = rs.getInt("album_count");
            row[8] = rs.getInt("song_count");
            return row;
        }, name, name, perPage, offset);
        
        List<GenderCardDTO> genders = new ArrayList<>();
        for (Object[] row : results) {
            GenderCardDTO dto = new GenderCardDTO();
            dto.setId((Integer) row[0]);
            dto.setName((String) row[1]);
            dto.setPlayCount((Integer) row[2]);
            dto.setVatitoPlayCount((Integer) row[3]);
            dto.setRobertloverPlayCount((Integer) row[4]);
            dto.setTimeListened((Long) row[5]);
            dto.setTimeListenedFormatted(formatTime((Long) row[5]));
            dto.setArtistCount((Integer) row[6]);
            dto.setAlbumCount((Integer) row[7]);
            dto.setSongCount((Integer) row[8]);
            genders.add(dto);
        }
        
        // Fetch top artist/album/song for all genders on this page
        if (!genders.isEmpty()) {
            populateTopItems(genders);
        }

        return genders;
    }
    
    /**
     * Populates the top artist, album, and song for each gender in the list.
     */
    private void populateTopItems(List<GenderCardDTO> genders) {
        List<Integer> genderIds = genders.stream().map(GenderCardDTO::getId).toList();
        if (genderIds.isEmpty()) return;

        String placeholders = String.join(",", genderIds.stream().map(id -> "?").toList());

        // Query for top artist per gender
        String topArtistSql =
            "WITH artist_plays AS ( " +
            "    SELECT " +
            "        COALESCE(s.override_gender_id, ar.gender_id) as gender_id, " +
            "        ar.id as artist_id, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_gender_id, ar.gender_id) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    WHERE COALESCE(s.override_gender_id, ar.gender_id) IN (" + placeholders + ") " +
            "    GROUP BY gender_id, ar.id, ar.name " +
            ") " +
            "SELECT gender_id, artist_id, artist_name FROM artist_plays WHERE rn = 1";

        List<Object[]> artistResults = jdbcTemplate.query(topArtistSql, (rs, rowNum) ->
            new Object[]{rs.getInt("gender_id"), rs.getInt("artist_id"), rs.getString("artist_name")},
            genderIds.toArray()
        );

        // Query for top album per gender
        String topAlbumSql =
            "WITH album_plays AS ( " +
            "    SELECT " +
            "        COALESCE(s.override_gender_id, ar.gender_id) as gender_id, " +
            "        al.id as album_id, " +
            "        al.name as album_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_gender_id, ar.gender_id) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE al.id IS NOT NULL AND COALESCE(s.override_gender_id, ar.gender_id) IN (" + placeholders + ") " +
            "    GROUP BY gender_id, al.id, al.name, ar.name " +
            ") " +
            "SELECT gender_id, album_id, album_name, artist_name FROM album_plays WHERE rn = 1";

        List<Object[]> albumResults = jdbcTemplate.query(topAlbumSql, (rs, rowNum) ->
            new Object[]{rs.getInt("gender_id"), rs.getInt("album_id"), rs.getString("album_name"), rs.getString("artist_name")},
            genderIds.toArray()
        );

        // Query for top song per gender
        String topSongSql =
            "WITH song_plays AS ( " +
            "    SELECT " +
            "        COALESCE(s.override_gender_id, ar.gender_id) as gender_id, " +
            "        s.id as song_id, " +
            "        s.name as song_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_gender_id, ar.gender_id) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    WHERE COALESCE(s.override_gender_id, ar.gender_id) IN (" + placeholders + ") " +
            "    GROUP BY gender_id, s.id, s.name, ar.name " +
            ") " +
            "SELECT gender_id, song_id, song_name, artist_name FROM song_plays WHERE rn = 1";

        List<Object[]> songResults = jdbcTemplate.query(topSongSql, (rs, rowNum) ->
            new Object[]{rs.getInt("gender_id"), rs.getInt("song_id"), rs.getString("song_name"), rs.getString("artist_name")},
            genderIds.toArray()
        );

        // Map results to genders
        for (GenderCardDTO gnd : genders) {
            for (Object[] row : artistResults) {
                if (gnd.getId().equals(row[0])) {
                    gnd.setTopArtistId((Integer) row[1]);
                    gnd.setTopArtistName((String) row[2]);
                    break;
                }
            }
            for (Object[] row : albumResults) {
                if (gnd.getId().equals(row[0])) {
                    gnd.setTopAlbumId((Integer) row[1]);
                    gnd.setTopAlbumName((String) row[2]);
                    gnd.setTopAlbumArtistName((String) row[3]);
                    break;
                }
            }
            for (Object[] row : songResults) {
                if (gnd.getId().equals(row[0])) {
                    gnd.setTopSongId((Integer) row[1]);
                    gnd.setTopSongName((String) row[2]);
                    gnd.setTopSongArtistName((String) row[3]);
                    break;
                }
            }
        }
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
    
    /**
     * Get all genders as simple id/name maps for dropdown lists.
     */
    public java.util.List<java.util.Map<String, Object>> getAllGendersSimple() {
        String sql = "SELECT id, name FROM gender ORDER BY name";
        return jdbcTemplate.queryForList(sql);
    }
}
