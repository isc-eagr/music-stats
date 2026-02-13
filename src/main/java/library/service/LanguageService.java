package library.service;

import library.dto.LanguageCardDTO;
import library.entity.Language;
import library.repository.LanguageRepository;
import library.repository.LookupRepository;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class LanguageService {
    
    private final LanguageRepository languageRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    
    public LanguageService(LanguageRepository languageRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate) {
        this.languageRepository = languageRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<LanguageCardDTO> getLanguages(String name, String sortBy, String sortDir, int page, int perPage) {
        int offset = page * perPage;
        
        // Determine sort direction
        String sortColumn = "l.name";
        String sortDirection = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        String nullsHandling = " NULLS LAST";

        if (sortBy != null) {
            switch (sortBy.toLowerCase()) {
                case "plays":
                    sortColumn = "play_count";
                    nullsHandling = "";
                    break;
                case "time":
                    sortColumn = "time_listened";
                    nullsHandling = "";
                    break;
                case "artists":
                    sortColumn = "artist_count";
                    nullsHandling = "";
                    break;
                case "songs":
                    sortColumn = "song_count";
                    nullsHandling = "";
                    break;
                case "albums":
                    sortColumn = "album_count";
                    nullsHandling = "";
                    break;
                case "maleartistpct":
                    sortColumn = "male_artist_pct";
                    break;
                case "malealbumpct":
                    sortColumn = "male_album_pct";
                    break;
                case "malesongpct":
                    sortColumn = "male_song_pct";
                    break;
                case "maleplaypct":
                    sortColumn = "male_play_pct";
                    break;
                case "maletimepct":
                    sortColumn = "male_time_pct";
                    break;
                default:
                    sortColumn = "l.name";
                    nullsHandling = "";
            }
        }

        String sql = """
            SELECT 
                l.id,
                l.name,
                CASE WHEN l.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                COALESCE(stats.play_count, 0) as play_count,
                COALESCE(stats.vatito_play_count, 0) as vatito_play_count,
                COALESCE(stats.robertlover_play_count, 0) as robertlover_play_count,
                COALESCE(stats.time_listened, 0) as time_listened,
                COALESCE(stats.artist_count, 0) as artist_count,
                COALESCE(stats.album_count, 0) as album_count,
                COALESCE(stats.song_count, 0) as song_count,
                COALESCE(stats.male_song_count, 0) as male_song_count,
                COALESCE(stats.female_song_count, 0) as female_song_count,
                COALESCE(stats.other_song_count, 0) as other_song_count,
                COALESCE(stats.male_artist_count, 0) as male_artist_count,
                COALESCE(stats.female_artist_count, 0) as female_artist_count,
                COALESCE(stats.other_artist_count, 0) as other_artist_count,
                COALESCE(stats.male_album_count, 0) as male_album_count,
                COALESCE(stats.female_album_count, 0) as female_album_count,
                COALESCE(stats.other_album_count, 0) as other_album_count,
                COALESCE(stats.male_play_count, 0) as male_play_count,
                COALESCE(stats.female_play_count, 0) as female_play_count,
                COALESCE(stats.other_play_count, 0) as other_play_count,
                COALESCE(stats.male_time_listened, 0) as male_time_listened,
                COALESCE(stats.female_time_listened, 0) as female_time_listened,
                COALESCE(stats.other_time_listened, 0) as other_time_listened,
                CASE WHEN COALESCE(stats.male_artist_count, 0) + COALESCE(stats.female_artist_count, 0) > 0 
                     THEN CAST(COALESCE(stats.male_artist_count, 0) AS REAL) / (COALESCE(stats.male_artist_count, 0) + COALESCE(stats.female_artist_count, 0)) 
                     ELSE NULL END as male_artist_pct,
                CASE WHEN COALESCE(stats.male_album_count, 0) + COALESCE(stats.female_album_count, 0) > 0 
                     THEN CAST(COALESCE(stats.male_album_count, 0) AS REAL) / (COALESCE(stats.male_album_count, 0) + COALESCE(stats.female_album_count, 0)) 
                     ELSE NULL END as male_album_pct,
                CASE WHEN COALESCE(stats.male_song_count, 0) + COALESCE(stats.female_song_count, 0) > 0 
                     THEN CAST(COALESCE(stats.male_song_count, 0) AS REAL) / (COALESCE(stats.male_song_count, 0) + COALESCE(stats.female_song_count, 0)) 
                     ELSE NULL END as male_song_pct,
                CASE WHEN COALESCE(stats.male_play_count, 0) + COALESCE(stats.female_play_count, 0) > 0 
                     THEN CAST(COALESCE(stats.male_play_count, 0) AS REAL) / (COALESCE(stats.male_play_count, 0) + COALESCE(stats.female_play_count, 0)) 
                     ELSE NULL END as male_play_pct,
                CASE WHEN COALESCE(stats.male_time_listened, 0) + COALESCE(stats.female_time_listened, 0) > 0 
                     THEN CAST(COALESCE(stats.male_time_listened, 0) AS REAL) / (COALESCE(stats.male_time_listened, 0) + COALESCE(stats.female_time_listened, 0)) 
                     ELSE NULL END as male_time_pct
            FROM Language l
            LEFT JOIN (
                SELECT 
                    COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) as effective_language_id,
                    COUNT(DISTINCT p.id) as play_count,
                    COUNT(DISTINCT CASE WHEN p.account = 'vatito' THEN p.id END) as vatito_play_count,
                    COUNT(DISTINCT CASE WHEN p.account = 'robertlover' THEN p.id END) as robertlover_play_count,
                    SUM(s.length_seconds) as time_listened,
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
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN p.id END) as male_play_count,
                    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN p.id END) as female_play_count,
                    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN p.id END) as other_play_count,
                    SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as male_time_listened,
                    SUM(CASE WHEN gn.name LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as female_time_listened,
                    SUM(CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as other_time_listened
                FROM Song s
                JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
                LEFT JOIN Play p ON s.id = p.song_id
                WHERE COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) IS NOT NULL
                GROUP BY effective_language_id
            ) stats ON l.id = stats.effective_language_id
            WHERE (? IS NULL OR l.name LIKE '%' || ? || '%')
            ORDER BY """ + " " + sortColumn + " " + sortDirection + nullsHandling + " LIMIT ? OFFSET ?";

        List<Object[]> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Object[] row = new Object[25];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getInt("has_image");
            row[3] = rs.getInt("play_count");
            row[4] = rs.getInt("vatito_play_count");
            row[5] = rs.getInt("robertlover_play_count");
            row[6] = rs.getLong("time_listened");
            row[7] = rs.getInt("artist_count");
            row[8] = rs.getInt("album_count");
            row[9] = rs.getInt("song_count");
            row[10] = rs.getInt("male_song_count");
            row[11] = rs.getInt("female_song_count");
            row[12] = rs.getInt("other_song_count");
            row[13] = rs.getInt("male_artist_count");
            row[14] = rs.getInt("female_artist_count");
            row[15] = rs.getInt("other_artist_count");
            row[16] = rs.getInt("male_album_count");
            row[17] = rs.getInt("female_album_count");
            row[18] = rs.getInt("other_album_count");
            row[19] = rs.getInt("male_play_count");
            row[20] = rs.getInt("female_play_count");
            row[21] = rs.getInt("other_play_count");
            row[22] = rs.getLong("male_time_listened");
            row[23] = rs.getLong("female_time_listened");
            row[24] = rs.getLong("other_time_listened");
            return row;
        }, name, name, perPage, offset);
        
        List<LanguageCardDTO> languages = new ArrayList<>();
        for (Object[] row : results) {
            LanguageCardDTO dto = new LanguageCardDTO();
            dto.setId((Integer) row[0]);
            dto.setName((String) row[1]);
            dto.setHasImage(((Integer) row[2]) == 1);
            dto.setPlayCount((Integer) row[3]);
            dto.setVatitoPlayCount((Integer) row[4]);
            dto.setRobertloverPlayCount((Integer) row[5]);
            dto.setTimeListened((Long) row[6]);
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime((Long) row[6]));
            dto.setArtistCount((Integer) row[7]);
            dto.setAlbumCount((Integer) row[8]);
            dto.setSongCount((Integer) row[9]);
            dto.setMaleCount((Integer) row[10]);
            dto.setFemaleCount((Integer) row[11]);
            dto.setOtherCount((Integer) row[12]);
            dto.setMaleArtistCount((Integer) row[13]);
            dto.setFemaleArtistCount((Integer) row[14]);
            dto.setOtherArtistCount((Integer) row[15]);
            dto.setMaleAlbumCount((Integer) row[16]);
            dto.setFemaleAlbumCount((Integer) row[17]);
            dto.setOtherAlbumCount((Integer) row[18]);
            dto.setMalePlayCount((Integer) row[19]);
            dto.setFemalePlayCount((Integer) row[20]);
            dto.setOtherPlayCount((Integer) row[21]);
            dto.setMaleTimeListened((Long) row[22]);
            dto.setFemaleTimeListened((Long) row[23]);
            dto.setOtherTimeListened((Long) row[24]);
            languages.add(dto);
        }
        
        // Fetch top artist/album/song for all languages on this page
        if (!languages.isEmpty()) {
            populateTopItems(languages);
        }

        return languages;
    }
    
    /**
     * Populates the top artist, album, and song for each language in the list.
     */
    private void populateTopItems(List<LanguageCardDTO> languages) {
        List<Integer> languageIds = languages.stream().map(LanguageCardDTO::getId).toList();
        if (languageIds.isEmpty()) return;

        String placeholders = String.join(",", languageIds.stream().map(id -> "?").toList());

        // Query for top artist per language
        String topArtistSql =
            "WITH artist_plays AS ( " +
            "    SELECT " +
            "        COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) as language_id, " +
            "        ar.id as artist_id, " +
            "        ar.name as artist_name, " +
            "        ar.gender_id as gender_id, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Play p " +
            "    JOIN Song s ON p.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) IN (" + placeholders + ") " +
            "    GROUP BY language_id, ar.id, ar.name, ar.gender_id " +
            ") " +
            "SELECT language_id, artist_id, artist_name, gender_id FROM artist_plays WHERE rn = 1";

        List<Object[]> artistResults = jdbcTemplate.query(topArtistSql, (rs, rowNum) ->
            new Object[]{rs.getInt("language_id"), rs.getInt("artist_id"), rs.getString("artist_name"),
                        rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null},
            languageIds.toArray()
        );

        // Query for top album per language
        String topAlbumSql =
            "WITH album_plays AS ( " +
            "    SELECT " +
            "        COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) as language_id, " +
            "        al.id as album_id, " +
            "        al.name as album_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Play p " +
            "    JOIN Song s ON p.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE al.id IS NOT NULL AND COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) IN (" + placeholders + ") " +
            "    GROUP BY language_id, al.id, al.name, ar.name " +
            ") " +
            "SELECT language_id, album_id, album_name, artist_name FROM album_plays WHERE rn = 1";

        List<Object[]> albumResults = jdbcTemplate.query(topAlbumSql, (rs, rowNum) ->
            new Object[]{rs.getInt("language_id"), rs.getInt("album_id"), rs.getString("album_name"), rs.getString("artist_name")},
            languageIds.toArray()
        );

        // Query for top song per language
        String topSongSql =
            "WITH song_plays AS ( " +
            "    SELECT " +
            "        COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) as language_id, " +
            "        s.id as song_id, " +
            "        s.name as song_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Play p " +
            "    JOIN Song s ON p.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) IN (" + placeholders + ") " +
            "    GROUP BY language_id, s.id, s.name, ar.name " +
            ") " +
            "SELECT language_id, song_id, song_name, artist_name FROM song_plays WHERE rn = 1";

        List<Object[]> songResults = jdbcTemplate.query(topSongSql, (rs, rowNum) ->
            new Object[]{rs.getInt("language_id"), rs.getInt("song_id"), rs.getString("song_name"), rs.getString("artist_name")},
            languageIds.toArray()
        );

        // Map results to languages
        for (LanguageCardDTO lang : languages) {
            for (Object[] row : artistResults) {
                if (lang.getId().equals(row[0])) {
                    lang.setTopArtistId((Integer) row[1]);
                    lang.setTopArtistName((String) row[2]);
                    lang.setTopArtistGenderId((Integer) row[3]);
                    break;
                }
            }
            for (Object[] row : albumResults) {
                if (lang.getId().equals(row[0])) {
                    lang.setTopAlbumId((Integer) row[1]);
                    lang.setTopAlbumName((String) row[2]);
                    lang.setTopAlbumArtistName((String) row[3]);
                    break;
                }
            }
            for (Object[] row : songResults) {
                if (lang.getId().equals(row[0])) {
                    lang.setTopSongId((Integer) row[1]);
                    lang.setTopSongName((String) row[2]);
                    lang.setTopSongArtistName((String) row[3]);
                    break;
                }
            }
        }
    }

    public long countLanguages(String name) {
        String sql = """
            SELECT COUNT(*)
            FROM Language l
            WHERE (? IS NULL OR l.name LIKE '%' || ? || '%')
            """;
        
        Long count = jdbcTemplate.queryForObject(sql, Long.class, name, name);
        return count != null ? count : 0;
    }
    
    public Optional<Language> getLanguageById(Integer id) {
        String sql = """
            SELECT id, name, creation_date, update_date
            FROM Language
            WHERE id = ?
            """;
        
        List<Language> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Language language = new Language();
            language.setId(rs.getInt("id"));
            language.setName(rs.getString("name"));
            language.setCreationDate(rs.getTimestamp("creation_date"));
            language.setUpdateDate(rs.getTimestamp("update_date"));
            return language;
        }, id);
        
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
    
    public Language createLanguage(Language language) {
        return languageRepository.save(language);
    }
    
    public byte[] getLanguageImage(Integer id) {
        String sql = "SELECT image FROM Language WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("image"), id);
        } catch (Exception e) {
            return null;
        }
    }
    
    public void updateLanguageImage(Integer id, byte[] imageData) {
        languageRepository.updateImage(id, imageData);
    }
    
    public Map<Integer, String> getLanguages() {
        return lookupRepository.getAllLanguages();
    }
    
    // Get top 50 artists for a language by play count
    public List<Map<String, Object>> getTopArtistsForLanguage(Integer languageId) {
        String sql = """
            SELECT 
                ar.id,
                ar.name,
                COUNT(p.id) as play_count,
                CASE WHEN ar.image IS NOT NULL THEN 1 ELSE 0 END as has_image
            FROM Artist ar
            JOIN Song s ON ar.id = s.artist_id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Play p ON s.id = p.song_id
            WHERE COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = ?
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
        }, languageId);
    }
    
    // Get top 50 albums for a language by play count
    public List<Map<String, Object>> getTopAlbumsForLanguage(Integer languageId) {
        String sql = """
            SELECT 
                al.id,
                al.name,
                ar.name as artist_name,
                COUNT(p.id) as play_count,
                CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END as has_image
            FROM Album al
            JOIN Artist ar ON al.artist_id = ar.id
            JOIN Song s ON al.id = s.album_id
            LEFT JOIN Play p ON s.id = p.song_id
            WHERE COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = ?
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
        }, languageId);
    }
    
    // Get top 50 songs for a language by play count
    public List<Map<String, Object>> getTopSongsForLanguage(Integer languageId) {
        String sql = """
            SELECT 
                s.id,
                s.name,
                ar.name as artist_name,
                al.name as album_name,
                COUNT(p.id) as play_count
            FROM Song s
            JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Play p ON s.id = p.song_id
            WHERE COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = ?
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
        }, languageId);
    }
    
    public Map<String, Object> getLanguageStats(Integer languageId) {
        String sql = """
            SELECT 
                COUNT(DISTINCT p.id) as play_count,
                COALESCE(SUM(s.length_seconds), 0) as total_length,
                COUNT(DISTINCT ar.id) as artist_count,
                COUNT(DISTINCT al.id) as album_count,
                COUNT(DISTINCT s.id) as song_count,
                MIN(p.play_date) as first_listened,
                MAX(p.play_date) as last_listened
            FROM Song s
            JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Play p ON s.id = p.song_id
            WHERE COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = ?
            """;
        
        return jdbcTemplate.queryForMap(sql, languageId);
    }
    
    /**
     * Get all languages as simple id/name maps for dropdown lists.
     */
    public java.util.List<java.util.Map<String, Object>> getAllLanguagesSimple() {
        String sql = "SELECT id, name FROM Language ORDER BY name";
        return jdbcTemplate.queryForList(sql);
    }
}
