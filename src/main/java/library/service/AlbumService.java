package library.service;

import library.dto.AlbumCardDTO;
import library.dto.AlbumSongDTO;
import library.entity.Album;
import library.repository.AlbumRepositoryNew;
import library.repository.LookupRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class AlbumService {
    
    private final AlbumRepositoryNew albumRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    
    public AlbumService(AlbumRepositoryNew albumRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate) {
        this.albumRepository = albumRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<AlbumCardDTO> getAlbums(String name, String artistName,
                                         List<Integer> genreIds, String genreMode,
                                         List<Integer> subgenreIds, String subgenreMode,
                                         List<Integer> languageIds, String languageMode,
                                         List<Integer> genderIds, String genderMode,
                                         List<Integer> ethnicityIds, String ethnicityMode,
                                         List<String> countries, String countryMode,
                                         String sortBy, int page, int perPage) {
        int offset = page * perPage;
        
        List<Object[]> results = albumRepository.findAlbumsWithStats(
                name, artistName, genreIds, genreMode, 
                subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                ethnicityIds, ethnicityMode, countries, countryMode, sortBy, perPage, offset
        );
        
        List<AlbumCardDTO> albums = new ArrayList<>();
        for (Object[] row : results) {
            AlbumCardDTO dto = new AlbumCardDTO();
            dto.setId(((Number) row[0]).intValue());
            dto.setName((String) row[1]);
            dto.setArtistName((String) row[2]);
            dto.setArtistId(((Number) row[3]).intValue());
            dto.setGenreName((String) row[4]);
            dto.setSubgenreName((String) row[5]);
            dto.setLanguageName((String) row[6]);
            dto.setReleaseYear((String) row[7]);
            dto.setSongCount(row[8] != null ? ((Number) row[8]).intValue() : 0);
            dto.setHasImage(row[9] != null && ((Number) row[9]).intValue() == 1);
            dto.setGenderName((String) row[10]);
            dto.setPlayCount(row[11] != null ? ((Number) row[11]).intValue() : 0);
            
            // Set time listened and format it
            long timeListened = row[12] != null ? ((Number) row[12]).longValue() : 0L;
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(formatTime(timeListened));
            
            albums.add(dto);
        }
        
        return albums;
    }
    
    public long countAlbums(String name, String artistName,
                           List<Integer> genreIds, String genreMode,
                           List<Integer> subgenreIds, String subgenreMode,
                           List<Integer> languageIds, String languageMode,
                           List<Integer> genderIds, String genderMode,
                           List<Integer> ethnicityIds, String ethnicityMode,
                           List<String> countries, String countryMode) {
        return albumRepository.countAlbumsWithFilters(name, artistName, 
                genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                genderIds, genderMode, ethnicityIds, ethnicityMode, countries, countryMode);
    }
    
    public Optional<Album> getAlbumById(Integer id) {
        String sql = """
            SELECT a.id, a.artist_id, a.name, a.release_date, a.number_of_songs, 
                   a.override_genre_id, a.override_subgenre_id, a.override_language_id,
                   a.creation_date, a.update_date,
                   ar.genre_id as artist_genre_id,
                   ar.subgenre_id as artist_subgenre_id,
                   ar.language_id as artist_language_id
            FROM Album a
            INNER JOIN Artist ar ON a.artist_id = ar.id
            WHERE a.id = ?
            """;
        
        List<Album> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Album album = new Album();
            album.setId(rs.getInt("id"));
            album.setArtistId(rs.getInt("artist_id"));
            album.setName(rs.getString("name"));
            // Read as string to avoid strict TIMESTAMP parsing in driver
            album.setReleaseDate(parseDate(rs.getString("release_date")));
            album.setNumberOfSongs(rs.getInt("number_of_songs"));
            
            int genreId = rs.getInt("override_genre_id");
            album.setOverrideGenreId(rs.wasNull() ? null : genreId);
            
            int subgenreId = rs.getInt("override_subgenre_id");
            album.setOverrideSubgenreId(rs.wasNull() ? null : subgenreId);
            
            int languageId = rs.getInt("override_language_id");
            album.setOverrideLanguageId(rs.wasNull() ? null : languageId);
            
            // Set inherited values from Artist
            int artistGenreId = rs.getInt("artist_genre_id");
            album.setArtistGenreId(rs.wasNull() ? null : artistGenreId);
            
            int artistSubgenreId = rs.getInt("artist_subgenre_id");
            album.setArtistSubgenreId(rs.wasNull() ? null : artistSubgenreId);
            
            int artistLanguageId = rs.getInt("artist_language_id");
            album.setArtistLanguageId(rs.wasNull() ? null : artistLanguageId);
            
            // Robust timestamp parsing: handle date-only values gracefully
            String creation = rs.getString("creation_date");
            String update = rs.getString("update_date");
            album.setCreationDate(parseTimestamp(creation));
            album.setUpdateDate(parseTimestamp(update));
            
            return album;
        }, id);
        
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
    
    public Album saveAlbum(Album album) {
        String sql = """
            UPDATE Album 
            SET name = ?, artist_id = ?, release_date = ?, 
                override_genre_id = ?, override_subgenre_id = ?, override_language_id = ?,
                update_date = CURRENT_TIMESTAMP
            WHERE id = ?
            """;
        
        // Convert java.sql.Date to yyyy-MM-dd string format for database
        String releaseDateStr = null;
        if (album.getReleaseDate() != null) {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            releaseDateStr = sdf.format(album.getReleaseDate());
        }
        
        jdbcTemplate.update(sql, 
            album.getName(),
            album.getArtistId(),
            releaseDateStr,
            album.getOverrideGenreId(),
            album.getOverrideSubgenreId(),
            album.getOverrideLanguageId(),
            album.getId()
        );
        
        return album;
    }
    
    public void updateAlbumImage(Integer id, byte[] imageData) {
        String sql = "UPDATE Album SET image = ? WHERE id = ?";
        jdbcTemplate.update(sql, imageData, id);
    }
    
    public byte[] getAlbumImage(Integer id) {
        String sql = "SELECT image FROM Album WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("image"), id);
        } catch (Exception e) {
            return null;
        }
    }
    
    public int getSongCount(int albumId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Song WHERE album_id = ?", Integer.class, albumId);
        return count != null ? count : 0;
    }
    
    public String getArtistName(int artistId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT name FROM Artist WHERE id = ?", String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    public Map<Integer, String> getGenres() {
        return lookupRepository.getAllGenres();
    }
    
    public Map<Integer, String> getSubGenres() {
        return lookupRepository.getAllSubGenres();
    }
    
    public Map<Integer, String> getLanguages() {
        return lookupRepository.getAllLanguages();
    }
    
    public Map<Integer, String> getGenders() {
        return lookupRepository.getAllGenders();
    }
    
    public Map<Integer, String> getEthnicities() {
        return lookupRepository.getAllEthnicities();
    }
    
    public List<String> getCountries() {
        String sql = "SELECT DISTINCT country FROM Artist WHERE country IS NOT NULL ORDER BY country";
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    public String getArtistGender(Integer artistId) {
        String sql = "SELECT g.name FROM Artist a LEFT JOIN Gender g ON a.gender_id = g.id WHERE a.id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    // NEW: total plays for an album (count scrobbles for songs in this album)
    public int getPlayCountForAlbum(int albumId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(scr.id) FROM Scrobble scr JOIN Song s ON scr.song_id = s.id WHERE s.album_id = ?",
                Integer.class, albumId);
        return count != null ? count : 0;
    }

    // Return a string with per-account play counts for this album (e.g. "lastfm: 12\nspotify: 3\n")
    public String getPlaysByAccountForAlbum(int albumId) {
        String sql = "SELECT scr.account, COUNT(*) as cnt FROM Scrobble scr JOIN Song s ON scr.song_id = s.id WHERE s.album_id = ? GROUP BY scr.account ORDER BY cnt DESC";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, albumId);
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> row : rows) {
            Object account = row.get("account");
            Object cnt = row.get("cnt");
            sb.append(account != null ? account.toString() : "unknown");
            sb.append(": ");
            sb.append(cnt != null ? cnt.toString() : "0");
            sb.append("\n");
        }
        return sb.toString();
    }
    
    // Get songs for an album with play counts per account
    public List<AlbumSongDTO> getSongsForAlbum(int albumId) {
        String sql = """
            SELECT 
                s.id,
                s.name,
                s.length_seconds,
                COALESCE(SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END), 0) as vatito_plays,
                COALESCE(SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END), 0) as robertlover_plays,
                COUNT(scr.id) as total_plays,
                MIN(scr.scrobble_date) as first_listen,
                MAX(scr.scrobble_date) as last_listen
            FROM Song s
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE s.album_id = ?
            GROUP BY s.id, s.name, s.length_seconds
            ORDER BY s.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            AlbumSongDTO dto = new AlbumSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            
            // No track number column in database
            dto.setTrackNumber(null);
            
            int length = rs.getInt("length_seconds");
            dto.setLength(rs.wasNull() ? null : length);
            
            dto.setVatitoPlays(rs.getInt("vatito_plays"));
            dto.setRobertloverPlays(rs.getInt("robertlover_plays"));
            dto.setTotalPlays(rs.getInt("total_plays"));
            
            // Format first and last listen dates
            String firstListen = rs.getString("first_listen");
            String lastListen = rs.getString("last_listen");
            dto.setFirstListenedDate(formatDate(firstListen));
            dto.setLastListenedDate(formatDate(lastListen));
            
            // Calculate total listening time
            dto.calculateTotalListeningTime();
            
            return dto;
        }, albumId);
    }
    
    // Calculate total listening time for an album (sum of all songs' listening time)
    public String getTotalListeningTimeForAlbum(int albumId) {
        String sql = """
            SELECT 
                SUM(s.length_seconds * COALESCE(play_count, 0)) as total_seconds
            FROM Song s
            LEFT JOIN (
                SELECT song_id, COUNT(*) as play_count
                FROM Scrobble
                GROUP BY song_id
            ) scr ON s.id = scr.song_id
            WHERE s.album_id = ?
            """;
        
        Long totalSeconds = jdbcTemplate.queryForObject(sql, Long.class, albumId);
        if (totalSeconds == null || totalSeconds == 0) {
            return "-";
        }
        
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        
        // Smart formatting
        if (days > 0) {
            return String.format("%dd:%02d:%02d:%02d", days, hours, minutes, seconds);
        } else if (hours > 0) {
            return String.format("%d:%02d:%02d", hours, minutes, seconds);
        } else {
            return String.format("%d:%02d", minutes, seconds);
        }
    }
    
    // Get first listened date for an album (earliest scrobble)
    public String getFirstListenedDateForAlbum(int albumId) {
        String sql = """
            SELECT MIN(scr.scrobble_date)
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.album_id = ?
            """;
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, albumId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }
    
    // Get last listened date for an album (most recent scrobble)
    public String getLastListenedDateForAlbum(int albumId) {
        String sql = """
            SELECT MAX(scr.scrobble_date)
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.album_id = ?
            """;
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, albumId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }
    
    // Delete album and all associated songs (only if play count is 0)
    public void deleteAlbum(Integer albumId) {
        // First check if album has any plays
        int playCount = getPlayCountForAlbum(albumId);
        if (playCount > 0) {
            throw new IllegalStateException("Cannot delete album with existing plays");
        }
        
        // Delete all songs for this album
        jdbcTemplate.update("DELETE FROM Song WHERE album_id = ?", albumId);
        
        // Delete the album
        jdbcTemplate.update("DELETE FROM Album WHERE id = ?", albumId);
    }
    
    // Create a new album
    public Album createAlbum(Album album) {
        String sql = """
            INSERT INTO Album (artist_id, name, release_date, override_genre_id, 
                              override_subgenre_id, override_language_id, number_of_songs,
                              creation_date, update_date)
            VALUES (?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """;
        
        // Convert java.sql.Date to yyyy-MM-dd string format for database
        String releaseDateStr = null;
        if (album.getReleaseDate() != null) {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            releaseDateStr = sdf.format(album.getReleaseDate());
        }
        
        jdbcTemplate.update(sql,
            album.getArtistId(),
            album.getName(),
            releaseDateStr,
            album.getOverrideGenreId(),
            album.getOverrideSubgenreId(),
            album.getOverrideLanguageId()
        );
        
        // Get the ID of the newly created album
        Integer id = jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Integer.class);
        album.setId(id);
        
        return album;
    }
    
    // Get albums by artist for API (id and name only)
    public List<Map<String, Object>> getAlbumsByArtistForApi(Integer artistId) {
        String sql = "SELECT id, name FROM Album WHERE artist_id = ? ORDER BY name";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> album = new java.util.HashMap<>();
            album.put("id", rs.getInt("id"));
            album.put("name", rs.getString("name"));
            return album;
        }, artistId);
    }
    
    // Helper method to format date strings (from scrobble_date)
    private String formatDate(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
            return "-";
        }
        
        try {
            // Parse various formats (handles "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD")
            String datePart = dateTimeString.trim();
            if (datePart.contains(" ")) {
                datePart = datePart.split(" ")[0];
            }
            
            // Parse YYYY-MM-DD
            String[] parts = datePart.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                
                // Month names (3-character)
                String[] monthNames = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
                
                // Format as DD Mon YYYY (e.g., "21 Nov 2025")
                return day + " " + monthNames[month - 1] + " " + year;
            }
            
            return datePart;
        } catch (Exception e) {
            return dateTimeString;
        }
    }

    // Helper to parse various SQLite timestamp representations
    private static java.sql.Timestamp parseTimestamp(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        try {
            // Normalize common variants
            if (v.length() == 10 && v.matches("\\d{4}-\\d{2}-\\d{2}")) {
                v = v + " 00:00:00";
            } else if (v.contains("T") && v.matches("\\d{4}-\\d{2}-\\d{2}T.*")) {
                v = v.replace('T', ' ');
            }
            return java.sql.Timestamp.valueOf(v);
        } catch (Exception e) {
            // As a last resort, try parsing only the date part
            try {
                if (v.length() >= 10) {
                    String datePart = v.substring(0, 10);
                    if (datePart.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        return java.sql.Timestamp.valueOf(datePart + " 00:00:00");
                    }
                }
            } catch (Exception ignore) {}
            return null;
        }
    }
    
    // Helper to parse date values from database (yyyy-MM-dd format)
    private static java.sql.Date parseDate(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        
        // Replace ISO T if present and use date part
        if (v.contains("T")) v = v.replace('T', ' ');
        if (v.length() >= 10) v = v.substring(0, 10);
        if (v.matches("\\d{4}-\\d{2}-\\d{2}")) {
            try {
                return java.sql.Date.valueOf(v);
            } catch (Exception ignore) {}
        }
        return null;
    }
    
    // Helper method to format time in seconds to human-readable format
    private String formatTime(long totalSeconds) {
        if (totalSeconds == 0) {
            return "0m";
        }
        
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        
        // Smart formatting for card display
        if (days > 0) {
            return String.format("%dd %dh", days, hours);
        } else if (hours > 0) {
            return String.format("%dh %dm", hours, minutes);
        } else {
            return String.format("%dm", minutes);
        }
    }
}
