package library.service;

import library.dto.ArtistAlbumDTO;
import library.dto.ArtistCardDTO;
import library.dto.ArtistSongDTO;
import library.entity.Artist;
import library.repository.ArtistRepositoryNew;
import library.repository.LookupRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

@Service
public class ArtistService {
    
    private final ArtistRepositoryNew artistRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    
    public ArtistService(ArtistRepositoryNew artistRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate) {
        this.artistRepository = artistRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<ArtistCardDTO> getArtists(String name, List<Integer> genderIds, String genderMode,
                                          List<Integer> ethnicityIds, String ethnicityMode,
                                          List<Integer> genreIds, String genreMode,
                                          List<Integer> subgenreIds, String subgenreMode,
                                          List<Integer> languageIds, String languageMode,
                                          List<String> countries, String countryMode,
                                          String sortBy, int page, int perPage) {
        int offset = page * perPage;

        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (genderIds != null && genderIds.isEmpty()) genderIds = null;
        if (ethnicityIds != null && ethnicityIds.isEmpty()) ethnicityIds = null;
        if (genreIds != null && genreIds.isEmpty()) genreIds = null;
        if (subgenreIds != null && subgenreIds.isEmpty()) subgenreIds = null;
        if (languageIds != null && languageIds.isEmpty()) languageIds = null;
        if (countries != null && countries.isEmpty()) countries = null;
        
        List<Object[]> results = artistRepository.findArtistsWithStats(
                name, genderIds, genderMode, ethnicityIds, ethnicityMode, 
                genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                countries, countryMode, sortBy, perPage, offset
        );
        
        List<ArtistCardDTO> artists = new ArrayList<>();
        for (Object[] row : results) {
            ArtistCardDTO dto = new ArtistCardDTO();
            dto.setId(((Number) row[0]).intValue());
            dto.setName((String) row[1]);
            dto.setGenderName((String) row[2]);
            dto.setEthnicityName((String) row[3]);
            dto.setGenreName((String) row[4]);
            dto.setSubgenreName((String) row[5]);
            dto.setLanguageName((String) row[6]);
            dto.setCountry((String) row[7]);
            dto.setSongCount(row[8] != null ? ((Number) row[8]).intValue() : 0);
            dto.setAlbumCount(row[9] != null ? ((Number) row[9]).intValue() : 0);
            dto.setHasImage(row[10] != null && ((Number) row[10]).intValue() == 1);
            dto.setPlayCount(row[11] != null ? ((Number) row[11]).intValue() : 0);
            
            // Set time listened and format it
            long timeListened = row[12] != null ? ((Number) row[12]).longValue() : 0L;
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(formatTime(timeListened));
            
            artists.add(dto);
        }
        
        return artists;
    }
    
    public long countArtists(String name, List<Integer> genderIds, String genderMode,
                            List<Integer> ethnicityIds, String ethnicityMode,
                            List<Integer> genreIds, String genreMode,
                            List<Integer> subgenreIds, String subgenreMode,
                            List<Integer> languageIds, String languageMode,
                            List<String> countries, String countryMode) {
        // Normalize empty lists to null here as well
        if (genderIds != null && genderIds.isEmpty()) genderIds = null;
        if (ethnicityIds != null && ethnicityIds.isEmpty()) ethnicityIds = null;
        if (genreIds != null && genreIds.isEmpty()) genreIds = null;
        if (subgenreIds != null && subgenreIds.isEmpty()) subgenreIds = null;
        if (languageIds != null && languageIds.isEmpty()) languageIds = null;
        if (countries != null && countries.isEmpty()) countries = null;
        
        return artistRepository.countArtistsWithFilters(name, genderIds, genderMode, 
                ethnicityIds, ethnicityMode, genreIds, genreMode, subgenreIds, subgenreMode,
                languageIds, languageMode, countries, countryMode);
    }
    
    public Optional<Artist> getArtistById(Integer id) {
        String sql = """
            SELECT id, name, gender_id, country, ethnicity_id, 
                   genre_id, subgenre_id, language_id, 
                   creation_date, update_date
            FROM Artist
            WHERE id = ?
            """;
        
        List<Artist> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Artist artist = new Artist();
            artist.setId(rs.getInt("id"));
            artist.setName(rs.getString("name"));
            
            // Handle nullable Integer fields
            int genderId = rs.getInt("gender_id");
            artist.setGenderId(rs.wasNull() ? null : genderId);
            
            artist.setCountry(rs.getString("country"));
            
            int ethnicityId = rs.getInt("ethnicity_id");
            artist.setEthnicityId(rs.wasNull() ? null : ethnicityId);
            
            int genreId = rs.getInt("genre_id");
            artist.setGenreId(rs.wasNull() ? null : genreId);
            
            int subgenreId = rs.getInt("subgenre_id");
            artist.setSubgenreId(rs.wasNull() ? null : subgenreId);
            
            int languageId = rs.getInt("language_id");
            artist.setLanguageId(rs.wasNull() ? null : languageId);
            
            artist.setCreationDate(rs.getTimestamp("creation_date"));
            artist.setUpdateDate(rs.getTimestamp("update_date"));
            
            return artist;
        }, id);
        
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
    
    public Artist saveArtist(Artist artist) {
        String sql = """
            UPDATE Artist 
            SET name = ?, gender_id = ?, country = ?, ethnicity_id = ?, 
                genre_id = ?, subgenre_id = ?, language_id = ?, 
                update_date = CURRENT_TIMESTAMP
            WHERE id = ?
            """;
        
        jdbcTemplate.update(sql, 
            artist.getName(),
            artist.getGenderId(),
            artist.getCountry(),
            artist.getEthnicityId(),
            artist.getGenreId(),
            artist.getSubgenreId(),
            artist.getLanguageId(),
            artist.getId()
        );
        
        return artist;
    }
    
    public void updateArtistImage(Integer id, byte[] imageData) {
        String sql = "UPDATE Artist SET image = ? WHERE id = ?";
        jdbcTemplate.update(sql, imageData, id);
    }
    
    public byte[] getArtistImage(Integer id) {
        String sql = "SELECT image FROM Artist WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("image"), id);
        } catch (Exception e) {
            return null;
        }
    }
    
    public Map<Integer, String> getGenders() {
        return lookupRepository.getAllGenders();
    }
    
    public Map<Integer, String> getEthnicities() {
        return lookupRepository.getAllEthnicities();
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
    
    public List<String> getCountries() {
        String[] iso = Locale.getISOCountries();
        Set<String> names = new TreeSet<>();
        for (String code : iso) {
            Locale loc = new Locale("", code);
            String name = loc.getDisplayCountry(Locale.ENGLISH);
            if (name != null && !name.isBlank()) {
                names.add(name);
            }
        }
        return new ArrayList<>(names);
    }
    
    public int[] getAlbumAndSongCounts(int artistId) {
        Integer songCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Song WHERE artist_id = ?", Integer.class, artistId);
        if (songCount == null) songCount = 0;
        Integer albumCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT album_id) FROM Song WHERE artist_id = ? AND album_id IS NOT NULL", Integer.class, artistId);
        if (albumCount == null) albumCount = 0;
        return new int[]{albumCount, songCount};
    }
    
    public int getPlayCountForArtist(int artistId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(scr.id) FROM Scrobble scr JOIN Song s ON scr.song_id = s.id WHERE s.artist_id = ?",
                Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    // Return a string with per-account play counts for this artist (e.g. "lastfm: 12\nspotify: 3\n")
    public String getPlaysByAccountForArtist(int artistId) {
        String sql = "SELECT scr.account, COUNT(*) as cnt FROM Scrobble scr JOIN Song s ON scr.song_id = s.id WHERE s.artist_id = ? GROUP BY scr.account ORDER BY cnt DESC";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, artistId);
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
    
    // Get total listening time for an artist (sum of all songs' listening time)
    public String getTotalListeningTimeForArtist(int artistId) {
        String sql = """
            SELECT 
                SUM(s.length_seconds * COALESCE(play_count, 0)) as total_seconds
            FROM Song s
            LEFT JOIN (
                SELECT song_id, COUNT(*) as play_count
                FROM Scrobble
                GROUP BY song_id
            ) scr ON s.id = scr.song_id
            WHERE s.artist_id = ?
            """;
        
        Long totalSeconds = jdbcTemplate.queryForObject(sql, Long.class, artistId);
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
    
    // Get first listened date for an artist (earliest scrobble)
    public String getFirstListenedDateForArtist(int artistId) {
        String sql = """
            SELECT MIN(scr.scrobble_date)
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ?
            """;
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, artistId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }
    
    // Get last listened date for an artist (most recent scrobble)
    public String getLastListenedDateForArtist(int artistId) {
        String sql = """
            SELECT MAX(scr.scrobble_date)
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ?
            """;
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, artistId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }
    
    // Get all songs for an artist with play counts
    public List<ArtistSongDTO> getSongsForArtist(int artistId) {
        String sql = """
            SELECT 
                s.id,
                s.name,
                s.length_seconds,
                a.name as album_name,
                a.id as album_id,
                COALESCE(SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END), 0) as vatito_plays,
                COALESCE(SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END), 0) as robertlover_plays,
                COUNT(scr.id) as total_plays,
                MIN(scr.scrobble_date) as first_listen,
                MAX(scr.scrobble_date) as last_listen
            FROM Song s
            LEFT JOIN Album a ON s.album_id = a.id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE s.artist_id = ?
            GROUP BY s.id, s.name, s.length_seconds, a.name, a.id
            ORDER BY s.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistSongDTO dto = new ArtistSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            
            // Set album info
            int albumId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : albumId);
            dto.setAlbumName(rs.getString("album_name"));
            
            // Set length (this will auto-format via the setter)
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
        }, artistId);
    }
    
    // Get all albums for an artist with play counts
    public List<ArtistAlbumDTO> getAlbumsForArtist(int artistId) {
        String sql = """
            SELECT 
                a.id,
                a.name,
                (SELECT COUNT(*) FROM Song WHERE album_id = a.id) as song_count,
                (SELECT SUM(length_seconds) FROM Song WHERE album_id = a.id) as total_length_seconds,
                COALESCE(SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END), 0) as vatito_plays,
                COALESCE(SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END), 0) as robertlover_plays,
                COUNT(scr.id) as total_plays,
                MIN(scr.scrobble_date) as first_listen,
                MAX(scr.scrobble_date) as last_listen,
                (SELECT SUM(s.length_seconds * 
                    (SELECT COUNT(*) FROM Scrobble WHERE song_id = s.id)) 
                 FROM Song s WHERE s.album_id = a.id) as total_listening_seconds
            FROM Album a
            LEFT JOIN Song s ON a.id = s.album_id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            WHERE a.artist_id = ?
            GROUP BY a.id, a.name
            ORDER BY a.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistAlbumDTO dto = new ArtistAlbumDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setSongCount(rs.getInt("song_count"));
            
            // Format total length
            int totalSeconds = rs.getInt("total_length_seconds");
            if (!rs.wasNull() && totalSeconds > 0) {
                int minutes = totalSeconds / 60;
                int seconds = totalSeconds % 60;
                dto.setTotalLength(String.format("%d:%02d", minutes, seconds));
            } else {
                dto.setTotalLength("-");
            }
            
            dto.setVatitoPlays(rs.getInt("vatito_plays"));
            dto.setRobertloverPlays(rs.getInt("robertlover_plays"));
            dto.setTotalPlays(rs.getInt("total_plays"));
            
            // Format first and last listen dates
            String firstListen = rs.getString("first_listen");
            String lastListen = rs.getString("last_listen");
            dto.setFirstListenedDate(formatDate(firstListen));
            dto.setLastListenedDate(formatDate(lastListen));
            
            // Calculate and format total listening time
            int listeningSeconds = rs.getInt("total_listening_seconds");
            if (!rs.wasNull() && listeningSeconds > 0) {
                long days = listeningSeconds / 86400;
                long hours = (listeningSeconds % 86400) / 3600;
                long mins = (listeningSeconds % 3600) / 60;
                long secs = listeningSeconds % 60;
                
                if (days > 0) {
                    dto.setTotalListeningTime(String.format("%dd:%02d:%02d:%02d", days, hours, mins, secs));
                } else if (hours > 0) {
                    dto.setTotalListeningTime(String.format("%d:%02d:%02d", hours, mins, secs));
                } else {
                    dto.setTotalListeningTime(String.format("%d:%02d", mins, secs));
                }
            } else {
                dto.setTotalListeningTime("-");
            }
            
            return dto;
        }, artistId);
    }
    
    // Delete artist and all associated albums and songs (only if play count is 0)
    public void deleteArtist(Integer artistId) {
        // First check if artist has any plays
        int playCount = getPlayCountForArtist(artistId);
        if (playCount > 0) {
            throw new IllegalStateException("Cannot delete artist with existing plays");
        }
        
        // Delete all songs for this artist
        jdbcTemplate.update("DELETE FROM Song WHERE artist_id = ?", artistId);
        
        // Delete all albums for this artist
        jdbcTemplate.update("DELETE FROM Album WHERE artist_id = ?", artistId);
        
        // Delete the artist
        jdbcTemplate.update("DELETE FROM Artist WHERE id = ?", artistId);
    }
    
    // Create a new artist
    public Artist createArtist(Artist artist) {
        String sql = """
            INSERT INTO Artist (name, gender_id, country, ethnicity_id, 
                               genre_id, subgenre_id, language_id, creation_date, update_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """;
        
        jdbcTemplate.update(sql,
            artist.getName(),
            artist.getGenderId(),
            artist.getCountry(),
            artist.getEthnicityId(),
            artist.getGenreId(),
            artist.getSubgenreId(),
            artist.getLanguageId()
        );
        
        // Get the ID of the newly created artist
        Integer id = jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Integer.class);
        artist.setId(id);
        
        return artist;
    }
    
    // Get all artists for API (id and name only)
    public List<Map<String, Object>> getAllArtistsForApi() {
        String sql = "SELECT id, name FROM Artist ORDER BY name";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> artist = new java.util.HashMap<>();
            artist.put("id", rs.getInt("id"));
            artist.put("name", rs.getString("name"));
            return artist;
        });
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
