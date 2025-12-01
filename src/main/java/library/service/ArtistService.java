package library.service;

import library.dto.ArtistAlbumDTO;
import library.dto.ArtistCardDTO;
import library.dto.ArtistSongDTO;
import library.dto.FeaturedArtistCardDTO;
import library.dto.PlaysByYearDTO;
import library.dto.ScrobbleDTO;
import library.entity.Artist;
import library.repository.ArtistRepositoryNew;
import library.repository.LookupRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

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
                                          String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                          String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                          String organized,
                                          String sortBy, String sortDir, int page, int perPage) {
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
                countries, countryMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                organized,
                sortBy, sortDir, perPage, offset
        );
        
        List<ArtistCardDTO> artists = new ArrayList<>();
        for (Object[] row : results) {
            ArtistCardDTO dto = new ArtistCardDTO();
            dto.setId(((Number) row[0]).intValue());
            dto.setName((String) row[1]);
            dto.setGenderId(row[2] != null ? ((Number) row[2]).intValue() : null);
            dto.setGenderName((String) row[3]);
            dto.setEthnicityId(row[4] != null ? ((Number) row[4]).intValue() : null);
            dto.setEthnicityName((String) row[5]);
            dto.setGenreId(row[6] != null ? ((Number) row[6]).intValue() : null);
            dto.setGenreName((String) row[7]);
            dto.setSubgenreId(row[8] != null ? ((Number) row[8]).intValue() : null);
            dto.setSubgenreName((String) row[9]);
            dto.setLanguageId(row[10] != null ? ((Number) row[10]).intValue() : null);
            dto.setLanguageName((String) row[11]);
            dto.setCountry((String) row[12]);
            dto.setSongCount(row[13] != null ? ((Number) row[13]).intValue() : 0);
            dto.setAlbumCount(row[14] != null ? ((Number) row[14]).intValue() : 0);
            dto.setHasImage(row[15] != null && ((Number) row[15]).intValue() == 1);
            dto.setPlayCount(row[16] != null ? ((Number) row[16]).intValue() : 0);
            dto.setVatitoPlayCount(row[17] != null ? ((Number) row[17]).intValue() : 0);
            dto.setRobertloverPlayCount(row[18] != null ? ((Number) row[18]).intValue() : 0);
            
            // Set time listened and format it
            long timeListened = row[19] != null ? ((Number) row[19]).longValue() : 0L;
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(formatTime(timeListened));
            
            // Set first and last listened dates (indices 20 and 21)
            dto.setFirstListenedDate(row[20] != null ? formatDate((String) row[20]) : null);
            dto.setLastListenedDate(row[21] != null ? formatDate((String) row[21]) : null);
            
            // Set organized (index 22)
            dto.setOrganized(row[22] != null && ((Number) row[22]).intValue() == 1);
            
            artists.add(dto);
        }
        
        return artists;
    }
    
    public long countArtists(String name, List<Integer> genderIds, String genderMode,
                            List<Integer> ethnicityIds, String ethnicityMode,
                            List<Integer> genreIds, String genreMode,
                            List<Integer> subgenreIds, String subgenreMode,
                            List<Integer> languageIds, String languageMode,
                            List<String> countries, String countryMode,
                            String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                            String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                            String organized) {
        // Normalize empty lists to null here as well
        if (genderIds != null && genderIds.isEmpty()) genderIds = null;
        if (ethnicityIds != null && ethnicityIds.isEmpty()) ethnicityIds = null;
        if (genreIds != null && genreIds.isEmpty()) genreIds = null;
        if (subgenreIds != null && subgenreIds.isEmpty()) subgenreIds = null;
        if (languageIds != null && languageIds.isEmpty()) languageIds = null;
        if (countries != null && countries.isEmpty()) countries = null;
        
        return artistRepository.countArtistsWithFilters(name, genderIds, genderMode, 
                ethnicityIds, ethnicityMode, genreIds, genreMode, subgenreIds, subgenreMode,
                languageIds, languageMode, countries, countryMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                organized);
    }
    
    public Optional<Artist> getArtistById(Integer id) {
        String sql = """
            SELECT id, name, gender_id, country, ethnicity_id, 
                   genre_id, subgenre_id, language_id, is_band, organized,
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
            
            int isBand = rs.getInt("is_band");
            artist.setIsBand(rs.wasNull() ? null : isBand == 1);
            
            int organized = rs.getInt("organized");
            artist.setOrganized(rs.wasNull() ? null : organized == 1);
            
            artist.setCreationDate(rs.getTimestamp("creation_date"));
            artist.setUpdateDate(rs.getTimestamp("update_date"));
            
            return artist;
        }, id);
        
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
    
    public Artist saveArtist(Artist artist) {
        // Get the old artist name to check if it changed
        String oldName = jdbcTemplate.queryForObject(
            "SELECT name FROM Artist WHERE id = ?", String.class, artist.getId());
        
        String sql = """
            UPDATE Artist 
            SET name = ?, gender_id = ?, country = ?, ethnicity_id = ?, 
                genre_id = ?, subgenre_id = ?, language_id = ?, is_band = ?, organized = ?,
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
            artist.getIsBand() != null && artist.getIsBand() ? 1 : 0,
            artist.getOrganized() != null && artist.getOrganized() ? 1 : 0,
            artist.getId()
        );
        
        // If artist name changed, update all associated scrobbles
        if (oldName != null && !oldName.equals(artist.getName())) {
            updateScrobblesForArtistNameChange(artist.getId(), artist.getName());
        }
        
        // Try to match unmatched scrobbles with the new artist name
        // This catches scrobbles that might now match after the artist was renamed
        tryMatchUnmatchedScrobblesForArtist(artist.getId(), artist.getName());
        
        return artist;
    }
    
    /**
     * Update the artist name in all scrobbles for songs belonging to this artist
     */
    private void updateScrobblesForArtistNameChange(int artistId, String newArtistName) {
        String sql = """
            UPDATE Scrobble 
            SET artist = ?
            WHERE song_id IN (SELECT id FROM Song WHERE artist_id = ?)
            """;
        jdbcTemplate.update(sql, newArtistName, artistId);
    }
    
    /**
     * Try to match unmatched scrobbles to songs by this artist.
     * For each song by this artist, look for unmatched scrobbles that match
     * the artist name, album name, and song name.
     */
    private void tryMatchUnmatchedScrobblesForArtist(int artistId, String artistName) {
        // Match scrobbles to songs by this artist where artist, album, and song name all match
        String sql = """
            UPDATE Scrobble 
            SET song_id = (
                SELECT s.id FROM Song s 
                LEFT JOIN Album al ON s.album_id = al.id
                WHERE s.artist_id = ?
                AND LOWER(s.name) = LOWER(Scrobble.song)
                AND LOWER(COALESCE(al.name, '')) = LOWER(COALESCE(Scrobble.album, ''))
                LIMIT 1
            ),
            artist = ?
            WHERE song_id IS NULL
            AND LOWER(COALESCE(artist, '')) = LOWER(?)
            AND EXISTS (
                SELECT 1 FROM Song s 
                LEFT JOIN Album al ON s.album_id = al.id
                WHERE s.artist_id = ?
                AND LOWER(s.name) = LOWER(Scrobble.song)
                AND LOWER(COALESCE(al.name, '')) = LOWER(COALESCE(Scrobble.album, ''))
            )
            """;
        
        jdbcTemplate.update(sql, artistId, artistName, artistName, artistId);
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
    
    // Get vatito (primary) play count for artist
    public int getVatitoPlayCountForArtist(int artistId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(scr.id) FROM Scrobble scr JOIN Song s ON scr.song_id = s.id WHERE s.artist_id = ? AND scr.account = 'vatito'",
                Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    // Get robertlover (legacy) play count for artist
    public int getRobertloverPlayCountForArtist(int artistId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(scr.id) FROM Scrobble scr JOIN Song s ON scr.song_id = s.id WHERE s.artist_id = ? AND scr.account = 'robertlover'",
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
                s.single_cover IS NOT NULL as has_image,
                COALESCE(s.release_date, a.release_date) as release_date,
                a.name as album_name,
                a.id as album_id,
                ar.country,
                COALESCE(g_song.name, g_album.name, g_artist.name) as genre,
                COALESCE(sg_song.name, sg_album.name, sg_artist.name) as subgenre,
                COALESCE(eth_song.name, eth_artist.name) as ethnicity,
                COALESCE(l_song.name, l_album.name, l_artist.name) as language,
                COALESCE(SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END), 0) as vatito_plays,
                COALESCE(SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END), 0) as robertlover_plays,
                COUNT(scr.id) as total_plays,
                MIN(scr.scrobble_date) as first_listen,
                MAX(scr.scrobble_date) as last_listen
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            LEFT JOIN Genre g_song ON s.override_genre_id = g_song.id
            LEFT JOIN Genre g_album ON a.override_genre_id = g_album.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_song ON s.override_subgenre_id = sg_song.id
            LEFT JOIN SubGenre sg_album ON a.override_subgenre_id = sg_album.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_song ON s.override_language_id = l_song.id
            LEFT JOIN Language l_album ON a.override_language_id = l_album.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth_song ON s.override_ethnicity_id = eth_song.id
            LEFT JOIN Ethnicity eth_artist ON ar.ethnicity_id = eth_artist.id
            WHERE s.artist_id = ?
            GROUP BY s.id, s.name, s.length_seconds, s.single_cover, COALESCE(s.release_date, a.release_date), a.name, a.id, ar.country, 
                     COALESCE(g_song.name, g_album.name, g_artist.name), 
                     COALESCE(sg_song.name, sg_album.name, sg_artist.name), 
                     COALESCE(eth_song.name, eth_artist.name), 
                     COALESCE(l_song.name, l_album.name, l_artist.name)
            ORDER BY s.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistSongDTO dto = new ArtistSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setHasImage(rs.getBoolean("has_image"));
            dto.setReleaseDate(formatDate(rs.getString("release_date")));
            dto.setCountry(rs.getString("country"));
            dto.setGenre(rs.getString("genre"));
            dto.setSubgenre(rs.getString("subgenre"));
            dto.setEthnicity(rs.getString("ethnicity"));
            dto.setLanguage(rs.getString("language"));
            
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
            if (dto.getLength() != null && dto.getTotalPlays() != null) {
                dto.setTotalListeningTimeSeconds(dto.getLength() * dto.getTotalPlays());
            } else {
                dto.setTotalListeningTimeSeconds(0);
            }
            
            return dto;
        }, artistId);
    }
    
    // Get all albums for an artist with play counts
    public List<ArtistAlbumDTO> getAlbumsForArtist(int artistId) {
        String sql = """
            SELECT 
                a.id,
                a.name,
                a.release_date,
                ar.country,
                COALESCE(g_override.name, g_artist.name) as genre,
                COALESCE(sg_override.name, sg_artist.name) as subgenre,
                eth.name as ethnicity,
                COALESCE(l_override.name, l_artist.name) as language,
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
            INNER JOIN Artist ar ON a.artist_id = ar.id
            LEFT JOIN Song s ON a.id = s.album_id
            LEFT JOIN Scrobble scr ON s.id = scr.song_id
            LEFT JOIN Genre g_override ON a.override_genre_id = g_override.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON a.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON a.override_language_id = l_override.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            WHERE a.artist_id = ?
            GROUP BY a.id, a.name, a.release_date, ar.country, genre, subgenre, eth.name, language
            ORDER BY a.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistAlbumDTO dto = new ArtistAlbumDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setReleaseDate(formatDate(rs.getString("release_date")));
            dto.setCountry(rs.getString("country"));
            dto.setGenre(rs.getString("genre"));
            dto.setSubgenre(rs.getString("subgenre"));
            dto.setEthnicity(rs.getString("ethnicity"));
            dto.setLanguage(rs.getString("language"));
            dto.setSongCount(rs.getInt("song_count"));
            
            // Format total length
            int totalSeconds = rs.getInt("total_length_seconds");
            if (!rs.wasNull() && totalSeconds > 0) {
                int minutes = totalSeconds / 60;
                int seconds = totalSeconds % 60;
                dto.setTotalLength(String.format("%d:%02d", minutes, seconds));
                dto.setTotalLengthSeconds(totalSeconds);
            } else {
                dto.setTotalLength("-");
                dto.setTotalLengthSeconds(0);
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
                dto.setTotalListeningTimeSeconds(listeningSeconds);
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
                dto.setTotalListeningTimeSeconds(0);
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
                               genre_id, subgenre_id, language_id, is_band, creation_date, update_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """;
        
        jdbcTemplate.update(sql,
            artist.getName(),
            artist.getGenderId(),
            artist.getCountry(),
            artist.getEthnicityId(),
            artist.getGenreId(),
            artist.getSubgenreId(),
            artist.getLanguageId(),
            artist.getIsBand() != null && artist.getIsBand() ? 1 : 0
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
    
    // Search artists by name for API
    public List<Map<String, Object>> searchArtists(String query, int limit) {
        String sql = "SELECT id, name FROM Artist WHERE LOWER(name) LIKE ? ORDER BY name LIMIT ?";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> artist = new java.util.HashMap<>();
            artist.put("id", rs.getInt("id"));
            artist.put("name", rs.getString("name"));
            return artist;
        }, "%" + query.toLowerCase() + "%", limit);
    }
    
    // Find artist by ID
    public Artist findById(Integer id) {
        if (id == null) return null;
        String sql = "SELECT id, name, gender_id, country, ethnicity_id, genre_id, subgenre_id, language_id, is_band FROM Artist WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Artist a = new Artist();
                a.setId(rs.getInt("id"));
                a.setName(rs.getString("name"));
                a.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
                a.setCountry(rs.getString("country"));
                a.setEthnicityId(rs.getObject("ethnicity_id") != null ? rs.getInt("ethnicity_id") : null);
                a.setGenreId(rs.getObject("genre_id") != null ? rs.getInt("genre_id") : null);
                a.setSubgenreId(rs.getObject("subgenre_id") != null ? rs.getInt("subgenre_id") : null);
                a.setLanguageId(rs.getObject("language_id") != null ? rs.getInt("language_id") : null);
                a.setIsBand(rs.getInt("is_band") == 1);
                return a;
            }, id);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }
    
    // Create artist from map (for API)
    public Integer createArtist(java.util.Map<String, Object> data) {
        Artist artist = new Artist();
        artist.setName((String) data.get("name"));
        if (data.get("genderId") != null) {
            artist.setGenderId(((Number) data.get("genderId")).intValue());
        }
        if (data.get("country") != null) {
            artist.setCountry((String) data.get("country"));
        }
        if (data.get("ethnicityId") != null) {
            artist.setEthnicityId(((Number) data.get("ethnicityId")).intValue());
        }
        if (data.get("genreId") != null) {
            artist.setGenreId(((Number) data.get("genreId")).intValue());
        }
        if (data.get("languageId") != null) {
            artist.setLanguageId(((Number) data.get("languageId")).intValue());
        }
        Artist created = createArtist(artist);
        return created.getId();
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
    
    // Get scrobbles for an artist with pagination
    public List<ScrobbleDTO> getScrobblesForArtist(int artistId, int page, int pageSize) {
        int offset = page * pageSize;
        String sql = """
            SELECT 
                scr.id,
                scr.scrobble_date,
                s.name as song_name,
                s.id as song_id,
                a.name as album_name,
                a.id as album_id,
                ar.name as artist_name,
                ar.id as artist_id,
                scr.account
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            WHERE ar.id = ?
            ORDER BY scr.scrobble_date DESC
            LIMIT ? OFFSET ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ScrobbleDTO dto = new ScrobbleDTO();
            dto.setId(rs.getInt("id"));
            dto.setScrobbleDate(rs.getString("scrobble_date"));
            dto.setSongName(rs.getString("song_name"));
            dto.setSongId(rs.getInt("song_id"));
            dto.setAlbumName(rs.getString("album_name"));
            int albumId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : albumId);
            dto.setArtistName(rs.getString("artist_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setAccount(rs.getString("account"));
            return dto;
        }, artistId, pageSize, offset);
    }
    
    // Count total scrobbles for an artist
    public long countScrobblesForArtist(int artistId) {
        String sql = """
            SELECT COUNT(*)
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ?
            """;
        Long count = jdbcTemplate.queryForObject(sql, Long.class, artistId);
        return count != null ? count : 0;
    }
    
    // Get plays by year for an artist
    public List<PlaysByYearDTO> getPlaysByYearForArtist(int artistId) {
        String sql = """
            SELECT 
                strftime('%Y', scr.scrobble_date) as year,
                COUNT(*) as play_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ? AND scr.scrobble_date IS NOT NULL
            GROUP BY strftime('%Y', scr.scrobble_date)
            ORDER BY year ASC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlaysByYearDTO dto = new PlaysByYearDTO();
            dto.setYear(rs.getString("year"));
            dto.setPlayCount(rs.getLong("play_count"));
            return dto;
        }, artistId);
    }
    
    // Get artist gender name
    public String getArtistGenderName(int artistId) {
        String sql = "SELECT g.name FROM Artist a LEFT JOIN Gender g ON a.gender_id = g.id WHERE a.id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get featured artist cards for an artist (artists who are featured on this artist's songs)
     * Returns full artist card data with feature count, sorted alphabetically
     */
    public List<FeaturedArtistCardDTO> getCollaboratedArtistsForArtist(int artistId) {
        String sql = """
            SELECT 
                a.id,
                a.name,
                a.gender_id,
                g.name as gender_name,
                a.ethnicity_id,
                e.name as ethnicity_name,
                a.genre_id,
                gr.name as genre_name,
                a.subgenre_id,
                sg.name as subgenre_name,
                a.language_id,
                l.name as language_name,
                a.country,
                (SELECT COUNT(*) FROM Song WHERE artist_id = a.id) as song_count,
                (SELECT COUNT(*) FROM Album WHERE artist_id = a.id) as album_count,
                CASE WHEN a.image IS NOT NULL AND LENGTH(a.image) > 0 THEN 1 ELSE 0 END as has_image,
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened,
                COUNT(sfa.song_id) as feature_count
            FROM SongFeaturedArtist sfa
            INNER JOIN Song s ON sfa.song_id = s.id
            INNER JOIN Artist a ON sfa.artist_id = a.id
            LEFT JOIN Gender g ON a.gender_id = g.id
            LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
            LEFT JOIN Genre gr ON a.genre_id = gr.id
            LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id
            LEFT JOIN Language l ON a.language_id = l.id
            LEFT JOIN (
                SELECT s2.artist_id, COUNT(*) as play_count, 
                       SUM(COALESCE(s2.length_seconds, 0)) as time_listened
                FROM Scrobble scr2
                INNER JOIN Song s2 ON scr2.song_id = s2.id
                GROUP BY s2.artist_id
            ) scr ON a.id = scr.artist_id
            WHERE s.artist_id = ?
            GROUP BY a.id, a.name, a.gender_id, g.name, a.ethnicity_id, e.name,
                     a.genre_id, gr.name, a.subgenre_id, sg.name, a.language_id, l.name, a.country
            ORDER BY a.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeaturedArtistCardDTO dto = new FeaturedArtistCardDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setGenderName(rs.getString("gender_name"));
            dto.setEthnicityId(rs.getObject("ethnicity_id") != null ? rs.getInt("ethnicity_id") : null);
            dto.setEthnicityName(rs.getString("ethnicity_name"));
            dto.setGenreId(rs.getObject("genre_id") != null ? rs.getInt("genre_id") : null);
            dto.setGenreName(rs.getString("genre_name"));
            dto.setSubgenreId(rs.getObject("subgenre_id") != null ? rs.getInt("subgenre_id") : null);
            dto.setSubgenreName(rs.getString("subgenre_name"));
            dto.setLanguageId(rs.getObject("language_id") != null ? rs.getInt("language_id") : null);
            dto.setLanguageName(rs.getString("language_name"));
            dto.setCountry(rs.getString("country"));
            dto.setSongCount(rs.getInt("song_count"));
            dto.setAlbumCount(rs.getInt("album_count"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            dto.setPlayCount(rs.getInt("play_count"));
            long timeListened = rs.getLong("time_listened");
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(formatTime(timeListened));
            dto.setFeatureCount(rs.getInt("feature_count"));
            return dto;
        }, artistId);
    }
}