package library.service;

import library.dto.ArtistAlbumDTO;
import library.dto.ArtistCardDTO;
import library.dto.ArtistSongDTO;
import library.dto.FeaturedArtistCardDTO;
import library.dto.PlaysByYearDTO;
import library.dto.ScrobbleDTO;
import library.entity.Artist;
import library.entity.ArtistImage;
import library.repository.ArtistImageRepository;
import library.repository.ArtistRepository;
import library.repository.LookupRepository;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

@Service
public class ArtistService {
    
    private final ArtistRepository artistRepository;
    private final ArtistImageRepository artistImageRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    private final ItunesService itunesService;
    
    public ArtistService(ArtistRepository artistRepository, ArtistImageRepository artistImageRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate, ItunesService itunesService) {
        this.artistRepository = artistRepository;
        this.artistImageRepository = artistImageRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.itunesService = itunesService;
    }
    
    public List<ArtistCardDTO> getArtists(String name, List<Integer> genderIds, String genderMode,
                                          List<Integer> ethnicityIds, String ethnicityMode,
                                          List<Integer> genreIds, String genreMode,
                                          List<Integer> subgenreIds, String subgenreMode,
                                          List<Integer> languageIds, String languageMode,
                                          List<String> countries, String countryMode,
                                          String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                          List<String> accounts, String accountMode,
                                          Integer ageMin, Integer ageMax, String ageMode,
                                          String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                          String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                          String listenedDateFrom, String listenedDateTo,
                                          String organized, Integer imageCountMin, Integer imageCountMax, String isBand, String inItunes,
                                          Integer playCountMin, Integer playCountMax,
                                          Integer albumCountMin, Integer albumCountMax,
                                          String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                          Integer songCountMin, Integer songCountMax,
                                          String sortBy, String sortDir, int page, int perPage) {
        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (genderIds != null && genderIds.isEmpty()) genderIds = null;
        if (ethnicityIds != null && ethnicityIds.isEmpty()) ethnicityIds = null;
        if (genreIds != null && genreIds.isEmpty()) genreIds = null;
        if (subgenreIds != null && subgenreIds.isEmpty()) subgenreIds = null;
        if (languageIds != null && languageIds.isEmpty()) languageIds = null;
        if (countries != null && countries.isEmpty()) countries = null;
        if (accounts != null && accounts.isEmpty()) accounts = null;
        
        // If inItunes filter is active, we need to get all results first, then filter in memory
        boolean filterByItunes = inItunes != null && !inItunes.isEmpty();
        int actualLimit = filterByItunes ? Integer.MAX_VALUE : perPage;
        int actualOffset = filterByItunes ? 0 : page * perPage;
        
        List<Object[]> results = artistRepository.findArtistsWithStats(
                name, genderIds, genderMode, ethnicityIds, ethnicityMode, 
                genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                countries, countryMode, deathDate, deathDateFrom, deathDateTo, deathDateMode,
                accounts, accountMode, ageMin, ageMax, ageMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, isBand,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax, birthDate, birthDateFrom, birthDateTo, birthDateMode,
                songCountMin, songCountMax,
                sortBy, sortDir, actualLimit, actualOffset
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
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            
            // Set first and last listened dates (indices 20 and 21)
            dto.setFirstListenedDate(row[20] != null ? formatDate((String) row[20]) : null);
            dto.setLastListenedDate(row[21] != null ? formatDate((String) row[21]) : null);
            
            // Set organized (index 22)
            dto.setOrganized(row[22] != null && ((Number) row[22]).intValue() == 1);
            
            // Set featured song count (index 23)
            dto.setFeaturedSongCount(row[23] != null ? ((Number) row[23]).intValue() : 0);
            
            // Set birth date (index 24) and death date (index 25)
            dto.setBirthDate(parseDateSafely(row[24]));
            dto.setDeathDate(parseDateSafely(row[25]));
            
            // Check iTunes presence for badge display
            dto.setInItunes(itunesService.artistExistsInItunes(dto.getName()));
            
            artists.add(dto);
        }
        
        // Apply iTunes filter if needed
        if (filterByItunes) {
            boolean wantInItunes = "true".equalsIgnoreCase(inItunes);
            artists = artists.stream()
                    .filter(a -> a.getInItunes() != null && a.getInItunes() == wantInItunes)
                    .toList();
            
            // Apply pagination manually
            int offset = page * perPage;
            int end = Math.min(offset + perPage, artists.size());
            if (offset >= artists.size()) {
                artists = new ArrayList<>();
            } else {
                artists = new ArrayList<>(artists.subList(offset, end));
            }
        }
        
        return artists;
    }
    
    public long countArtists(String name, List<Integer> genderIds, String genderMode,
                            List<Integer> ethnicityIds, String ethnicityMode,
                            List<Integer> genreIds, String genreMode,
                            List<Integer> subgenreIds, String subgenreMode,
                            List<Integer> languageIds, String languageMode,
                            List<String> countries, String countryMode,
                            String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                            List<String> accounts, String accountMode,
                            Integer ageMin, Integer ageMax, String ageMode,
                            String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                            String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                            String listenedDateFrom, String listenedDateTo,
                            String organized, Integer imageCountMin, Integer imageCountMax, String isBand, String inItunes,
                            Integer playCountMin, Integer playCountMax,
                            Integer albumCountMin, Integer albumCountMax,
                            String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                            Integer songCountMin, Integer songCountMax) {
        // Normalize empty lists to null here as well
        if (genderIds != null && genderIds.isEmpty()) genderIds = null;
        if (ethnicityIds != null && ethnicityIds.isEmpty()) ethnicityIds = null;
        if (genreIds != null && genreIds.isEmpty()) genreIds = null;
        if (subgenreIds != null && subgenreIds.isEmpty()) subgenreIds = null;
        if (languageIds != null && languageIds.isEmpty()) languageIds = null;
        if (countries != null && countries.isEmpty()) countries = null;
        if (accounts != null && accounts.isEmpty()) accounts = null;
        
        // If inItunes filter is active, we need to count manually
        if (inItunes != null && !inItunes.isEmpty()) {
            // Get all artists matching filters (without pagination) and filter by iTunes
            List<ArtistCardDTO> allArtists = getArtists(name, genderIds, genderMode, ethnicityIds, ethnicityMode,
                    genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                    countries, countryMode, deathDate, deathDateFrom, deathDateTo, deathDateMode,
                    accounts, accountMode, ageMin, ageMax, ageMode,
                    firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                    lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                    listenedDateFrom, listenedDateTo,
                    organized, imageCountMin, imageCountMax, isBand, inItunes,
                    playCountMin, playCountMax, albumCountMin, albumCountMax,
                    birthDate, birthDateFrom, birthDateTo, birthDateMode, songCountMin, songCountMax,
                    "plays", "desc", 0, Integer.MAX_VALUE);
            return allArtists.size();
        }
        
        return artistRepository.countArtistsWithFilters(name, genderIds, genderMode, 
                ethnicityIds, ethnicityMode, genreIds, genreMode, subgenreIds, subgenreMode,
                languageIds, languageMode, countries, countryMode, deathDate, deathDateFrom, deathDateTo, deathDateMode,
                accounts, accountMode, ageMin, ageMax, ageMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, isBand,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax, birthDate, birthDateFrom, birthDateTo, birthDateMode,
                songCountMin, songCountMax);
    }
    
    public Optional<Artist> getArtistById(Integer id) {
        String sql = """
            SELECT id, name, gender_id, country, ethnicity_id, 
                   genre_id, subgenre_id, language_id, is_band, organized,
                   birth_date, death_date, creation_date, update_date
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
            
            // Handle date fields - SQLite stores dates as strings, parse them directly
            String birthDateStr = rs.getString("birth_date");
            artist.setBirthDate(birthDateStr != null && !birthDateStr.isEmpty() ? java.time.LocalDate.parse(birthDateStr) : null);
            
            String deathDateStr = rs.getString("death_date");
            artist.setDeathDate(deathDateStr != null && !deathDateStr.isEmpty() ? java.time.LocalDate.parse(deathDateStr) : null);
            
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
                birth_date = ?, death_date = ?, update_date = CURRENT_TIMESTAMP
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
            artist.getBirthDate(),
            artist.getDeathDate(),
            artist.getId()
        );
        
        // If artist name changed, update all associated scrobbles and try to match unmatched ones
        if (oldName != null && !oldName.equals(artist.getName())) {
            updateScrobblesForArtistNameChange(artist.getId(), artist.getName());
            // Only try to match unmatched scrobbles if name changed (expensive operation)
            tryMatchUnmatchedScrobblesForArtist(artist.getId(), artist.getName());
        }
        
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
    
    // Gallery methods for secondary images
    public List<ArtistImage> getSecondaryImages(Integer artistId) {
        return artistImageRepository.findByArtistIdOrderByDisplayOrderAsc(artistId);
    }

    public int getSecondaryImageCount(Integer artistId) {
        return artistImageRepository.countByArtistId(artistId);
    }

    public byte[] getSecondaryImage(Integer imageId) {
        return artistImageRepository.findById(imageId)
                .map(ArtistImage::getImage)
                .orElse(null);
    }

    /**
     * Check if an image already exists for this artist (either as primary or in gallery).
     * Uses byte length first for speed, then compares actual data.
     */
    public boolean isDuplicateImage(Integer artistId, byte[] imageData) {
        if (imageData == null || imageData.length == 0) return false;
        
        // Check against primary image
        byte[] primaryImage = getArtistImage(artistId);
        if (primaryImage != null && primaryImage.length == imageData.length && java.util.Arrays.equals(primaryImage, imageData)) {
            return true;
        }
        
        // Check against gallery images
        List<ArtistImage> galleryImages = artistImageRepository.findByArtistIdOrderByDisplayOrderAsc(artistId);
        for (ArtistImage existing : galleryImages) {
            byte[] existingData = existing.getImage();
            if (existingData != null && existingData.length == imageData.length && java.util.Arrays.equals(existingData, imageData)) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Add a secondary image to the artist gallery.
     * @return true if image was added, false if it was a duplicate and skipped
     */
    public boolean addSecondaryImage(Integer artistId, byte[] imageData) {
        // Check for duplicates first
        if (isDuplicateImage(artistId, imageData)) {
            return false; // Skip duplicate
        }
        
        Integer maxOrder = artistImageRepository.getMaxDisplayOrder(artistId);
        ArtistImage image = new ArtistImage();
        image.setArtistId(artistId);
        image.setImage(imageData);
        image.setDisplayOrder(maxOrder + 1);
        image.setCreationDate(new java.sql.Timestamp(System.currentTimeMillis()));
        artistImageRepository.save(image);
        return true;
    }

    @Transactional
    public void deleteSecondaryImage(Integer imageId) {
        artistImageRepository.deleteById(imageId);
    }

    @Transactional
    public void swapToDefault(Integer artistId, Integer imageId) {
        // Get the current default image from the main Artist table
        byte[] currentDefault = getArtistImage(artistId);

        // Get the secondary image to promote
        ArtistImage secondaryImage = artistImageRepository.findById(imageId)
                .orElseThrow(() -> new IllegalArgumentException("Image not found: " + imageId));

        // Set the secondary image as the new default
        updateArtistImage(artistId, secondaryImage.getImage());

        // If there was a previous default, move it to secondary images
        if (currentDefault != null && currentDefault.length > 0) {
            // Update the secondary image record with the old default
            secondaryImage.setImage(currentDefault);
            artistImageRepository.save(secondaryImage);
        } else {
            // No previous default, just delete the secondary record
            artistImageRepository.deleteById(imageId);
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
            @SuppressWarnings("deprecation")
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
        Integer albumCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT album_id) FROM Song WHERE artist_id = ? AND album_id IS NOT NULL", Integer.class, artistId);
        return new int[]{albumCount != null ? albumCount : 0, songCount != null ? songCount : 0};
    }
    
    // Cached play stats to avoid multiple queries
    private static class PlayStats {
        int totalPlays;
        int vatitoPlays;
        int robertloverPlays;
    }
    
    private PlayStats getPlayStatsForArtist(int artistId) {
        String sql = """
            SELECT 
                COUNT(*) as total_plays,
                SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as vatito_plays,
                SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_plays
            FROM Scrobble scr 
            INNER JOIN Song s ON scr.song_id = s.id 
            WHERE s.artist_id = ?
            """;
        
        PlayStats stats = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
            PlayStats ps = new PlayStats();
            ps.totalPlays = rs.getInt("total_plays");
            ps.vatitoPlays = rs.getInt("vatito_plays");
            ps.robertloverPlays = rs.getInt("robertlover_plays");
            return ps;
        }, artistId);
        
        return stats != null ? stats : new PlayStats();
    }
    
    public int getPlayCountForArtist(int artistId) {
        return getPlayStatsForArtist(artistId).totalPlays;
    }
    
    // Get vatito (primary) play count for artist
    public int getVatitoPlayCountForArtist(int artistId) {
        return getPlayStatsForArtist(artistId).vatitoPlays;
    }
    
    // Get robertlover (legacy) play count for artist
    public int getRobertloverPlayCountForArtist(int artistId) {
        return getPlayStatsForArtist(artistId).robertloverPlays;
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
            SELECT SUM(s.length_seconds) as total_seconds
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
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

    // Get first listened date for an artist as LocalDate (for calculations)
    public java.time.LocalDate getFirstListenedDateAsLocalDateForArtist(int artistId) {
        String sql = """
            SELECT MIN(DATE(scr.scrobble_date))
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ?
            """;
        
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, artistId);
            return dateStr != null ? java.time.LocalDate.parse(dateStr) : null;
        } catch (Exception e) {
            return null;
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

    // Get unique days played for an artist
    public int getUniqueDaysPlayedForArtist(int artistId) {
        String sql = """
            SELECT COUNT(DISTINCT DATE(scr.scrobble_date))
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ? AND scr.scrobble_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique weeks played for an artist
    public int getUniqueWeeksPlayedForArtist(int artistId) {
        String sql = """
            SELECT COUNT(DISTINCT strftime('%Y-%W', scr.scrobble_date))
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ? AND scr.scrobble_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique months played for an artist
    public int getUniqueMonthsPlayedForArtist(int artistId) {
        String sql = """
            SELECT COUNT(DISTINCT strftime('%Y-%m', scr.scrobble_date))
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ? AND scr.scrobble_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique years played for an artist
    public int getUniqueYearsPlayedForArtist(int artistId) {
        String sql = """
            SELECT COUNT(DISTINCT strftime('%Y', scr.scrobble_date))
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            WHERE s.artist_id = ? AND scr.scrobble_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get earliest release date for an artist (from all songs)
    public String getEarliestReleaseDateForArtist(int artistId) {
        String sql = "SELECT MIN(release_date) FROM Song WHERE artist_id = ? AND release_date IS NOT NULL";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    // Get average song length for an artist (formatted as mm:ss)
    public String getAverageSongLengthFormatted(int artistId) {
        String sql = "SELECT AVG(length_seconds) FROM Song WHERE artist_id = ? AND length_seconds IS NOT NULL";
        try {
            Double avgSeconds = jdbcTemplate.queryForObject(sql, Double.class, artistId);
            if (avgSeconds == null || avgSeconds == 0) {
                return "-";
            }
            int totalSeconds = (int) Math.round(avgSeconds);
            int minutes = totalSeconds / 60;
            int seconds = totalSeconds % 60;
            return String.format("%d:%02d", minutes, seconds);
        } catch (Exception e) {
            return "-";
        }
    }
    
    // Get average plays per song for an artist
    public String getAveragePlaysPerSong(int artistId) {
        String sql = """
            SELECT AVG(play_count) FROM (
                SELECT COALESCE(COUNT(scr.id), 0) as play_count
                FROM Song s
                LEFT JOIN Scrobble scr ON s.id = scr.song_id
                WHERE s.artist_id = ?
                GROUP BY s.id
            )
            """;
        try {
            Double avgPlays = jdbcTemplate.queryForObject(sql, Double.class, artistId);
            if (avgPlays == null || avgPlays == 0) {
                return "-";
            }
            // Format with one decimal place if not a whole number
            if (avgPlays == Math.floor(avgPlays)) {
                return String.format("%.0f", avgPlays);
            } else {
                return String.format("%.1f", avgPlays);
            }
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
                (s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id)) as has_image,
                a.image IS NOT NULL as album_has_image,
                COALESCE(s.release_date, a.release_date) as release_date,
                a.name as album_name,
                a.id as album_id,
                ar.country,
                COALESCE(g_song.name, g_album.name, g_artist.name) as genre,
                COALESCE(sg_song.name, sg_album.name, sg_artist.name) as subgenre,
                COALESCE(eth_song.name, eth_artist.name) as ethnicity,
                COALESCE(l_song.name, l_album.name, l_artist.name) as language,
                COALESCE(play_stats.vatito_plays, 0) as vatito_plays,
                COALESCE(play_stats.robertlover_plays, 0) as robertlover_plays,
                COALESCE(play_stats.total_plays, 0) as total_plays,
                play_stats.first_listen,
                play_stats.last_listen,
                s.is_single
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            LEFT JOIN (
                SELECT 
                    song_id,
                    SUM(CASE WHEN account = 'vatito' THEN 1 ELSE 0 END) as vatito_plays,
                    SUM(CASE WHEN account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_plays,
                    COUNT(*) as total_plays,
                    MIN(scrobble_date) as first_listen,
                    MAX(scrobble_date) as last_listen
                FROM Scrobble
                GROUP BY song_id
            ) play_stats ON s.id = play_stats.song_id
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
            ORDER BY total_plays DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistSongDTO dto = new ArtistSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setHasImage(rs.getBoolean("has_image"));
            dto.setAlbumHasImage(rs.getBoolean("album_has_image"));
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
            
            // Set isSingle
            dto.setIsSingle(rs.getBoolean("is_single"));
            
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
                COALESCE(song_stats.song_count, 0) as song_count,
                COALESCE(song_stats.total_length_seconds, 0) as total_length_seconds,
                COALESCE(play_stats.vatito_plays, 0) as vatito_plays,
                COALESCE(play_stats.robertlover_plays, 0) as robertlover_plays,
                COALESCE(play_stats.total_plays, 0) as total_plays,
                play_stats.first_listen,
                play_stats.last_listen,
                COALESCE(play_stats.total_listening_seconds, 0) as total_listening_seconds
            FROM Album a
            INNER JOIN Artist ar ON a.artist_id = ar.id
            LEFT JOIN Genre g_override ON a.override_genre_id = g_override.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON a.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON a.override_language_id = l_override.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            LEFT JOIN (
                SELECT album_id, COUNT(*) as song_count, SUM(length_seconds) as total_length_seconds
                FROM Song
                GROUP BY album_id
            ) song_stats ON a.id = song_stats.album_id
            LEFT JOIN (
                SELECT 
                    s.album_id,
                    SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as vatito_plays,
                    SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_plays,
                    COUNT(*) as total_plays,
                    MIN(scr.scrobble_date) as first_listen,
                    MAX(scr.scrobble_date) as last_listen,
                    SUM(s.length_seconds) as total_listening_seconds
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                WHERE s.album_id IS NOT NULL
                GROUP BY s.album_id
            ) play_stats ON a.id = play_stats.album_id
            WHERE a.artist_id = ?
            ORDER BY total_plays DESC
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
                               genre_id, subgenre_id, language_id, is_band, birth_date, death_date, creation_date, update_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
            artist.getBirthDate(),
            artist.getDeathDate()
        );
        
        // Get the ID of the newly created artist
        Integer id = jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Integer.class);
        artist.setId(id);
        
        return artist;
    }
    
    // Get all artists for API (id, name, and attribute IDs for inheritance display)
    public List<Map<String, Object>> getAllArtistsForApi() {
        String sql = """
            SELECT a.id, a.name, a.genre_id, a.subgenre_id, a.language_id, a.gender_id, a.ethnicity_id,
                   g.name as genre_name, sg.name as subgenre_name, l.name as language_name,
                   gn.name as gender_name, e.name as ethnicity_name
            FROM Artist a
            LEFT JOIN Genre g ON a.genre_id = g.id
            LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id
            LEFT JOIN Language l ON a.language_id = l.id
            LEFT JOIN Gender gn ON a.gender_id = gn.id
            LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
            ORDER BY a.name
            """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> artist = new java.util.HashMap<>();
            artist.put("id", rs.getInt("id"));
            artist.put("name", rs.getString("name"));
            artist.put("genreId", rs.getObject("genre_id") != null ? rs.getInt("genre_id") : null);
            artist.put("subgenreId", rs.getObject("subgenre_id") != null ? rs.getInt("subgenre_id") : null);
            artist.put("languageId", rs.getObject("language_id") != null ? rs.getInt("language_id") : null);
            artist.put("genderId", rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            artist.put("ethnicityId", rs.getObject("ethnicity_id") != null ? rs.getInt("ethnicity_id") : null);
            artist.put("genreName", rs.getString("genre_name"));
            artist.put("subgenreName", rs.getString("subgenre_name"));
            artist.put("languageName", rs.getString("language_name"));
            artist.put("genderName", rs.getString("gender_name"));
            artist.put("ethnicityName", rs.getString("ethnicity_name"));
            return artist;
        });
    }
    
    // Search artists by name for API
    public List<Map<String, Object>> searchArtists(String query, int limit) {
        String sql = """
            SELECT a.id, a.name, a.genre_id, a.subgenre_id, a.language_id, a.gender_id, a.ethnicity_id,
                   g.name as genre_name, sg.name as subgenre_name, l.name as language_name,
                   gn.name as gender_name, e.name as ethnicity_name
            FROM Artist a
            LEFT JOIN Genre g ON a.genre_id = g.id
            LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id
            LEFT JOIN Language l ON a.language_id = l.id
            LEFT JOIN Gender gn ON a.gender_id = gn.id
            LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
            WHERE LOWER(a.name) LIKE ?
            ORDER BY a.name
            LIMIT ?
            """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> artist = new java.util.HashMap<>();
            artist.put("id", rs.getInt("id"));
            artist.put("name", rs.getString("name"));
            artist.put("genreId", rs.getObject("genre_id") != null ? rs.getInt("genre_id") : null);
            artist.put("subgenreId", rs.getObject("subgenre_id") != null ? rs.getInt("subgenre_id") : null);
            artist.put("languageId", rs.getObject("language_id") != null ? rs.getInt("language_id") : null);
            artist.put("genderId", rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            artist.put("ethnicityId", rs.getObject("ethnicity_id") != null ? rs.getInt("ethnicity_id") : null);
            artist.put("genreName", rs.getString("genre_name"));
            artist.put("subgenreName", rs.getString("subgenre_name"));
            artist.put("languageName", rs.getString("language_name"));
            artist.put("genderName", rs.getString("gender_name"));
            artist.put("ethnicityName", rs.getString("ethnicity_name"));
            return artist;
        }, "%" + query.toLowerCase() + "%", limit);
    }
    
    // Find artist by ID
    public Artist findById(Integer id) {
        if (id == null) return null;
        String sql = "SELECT id, name, gender_id, country, ethnicity_id, genre_id, subgenre_id, language_id, is_band, birth_date, death_date FROM Artist WHERE id = ?";
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
                String birthStr = rs.getString("birth_date");
                if (birthStr != null) a.setBirthDate(java.time.LocalDate.parse(birthStr));
                String deathStr = rs.getString("death_date");
                if (deathStr != null) a.setDeathDate(java.time.LocalDate.parse(deathStr));
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
        if (data.get("subgenreId") != null) {
            artist.setSubgenreId(((Number) data.get("subgenreId")).intValue());
        }
        if (data.get("languageId") != null) {
            artist.setLanguageId(((Number) data.get("languageId")).intValue());
        }
        // Parse birth date from string (YYYY-MM-DD or YYYY-MM or YYYY)
        if (data.get("birthDate") != null) {
            String birthDateStr = (String) data.get("birthDate");
            java.time.LocalDate birthDate = parseFlexibleDate(birthDateStr);
            if (birthDate != null) {
                artist.setBirthDate(birthDate);
            }
        }
        // Parse death date from string
        if (data.get("deathDate") != null) {
            String deathDateStr = (String) data.get("deathDate");
            java.time.LocalDate deathDate = parseFlexibleDate(deathDateStr);
            if (deathDate != null) {
                artist.setDeathDate(deathDate);
            }
        }
        // Set isBand flag
        if (data.get("isBand") != null) {
            artist.setIsBand((Boolean) data.get("isBand"));
        }
        Artist created = createArtist(artist);
        return created.getId();
    }

    // Parse flexible date format from MusicBrainz (YYYY, YYYY-MM, YYYY-MM-DD)
    private java.time.LocalDate parseFlexibleDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        dateStr = dateStr.trim();
        try {
            // Full date: YYYY-MM-DD
            if (dateStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return java.time.LocalDate.parse(dateStr, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
            }
            // Year and month: YYYY-MM -> assume first day of month
            if (dateStr.matches("\\d{4}-\\d{2}")) {
                return java.time.LocalDate.parse(dateStr + "-01", java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
            }
            // Year only: YYYY -> assume January 1st
            if (dateStr.matches("\\d{4}")) {
                return java.time.LocalDate.parse(dateStr + "-01-01", java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
            }
        } catch (Exception e) {
            System.err.println("Failed to parse date: " + dateStr + " - " + e.getMessage());
        }
        return null;
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
                
                // Format as DD-Mon-YYYY (e.g., "01-Nov-2025") with zero-padded day
                return String.format("%02d-%s-%d", day, monthNames[month - 1], year);
            }
            
            return datePart;
        } catch (Exception e) {
            return dateTimeString;
        }
    }
    
    /**
     * Safely parse a date stored as ISO string (YYYY-MM-DD).
     * Will fail on numeric timestamps - those should be fixed in the database.
     */
    private LocalDate parseDateSafely(Object value) {
        if (value == null) {
            return null;
        }
        
        String strValue = value.toString().trim();
        if (strValue.isEmpty()) {
            return null;
        }
        
        // Parse as ISO date string (YYYY-MM-DD)
        return LocalDate.parse(strValue);
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
                a.birth_date,
                a.death_date,
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
                MAX(CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) as has_image,
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened,
                COUNT(sfa.song_id) as feature_count,
                GROUP_CONCAT(s.name, '||') as song_names
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
            GROUP BY a.id
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
            dto.setGenreId(rs.getObject("gender_id") != null ? rs.getInt("genre_id") : null);
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
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            dto.setFeatureCount(rs.getInt("feature_count"));
            // Parse song names from GROUP_CONCAT result
            String songNamesConcat = rs.getString("song_names");
            if (songNamesConcat != null && !songNamesConcat.isEmpty()) {
                dto.setSongNames(java.util.Arrays.asList(songNamesConcat.split("\\|\\|")));
            }
            String birthDateStr = rs.getString("birth_date");
            dto.setBirthDate(birthDateStr != null ? java.time.LocalDate.parse(birthDateStr) : null);
            String deathDateStr = rs.getString("death_date");
            dto.setDeathDate(deathDateStr != null ? java.time.LocalDate.parse(deathDateStr) : null);
            return dto;
        }, artistId);
    }
    
    /**
     * Get collaborated artists from songs where this artist is FEATURED
     * (i.e., the main artist of songs where this artist appears as a feature, plus other featured artists on those songs)
     */
    public List<FeaturedArtistCardDTO> getFeaturedCollaboratedArtists(int artistId) {
        // This gets both the main artist of songs where artistId is featured,
        // AND other featured artists on those same songs
        String sql = """
            SELECT 
                a.id,
                a.name,
                a.birth_date,
                a.death_date,
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
                MAX(CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) as has_image,
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened,
                COUNT(DISTINCT s.id) as feature_count,
                GROUP_CONCAT(DISTINCT s.name, '||') as song_names
            FROM SongFeaturedArtist sfa_me
            INNER JOIN Song s ON sfa_me.song_id = s.id
            INNER JOIN Artist a ON s.artist_id = a.id
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
            WHERE sfa_me.artist_id = ?
            AND a.id != ?
            GROUP BY a.id
            
            UNION
            
            SELECT 
                a.id,
                a.name,
                a.birth_date,
                a.death_date,
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
                MAX(CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) as has_image,
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened,
                COUNT(DISTINCT s.id) as feature_count,
                GROUP_CONCAT(DISTINCT s.name, '||') as song_names
            FROM SongFeaturedArtist sfa_me
            INNER JOIN Song s ON sfa_me.song_id = s.id
            INNER JOIN SongFeaturedArtist sfa_other ON s.id = sfa_other.song_id
            INNER JOIN Artist a ON sfa_other.artist_id = a.id
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
            WHERE sfa_me.artist_id = ?
            AND sfa_other.artist_id != ?
            GROUP BY a.id
            
            ORDER BY name
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
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            dto.setFeatureCount(rs.getInt("feature_count"));
            String songNamesConcat = rs.getString("song_names");
            if (songNamesConcat != null && !songNamesConcat.isEmpty()) {
                dto.setSongNames(java.util.Arrays.asList(songNamesConcat.split("\\|\\|")));
            }
            String birthDateStr = rs.getString("birth_date");
            dto.setBirthDate(birthDateStr != null ? java.time.LocalDate.parse(birthDateStr) : null);
            String deathDateStr = rs.getString("death_date");
            dto.setDeathDate(deathDateStr != null ? java.time.LocalDate.parse(deathDateStr) : null);
            return dto;
        }, artistId, artistId, artistId, artistId);
    }
    
    // ========================================
    // Artist Membership Methods (Group relationships)
    // ========================================
    
    /**
     * Get groups that this artist is a member of (e.g., Destiny's Child for Beyoncé)
     * Returns full artist card data for display
     */
    public List<FeaturedArtistCardDTO> getGroupsForArtist(int artistId) {
        String sql = """
            SELECT 
                a.id,
                a.name,
                a.birth_date,
                a.death_date,
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
                a.is_band,
                (SELECT COUNT(*) FROM Song WHERE artist_id = a.id) as song_count,
                (SELECT COUNT(*) FROM Album WHERE artist_id = a.id) as album_count,
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened
            FROM ArtistMember am
            INNER JOIN Artist a ON am.group_artist_id = a.id
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
            WHERE am.member_artist_id = ?
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
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            String birthDateStr = rs.getString("birth_date");
            dto.setBirthDate(birthDateStr != null ? java.time.LocalDate.parse(birthDateStr) : null);
            String deathDateStr = rs.getString("death_date");
            dto.setDeathDate(deathDateStr != null ? java.time.LocalDate.parse(deathDateStr) : null);
            return dto;
        }, artistId);
    }
    
    /**
     * Get members of a group artist (e.g., Beyoncé for Destiny's Child)
     * Returns full artist card data for display
     */
    public List<FeaturedArtistCardDTO> getMembersForArtist(int artistId) {
        String sql = """
            SELECT 
                a.id,
                a.name,
                a.birth_date,
                a.death_date,
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
                a.is_band,
                (SELECT COUNT(*) FROM Song WHERE artist_id = a.id) as song_count,
                (SELECT COUNT(*) FROM Album WHERE artist_id = a.id) as album_count,
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened
            FROM ArtistMember am
            INNER JOIN Artist a ON am.member_artist_id = a.id
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
            WHERE am.group_artist_id = ?
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
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            String birthDateStr = rs.getString("birth_date");
            dto.setBirthDate(birthDateStr != null ? java.time.LocalDate.parse(birthDateStr) : null);
            String deathDateStr = rs.getString("death_date");
            dto.setDeathDate(deathDateStr != null ? java.time.LocalDate.parse(deathDateStr) : null);
            return dto;
        }, artistId);
    }
    
    /**
     * Get the IDs of groups that this artist is a member of
     */
    public List<Integer> getGroupIdsForArtist(int artistId) {
        String sql = "SELECT group_artist_id FROM ArtistMember WHERE member_artist_id = ?";
        return jdbcTemplate.queryForList(sql, Integer.class, artistId);
    }
    
    /**
     * Save the groups that an artist belongs to.
     * Deletes all existing memberships and inserts new ones.
     */
    public void saveArtistGroups(int artistId, List<Integer> groupIds) {
        // Delete all existing memberships where this artist is a member
        jdbcTemplate.update("DELETE FROM ArtistMember WHERE member_artist_id = ?", artistId);
        
        // Insert new memberships
        if (groupIds != null && !groupIds.isEmpty()) {
            String insertSql = "INSERT INTO ArtistMember (group_artist_id, member_artist_id) VALUES (?, ?)";
            for (Integer groupId : groupIds) {
                if (groupId != null && !groupId.equals(artistId)) { // Prevent self-reference
                    jdbcTemplate.update(insertSql, groupId, artistId);
                }
            }
        }
    }
    
    // ========================================
    // Aggregated Stats Methods (for includeGroups toggle)
    // ========================================
    
    /**
     * Get aggregated play count for an artist including all groups they belong to
     */
    public int getAggregatedPlayCount(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT COUNT(*) FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id WHERE s.artist_id IN (" + placeholders + ")";
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, allArtistIds.toArray());
        return count != null ? count : 0;
    }
    
    /**
     * Get aggregated vatito (primary) play count for an artist including all groups
     */
    public int getAggregatedVatitoPlayCount(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT COUNT(*) FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id WHERE s.artist_id IN (" + placeholders + ") AND scr.account = 'vatito'";
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, allArtistIds.toArray());
        return count != null ? count : 0;
    }
    
    /**
     * Get aggregated robertlover (legacy) play count for an artist including all groups
     */
    public int getAggregatedRobertloverPlayCount(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT COUNT(*) FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id WHERE s.artist_id IN (" + placeholders + ") AND scr.account = 'robertlover'";
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, allArtistIds.toArray());
        return count != null ? count : 0;
    }
    
    /**
     * Get aggregated listening time for an artist including all groups they belong to
     */
    public String getAggregatedListeningTime(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT SUM(s.length_seconds) as total_seconds FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id WHERE s.artist_id IN (" + placeholders + ")";
        
        Long totalSeconds = jdbcTemplate.queryForObject(sql, Long.class, allArtistIds.toArray());
        if (totalSeconds == null || totalSeconds == 0) {
            return "-";
        }
        
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        
        if (days > 0) {
            return String.format("%dd:%02d:%02d:%02d", days, hours, minutes, seconds);
        } else if (hours > 0) {
            return String.format("%d:%02d:%02d", hours, minutes, seconds);
        } else {
            return String.format("%d:%02d", minutes, seconds);
        }
    }
    
    /**
     * Get aggregated first listened date for an artist including all groups
     */
    public String getAggregatedFirstListenedDate(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT MIN(scr.scrobble_date) FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id WHERE s.artist_id IN (" + placeholders + ")";
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, allArtistIds.toArray());
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }
    
    /**
     * Get aggregated last listened date for an artist including all groups
     */
    public String getAggregatedLastListenedDate(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT MAX(scr.scrobble_date) FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id WHERE s.artist_id IN (" + placeholders + ")";
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, allArtistIds.toArray());
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }
    
    /**
     * Get aggregated plays by year for an artist including all groups
     */
    public List<PlaysByYearDTO> getAggregatedPlaysByYear(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT strftime('%Y', scr.scrobble_date) as year, COUNT(*) as play_count " +
            "FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id " +
            "WHERE s.artist_id IN (" + placeholders + ") AND scr.scrobble_date IS NOT NULL " +
            "GROUP BY strftime('%Y', scr.scrobble_date) ORDER BY year ASC";
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlaysByYearDTO dto = new PlaysByYearDTO();
            dto.setYear(rs.getString("year"));
            dto.setPlayCount(rs.getLong("play_count"));
            return dto;
        }, allArtistIds.toArray());
    }
    
    /**
     * Get aggregated songs for an artist including all groups.
     * Returns songs from all artists combined.
     */
    public List<ArtistSongDTO> getAggregatedSongsForArtist(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = """
            SELECT 
                s.id,
                s.name,
                s.length_seconds,
                (s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id)) as has_image,
                a.image IS NOT NULL as album_has_image,
                COALESCE(s.release_date, a.release_date) as release_date,
                a.name as album_name,
                a.id as album_id,
                ar.name as artist_name,
                ar.id as artist_id,
                ar.country,
                COALESCE(g_song.name, g_album.name, g_artist.name) as genre,
                COALESCE(sg_song.name, sg_album.name, sg_artist.name) as subgenre,
                COALESCE(eth_song.name, eth_artist.name) as ethnicity,
                COALESCE(l_song.name, l_album.name, l_artist.name) as language,
                COALESCE(play_stats.vatito_plays, 0) as vatito_plays,
                COALESCE(play_stats.robertlover_plays, 0) as robertlover_plays,
                COALESCE(play_stats.total_plays, 0) as total_plays,
                play_stats.first_listen,
                play_stats.last_listen,
                s.is_single
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            LEFT JOIN (
                SELECT 
                    song_id,
                    SUM(CASE WHEN account = 'vatito' THEN 1 ELSE 0 END) as vatito_plays,
                    SUM(CASE WHEN account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_plays,
                    COUNT(*) as total_plays,
                    MIN(scrobble_date) as first_listen,
                    MAX(scrobble_date) as last_listen
                FROM Scrobble
                GROUP BY song_id
            ) play_stats ON s.id = play_stats.song_id
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
            WHERE s.artist_id IN (%s)
            ORDER BY total_plays DESC
            """.formatted(placeholders);
        
        final int mainArtistId = artistId; // Capture for use in lambda
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistSongDTO dto = new ArtistSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setHasImage(rs.getBoolean("has_image"));
            dto.setAlbumHasImage(rs.getBoolean("album_has_image"));
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
            
            // Set isSingle
            dto.setIsSingle(rs.getBoolean("is_single"));
            
            // Check if song is from a group (artist_id != main artist id)
            int songArtistId = rs.getInt("artist_id");
            if (songArtistId != mainArtistId) {
                dto.setFromGroup(true);
                dto.setSourceArtistId(songArtistId);
                dto.setSourceArtistName(rs.getString("artist_name"));
            }
            
            return dto;
        }, allArtistIds.toArray());
    }
    
    /**
     * Get aggregated albums for an artist including all groups.
     * Returns albums from all artists combined.
     */
    public List<ArtistAlbumDTO> getAggregatedAlbumsForArtist(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = """
            SELECT 
                a.id,
                a.name,
                a.release_date,
                ar.name as artist_name,
                ar.id as artist_id,
                ar.country,
                COALESCE(g_override.name, g_artist.name) as genre,
                COALESCE(sg_override.name, sg_artist.name) as subgenre,
                eth.name as ethnicity,
                COALESCE(l_override.name, l_artist.name) as language,
                COALESCE(song_stats.song_count, 0) as song_count,
                COALESCE(song_stats.total_length_seconds, 0) as total_length_seconds,
                COALESCE(play_stats.vatito_plays, 0) as vatito_plays,
                COALESCE(play_stats.robertlover_plays, 0) as robertlover_plays,
                COALESCE(play_stats.total_plays, 0) as total_plays,
                play_stats.first_listen,
                play_stats.last_listen,
                COALESCE(play_stats.total_listening_seconds, 0) as total_listening_seconds
            FROM Album a
            INNER JOIN Artist ar ON a.artist_id = ar.id
            LEFT JOIN Genre g_override ON a.override_genre_id = g_override.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON a.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON a.override_language_id = l_override.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            LEFT JOIN (
                SELECT album_id, COUNT(*) as song_count, SUM(length_seconds) as total_length_seconds
                FROM Song
                GROUP BY album_id
            ) song_stats ON a.id = song_stats.album_id
            LEFT JOIN (
                SELECT 
                    s.album_id,
                    SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as vatito_plays,
                    SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_plays,
                    COUNT(*) as total_plays,
                    MIN(scr.scrobble_date) as first_listen,
                    MAX(scr.scrobble_date) as last_listen,
                    SUM(s.length_seconds) as total_listening_seconds
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                WHERE s.album_id IS NOT NULL
                GROUP BY s.album_id
            ) play_stats ON a.id = play_stats.album_id
            WHERE a.artist_id IN (%s)
            ORDER BY total_plays DESC
            """.formatted(placeholders);
        
        final int mainArtistId = artistId; // Capture for use in lambda
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
            
            // Check if album is from a group (artist_id != main artist id)
            int albumArtistId = rs.getInt("artist_id");
            if (albumArtistId != mainArtistId) {
                dto.setFromGroup(true);
                dto.setSourceArtistId(albumArtistId);
                dto.setSourceArtistName(rs.getString("artist_name"));
            }
            
            return dto;
        }, allArtistIds.toArray());
    }
    
    /**
     * Get aggregated song count for an artist including all groups
     */
    public int getAggregatedSongCount(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT COUNT(*) FROM Song WHERE artist_id IN (" + placeholders + ")";
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, allArtistIds.toArray());
        return count != null ? count : 0;
    }
    
    /**
     * Get aggregated album count for an artist including all groups
     */
    public int getAggregatedAlbumCount(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT COUNT(*) FROM Album WHERE artist_id IN (" + placeholders + ")";
        
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, allArtistIds.toArray());
        return count != null ? count : 0;
    }
    
    /**
     * Count total scrobbles for an artist including all groups
     */
    public long getAggregatedScrobbleCount(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = "SELECT COUNT(*) FROM Scrobble scr INNER JOIN Song s ON scr.song_id = s.id WHERE s.artist_id IN (" + placeholders + ")";
        
        Long count = jdbcTemplate.queryForObject(sql, Long.class, allArtistIds.toArray());
        return count != null ? count : 0;
    }
    
    /**
     * Get aggregated scrobbles with pagination for an artist including all groups
     */
    public List<ScrobbleDTO> getAggregatedScrobblesForArtist(int artistId, List<Integer> groupIds, int page, int pageSize) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        int offset = page * pageSize;
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
        String sql = """
            SELECT 
                scr.scrobble_date,
                s.name as song_name,
                s.id as song_id,
                al.name as album_name,
                al.id as album_id,
                ar.name as artist_name,
                ar.id as artist_id,
                scr.account
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            LEFT JOIN Album al ON s.album_id = al.id
            LEFT JOIN Artist ar ON s.artist_id = ar.id
            WHERE s.artist_id IN (%s)
            ORDER BY scr.scrobble_date DESC
            LIMIT ? OFFSET ?
            """.formatted(placeholders);
        
        List<Object> params = new ArrayList<>(allArtistIds);
        params.add(pageSize);
        params.add(offset);
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ScrobbleDTO dto = new ScrobbleDTO();
            dto.setScrobbleDate(rs.getString("scrobble_date"));
            dto.setSongName(rs.getString("song_name"));
            dto.setSongId(rs.getInt("song_id"));
            dto.setAlbumName(rs.getString("album_name"));
            int albumId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : albumId);
            dto.setArtistName(rs.getString("artist_name"));
            int fetchedArtistId = rs.getInt("artist_id");
            dto.setArtistId(fetchedArtistId);
            dto.setAccount(rs.getString("account"));
            // Mark as from group if the artist is not the main artist
            dto.setFromGroup(fetchedArtistId != artistId);
            return dto;
        }, params.toArray());
    }
    
    /**
     * Get aggregated collaborated artists for an artist including all groups
     */
    public List<FeaturedArtistCardDTO> getAggregatedCollaboratedArtists(int artistId, List<Integer> groupIds) {
        List<Integer> allArtistIds = new ArrayList<>();
        allArtistIds.add(artistId);
        if (groupIds != null) {
            allArtistIds.addAll(groupIds);
        }
        
        String placeholders = String.join(",", allArtistIds.stream().map(id -> "?").toArray(String[]::new));
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
                MAX(CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) as has_image,
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened,
                COUNT(sfa.song_id) as feature_count,
                GROUP_CONCAT(s.name, '||') as song_names
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
            WHERE s.artist_id IN (%s)
            GROUP BY a.id
            ORDER BY a.name
            """.formatted(placeholders);
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeaturedArtistCardDTO dto = new FeaturedArtistCardDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setGenderName(rs.getString("gender_name"));
            dto.setEthnicityId(rs.getObject("ethnicity_id") != null ? rs.getInt("ethnicity_id") : null);
            dto.setEthnicityName(rs.getString("ethnicity_name"));
            dto.setGenreId(rs.getObject("gender_id") != null ? rs.getInt("genre_id") : null);
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
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            dto.setFeatureCount(rs.getInt("feature_count"));
            String songNamesConcat = rs.getString("song_names");
            if (songNamesConcat != null && !songNamesConcat.isEmpty()) {
                dto.setSongNames(java.util.Arrays.asList(songNamesConcat.split("\\|\\|")));
            }
            return dto;
        }, allArtistIds.toArray());
    }
    
    // ===== FEATURED SONGS METHODS =====
    
    /**
     * Check if an artist has any songs where they are featured (not the main artist)
     */
    public boolean hasFeaturedSongs(int artistId) {
        String sql = """
            SELECT COUNT(*) > 0 
            FROM SongFeaturedArtist sfa
            WHERE sfa.artist_id = ?
            """;
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(sql, Boolean.class, artistId));
    }
    
    /**
     * Get count of songs where this artist is featured
     */
    public int getFeaturedSongCount(int artistId) {
        String sql = """
            SELECT COUNT(DISTINCT sfa.song_id)
            FROM SongFeaturedArtist sfa
            WHERE sfa.artist_id = ?
            """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get play count for songs where this artist is featured
     */
    public int getFeaturedPlayCount(int artistId) {
        String sql = """
            SELECT COUNT(*) 
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            WHERE sfa.artist_id = ?
            """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get primary (vatito) play count for songs where this artist is featured
     */
    public int getFeaturedVatitoPlayCount(int artistId) {
        String sql = """
            SELECT COUNT(*) 
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            WHERE sfa.artist_id = ? AND sc.account = 'vatito'
            """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get legacy (robertlover) play count for songs where this artist is featured
     */
    public int getFeaturedRobertloverPlayCount(int artistId) {
        String sql = """
            SELECT COUNT(*) 
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            WHERE sfa.artist_id = ? AND sc.account = 'robertlover'
            """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    /**
     * Get total listening time for songs where this artist is featured
     */
    public String getFeaturedListeningTime(int artistId) {
        String sql = """
            SELECT COALESCE(SUM(s.length_seconds), 0)
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            INNER JOIN Song s ON sc.song_id = s.id
            WHERE sfa.artist_id = ?
            """;
        Integer totalSeconds = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return TimeFormatUtils.formatTime(totalSeconds != null ? totalSeconds : 0);
    }
    
    /**
     * Get first listened date for songs where this artist is featured
     */
    public String getFeaturedFirstListenedDate(int artistId) {
        String sql = """
            SELECT MIN(sc.scrobble_date)
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            WHERE sfa.artist_id = ?
            """;
        String date = jdbcTemplate.queryForObject(sql, String.class, artistId);
        return formatDate(date);
    }
    
    /**
     * Get last listened date for songs where this artist is featured
     */
    public String getFeaturedLastListenedDate(int artistId) {
        String sql = """
            SELECT MAX(sc.scrobble_date)
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            WHERE sfa.artist_id = ?
            """;
        String date = jdbcTemplate.queryForObject(sql, String.class, artistId);
        return formatDate(date);
    }
    
    /**
     * Get songs where this artist is featured (for Songs table in General tab)
     */
    public List<ArtistSongDTO> getFeaturedSongsForArtist(int artistId) {
        String sql = """
            SELECT 
                s.id,
                s.name,
                s.length_seconds,
                (s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id)) as has_image,
                a.image IS NOT NULL as album_has_image,
                COALESCE(s.release_date, a.release_date) as release_date,
                a.name as album_name,
                a.id as album_id,
                ar.id as primary_artist_id,
                ar.name as primary_artist_name,
                ar.country,
                COALESCE(g_song.name, g_album.name, g_artist.name) as genre,
                COALESCE(sg_song.name, sg_album.name, sg_artist.name) as subgenre,
                COALESCE(eth_song.name, eth_artist.name) as ethnicity,
                COALESCE(l_song.name, l_album.name, l_artist.name) as language,
                COALESCE(play_stats.vatito_plays, 0) as vatito_plays,
                COALESCE(play_stats.robertlover_plays, 0) as robertlover_plays,
                COALESCE(play_stats.total_plays, 0) as total_plays,
                play_stats.first_listen,
                play_stats.last_listen,
                s.is_single
            FROM SongFeaturedArtist sfa
            INNER JOIN Song s ON sfa.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            LEFT JOIN (
                SELECT 
                    song_id,
                    SUM(CASE WHEN account = 'vatito' THEN 1 ELSE 0 END) as vatito_plays,
                    SUM(CASE WHEN account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_plays,
                    COUNT(*) as total_plays,
                    MIN(scrobble_date) as first_listen,
                    MAX(scrobble_date) as last_listen
                FROM Scrobble
                GROUP BY song_id
            ) play_stats ON s.id = play_stats.song_id
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
            WHERE sfa.artist_id = ?
            ORDER BY total_plays DESC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistSongDTO dto = new ArtistSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setHasImage(rs.getBoolean("has_image"));
            dto.setAlbumHasImage(rs.getBoolean("album_has_image"));
            dto.setReleaseDate(formatDate(rs.getString("release_date")));
            dto.setCountry(rs.getString("country"));
            dto.setGenre(rs.getString("genre"));
            dto.setSubgenre(rs.getString("subgenre"));
            dto.setEthnicity(rs.getString("ethnicity"));
            dto.setLanguage(rs.getString("language"));
            dto.setFeaturedOn(true); // Mark as featured song
            dto.setPrimaryArtistId(rs.getInt("primary_artist_id"));
            dto.setPrimaryArtistName(rs.getString("primary_artist_name"));
            
            int albumId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : albumId);
            dto.setAlbumName(rs.getString("album_name"));
            
            int length = rs.getInt("length_seconds");
            dto.setLength(rs.wasNull() ? null : length);
            
            dto.setVatitoPlays(rs.getInt("vatito_plays"));
            dto.setRobertloverPlays(rs.getInt("robertlover_plays"));
            dto.setTotalPlays(rs.getInt("total_plays"));
            
            dto.setFirstListenedDate(formatDate(rs.getString("first_listen")));
            dto.setLastListenedDate(formatDate(rs.getString("last_listen")));
            
            dto.calculateTotalListeningTime();
            if (dto.getLength() != null && dto.getTotalPlays() != null) {
                dto.setTotalListeningTimeSeconds(dto.getLength() * dto.getTotalPlays());
            } else {
                dto.setTotalListeningTimeSeconds(0);
            }
            
            dto.setIsSingle(rs.getBoolean("is_single"));
            
            return dto;
        }, artistId);
    }
    
    /**
     * Get scrobbles for songs where this artist is featured
     */
    public List<ScrobbleDTO> getFeaturedScrobblesForArtist(int artistId, int page, int pageSize) {
        String sql = """
            SELECT sc.id, sc.scrobble_date, sc.account,
                   s.id as song_id, s.name as song_name,
                   ar.id as artist_id, ar.name as artist_name,
                   a.id as album_id, a.name as album_name
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            INNER JOIN Song s ON sc.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            WHERE sfa.artist_id = ?
            ORDER BY sc.scrobble_date DESC
            LIMIT ? OFFSET ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            ScrobbleDTO dto = new ScrobbleDTO();
            dto.setId(rs.getInt("id"));
            dto.setScrobbleDate(rs.getString("scrobble_date"));
            dto.setAccount(rs.getString("account"));
            dto.setSongId(rs.getInt("song_id"));
            dto.setSongName(rs.getString("song_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            Integer albumId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : albumId);
            dto.setAlbumName(rs.getString("album_name"));
            dto.setFromFeatured(true);
            return dto;
        }, artistId, pageSize, page * pageSize);
    }
    
    /**
     * Count scrobbles for songs where this artist is featured
     */
    public long countFeaturedScrobblesForArtist(int artistId) {
        String sql = """
            SELECT COUNT(*)
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            WHERE sfa.artist_id = ?
            """;
        Long count = jdbcTemplate.queryForObject(sql, Long.class, artistId);
        return count != null ? count : 0L;
    }
    
    /**
     * Get plays by year for songs where this artist is featured
     */
    public java.util.Map<Integer, Integer> getFeaturedPlaysByYear(int artistId) {
        String sql = """
            SELECT strftime('%Y', sc.scrobble_date) as year, COUNT(*) as count
            FROM Scrobble sc
            INNER JOIN SongFeaturedArtist sfa ON sc.song_id = sfa.song_id
            WHERE sfa.artist_id = ?
            GROUP BY year
            ORDER BY year
            """;
        
        java.util.Map<Integer, Integer> result = new java.util.LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            String yearStr = rs.getString("year");
            if (yearStr != null) {
                result.put(Integer.parseInt(yearStr), rs.getInt("count"));
            }
        }, artistId);
        return result;
    }
    
    // ============= RANKING METHODS =============
    
    /**
     * Get all rankings for an artist in a single query (optimized)
     * Returns a Map with keys: "gender", "genre", "subgenre", "ethnicity", "language", "country"
     */
    public java.util.Map<String, Integer> getAllArtistRankings(int artistId) {
        String sql = """
            WITH artist_play_counts AS (
                SELECT a.id, 
                       a.gender_id,
                       a.genre_id,
                       a.subgenre_id,
                       a.ethnicity_id,
                       a.language_id,
                       a.country,
                       COALESCE(COUNT(sc.id), 0) as play_count
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                GROUP BY a.id, a.gender_id, a.genre_id, a.subgenre_id, a.ethnicity_id, a.language_id, a.country
            ),
            ranked_artists AS (
                SELECT id,
                       gender_id,
                       genre_id,
                       subgenre_id,
                       ethnicity_id,
                       language_id,
                       country,
                       play_count,
                       CASE WHEN gender_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY gender_id ORDER BY play_count DESC) 
                            END as gender_rank,
                       CASE WHEN genre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY genre_id ORDER BY play_count DESC) 
                            END as genre_rank,
                       CASE WHEN subgenre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY subgenre_id ORDER BY play_count DESC) 
                            END as subgenre_rank,
                       CASE WHEN ethnicity_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY ethnicity_id ORDER BY play_count DESC) 
                            END as ethnicity_rank,
                       CASE WHEN language_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY language_id ORDER BY play_count DESC) 
                            END as language_rank,
                       CASE WHEN country IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY country ORDER BY play_count DESC) 
                            END as country_rank
                FROM artist_play_counts
            )
            SELECT gender_rank, genre_rank, subgenre_rank, ethnicity_rank, language_rank, country_rank
            FROM ranked_artists
            WHERE id = ?
            """;
        
        java.util.Map<String, Integer> rankings = new java.util.HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            Integer genderRank = (Integer) rs.getObject("gender_rank");
            Integer genreRank = (Integer) rs.getObject("genre_rank");
            Integer subgenreRank = (Integer) rs.getObject("subgenre_rank");
            Integer ethnicityRank = (Integer) rs.getObject("ethnicity_rank");
            Integer languageRank = (Integer) rs.getObject("language_rank");
            Integer countryRank = (Integer) rs.getObject("country_rank");
            
            if (genderRank != null) rankings.put("gender", genderRank);
            if (genreRank != null) rankings.put("genre", genreRank);
            if (subgenreRank != null) rankings.put("subgenre", subgenreRank);
            if (ethnicityRank != null) rankings.put("ethnicity", ethnicityRank);
            if (languageRank != null) rankings.put("language", languageRank);
            if (countryRank != null) rankings.put("country", countryRank);
        }, artistId);
        
        return rankings;
    }
    
    /**
     * Get artist rank by gender (position within same gender based on play count)
     * @deprecated Use getAllArtistRankings() instead for better performance
     */
    @Deprecated
    public Integer getArtistRankByGender(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (PARTITION BY a.gender_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE a.gender_id IS NOT NULL
                GROUP BY a.id, a.gender_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get artist rank by genre (position within same genre based on play count)
     */
    public Integer getArtistRankByGenre(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (PARTITION BY a.genre_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE a.genre_id IS NOT NULL
                GROUP BY a.id, a.genre_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get artist rank by subgenre (position within same subgenre based on play count)
     */
    public Integer getArtistRankBySubgenre(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (PARTITION BY a.subgenre_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE a.subgenre_id IS NOT NULL
                GROUP BY a.id, a.subgenre_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get artist rank by ethnicity (position within same ethnicity based on play count)
     */
    public Integer getArtistRankByEthnicity(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (PARTITION BY a.ethnicity_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE a.ethnicity_id IS NOT NULL
                GROUP BY a.id, a.ethnicity_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get artist rank by language (position within same language based on play count)
     */
    public Integer getArtistRankByLanguage(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (PARTITION BY a.language_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE a.language_id IS NOT NULL
                GROUP BY a.id, a.language_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get artist rank by country (position within same country based on play count)
     */
    public Integer getArtistRankByCountry(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (PARTITION BY a.country ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE a.country IS NOT NULL
                GROUP BY a.id, a.country
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get artist ranks by year (position within artists that had plays in each year)
     * Returns a map of year -> rank
     */
    public Map<Integer, Integer> getArtistRanksByYear(int artistId) {
        String sql = """
            SELECT year, rank FROM (
                SELECT a.id, 
                       strftime('%Y', sc.scrobble_date) as year,
                       ROW_NUMBER() OVER (PARTITION BY strftime('%Y', sc.scrobble_date) ORDER BY COUNT(sc.id) DESC) as rank
                FROM Artist a
                INNER JOIN Song s ON s.artist_id = a.id
                INNER JOIN Scrobble sc ON sc.song_id = s.id
                GROUP BY a.id, strftime('%Y', sc.scrobble_date)
            ) ranked
            WHERE id = ?
            ORDER BY year
            """;
        
        java.util.Map<Integer, Integer> result = new java.util.LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            String yearStr = rs.getString("year");
            if (yearStr != null) {
                result.put(Integer.parseInt(yearStr), rs.getInt("rank"));
            }
        }, artistId);
        return result;
    }

    /**
     * Get artist's overall position among all artists by play count.
     */
    public Integer getArtistOverallPosition(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                GROUP BY a.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get artist rank for Spanish Rap (artists with genre=Rap and language=Spanish).
     * This is a special combined ranking for a subculture/scene.
     */
    public Integer getArtistSpanishRapRank(int artistId) {
        String sql = """
            SELECT rank FROM (
                SELECT a.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Artist a
                INNER JOIN Genre g ON a.genre_id = g.id
                INNER JOIN Language l ON a.language_id = l.id
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE g.name = 'Rap' AND l.name = 'Spanish'
                GROUP BY a.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Check if an artist is in the Spanish Rap category (genre=Rap AND language=Spanish)
     */
    public boolean isArtistSpanishRap(int artistId) {
        String sql = """
            SELECT COUNT(*) FROM Artist a
            INNER JOIN Genre g ON a.genre_id = g.id
            INNER JOIN Language l ON a.language_id = l.id
            WHERE a.id = ? AND g.name = 'Rap' AND l.name = 'Spanish'
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public List<Map<String, Object>> getArtistDetailsForIds(List<Integer> ids) {
        if (ids == null || ids.isEmpty()) return new ArrayList<>();
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        String sql = "SELECT id, name, gender_id as genderId FROM Artist WHERE id IN (" + placeholders + ")";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> artist = new HashMap<>();
            artist.put("id", rs.getInt("id"));
            artist.put("name", rs.getString("name"));
            artist.put("genderId", rs.getObject("genderId") != null ? rs.getInt("genderId") : null);
            return artist;
        }, ids.toArray());
    }

    /**
     * Update artist metadata from external source (MusicBrainz).
     * Only updates fields that are provided (non-null).
     */
    public void updateArtistMetadata(Long artistId, java.time.LocalDate birthDate, 
                                      java.time.LocalDate deathDate, String country, Boolean isBand) {
        StringBuilder sql = new StringBuilder("UPDATE Artist SET ");
        List<Object> params = new ArrayList<>();
        List<String> updates = new ArrayList<>();
        
        if (birthDate != null) {
            updates.add("birth_date = ?");
            params.add(birthDate.toString()); // Store as ISO string: YYYY-MM-DD
        }
        
        if (deathDate != null) {
            updates.add("death_date = ?");
            params.add(deathDate.toString()); // Store as ISO string: YYYY-MM-DD
        }
        
        if (country != null && !country.trim().isEmpty()) {
            updates.add("country = ?");
            params.add(country.trim());
        }
        
        if (isBand != null) {
            updates.add("is_band = ?");
            params.add(isBand ? 1 : 0);
        }
        
        if (updates.isEmpty()) {
            return; // Nothing to update
        }
        
        sql.append(String.join(", ", updates));
        sql.append(" WHERE id = ?");
        params.add(artistId);
        
        jdbcTemplate.update(sql.toString(), params.toArray());
    }
}
