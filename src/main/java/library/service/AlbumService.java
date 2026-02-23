package library.service;

import library.dto.AlbumCardDTO;
import library.dto.AlbumSongDTO;
import library.dto.FeaturedArtistCardDTO;
import library.dto.GenderCountDTO;
import library.dto.PlaysByYearDTO;
import library.dto.PlaysByMonthDTO;
import library.dto.PlayDTO;
import library.entity.Album;
import library.entity.AlbumImage;
import library.repository.AlbumImageRepository;
import library.repository.AlbumRepository;
import library.repository.LookupRepository;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

@Service
public class AlbumService {
    
    private final AlbumRepository albumRepository;
    private final AlbumImageRepository albumImageRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    private final ItunesService itunesService;
    
    public AlbumService(AlbumRepository albumRepository, AlbumImageRepository albumImageRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate, ItunesService itunesService) {
        this.albumRepository = albumRepository;
        this.albumImageRepository = albumImageRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.itunesService = itunesService;
    }
    
    public List<AlbumCardDTO> getAlbums(String name, String artistName,
                                         List<Integer> genreIds, String genreMode,
                                         List<Integer> subgenreIds, String subgenreMode,
                                         List<Integer> languageIds, String languageMode,
                                         List<Integer> genderIds, String genderMode,
                                         List<Integer> ethnicityIds, String ethnicityMode,
                                         List<String> countries, String countryMode,
                                         List<String> accounts, String accountMode,
                                         String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                         String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                         String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                         String listenedDateFrom, String listenedDateTo,
                                         String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand,
                                         Integer ageMin, Integer ageMax, String ageMode,
                                         Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                         String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                         String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                         String inItunes,
                                         Integer playCountMin, Integer playCountMax, Integer songCountMin, Integer songCountMax,
                                         Integer lengthMin, Integer lengthMax, String lengthMode,
                                         Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                         Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                         Integer yearlyChartPeak, Integer yearlyChartYears,
                                         String sortBy, String sortDir, int page, int perPage) {
        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (accounts != null && accounts.isEmpty()) accounts = null;
        
        // If inItunes filter is active, we need to get all results first, then filter in memory
        boolean filterByItunes = inItunes != null && !inItunes.isEmpty();
        int actualLimit = filterByItunes ? Integer.MAX_VALUE : perPage;
        int actualOffset = filterByItunes ? 0 : page * perPage;
        
        List<Object[]> results = albumRepository.findAlbumsWithStats(
                name, artistName, genreIds, genreMode, 
                subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                ethnicityIds, ethnicityMode, countries, countryMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode,
                ageAtReleaseMin, ageAtReleaseMax,
                birthDate, birthDateFrom, birthDateTo, birthDateMode,
                deathDate, deathDateFrom, deathDateTo, deathDateMode,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears,
                sortBy, sortDir, actualLimit, actualOffset
        );
        
        List<AlbumCardDTO> albums = new ArrayList<>();
        for (Object[] row : results) {
            AlbumCardDTO dto = new AlbumCardDTO();
            dto.setId(((Number) row[0]).intValue());
            dto.setName((String) row[1]);
            dto.setArtistName((String) row[2]);
            dto.setArtistId(((Number) row[3]).intValue());
            dto.setGenreId(row[4] != null ? ((Number) row[4]).intValue() : null);
            dto.setGenreName((String) row[5]);
            dto.setSubgenreId(row[6] != null ? ((Number) row[6]).intValue() : null);
            dto.setSubgenreName((String) row[7]);
            dto.setLanguageId(row[8] != null ? ((Number) row[8]).intValue() : null);
            dto.setLanguageName((String) row[9]);
            dto.setEthnicityId(row[10] != null ? ((Number) row[10]).intValue() : null);
            dto.setEthnicityName((String) row[11]);
            dto.setReleaseYear((String) row[12]);
            dto.setReleaseDate(row[13] != null ? formatDate((String) row[13]) : null);
            dto.setSongCount(row[14] != null ? ((Number) row[14]).intValue() : 0);
            
            // Set album length and format it (index 15)
            long albumLength = row[15] != null ? ((Number) row[15]).longValue() : 0L;
            dto.setAlbumLength(albumLength);
            dto.setAlbumLengthFormatted(TimeFormatUtils.formatTimeHMS(albumLength));
            
            dto.setHasImage(row[16] != null && ((Number) row[16]).intValue() == 1);
            dto.setGenderName((String) row[17]);
            dto.setPlayCount(row[18] != null ? ((Number) row[18]).intValue() : 0);
            dto.setVatitoPlayCount(row[19] != null ? ((Number) row[19]).intValue() : 0);
            dto.setRobertloverPlayCount(row[20] != null ? ((Number) row[20]).intValue() : 0);
            
            // Set time listened and format it
            long timeListened = row[21] != null ? ((Number) row[21]).longValue() : 0L;
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            
            // Set first and last listened dates (indices 22 and 23)
            dto.setFirstListenedDate(row[22] != null ? formatDate((String) row[22]) : null);
            dto.setLastListenedDate(row[23] != null ? formatDate((String) row[23]) : null);
            
            // Set country (inherited from artist, index 24)
            dto.setCountry((String) row[24]);
            
            // Set organized (index 25)
            dto.setOrganized(row[25] != null && ((Number) row[25]).intValue() == 1);
            
            // Set birth date (index 26) and death date (index 27)
            dto.setBirthDate(row[26] != null ? formatDate((String) row[26]) : null);
            dto.setDeathDate(row[27] != null ? formatDate((String) row[27]) : null);
            
            // Set image count (index 28)
            dto.setImageCount(row[28] != null ? ((Number) row[28]).intValue() : 0);
            
            // Set chart stats (indices 29-32)
            dto.setSeasonalChartPeak(row[29] != null ? ((Number) row[29]).intValue() : null);
            dto.setWeeklyChartPeak(row[30] != null ? ((Number) row[30]).intValue() : null);
            dto.setWeeklyChartWeeks(row[31] != null ? ((Number) row[31]).intValue() : 0);
            dto.setYearlyChartPeak(row[32] != null ? ((Number) row[32]).intValue() : null);
            dto.setWeeklyChartPeakStartDate(row[33] != null ? formatDate((String) row[33]) : null);
            dto.setSeasonalChartPeakPeriod((String) row[34]);
            dto.setYearlyChartPeakPeriod((String) row[35]);

            // Compute average plays and average length
            int sc = dto.getSongCount();
            dto.setAvgPlays(sc > 0 ? (double) dto.getPlayCount() / sc : null);
            dto.setAvgLengthFormatted(sc > 0 ? TimeFormatUtils.formatTimeHMS(dto.getAlbumLength() / sc) : null);

            // Set featured artist count (index 37), solo song count (index 38), songs with feat count (index 39), age at release (index 40)
            dto.setFeaturedArtistCount(row[37] != null ? ((Number) row[37]).intValue() : 0);
            dto.setSoloSongCount(row[38] != null ? ((Number) row[38]).intValue() : 0);
            dto.setSongsWithFeatCount(row[39] != null ? ((Number) row[39]).intValue() : 0);
            dto.setAgeAtRelease(row[40] != null ? ((Number) row[40]).intValue() : null);
            
            // Set peak durations (indices 41-43)
            dto.setWeeklyChartPeakWeeks(row[41] != null ? ((Number) row[41]).intValue() : null);
            dto.setSeasonalChartPeakSeasons(row[42] != null ? ((Number) row[42]).intValue() : null);
            dto.setYearlyChartPeakYears(row[43] != null ? ((Number) row[43]).intValue() : null);

            // Check iTunes presence for badge display
            dto.setInItunes(itunesService.albumExistsInItunes(dto.getArtistName(), dto.getName()));
            
            albums.add(dto);
        }
        
        // Apply iTunes filter if needed
        if (filterByItunes) {
            boolean wantInItunes = "true".equalsIgnoreCase(inItunes);
            albums = albums.stream()
                    .filter(a -> a.getInItunes() != null && a.getInItunes() == wantInItunes)
                    .toList();
            
            // Apply pagination manually
            int offset = page * perPage;
            int end = Math.min(offset + perPage, albums.size());
            if (offset >= albums.size()) {
                albums = new ArrayList<>();
            } else {
                albums = new ArrayList<>(albums.subList(offset, end));
            }
        }
        
        return albums;
    }
    
    public long countAlbums(String name, String artistName,
                           List<Integer> genreIds, String genreMode,
                           List<Integer> subgenreIds, String subgenreMode,
                           List<Integer> languageIds, String languageMode,
                           List<Integer> genderIds, String genderMode,
                           List<Integer> ethnicityIds, String ethnicityMode,
                           List<String> countries, String countryMode,
                           List<String> accounts, String accountMode,
                           String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                           String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                           String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                           String listenedDateFrom, String listenedDateTo,
                           String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand,
                           Integer ageMin, Integer ageMax, String ageMode,
                           Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                           String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                           String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                           String inItunes,
                           Integer playCountMin, Integer playCountMax, Integer songCountMin, Integer songCountMax,
                           Integer lengthMin, Integer lengthMax, String lengthMode,
                           Integer weeklyChartPeak, Integer weeklyChartWeeks,
                           Integer seasonalChartPeak, Integer seasonalChartSeasons,
                           Integer yearlyChartPeak, Integer yearlyChartYears) {
        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (accounts != null && accounts.isEmpty()) accounts = null;
        
        // If inItunes filter is active, we need to count manually
        if (inItunes != null && !inItunes.isEmpty()) {
            List<AlbumCardDTO> allAlbums = getAlbums(name, artistName, genreIds, genreMode,
                    subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                    ethnicityIds, ethnicityMode, countries, countryMode, accounts, accountMode,
                    releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                    firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                    lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                    listenedDateFrom, listenedDateTo,
                    organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                    ageMin, ageMax, ageMode,
                    ageAtReleaseMin, ageAtReleaseMax,
                    birthDate, birthDateFrom, birthDateTo, birthDateMode,
                    deathDate, deathDateFrom, deathDateTo, deathDateMode,
                    inItunes,
                    playCountMin, playCountMax, songCountMin, songCountMax,
                    lengthMin, lengthMax, lengthMode,
                    weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears,
                    "plays", "desc", 0, Integer.MAX_VALUE);
            return allAlbums.size();
        }
        
        return albumRepository.countAlbumsWithFilters(name, artistName, 
                genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                genderIds, genderMode, ethnicityIds, ethnicityMode, countries, countryMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode,
                ageAtReleaseMin, ageAtReleaseMax,
                birthDate, birthDateFrom, birthDateTo, birthDateMode,
                deathDate, deathDateFrom, deathDateTo, deathDateMode,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears);
    }
    
    /**
     * Count albums by gender for the filtered dataset.
     * Returns a GenderCountDTO with male, female, and other counts.
     * Uses efficient SQL GROUP BY instead of loading all records.
     */
    public GenderCountDTO countAlbumsByGender(String name, String artistName,
                           List<Integer> genreIds, String genreMode,
                           List<Integer> subgenreIds, String subgenreMode,
                           List<Integer> languageIds, String languageMode,
                           List<Integer> genderIds, String genderMode,
                           List<Integer> ethnicityIds, String ethnicityMode,
                           List<String> countries, String countryMode,
                           List<String> accounts, String accountMode,
                           String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                           String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                           String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                           String listenedDateFrom, String listenedDateTo,
                           String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand,
                           Integer ageMin, Integer ageMax, String ageMode,
                           Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                           String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                           String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                           String inItunes,
                           Integer playCountMin, Integer playCountMax, Integer songCountMin, Integer songCountMax,
                           Integer lengthMin, Integer lengthMax, String lengthMode,
                           Integer weeklyChartPeak, Integer weeklyChartWeeks,
                           Integer seasonalChartPeak, Integer seasonalChartSeasons,
                           Integer yearlyChartPeak, Integer yearlyChartYears) {
        // Normalize empty lists to null
        if (accounts != null && accounts.isEmpty()) accounts = null;
        
        // Use efficient SQL-based counting with GROUP BY
        Map<Integer, Long> genderCounts = albumRepository.countAlbumsByGenderWithFilters(
                name, artistName, genreIds, genreMode,
                subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                ethnicityIds, ethnicityMode, countries, countryMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode,
                ageAtReleaseMin, ageAtReleaseMax,
                birthDate, birthDateFrom, birthDateTo, birthDateMode,
                deathDate, deathDateFrom, deathDateTo, deathDateMode,
                inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears);
        
        // Gender ID 1 = Female, Gender ID 2 = Male
        long femaleCount = genderCounts.getOrDefault(1, 0L);
        long maleCount = genderCounts.getOrDefault(2, 0L);
        long otherCount = 0L;
        
        // Sum up all other gender IDs (including null)
        for (Map.Entry<Integer, Long> entry : genderCounts.entrySet()) {
            Integer genderId = entry.getKey();
            if (genderId == null || (genderId != 1 && genderId != 2)) {
                otherCount += entry.getValue();
            }
        }
        
        return new GenderCountDTO(maleCount, femaleCount, otherCount);
    }
    
    public Optional<Album> getAlbumById(Integer id) {
        String sql = """
            SELECT a.id, a.artist_id, a.name, a.release_date, a.number_of_songs, 
                   a.override_genre_id, a.override_subgenre_id, a.override_language_id,
                   a.organized, a.creation_date, a.update_date,
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
            
            int organized = rs.getInt("organized");
            album.setOrganized(rs.wasNull() ? null : organized == 1);
            
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

    /**
     * Find an album release_date (YYYY-MM-DD) by album name and artist name.
     * Returns the date string (first 10 chars) or null if not found.
     */
    public String findAlbumReleaseDateByNameAndArtist(String albumName, String artistName) {
        if (albumName == null || albumName.trim().isEmpty() || artistName == null || artistName.trim().isEmpty()) {
            return null;
        }

        String sql = "SELECT a.release_date FROM Album a INNER JOIN Artist ar ON a.artist_id = ar.id WHERE lower(a.name) = lower(?) AND lower(ar.name) = lower(?) LIMIT 1";
        try {
            String releaseDate = jdbcTemplate.queryForObject(sql, String.class, albumName.trim(), artistName.trim());
            // Normalize to YYYY-MM-DD if possible
            if (releaseDate != null && releaseDate.contains("T")) releaseDate = releaseDate.replace('T', ' ');
            return releaseDate != null && releaseDate.length() >= 10 ? releaseDate.substring(0, 10) : releaseDate;
        } catch (Exception e) {
            return null;
        }
    }
    
    public Album saveAlbum(Album album) {
        // Get the old album name to check if it changed
        String oldName = jdbcTemplate.queryForObject(
            "SELECT name FROM Album WHERE id = ?", String.class, album.getId());
        
        String sql = """
            UPDATE Album 
            SET name = ?, artist_id = ?, release_date = ?, 
                override_genre_id = ?, override_subgenre_id = ?, override_language_id = ?,
                organized = ?, update_date = CURRENT_TIMESTAMP
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
            album.getOrganized() != null && album.getOrganized() ? 1 : 0,
            album.getId()
        );
        
        // If album name changed, update all associated plays and try to match unmatched ones
        if (oldName != null && !oldName.equals(album.getName())) {
            updatePlaysForAlbumNameChange(album.getId(), album.getName());
            // Only try to match unmatched plays if name changed (expensive operation)
            tryMatchUnmatchedPlaysForAlbum(album.getId(), album.getName());
        }
        
        return album;
    }
    
    /**
     * Update the album name in all plays for songs belonging to this album
     */
    private void updatePlaysForAlbumNameChange(int albumId, String newAlbumName) {
        String sql = """
            UPDATE Play 
            SET album = ?
            WHERE song_id IN (SELECT id FROM Song WHERE album_id = ?)
            """;
        jdbcTemplate.update(sql, newAlbumName, albumId);
    }
    
    /**
     * Try to match unmatched plays to songs in this album.
     * For each song in the album, look for unmatched plays that match
     * the artist name, album name, and song name.
     */
    private void tryMatchUnmatchedPlaysForAlbum(int albumId, String albumName) {
        // Get artist name for this album
        String artistName;
        try {
            artistName = jdbcTemplate.queryForObject(
                "SELECT a.name FROM Artist a JOIN Album al ON a.id = al.artist_id WHERE al.id = ?",
                String.class, albumId);
        } catch (Exception e) {
            return; // Can't match without artist name (no result or error)
        }
        
        // Get all songs in this album and try to match unmatched plays
        String sql = """
            UPDATE Play 
            SET song_id = (
                SELECT s.id FROM Song s 
                WHERE s.album_id = ? 
                AND LOWER(s.name) = LOWER(Play.song)
            ),
            artist = ?,
            album = ?
            WHERE song_id IS NULL
            AND LOWER(COALESCE(artist, '')) = LOWER(?)
            AND LOWER(COALESCE(album, '')) = LOWER(?)
            AND EXISTS (
                SELECT 1 FROM Song s 
                WHERE s.album_id = ? 
                AND LOWER(s.name) = LOWER(Play.song)
            )
            """;
        
        jdbcTemplate.update(sql, albumId, artistName, albumName, artistName, albumName, albumId);
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
    
    // Gallery methods for secondary images
    public List<AlbumImage> getSecondaryImages(Integer albumId) {
        return albumImageRepository.findByAlbumIdOrderByDisplayOrderAsc(albumId);
    }

    public int getSecondaryImageCount(Integer albumId) {
        return albumImageRepository.countByAlbumId(albumId);
    }

    public byte[] getSecondaryImage(Integer imageId) {
        return albumImageRepository.findById(imageId)
                .map(AlbumImage::getImage)
                .orElse(null);
    }

    /**
     * Check if an image already exists for this album (either as primary or in gallery).
     * Uses byte length first for speed, then compares actual data.
     */
    public boolean isDuplicateImage(Integer albumId, byte[] imageData) {
        if (imageData == null || imageData.length == 0) return false;
        
        // Check against primary image
        byte[] primaryImage = getAlbumImage(albumId);
        if (primaryImage != null && primaryImage.length == imageData.length && java.util.Arrays.equals(primaryImage, imageData)) {
            return true;
        }
        
        // Check against gallery images
        List<AlbumImage> galleryImages = albumImageRepository.findByAlbumIdOrderByDisplayOrderAsc(albumId);
        for (AlbumImage existing : galleryImages) {
            byte[] existingData = existing.getImage();
            if (existingData != null && existingData.length == imageData.length && java.util.Arrays.equals(existingData, imageData)) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Add a secondary image to the album gallery.
     * @return true if image was added, false if it was a duplicate and skipped
     */
    public boolean addSecondaryImage(Integer albumId, byte[] imageData) {
        // Check for duplicates first
        if (isDuplicateImage(albumId, imageData)) {
            return false; // Skip duplicate
        }
        
        Integer maxOrder = albumImageRepository.getMaxDisplayOrder(albumId);
        AlbumImage image = new AlbumImage();
        image.setAlbumId(albumId);
        image.setImage(imageData);
        image.setDisplayOrder(maxOrder + 1);
        image.setCreationDate(new java.sql.Timestamp(System.currentTimeMillis()));
        albumImageRepository.save(image);
        return true;
    }

    @Transactional
    public void deleteSecondaryImage(Integer imageId) {
        albumImageRepository.deleteById(imageId);
    }

    @Transactional
    public void swapToDefault(Integer albumId, Integer imageId) {
        // Get the current default image from the main Album table
        byte[] currentDefault = getAlbumImage(albumId);

        // Get the secondary image to promote
        AlbumImage secondaryImage = albumImageRepository.findById(imageId)
                .orElseThrow(() -> new IllegalArgumentException("Image not found: " + imageId));

        // Set the secondary image as the new default
        updateAlbumImage(albumId, secondaryImage.getImage());

        // If there was a previous default, move it to secondary images
        if (currentDefault != null && currentDefault.length > 0) {
            // Update the secondary image record with the old default
            secondaryImage.setImage(currentDefault);
            albumImageRepository.save(secondaryImage);
        } else {
            // No previous default, just delete the secondary record
            albumImageRepository.deleteById(imageId);
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
    
    /**
     * Get only the distinct countries that exist in the Artist table (for filters)
     */
    public List<String> getCountries() {
        String sql = "SELECT DISTINCT country FROM Artist WHERE country IS NOT NULL AND country != '' ORDER BY country";
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
    
    public String getArtistCountry(Integer artistId) {
        String sql = "SELECT country FROM Artist WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    public String getArtistEthnicityName(Integer artistId) {
        String sql = "SELECT e.name FROM Artist a JOIN Ethnicity e ON a.ethnicity_id = e.id WHERE a.id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    // NEW: total plays for an album (count plays for songs in this album)
    public int getPlayCountForAlbum(int albumId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(p.id) FROM Play p JOIN Song s ON p.song_id = s.id WHERE s.album_id = ?",
                Integer.class, albumId);
        return count != null ? count : 0;
    }

    // Get vatito (primary) play count for album
    public int getVatitoPlayCountForAlbum(int albumId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(p.id) FROM Play p JOIN Song s ON p.song_id = s.id WHERE s.album_id = ? AND p.account = 'vatito'",
                Integer.class, albumId);
        return count != null ? count : 0;
    }
    
    // Get robertlover (legacy) play count for album
    public int getRobertloverPlayCountForAlbum(int albumId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(p.id) FROM Play p JOIN Song s ON p.song_id = s.id WHERE s.album_id = ? AND p.account = 'robertlover'",
                Integer.class, albumId);
        return count != null ? count : 0;
    }

    // Return a string with per-account play counts for this album (e.g. "lastfm: 12\nspotify: 3\n")
    public String getPlaysByAccountForAlbum(int albumId) {
        String sql = "SELECT p.account, COUNT(*) as cnt FROM Play p JOIN Song s ON p.song_id = s.id WHERE s.album_id = ? GROUP BY p.account ORDER BY cnt DESC";
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
                COALESCE(s.release_date, alb.release_date) as release_date,
                ar.country,
                COALESCE(g_song.name, g_album.name, g_artist.name) as genre,
                COALESCE(sg_song.name, sg_album.name, sg_artist.name) as subgenre,
                COALESCE(eth_song.name, eth_artist.name) as ethnicity,
                COALESCE(l_song.name, l_album.name, l_artist.name) as language,
                COALESCE(SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END), 0) as vatito_plays,
                COALESCE(SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END), 0) as robertlover_plays,
                COUNT(p.id) as total_plays,
                MIN(p.play_date) as first_listen,
                MAX(p.play_date) as last_listen,
                s.is_single
            FROM Song s
            INNER JOIN Album alb ON s.album_id = alb.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Play p ON s.id = p.song_id
            LEFT JOIN Genre g_song ON s.override_genre_id = g_song.id
            LEFT JOIN Genre g_album ON alb.override_genre_id = g_album.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_song ON s.override_subgenre_id = sg_song.id
            LEFT JOIN SubGenre sg_album ON alb.override_subgenre_id = sg_album.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_song ON s.override_language_id = l_song.id
            LEFT JOIN Language l_album ON alb.override_language_id = l_album.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth_song ON s.override_ethnicity_id = eth_song.id
            LEFT JOIN Ethnicity eth_artist ON ar.ethnicity_id = eth_artist.id
            WHERE s.album_id = ?
            GROUP BY s.id, s.name, s.length_seconds, COALESCE(s.release_date, alb.release_date), ar.country, 
                     COALESCE(g_song.name, g_album.name, g_artist.name), 
                     COALESCE(sg_song.name, sg_album.name, sg_artist.name), 
                     COALESCE(eth_song.name, eth_artist.name), 
                     COALESCE(l_song.name, l_album.name, l_artist.name),
                     s.is_single
            ORDER BY s.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            AlbumSongDTO dto = new AlbumSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setReleaseDate(formatDate(rs.getString("release_date")));
            dto.setCountry(rs.getString("country"));
            dto.setGenre(rs.getString("genre"));
            dto.setSubgenre(rs.getString("subgenre"));
            dto.setEthnicity(rs.getString("ethnicity"));
            dto.setLanguage(rs.getString("language"));
            
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
            if (dto.getLength() != null && dto.getTotalPlays() != null) {
                dto.setTotalListeningTimeSeconds(dto.getLength() * dto.getTotalPlays());
            } else {
                dto.setTotalListeningTimeSeconds(0);
            }
            
            // Set isSingle
            dto.setIsSingle(rs.getBoolean("is_single"));
            
            return dto;
        }, albumId);
    }
    
    // Get formatted album length (sum of all song lengths in MM:SS or HH:MM:SS format)
    public String getAlbumLengthFormatted(int albumId) {
        String sql = "SELECT SUM(length_seconds) FROM Song WHERE album_id = ?";
        Long totalSeconds = jdbcTemplate.queryForObject(sql, Long.class, albumId);
        if (totalSeconds == null || totalSeconds == 0) {
            return null;
        }
        return TimeFormatUtils.formatTimeHMS(totalSeconds);
    }
    
    // Get album length in seconds (for sorting/calculations)
    public Long getAlbumLengthSeconds(int albumId) {
        String sql = "SELECT SUM(length_seconds) FROM Song WHERE album_id = ?";
        return jdbcTemplate.queryForObject(sql, Long.class, albumId);
    }
    
    // Get average song length for an album (formatted as mm:ss)
    public String getAverageSongLengthFormatted(int albumId) {
        String sql = "SELECT AVG(length_seconds) FROM Song WHERE album_id = ? AND length_seconds IS NOT NULL";
        Double avgSeconds = jdbcTemplate.queryForObject(sql, Double.class, albumId);
        if (avgSeconds == null || avgSeconds == 0) {
            return "-";
        }
        int totalSeconds = (int) Math.round(avgSeconds);
        int minutes = totalSeconds / 60;
        int seconds = totalSeconds % 60;
        return String.format("%d:%02d", minutes, seconds);
    }
    
    // Get average plays per song for an album
    public String getAveragePlaysPerSong(int albumId) {
        String sql = """
            SELECT AVG(play_count) FROM (
                SELECT COALESCE(COUNT(p.id), 0) as play_count
                FROM Song s
                LEFT JOIN Play p ON s.id = p.song_id
                WHERE s.album_id = ?
                GROUP BY s.id
            )
            """;
        try {
            Double avgPlays = jdbcTemplate.queryForObject(sql, Double.class, albumId);
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
    
    // Calculate total listening time for an album (sum of all songs' listening time)
    public String getTotalListeningTimeForAlbum(int albumId) {
        String sql = """
            SELECT 
                SUM(s.length_seconds * COALESCE(play_count, 0)) as total_seconds
            FROM Song s
            LEFT JOIN (
                SELECT song_id, COUNT(*) as play_count
                FROM Play
                GROUP BY song_id
            ) p ON s.id = p.song_id
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
    
    // Get first listened date for an album (earliest play)
    public String getFirstListenedDateForAlbum(int albumId) {
        String sql = """
            SELECT MIN(p.play_date)
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ?
            """;
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, albumId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get first listened date for an album as LocalDate (for calculations)
    public java.time.LocalDate getFirstListenedDateAsLocalDateForAlbum(int albumId) {
        String sql = """
            SELECT MIN(DATE(p.play_date))
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ?
            """;
        
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, albumId);
            return dateStr != null ? java.time.LocalDate.parse(dateStr) : null;
        } catch (Exception e) {
            return null;
        }
    }

    // Get last listened date for an album (most recent play)
    public String getLastListenedDateForAlbum(int albumId) {
        String sql = """
            SELECT MAX(p.play_date)
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ?
            """;
        
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, albumId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get unique days played for an album
    public int getUniqueDaysPlayedForAlbum(int albumId) {
        String sql = """
            SELECT COUNT(DISTINCT DATE(p.play_date))
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ? AND p.play_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, albumId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique weeks played for an album
    public int getUniqueWeeksPlayedForAlbum(int albumId) {
        String sql = """
            SELECT COUNT(DISTINCT strftime('%Y-%W', p.play_date))
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ? AND p.play_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, albumId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique months played for an album
    public int getUniqueMonthsPlayedForAlbum(int albumId) {
        String sql = """
            SELECT COUNT(DISTINCT strftime('%Y-%m', p.play_date))
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ? AND p.play_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, albumId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique years played for an album
    public int getUniqueYearsPlayedForAlbum(int albumId) {
        String sql = """
            SELECT COUNT(DISTINCT strftime('%Y', p.play_date))
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ? AND p.play_date IS NOT NULL
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, albumId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get earliest release date for an album (from album or songs)
    public String getEarliestReleaseDateForAlbum(int albumId) {
        String sql = """
            SELECT COALESCE(MIN(s.release_date), a.release_date)
            FROM Album a
            LEFT JOIN Song s ON s.album_id = a.id AND s.release_date IS NOT NULL
            WHERE a.id = ?
            GROUP BY a.release_date
            """;
        try {
            return jdbcTemplate.queryForObject(sql, String.class, albumId);
        } catch (Exception e) {
            return null;
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
        String sql = """
            SELECT a.id, a.name, 
                   a.override_genre_id, g.name as override_genre_name,
                   a.override_subgenre_id, sg.name as override_subgenre_name,
                   a.override_language_id, l.name as override_language_name
            FROM Album a
            LEFT JOIN Genre g ON a.override_genre_id = g.id
            LEFT JOIN SubGenre sg ON a.override_subgenre_id = sg.id
            LEFT JOIN Language l ON a.override_language_id = l.id
            WHERE a.artist_id = ? 
            ORDER BY a.name
            """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> album = new java.util.HashMap<>();
            album.put("id", rs.getInt("id"));
            album.put("name", rs.getString("name"));
            album.put("overrideGenreId", rs.getObject("override_genre_id"));
            album.put("overrideGenreName", rs.getString("override_genre_name"));
            album.put("overrideSubgenreId", rs.getObject("override_subgenre_id"));
            album.put("overrideSubgenreName", rs.getString("override_subgenre_name"));
            album.put("overrideLanguageId", rs.getObject("override_language_id"));
            album.put("overrideLanguageName", rs.getString("override_language_name"));
            return album;
        }, artistId);
    }
    
    /**
     * Search albums by name or artist name for chart editor.
     */
    public List<Map<String, Object>> searchAlbums(String query, int limit) {
        if (query == null || query.trim().isEmpty()) {
            return new ArrayList<>();
        }
        String normalized = library.util.StringNormalizer.normalizeForSearch(query);
        String searchTerm = "%" + normalized + "%";

        String sql = "SELECT a.id, a.name, ar.name as artist_name, "
            + "CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image "
            + "FROM Album a "
            + "JOIN Artist ar ON a.artist_id = ar.id "
            + "WHERE " + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ? OR " + library.util.StringNormalizer.sqlNormalizeColumn("ar.name") + " LIKE ? "
            + "ORDER BY a.name";

        boolean applyLimit = limit > 0;
        if (applyLimit) {
            sql = sql + " LIMIT ?";
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> album = new java.util.HashMap<>();
            album.put("id", rs.getInt("id"));
            album.put("name", rs.getString("name"));
            album.put("artistName", rs.getString("artist_name"));
            album.put("hasImage", rs.getInt("has_image") == 1);
            return album;
        }, applyLimit ? new Object[]{searchTerm, searchTerm, limit} : new Object[]{searchTerm, searchTerm});
    }
    
    // Find album by ID
    public Album findById(Integer id) {
        if (id == null) return null;
        String sql = "SELECT id, artist_id, name, release_date, override_genre_id, override_subgenre_id, override_language_id, number_of_songs FROM Album WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Album a = new Album();
                a.setId(rs.getInt("id"));
                a.setArtistId(rs.getInt("artist_id"));
                a.setName(rs.getString("name"));
                a.setReleaseDate(parseDate(rs.getString("release_date")));
                a.setOverrideGenreId(rs.getObject("override_genre_id") != null ? rs.getInt("override_genre_id") : null);
                a.setOverrideSubgenreId(rs.getObject("override_subgenre_id") != null ? rs.getInt("override_subgenre_id") : null);
                a.setOverrideLanguageId(rs.getObject("override_language_id") != null ? rs.getInt("override_language_id") : null);
                return a;
            }, id);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }
    
    // Create album from map (for API)
    public Integer createAlbum(java.util.Map<String, Object> data) {
        Album album = new Album();
        album.setName((String) data.get("name"));
        if (data.get("artistId") != null) {
            album.setArtistId(((Number) data.get("artistId")).intValue());
        }
        if (data.get("releaseDate") != null) {
            // Convert dd/MM/yyyy to yyyy-MM-dd for parsing
            String dateStr = (String) data.get("releaseDate");
            if (dateStr != null && dateStr.matches("\\d{2}/\\d{2}/\\d{4}")) {
                String[] parts = dateStr.split("/");
                String isoDate = parts[2] + "-" + parts[1] + "-" + parts[0];
                try {
                    album.setReleaseDate(java.sql.Date.valueOf(isoDate));
                } catch (Exception ignore) {}
            }
        }
        Album created = createAlbum(album);
        return created.getId();
    }

    // Helper method to format date strings (from play_date)
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
    
    // Get plays for an album with pagination
    public List<PlayDTO> getPlaysForAlbum(int albumId, int page, int pageSize) {
        int offset = page * pageSize;
        String sql = """
            SELECT 
                p.id,
                p.play_date,
                s.name as song_name,
                s.id as song_id,
                a.name as album_name,
                a.id as album_id,
                ar.name as artist_name,
                ar.id as artist_id,
                p.account
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            WHERE a.id = ?
            ORDER BY p.play_date DESC
            LIMIT ? OFFSET ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlayDTO dto = new PlayDTO();
            dto.setId(rs.getInt("id"));
            dto.setPlayDate(rs.getString("play_date"));
            dto.setSongName(rs.getString("song_name"));
            dto.setSongId(rs.getInt("song_id"));
            dto.setAlbumName(rs.getString("album_name"));
            int aId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : aId);
            dto.setArtistName(rs.getString("artist_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setAccount(rs.getString("account"));
            return dto;
        }, albumId, pageSize, offset);
    }
    
    // Count total plays for an album
    public long countPlaysForAlbum(int albumId) {
        String sql = """
            SELECT COUNT(*)
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ?
            """;
        Long count = jdbcTemplate.queryForObject(sql, Long.class, albumId);
        return count != null ? count : 0;
    }
    
    // Get plays by year for an album
    public List<PlaysByYearDTO> getPlaysByYearForAlbum(int albumId) {
        String sql = """
            SELECT 
                strftime('%Y', p.play_date) as year,
                COUNT(*) as play_count
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ? AND p.play_date IS NOT NULL
            GROUP BY strftime('%Y', p.play_date)
            ORDER BY year ASC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlaysByYearDTO dto = new PlaysByYearDTO();
            dto.setYear(rs.getString("year"));
            dto.setPlayCount(rs.getLong("play_count"));
            return dto;
        }, albumId);
    }
    
    // Get plays by month for an album
    public List<PlaysByMonthDTO> getPlaysByMonthForAlbum(int albumId) {
        String sql = """
            SELECT 
                strftime('%Y', p.play_date) as year,
                strftime('%m', p.play_date) as month,
                COUNT(*) as play_count
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE s.album_id = ? AND p.play_date IS NOT NULL
            GROUP BY strftime('%Y', p.play_date), strftime('%m', p.play_date)
            ORDER BY year ASC, month ASC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlaysByMonthDTO dto = new PlaysByMonthDTO();
            dto.setYear(rs.getString("year"));
            dto.setMonth(rs.getString("month"));
            dto.setPlayCount(rs.getLong("play_count"));
            return dto;
        }, albumId);
    }
    
    /**
     * Get featured artist cards for an album (aggregated from all songs in the album)
     * Returns full artist card data with feature count, sorted alphabetically
     */
    public List<FeaturedArtistCardDTO> getFeaturedArtistCardsForAlbum(int albumId) {
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
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                COALESCE(pc.play_count, 0) as play_count,
                COALESCE(pc.time_listened, 0) as time_listened,
                COUNT(sfa.song_id) as feature_count,
                GROUP_CONCAT(s.name, '||') as song_names,
                a.birth_date,
                a.death_date
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
                FROM Play p2
                INNER JOIN Song s2 ON p2.song_id = s2.id
                GROUP BY s2.artist_id
            ) pc ON a.id = pc.artist_id
            WHERE s.album_id = ?
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
        }, albumId);
    }
    
    // ============= RANKING METHODS =============
    
    /**
     * Get all rankings for an album in a single query (optimized)
     * Returns a Map with keys: "gender", "genre", "subgenre", "ethnicity", "language", "country"
     */
    public java.util.Map<String, Integer> getAllAlbumRankings(int albumId) {
        String sql = """
            WITH album_play_counts AS (
                SELECT alb.id, 
                       ar.gender_id,
                       COALESCE(alb.override_genre_id, ar.genre_id) as effective_genre_id,
                       COALESCE(alb.override_subgenre_id, ar.subgenre_id) as effective_subgenre_id,
                       ar.ethnicity_id,
                       COALESCE(alb.override_language_id, ar.language_id) as effective_language_id,
                       ar.country,
                       COALESCE(COUNT(p.id), 0) as play_count,
                       MIN(p.play_date) as first_play
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                GROUP BY alb.id, ar.gender_id, effective_genre_id, effective_subgenre_id, 
                         ar.ethnicity_id, effective_language_id, ar.country
            ),
            ranked_albums AS (
                SELECT id,
                       gender_id,
                       effective_genre_id,
                       effective_subgenre_id,
                       ethnicity_id,
                       effective_language_id,
                       country,
                       play_count,
                       first_play,
                       CASE WHEN gender_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY gender_id ORDER BY play_count DESC, first_play ASC) 
                            END as gender_rank,
                       CASE WHEN effective_genre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_genre_id ORDER BY play_count DESC, first_play ASC) 
                            END as genre_rank,
                       CASE WHEN effective_subgenre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_subgenre_id ORDER BY play_count DESC, first_play ASC) 
                            END as subgenre_rank,
                       CASE WHEN ethnicity_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY ethnicity_id ORDER BY play_count DESC, first_play ASC) 
                            END as ethnicity_rank,
                       CASE WHEN effective_language_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_language_id ORDER BY play_count DESC, first_play ASC) 
                            END as language_rank,
                       CASE WHEN country IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY country ORDER BY play_count DESC, first_play ASC) 
                            END as country_rank
                FROM album_play_counts
            )
            SELECT gender_rank, genre_rank, subgenre_rank, ethnicity_rank, language_rank, country_rank
            FROM ranked_albums
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
        }, albumId);
        
        return rankings;
    }
    
    /**
     * Get album rank by gender (position within same artist gender based on play count)
     * @deprecated Use getAllAlbumRankings() instead for better performance
     */
    @Deprecated
    public Integer getAlbumRankByGender(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.gender_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE ar.gender_id IS NOT NULL
                GROUP BY alb.id, ar.gender_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get album rank by genre (position within same genre based on play count, considering overrides)
     */
    public Integer getAlbumRankByGenre(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(alb.override_genre_id, ar.genre_id) 
                                          ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE COALESCE(alb.override_genre_id, ar.genre_id) IS NOT NULL
                GROUP BY alb.id, COALESCE(alb.override_genre_id, ar.genre_id)
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get album rank by subgenre (position within same subgenre based on play count, considering overrides)
     */
    public Integer getAlbumRankBySubgenre(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(alb.override_subgenre_id, ar.subgenre_id) 
                                          ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE COALESCE(alb.override_subgenre_id, ar.subgenre_id) IS NOT NULL
                GROUP BY alb.id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get album rank by ethnicity (position within same artist ethnicity based on play count)
     */
    public Integer getAlbumRankByEthnicity(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.ethnicity_id ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE ar.ethnicity_id IS NOT NULL
                GROUP BY alb.id, ar.ethnicity_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get album rank by language (position within same language based on play count, considering overrides)
     */
    public Integer getAlbumRankByLanguage(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(alb.override_language_id, ar.language_id) 
                                          ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE COALESCE(alb.override_language_id, ar.language_id) IS NOT NULL
                GROUP BY alb.id, COALESCE(alb.override_language_id, ar.language_id)
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get album rank by country (position within same artist country based on play count)
     */
    public Integer getAlbumRankByCountry(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.country ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE ar.country IS NOT NULL
                GROUP BY alb.id, ar.country
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get album ranks by year (position within albums that had plays in each year)
     * Returns a map of year -> rank
     */
    public Map<Integer, Integer> getAlbumRanksByYear(int albumId) {
        String sql = """
            SELECT year, rank FROM (
                SELECT alb.id, 
                       strftime('%Y', p.play_date) as year,
                       ROW_NUMBER() OVER (PARTITION BY strftime('%Y', p.play_date) ORDER BY COUNT(p.id) DESC) as rank
                FROM Album alb
                INNER JOIN Song s ON s.album_id = alb.id
                INNER JOIN Play p ON p.song_id = s.id
                GROUP BY alb.id, strftime('%Y', p.play_date)
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
        }, albumId);
        return result;
    }

    /**
     * Get album's overall position among all albums by play count.
     */
    public Integer getAlbumOverallPosition(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Album alb
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                GROUP BY alb.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get album's position among albums released in the same year.
     */
    public Integer getAlbumRankByReleaseYear(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id,
                       strftime('%Y', alb.release_date) as release_year,
                       ROW_NUMBER() OVER (
                           PARTITION BY strftime('%Y', alb.release_date) 
                           ORDER BY COALESCE(COUNT(p.id), 0) DESC
                       ) as rank
                FROM Album alb
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE alb.release_date IS NOT NULL
                GROUP BY alb.id, release_year
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get the release year for an album.
     */
    public Integer getAlbumReleaseYear(int albumId) {
        String sql = "SELECT strftime('%Y', release_date) FROM Album WHERE id = ?";

        try {
            String yearStr = jdbcTemplate.queryForObject(sql, String.class, albumId);
            return yearStr != null ? Integer.parseInt(yearStr) : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get album's position among all albums by the same artist.
     */
    public Integer getAlbumRankByArtist(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id,
                       alb.artist_id,
                       ROW_NUMBER() OVER (PARTITION BY alb.artist_id ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Album alb
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                GROUP BY alb.id, alb.artist_id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get album rank for Spanish Rap (albums with effective genre=Rap and effective language=Spanish).
     */
    public Integer getAlbumSpanishRapRank(int albumId) {
        String sql = """
            SELECT rank FROM (
                SELECT alb.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                INNER JOIN Genre g ON COALESCE(alb.override_genre_id, ar.genre_id) = g.id
                INNER JOIN Language l ON COALESCE(alb.override_language_id, ar.language_id) = l.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE g.name = 'Rap' AND l.name = 'Spanish'
                GROUP BY alb.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Check if an album is in the Spanish Rap category (effective genre=Rap AND effective language=Spanish)
     */
    public boolean isAlbumSpanishRap(int albumId) {
        String sql = """
            SELECT COUNT(*) FROM Album alb
            INNER JOIN Artist ar ON alb.artist_id = ar.id
            INNER JOIN Genre g ON COALESCE(alb.override_genre_id, ar.genre_id) = g.id
            INNER JOIN Language l ON COALESCE(alb.override_language_id, ar.language_id) = l.id
            WHERE alb.id = ? AND g.name = 'Rap' AND l.name = 'Spanish'
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, albumId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public List<Map<String, Object>> getAlbumDetailsForIds(List<Integer> ids) {
        if (ids == null || ids.isEmpty()) return new ArrayList<>();
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        String sql = "SELECT id, name FROM Album WHERE id IN (" + placeholders + ")";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> album = new HashMap<>();
            album.put("id", rs.getInt("id"));
            album.put("name", rs.getString("name"));
            return album;
        }, ids.toArray());
    }

    public int getSoloSongCountForAlbum(Integer albumId) {
        String sql = "SELECT COUNT(*) FROM Song s WHERE s.album_id = ? AND s.id NOT IN (SELECT sfa.song_id FROM SongFeaturedArtist sfa)";
        return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
    }

    public int getSongsWithFeatCountForAlbum(Integer albumId) {
        String sql = "SELECT COUNT(DISTINCT s.id) FROM Song s INNER JOIN SongFeaturedArtist sfa ON s.id = sfa.song_id WHERE s.album_id = ?";
        return jdbcTemplate.queryForObject(sql, Integer.class, albumId);
    }
}