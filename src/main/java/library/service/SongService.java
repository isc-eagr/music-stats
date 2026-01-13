package library.service;

import library.dto.ChartFilterDTO;
import library.dto.FeaturedArtistCardDTO;
import library.dto.FeaturedArtistDTO;
import library.dto.PlaysByYearDTO;
import library.dto.ScrobbleDTO;
import library.dto.SongCardDTO;
import library.entity.Song;
import library.entity.SongImage;
import library.repository.LookupRepository;
import library.repository.SongImageRepository;
import library.repository.SongRepository;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class SongService {
    
    private final SongRepository songRepository;
    private final SongImageRepository songImageRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    private final ItunesService itunesService;
    
    public SongService(SongRepository songRepository, SongImageRepository songImageRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate, ItunesService itunesService) {
        this.songRepository = songRepository;
        this.songImageRepository = songImageRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.itunesService = itunesService;
    }
    
    public List<SongCardDTO> getSongs(String name, String artistName, String albumName,
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
                                       String organized, String hasImage, String hasFeaturedArtists, String isBand, String isSingle, String inItunes,
                                       Integer playCountMin, Integer playCountMax,
                                       Integer lengthMin, Integer lengthMax, String lengthMode,
                                       Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                       Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                       Integer yearlyChartPeak, Integer yearlyChartYears,
                                       String sortBy, String sortDirection, int page, int perPage) {
        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (accounts != null && accounts.isEmpty()) accounts = null;
        
        // If inItunes filter is active, we need to get all results first, then filter in memory
        boolean filterByItunes = inItunes != null && !inItunes.isEmpty();
        int actualLimit = filterByItunes ? Integer.MAX_VALUE : perPage;
        int actualOffset = filterByItunes ? 0 : page * perPage;
        
        List<Object[]> results = songRepository.findSongsWithStats(
                name, artistName, albumName, genreIds, genreMode, 
                subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                ethnicityIds, ethnicityMode, countries, countryMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, hasImage, hasFeaturedArtists, isBand, isSingle,
                playCountMin, playCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears,
                sortBy, sortDirection, actualLimit, actualOffset
        );
        
        List<SongCardDTO> songs = new ArrayList<>();
        for (Object[] row : results) {
            SongCardDTO dto = new SongCardDTO();
            dto.setId(((Number) row[0]).intValue());
            dto.setName((String) row[1]);
            dto.setArtistName((String) row[2]);
            dto.setArtistId(((Number) row[3]).intValue());
            dto.setAlbumName((String) row[4]);
            dto.setAlbumId(row[5] != null ? ((Number) row[5]).intValue() : null);
            dto.setGenreId(row[6] != null ? ((Number) row[6]).intValue() : null);
            dto.setGenreName((String) row[7]);
            dto.setSubgenreId(row[8] != null ? ((Number) row[8]).intValue() : null);
            dto.setSubgenreName((String) row[9]);
            dto.setLanguageId(row[10] != null ? ((Number) row[10]).intValue() : null);
            dto.setLanguageName((String) row[11]);
            dto.setEthnicityId(row[12] != null ? ((Number) row[12]).intValue() : null);
            dto.setEthnicityName((String) row[13]);
            dto.setReleaseYear((String) row[14]);
            dto.setReleaseDate(row[15] != null ? formatDate((String) row[15]) : null);
            dto.setLengthSeconds(row[16] != null ? ((Number) row[16]).intValue() : null);
            dto.setHasImage(row[17] != null && ((Number) row[17]).intValue() == 1);
            dto.setGenderName((String) row[18]);
            dto.setPlayCount(row[19] != null ? ((Number) row[19]).intValue() : 0);
            dto.setVatitoPlayCount(row[20] != null ? ((Number) row[20]).intValue() : 0);
            dto.setRobertloverPlayCount(row[21] != null ? ((Number) row[21]).intValue() : 0);
            
            // Set time listened and format it
            long timeListened = row[22] != null ? ((Number) row[22]).longValue() : 0L;
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            
            // Set first and last listened dates (indices 23 and 24)
            dto.setFirstListenedDate(row[23] != null ? formatDate((String) row[23]) : null);
            dto.setLastListenedDate(row[24] != null ? formatDate((String) row[24]) : null);
            
            // Set country (inherited from artist, index 25)
            dto.setCountry((String) row[25]);
            
            // Set organized (index 26)
            dto.setOrganized(row[26] != null && ((Number) row[26]).intValue() == 1);
            
            // Set album has image (index 27)
            dto.setAlbumHasImage(row[27] != null && ((Number) row[27]).intValue() == 1);
            
            // Set isSingle (index 28)
            dto.setIsSingle(row[28] != null && ((Number) row[28]).intValue() == 1);
            
            // Format length
            if (dto.getLengthSeconds() != null) {
                int minutes = dto.getLengthSeconds() / 60;
                int seconds = dto.getLengthSeconds() % 60;
                dto.setLengthFormatted(String.format("%d:%02d", minutes, seconds));
            }
            
            // Check iTunes presence only if filter is active (performance optimization)
            if (filterByItunes) {
                dto.setInItunes(itunesService.songExistsInItunes(dto.getArtistName(), dto.getName()));
            }
            
            songs.add(dto);
        }
        
        // Apply iTunes filter if needed
        if (filterByItunes) {
            boolean wantInItunes = "true".equalsIgnoreCase(inItunes);
            songs = songs.stream()
                    .filter(s -> s.getInItunes() != null && s.getInItunes() == wantInItunes)
                    .toList();
            
            // Apply pagination manually
            int offset = page * perPage;
            int end = Math.min(offset + perPage, songs.size());
            if (offset >= songs.size()) {
                songs = new ArrayList<>();
            } else {
                songs = new ArrayList<>(songs.subList(offset, end));
            }
        }
        
        return songs;
    }
    
    public long countSongs(String name, String artistName, String albumName,
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
                          String organized, String hasImage, String hasFeaturedArtists, String isBand, String isSingle, String inItunes,
                          Integer playCountMin, Integer playCountMax,
                          Integer lengthMin, Integer lengthMax, String lengthMode,
                          Integer weeklyChartPeak, Integer weeklyChartWeeks,
                          Integer seasonalChartPeak, Integer seasonalChartSeasons,
                          Integer yearlyChartPeak, Integer yearlyChartYears) {
        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (accounts != null && accounts.isEmpty()) accounts = null;
        
        // If inItunes filter is active, we need to count manually
        if (inItunes != null && !inItunes.isEmpty()) {
            List<SongCardDTO> allSongs = getSongs(name, artistName, albumName, genreIds, genreMode,
                    subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                    ethnicityIds, ethnicityMode, countries, countryMode, accounts, accountMode,
                    releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                    firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                    lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                    listenedDateFrom, listenedDateTo,
                    organized, hasImage, hasFeaturedArtists, isBand, isSingle, inItunes,
                    playCountMin, playCountMax,
                    lengthMin, lengthMax, lengthMode,
                    weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears,
                    "plays", "desc", 0, Integer.MAX_VALUE);
            return allSongs.size();
        }
        
        return songRepository.countSongsWithFilters(name, artistName, albumName, 
                genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                genderIds, genderMode, ethnicityIds, ethnicityMode, countries, countryMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, hasImage, hasFeaturedArtists, isBand, isSingle,
                playCountMin, playCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears);
    }
    
    public Optional<Song> getSongById(Integer id) {
        String sql = """
            SELECT s.id, s.artist_id, s.album_id, s.name, s.length_seconds, s.is_single,
                   s.override_genre_id, s.override_subgenre_id, s.override_language_id,
                   s.override_gender_id, s.override_ethnicity_id, s.release_date, s.organized,
                   s.creation_date, s.update_date,
                   al.override_genre_id as album_genre_id,
                   al.override_subgenre_id as album_subgenre_id,
                   al.override_language_id as album_language_id,
                   ar.genre_id as artist_genre_id,
                   ar.subgenre_id as artist_subgenre_id,
                   ar.language_id as artist_language_id,
                   ar.gender_id as artist_gender_id,
                   ar.ethnicity_id as artist_ethnicity_id
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE s.id = ?
            """;
        
        List<Song> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Song song = new Song();
            song.setId(rs.getInt("id"));
            song.setArtistId(rs.getInt("artist_id"));
            
            int albumId = rs.getInt("album_id");
            song.setAlbumId(rs.wasNull() ? null : albumId);
            
            song.setName(rs.getString("name"));
            
            int length = rs.getInt("length_seconds");
            song.setLengthSeconds(rs.wasNull() ? null : length);
            
            song.setIsSingle(rs.getInt("is_single") == 1);
            
            int genreId = rs.getInt("override_genre_id");
            song.setOverrideGenreId(rs.wasNull() ? null : genreId);
            
            int subgenreId = rs.getInt("override_subgenre_id");
            song.setOverrideSubgenreId(rs.wasNull() ? null : subgenreId);
            
            int languageId = rs.getInt("override_language_id");
            song.setOverrideLanguageId(rs.wasNull() ? null : languageId);
            
            int genderId = rs.getInt("override_gender_id");
            song.setOverrideGenderId(rs.wasNull() ? null : genderId);
            
            int ethnicityId = rs.getInt("override_ethnicity_id");
            song.setOverrideEthnicityId(rs.wasNull() ? null : ethnicityId);
            
            int organizedVal = rs.getInt("organized");
            song.setOrganized(rs.wasNull() ? null : organizedVal == 1);
            
            // Set inherited values from Album
            int albumGenreId = rs.getInt("album_genre_id");
            song.setAlbumGenreId(rs.wasNull() ? null : albumGenreId);
            
            int albumSubgenreId = rs.getInt("album_subgenre_id");
            song.setAlbumSubgenreId(rs.wasNull() ? null : albumSubgenreId);
            
            int albumLanguageId = rs.getInt("album_language_id");
            song.setAlbumLanguageId(rs.wasNull() ? null : albumLanguageId);
            
            // Set inherited values from Artist
            int artistGenreId = rs.getInt("artist_genre_id");
            song.setArtistGenreId(rs.wasNull() ? null : artistGenreId);
            
            int artistSubgenreId = rs.getInt("artist_subgenre_id");
            song.setArtistSubgenreId(rs.wasNull() ? null : artistSubgenreId);
            
            int artistLanguageId = rs.getInt("artist_language_id");
            song.setArtistLanguageId(rs.wasNull() ? null : artistLanguageId);
            
            int artistGenderId = rs.getInt("artist_gender_id");
            song.setArtistGenderId(rs.wasNull() ? null : artistGenderId);
            
            int artistEthnicityId = rs.getInt("artist_ethnicity_id");
            song.setArtistEthnicityId(rs.wasNull() ? null : artistEthnicityId);
            
            // Try multiple methods to read release_date from SQLite
            java.sql.Date releaseDate = null;
            try {
                // Try getting as Date object first
                releaseDate = rs.getDate("release_date");
            } catch (Exception e1) {
                // If that fails, try as string
                try {
                    String releaseDateStr = rs.getString("release_date");
                    if (releaseDateStr != null && !releaseDateStr.isEmpty()) {
                        // Try parsing as long (timestamp)
                        try {
                            long timestamp = Long.parseLong(releaseDateStr);
                            releaseDate = new java.sql.Date(timestamp);
                        } catch (NumberFormatException e2) {
                            // Try parsing as date string
                            releaseDate = parseDate(releaseDateStr);
                        }
                    }
                } catch (Exception e2) {
                    // Date parsing failed, leave as null
                }
            }
            song.setReleaseDate(releaseDate);
            
            // Robust timestamp parsing for SQLite date-only values
            String creation = rs.getString("creation_date");
            String update = rs.getString("update_date");
            song.setCreationDate(parseTimestamp(creation));
            song.setUpdateDate(parseTimestamp(update));
            
            return song;
        }, id);
        
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
    
    public Song saveSong(Song song) {
        // Get the old song data to check if name or album changed
        String oldName = null;
        Integer oldAlbumId = null;
        try {
            Map<String, Object> oldData = jdbcTemplate.queryForMap(
                "SELECT name, album_id FROM Song WHERE id = ?", song.getId());
            oldName = (String) oldData.get("name");
            oldAlbumId = oldData.get("album_id") != null ? ((Number) oldData.get("album_id")).intValue() : null;
        } catch (Exception e) {
            // Ignore if song doesn't exist
        }
        
        String sql = """
            UPDATE Song 
            SET name = ?, artist_id = ?, album_id = ?, release_date = ?,
                length_seconds = ?, is_single = ?,
                override_genre_id = ?, override_subgenre_id = ?, override_language_id = ?,
                override_gender_id = ?, override_ethnicity_id = ?, organized = ?,
                update_date = CURRENT_TIMESTAMP
            WHERE id = ?
            """;
        
        // Convert java.sql.Date to yyyy-MM-dd string format for database
        String releaseDateStr = null;
        if (song.getReleaseDate() != null) {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            releaseDateStr = sdf.format(song.getReleaseDate());
        }
        
        jdbcTemplate.update(sql, 
            song.getName(),
            song.getArtistId(),
            song.getAlbumId(),
            releaseDateStr,
            song.getLengthSeconds(),
            song.getIsSingle() ? 1 : 0,
            song.getOverrideGenreId(),
            song.getOverrideSubgenreId(),
            song.getOverrideLanguageId(),
            song.getOverrideGenderId(),
            song.getOverrideEthnicityId(),
            song.getOrganized() != null && song.getOrganized() ? 1 : 0,
            song.getId()
        );
        
        // If song name changed, update scrobbles
        if (oldName != null && !oldName.equals(song.getName())) {
            updateScrobblesForSongNameChange(song.getId(), song.getName());
        }
        
        // If album changed, update scrobbles with new album name
        boolean albumChanged = (oldAlbumId == null && song.getAlbumId() != null) ||
                              (oldAlbumId != null && !oldAlbumId.equals(song.getAlbumId()));
        if (albumChanged) {
            String newAlbumName = song.getAlbumId() != null ? getAlbumName(song.getAlbumId()) : null;
            updateScrobblesForSongAlbumChange(song.getId(), newAlbumName);
        }
        
        // Try to match unmatched scrobbles with the current song details
        // This catches scrobbles that might now match after the song was renamed or album changed
        tryMatchUnmatchedScrobblesForSong(song);
        
        return song;
    }
    
    /**
     * Update the song name in all scrobbles for this song
     */
    private void updateScrobblesForSongNameChange(int songId, String newSongName) {
        String sql = "UPDATE Scrobble SET song = ? WHERE song_id = ?";
        jdbcTemplate.update(sql, newSongName, songId);
    }
    
    /**
     * Update the album name in all scrobbles for this song
     */
    private void updateScrobblesForSongAlbumChange(int songId, String newAlbumName) {
        String sql = "UPDATE Scrobble SET album = ? WHERE song_id = ?";
        jdbcTemplate.update(sql, newAlbumName, songId);
    }
    
    /**
     * Try to match unmatched scrobbles to this song.
     * Looks for scrobbles where artist, album, and song name match.
     */
    private void tryMatchUnmatchedScrobblesForSong(Song song) {
        // Get artist name
        String artistName = getArtistName(song.getArtistId());
        if (artistName == null) return;
        
        // Get album name (may be null for singles)
        String albumName = song.getAlbumId() != null ? getAlbumName(song.getAlbumId()) : null;
        
        // Try to match unmatched scrobbles and update their canonical names
        String sql = """
            UPDATE Scrobble 
            SET song_id = ?,
                artist = ?,
                album = ?,
                song = ?
            WHERE song_id IS NULL
            AND LOWER(COALESCE(artist, '')) = LOWER(?)
            AND LOWER(COALESCE(album, '')) = LOWER(?)
            AND LOWER(COALESCE(song, '')) = LOWER(?)
            """;
        
        jdbcTemplate.update(sql, 
            song.getId(),
            artistName,
            albumName != null ? albumName : "",
            song.getName(),
            artistName,
            albumName != null ? albumName : "",
            song.getName()
        );
    }
    
    public void updateSongImage(Integer id, byte[] imageData) {
        String sql = "UPDATE Song SET single_cover = ? WHERE id = ?";
        jdbcTemplate.update(sql, imageData, id);
    }
    
    public byte[] getSongImage(Integer id) {
        // Use COALESCE to check single_cover first, then fall back to album image
        String sql = """
            SELECT COALESCE(s.single_cover, a.image) as image
            FROM Song s
            LEFT JOIN Album a ON s.album_id = a.id
            WHERE s.id = ?
            """;
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("image"), id);
        } catch (Exception e) {
            return null;
        }
    }
    
    // Get only the song's own image (single_cover), not falling back to album
    public byte[] getSongOwnImage(Integer id) {
        String sql = "SELECT single_cover FROM Song WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("single_cover"), id);
        } catch (Exception e) {
            return null;
        }
    }

    // Gallery methods for secondary images
    public List<SongImage> getSecondaryImages(Integer songId) {
        return songImageRepository.findBySongIdOrderByDisplayOrderAsc(songId);
    }

    public int getSecondaryImageCount(Integer songId) {
        return songImageRepository.countBySongId(songId);
    }

    public byte[] getSecondaryImage(Integer imageId) {
        return songImageRepository.findById(imageId)
                .map(SongImage::getImage)
                .orElse(null);
    }

    public void addSecondaryImage(Integer songId, byte[] imageData) {
        Integer maxOrder = songImageRepository.getMaxDisplayOrder(songId);
        SongImage image = new SongImage();
        image.setSongId(songId);
        image.setImage(imageData);
        image.setDisplayOrder(maxOrder + 1);
        image.setCreationDate(new java.sql.Timestamp(System.currentTimeMillis()));
        songImageRepository.save(image);
    }

    @Transactional
    public void deleteSecondaryImage(Integer imageId) {
        songImageRepository.deleteById(imageId);
    }

    @Transactional
    public void swapToDefault(Integer songId, Integer imageId) {
        // Get the current default image from the main Song table (single_cover only, not album fallback)
        byte[] currentDefault = getSongOwnImage(songId);

        // Get the secondary image to promote
        SongImage secondaryImage = songImageRepository.findById(imageId)
                .orElseThrow(() -> new IllegalArgumentException("Image not found: " + imageId));

        // Set the secondary image as the new default
        updateSongImage(songId, secondaryImage.getImage());

        // If there was a previous default, move it to secondary images
        if (currentDefault != null && currentDefault.length > 0) {
            // Update the secondary image record with the old default
            secondaryImage.setImage(currentDefault);
            songImageRepository.save(secondaryImage);
        } else {
            // No previous default, just delete the secondary record
            songImageRepository.deleteById(imageId);
        }
    }

    public String getArtistName(int artistId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT name FROM Artist WHERE id = ?", String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    public String getAlbumName(int albumId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT name FROM Album WHERE id = ?", String.class, albumId);
        } catch (Exception e) {
            return null;
        }
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
    
    // Helper to parse various SQLite timestamp representations
    private static java.sql.Timestamp parseTimestamp(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        try {
            if (v.length() == 10 && v.matches("\\d{4}-\\d{2}-\\d{2}")) {
                v = v + " 00:00:00";
            } else if (v.contains("T") && v.matches("\\d{4}-\\d{2}-\\d{2}T.*")) {
                v = v.replace('T', ' ');
            }
            return java.sql.Timestamp.valueOf(v);
        } catch (Exception e) {
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
        
        // Parse as date string
        if (v.contains("T")) v = v.replace('T', ' ');
        if (v.length() >= 10) v = v.substring(0, 10);
        if (v.matches("\\d{4}-\\d{2}-\\d{2}")) {
            try {
                return java.sql.Date.valueOf(v);
            } catch (Exception ignore) {}
        }
        return null;
    }

    // NEW: total plays for a song (count scrobbles for this song)
    public int getPlayCountForSong(int songId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Scrobble WHERE song_id = ?",
                Integer.class, songId);
        return count != null ? count : 0;
    }

    // Get vatito (primary) play count for song
    public int getVatitoPlayCountForSong(int songId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Scrobble WHERE song_id = ? AND account = 'vatito'",
                Integer.class, songId);
        return count != null ? count : 0;
    }
    
    // Get robertlover (legacy) play count for song
    public int getRobertloverPlayCountForSong(int songId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Scrobble WHERE song_id = ? AND account = 'robertlover'",
                Integer.class, songId);
        return count != null ? count : 0;
    }

    // Return a string with per-account play counts for this song (e.g. "lastfm: 12\nspotify: 3\n")
    public String getPlaysByAccountForSong(int songId) {
        String sql = "SELECT account, COUNT(*) as cnt FROM Scrobble WHERE song_id = ? GROUP BY account ORDER BY cnt DESC";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, songId);
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

    // Get total listening time for a song
    public String getTotalListeningTimeForSong(int songId) {
        String sql = """
            SELECT s.length_seconds * COALESCE(play_count, 0) as total_seconds
            FROM Song s
            LEFT JOIN (
                SELECT song_id, COUNT(*) as play_count
                FROM Scrobble
                WHERE song_id = ?
                GROUP BY song_id
            ) scr ON s.id = scr.song_id
            WHERE s.id = ?
            """;
        
        try {
            Integer totalSeconds = jdbcTemplate.queryForObject(sql, Integer.class, songId, songId);
            if (totalSeconds == null || totalSeconds == 0) {
                return "-";
            }
            return formatDuration(totalSeconds);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get first listened date for a song
    public String getFirstListenedDateForSong(int songId) {
        String sql = "SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = ?";
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, songId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get first listened date for a song as LocalDate (for calculations)
    public java.time.LocalDate getFirstListenedDateAsLocalDateForSong(int songId) {
        String sql = "SELECT MIN(DATE(scrobble_date)) FROM Scrobble WHERE song_id = ?";
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, songId);
            return dateStr != null ? java.time.LocalDate.parse(dateStr) : null;
        } catch (Exception e) {
            return null;
        }
    }

    // Get last listened date for a song
    public String getLastListenedDateForSong(int songId) {
        String sql = "SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = ?";
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, songId);
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get unique days played for a song
    public int getUniqueDaysPlayedForSong(int songId) {
        String sql = "SELECT COUNT(DISTINCT DATE(scrobble_date)) FROM Scrobble WHERE song_id = ? AND scrobble_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique weeks played for a song
    public int getUniqueWeeksPlayedForSong(int songId) {
        String sql = "SELECT COUNT(DISTINCT strftime('%Y-%W', scrobble_date)) FROM Scrobble WHERE song_id = ? AND scrobble_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique months played for a song
    public int getUniqueMonthsPlayedForSong(int songId) {
        String sql = "SELECT COUNT(DISTINCT strftime('%Y-%m', scrobble_date)) FROM Scrobble WHERE song_id = ? AND scrobble_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique years played for a song
    public int getUniqueYearsPlayedForSong(int songId) {
        String sql = "SELECT COUNT(DISTINCT strftime('%Y', scrobble_date)) FROM Scrobble WHERE song_id = ? AND scrobble_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId);
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Delete song (only if play count is 0)
    public void deleteSong(Integer songId) {
        // First check if song has any plays
        int playCount = getPlayCountForSong(songId);
        if (playCount > 0) {
            throw new IllegalStateException("Cannot delete song with existing plays");
        }
        
        // Delete the song
        jdbcTemplate.update("DELETE FROM Song WHERE id = ?", songId);
    }
    
    // Create a new song
    public Song createSong(Song song) {
        String sql = """
            INSERT INTO Song (artist_id, album_id, name, release_date, length_seconds, 
                             is_single, override_genre_id, override_subgenre_id, 
                             override_language_id, override_gender_id, override_ethnicity_id,
                             creation_date, update_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """;
        
        // Convert java.sql.Date to yyyy-MM-dd string format for database
        String releaseDateStr = null;
        if (song.getReleaseDate() != null) {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            releaseDateStr = sdf.format(song.getReleaseDate());
        }
        
        jdbcTemplate.update(sql,
            song.getArtistId(),
            song.getAlbumId(),
            song.getName(),
            releaseDateStr,
            song.getLengthSeconds(),
            song.getIsSingle() != null && song.getIsSingle() ? 1 : 0,
            song.getOverrideGenreId(),
            song.getOverrideSubgenreId(),
            song.getOverrideLanguageId(),
            song.getOverrideGenderId(),
            song.getOverrideEthnicityId()
        );
        
        // Get the ID of the newly created song
        Integer id = jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Integer.class);
        song.setId(id);
        
        return song;
    }
    
    // Create song from map (for API)
    public Integer createSong(java.util.Map<String, Object> data) {
        Song song = new Song();
        song.setName((String) data.get("name"));
        if (data.get("artistId") != null) {
            song.setArtistId(((Number) data.get("artistId")).intValue());
        }
        if (data.get("albumId") != null) {
            song.setAlbumId(((Number) data.get("albumId")).intValue());
        }
        if (data.get("releaseDate") != null) {
            // Convert dd/MM/yyyy to yyyy-MM-dd for parsing
            String dateStr = (String) data.get("releaseDate");
            if (dateStr != null && dateStr.matches("\\d{2}/\\d{2}/\\d{4}")) {
                String[] parts = dateStr.split("/");
                String isoDate = parts[2] + "-" + parts[1] + "-" + parts[0];
                try {
                    song.setReleaseDate(java.sql.Date.valueOf(isoDate));
                } catch (Exception ignore) {}
            }
        }
        if (data.get("lengthSeconds") != null) {
            song.setLengthSeconds(((Number) data.get("lengthSeconds")).intValue());
        }
        Song created = createSong(song);
        return created.getId();
    }
    
    // Helper method to format duration from seconds
    private String formatDuration(int totalSeconds) {
        if (totalSeconds <= 0) return "-";
        
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

    // Helper method to format date strings
    private String formatDate(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
            return "-";
        }
        
        try {
            String datePart = dateTimeString.trim();
            if (datePart.contains(" ")) {
                datePart = datePart.split(" ")[0];
            }
            
            String[] parts = datePart.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                
                String[] monthNames = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
                
                // Format as DD-Mon-YYYY (e.g., "01-Nov-2025") with zero-padded day
                return String.format("%02d-%s-%d", day, monthNames[month - 1], year);
            }
        } catch (Exception e) {
            // If parsing fails, return as is
        }
        
        return dateTimeString;
    }
    
    // Get filtered chart data for gender breakdown (using ChartFilterDTO)
    public Map<String, Object> getFilteredChartData(ChartFilterDTO filter) {
        return songRepository.getFilteredChartData(filter);
    }
    
    // Get General tab chart data (5 pie charts: Artists, Albums, Songs, Plays, Listening Time)
    public Map<String, Object> getGeneralChartData(ChartFilterDTO filter) {
        return songRepository.getGeneralChartData(filter);
    }
    
    // Get Genre tab chart data (5 bar charts grouped by genre)
    public Map<String, Object> getGenreChartData(ChartFilterDTO filter) {
        return songRepository.getGenreChartData(filter);
    }
    
    // Get Subgenre tab chart data (5 bar charts grouped by subgenre)
    public Map<String, Object> getSubgenreChartData(ChartFilterDTO filter) {
        return songRepository.getSubgenreChartData(filter);
    }
    
    // Get Ethnicity tab chart data (5 bar charts grouped by ethnicity)
    public Map<String, Object> getEthnicityChartData(ChartFilterDTO filter) {
        return songRepository.getEthnicityChartData(filter);
    }
    
    // Get Language tab chart data (5 bar charts grouped by language)
    public Map<String, Object> getLanguageChartData(ChartFilterDTO filter) {
        return songRepository.getLanguageChartData(filter);
    }
    
    // Get Country tab chart data (5 bar charts grouped by country)
    public Map<String, Object> getCountryChartData(ChartFilterDTO filter) {
        return songRepository.getCountryChartData(filter);
    }
    
    // Get Release Year tab chart data (4 bar charts grouped by release year - no artists)
    public Map<String, Object> getReleaseYearChartData(ChartFilterDTO filter) {
        return songRepository.getReleaseYearChartData(filter);
    }
    
    // Get Listen Year tab chart data (5 bar charts grouped by year listened)
    public Map<String, Object> getListenYearChartData(ChartFilterDTO filter) {
        return songRepository.getListenYearChartData(filter);
    }
    
    // Get Top tab data (top artists, albums, songs by play count)
    public Map<String, Object> getTopChartData(ChartFilterDTO filter) {
        return songRepository.getTopChartData(filter);
    }
    
    // Get scrobbles for a song with pagination
    public List<ScrobbleDTO> getScrobblesForSong(int songId, int page, int pageSize) {
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
            WHERE s.id = ?
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
        }, songId, pageSize, offset);
    }
    
    // Count total scrobbles for a song
    public long countScrobblesForSong(int songId) {
        String sql = "SELECT COUNT(*) FROM Scrobble WHERE song_id = ?";
        Long count = jdbcTemplate.queryForObject(sql, Long.class, songId);
        return count != null ? count : 0;
    }
    
    // Get plays by year for a song
    public List<PlaysByYearDTO> getPlaysByYearForSong(int songId) {
        String sql = """
            SELECT 
                strftime('%Y', scrobble_date) as year,
                COUNT(*) as play_count
            FROM Scrobble
            WHERE song_id = ? AND scrobble_date IS NOT NULL
            GROUP BY strftime('%Y', scrobble_date)
            ORDER BY year ASC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlaysByYearDTO dto = new PlaysByYearDTO();
            dto.setYear(rs.getString("year"));
            dto.setPlayCount(rs.getLong("play_count"));
            return dto;
        }, songId);
    }
    
    // ============================================
    // Featured Artists Methods
    // ============================================
    
    /**
     * Get all featured artists for a song
     */
    public List<FeaturedArtistDTO> getFeaturedArtistsForSong(int songId) {
        String sql = """
            SELECT sfa.artist_id, a.name as artist_name
            FROM SongFeaturedArtist sfa
            INNER JOIN Artist a ON sfa.artist_id = a.id
            WHERE sfa.song_id = ?
            ORDER BY a.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeaturedArtistDTO dto = new FeaturedArtistDTO();
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            return dto;
        }, songId);
    }
    
    /**
     * Search artists by name for the featured artists autocomplete
     */
    public List<FeaturedArtistDTO> searchArtists(String query, int limit) {
        String sql = """
            SELECT id, name
            FROM Artist
            WHERE LOWER(name) LIKE LOWER(?)
            ORDER BY name
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeaturedArtistDTO dto = new FeaturedArtistDTO();
            dto.setArtistId(rs.getInt("id"));
            dto.setArtistName(rs.getString("name"));
            return dto;
        }, "%" + query + "%", limit);
    }
    
    /**
     * Save featured artists for a song (replaces all existing)
     */
    @Transactional
    public void saveFeaturedArtists(int songId, List<Integer> artistIds) {
        // First, delete all existing featured artists for this song
        jdbcTemplate.update("DELETE FROM SongFeaturedArtist WHERE song_id = ?", songId);
        
        // Then insert the new ones
        if (artistIds != null && !artistIds.isEmpty()) {
            String insertSql = "INSERT INTO SongFeaturedArtist (song_id, artist_id, creation_date) VALUES (?, ?, CURRENT_TIMESTAMP)";
            for (Integer artistId : artistIds) {
                jdbcTemplate.update(insertSql, songId, artistId);
            }
        }
    }
    
    /**
     * Get featured artist cards for a song (for the Featured Artists tab)
     * Returns full artist card data sorted alphabetically
     */
    public List<FeaturedArtistCardDTO> getFeaturedArtistCardsForSong(int songId) {
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
                COALESCE(scr.play_count, 0) as play_count,
                COALESCE(scr.time_listened, 0) as time_listened
            FROM SongFeaturedArtist sfa
            INNER JOIN Artist a ON sfa.artist_id = a.id
            LEFT JOIN Gender g ON a.gender_id = g.id
            LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
            LEFT JOIN Genre gr ON a.genre_id = gr.id
            LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id
            LEFT JOIN Language l ON a.language_id = l.id
            LEFT JOIN (
                SELECT s.artist_id, COUNT(*) as play_count, 
                       SUM(COALESCE(s.length_seconds, 0)) as time_listened
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                GROUP BY s.artist_id
            ) scr ON a.id = scr.artist_id
            WHERE sfa.song_id = ?
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
            dto.setFeatureCount(1); // For songs, each featured artist only appears once
            return dto;
        }, songId);
    }
    
    // Search songs by artist and song name for API
    public List<Map<String, Object>> searchSongs(String artistQuery, String songQuery, int limit) {
        StringBuilder sql = new StringBuilder(
            "SELECT s.id, s.name, a.name as artist_name, al.name as album_name, " +
            "CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END as has_image " +
            "FROM Song s " +
            "JOIN Artist a ON s.artist_id = a.id " +
            "LEFT JOIN Album al ON s.album_id = al.id " +
            "WHERE 1=1 "
        );
        
        List<Object> params = new ArrayList<>();
        
        // Use normalized search (strip accents and special chars) and SQL normalization
        boolean hasArtist = artistQuery != null && !artistQuery.trim().isEmpty();
        boolean hasSong = songQuery != null && !songQuery.trim().isEmpty();
        String normalizedArtist = hasArtist ? library.util.StringNormalizer.normalizeForSearch(artistQuery) : null;
        String normalizedSong = hasSong ? library.util.StringNormalizer.normalizeForSearch(songQuery) : null;

        if (hasArtist && hasSong && normalizedArtist != null && normalizedArtist.equals(normalizedSong)) {
            // Unified search: match either artist OR song name
            sql.append("AND (" + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ? OR " + library.util.StringNormalizer.sqlNormalizeColumn("s.name") + " LIKE ?) ");
            params.add("%" + normalizedArtist + "%");
            params.add("%" + normalizedSong + "%");
        } else {
            // Separate filters: match both if provided
            if (hasArtist) {
                sql.append("AND " + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ? ");
                params.add("%" + normalizedArtist + "%");
            }

            if (hasSong) {
                sql.append("AND " + library.util.StringNormalizer.sqlNormalizeColumn("s.name") + " LIKE ? ");
                params.add("%" + normalizedSong + "%");
            }
        }
        
        sql.append("ORDER BY a.name, s.name ");
        
        // Only apply limit if > 0
        if (limit > 0) {
            sql.append("LIMIT ?");
            params.add(limit);
        }
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            Map<String, Object> song = new java.util.HashMap<>();
            song.put("id", rs.getInt("id"));
            song.put("name", rs.getString("name"));
            song.put("artistName", rs.getString("artist_name"));
            song.put("albumName", rs.getString("album_name"));
            song.put("hasImage", rs.getInt("has_image") == 1);
            return song;
        }, params.toArray());
    }
    
    // ============= RANKING METHODS =============
    
    /**
     * Get all rankings for a song in a single query (optimized)
     * Returns a Map with keys: "gender", "genre", "subgenre", "ethnicity", "language", "country"
     */
    public java.util.Map<String, Integer> getAllSongRankings(int songId) {
        String sql = """
            WITH song_play_counts AS (
                SELECT s.id, 
                       ar.gender_id,
                       COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) as effective_genre_id,
                       COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) as effective_subgenre_id,
                       ar.ethnicity_id,
                       COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) as effective_language_id,
                       ar.country,
                       COALESCE(COUNT(sc.id), 0) as play_count
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                GROUP BY s.id, ar.gender_id, effective_genre_id, effective_subgenre_id, 
                         ar.ethnicity_id, effective_language_id, ar.country
            ),
            ranked_songs AS (
                SELECT id,
                       gender_id,
                       effective_genre_id,
                       effective_subgenre_id,
                       ethnicity_id,
                       effective_language_id,
                       country,
                       play_count,
                       CASE WHEN gender_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY gender_id ORDER BY play_count DESC) 
                            END as gender_rank,
                       CASE WHEN effective_genre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_genre_id ORDER BY play_count DESC) 
                            END as genre_rank,
                       CASE WHEN effective_subgenre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_subgenre_id ORDER BY play_count DESC) 
                            END as subgenre_rank,
                       CASE WHEN ethnicity_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY ethnicity_id ORDER BY play_count DESC) 
                            END as ethnicity_rank,
                       CASE WHEN effective_language_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_language_id ORDER BY play_count DESC) 
                            END as language_rank,
                       CASE WHEN country IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY country ORDER BY play_count DESC) 
                            END as country_rank
                FROM song_play_counts
            )
            SELECT gender_rank, genre_rank, subgenre_rank, ethnicity_rank, language_rank, country_rank
            FROM ranked_songs
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
        }, songId);
        
        return rankings;
    }
    
    /**
     * Get song rank by gender (position within same artist gender based on play count)
     * @deprecated Use getAllSongRankings() instead for better performance
     */
    @Deprecated
    public Integer getSongRankByGender(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.gender_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE ar.gender_id IS NOT NULL
                GROUP BY s.id, ar.gender_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by genre (position within same genre based on play count, considering overrides)
     */
    public Integer getSongRankByGenre(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) 
                                          ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) IS NOT NULL
                GROUP BY s.id, COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id))
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by subgenre (position within same subgenre based on play count, considering overrides)
     */
    public Integer getSongRankBySubgenre(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) 
                                          ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) IS NOT NULL
                GROUP BY s.id, COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id))
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by ethnicity (position within same artist ethnicity based on play count)
     */
    public Integer getSongRankByEthnicity(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.ethnicity_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE ar.ethnicity_id IS NOT NULL
                GROUP BY s.id, ar.ethnicity_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by language (position within same language based on play count, considering overrides)
     */
    public Integer getSongRankByLanguage(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) 
                                          ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) IS NOT NULL
                GROUP BY s.id, COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id))
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by country (position within same artist country based on play count)
     */
    public Integer getSongRankByCountry(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.country ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE ar.country IS NOT NULL
                GROUP BY s.id, ar.country
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song ranks by year (position within songs that had plays in each year)
     * Returns a map of year -> rank
     */
    public Map<Integer, Integer> getSongRanksByYear(int songId) {
        String sql = """
            SELECT year, rank FROM (
                SELECT s.id, 
                       strftime('%Y', sc.scrobble_date) as year,
                       ROW_NUMBER() OVER (PARTITION BY strftime('%Y', sc.scrobble_date) ORDER BY COUNT(sc.id) DESC) as rank
                FROM Song s
                INNER JOIN Scrobble sc ON sc.song_id = s.id
                GROUP BY s.id, strftime('%Y', sc.scrobble_date)
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
        }, songId);
        return result;
    }

    /**
     * Get song's overall position among all songs by play count.
     */
    public Integer getSongOverallPosition(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                GROUP BY s.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song's position among songs released in the same year.
     * Uses the song's release date, or falls back to album's release date.
     */
    public Integer getSongRankByReleaseYear(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id,
                       strftime('%Y', COALESCE(s.release_date, alb.release_date)) as release_year,
                       ROW_NUMBER() OVER (
                           PARTITION BY strftime('%Y', COALESCE(s.release_date, alb.release_date)) 
                           ORDER BY COALESCE(COUNT(sc.id), 0) DESC
                       ) as rank
                FROM Song s
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE COALESCE(s.release_date, alb.release_date) IS NOT NULL
                GROUP BY s.id, release_year
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get the release year for a song (from song's release_date or album's release_date).
     */
    public Integer getSongReleaseYear(int songId) {
        String sql = """
            SELECT strftime('%Y', COALESCE(s.release_date, alb.release_date)) as release_year
            FROM Song s
            LEFT JOIN Album alb ON s.album_id = alb.id
            WHERE s.id = ?
            """;

        try {
            String yearStr = jdbcTemplate.queryForObject(sql, String.class, songId);
            return yearStr != null ? Integer.parseInt(yearStr) : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song's position among all songs by the same artist.
     */
    public Integer getSongRankByArtist(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id,
                       s.artist_id,
                       ROW_NUMBER() OVER (PARTITION BY s.artist_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                GROUP BY s.id, s.artist_id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song's position among all songs in the same album.
     * Returns null if the song doesn't have an album.
     */
    public Integer getSongRankByAlbum(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id,
                       s.album_id,
                       ROW_NUMBER() OVER (PARTITION BY s.album_id ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE s.album_id IS NOT NULL
                GROUP BY s.id, s.album_id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song rank for Spanish Rap (songs with effective genre=Rap and effective language=Spanish).
     */
    public Integer getSongSpanishRapRank(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(sc.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                INNER JOIN Genre g ON COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) = g.id
                INNER JOIN Language l ON COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) = l.id
                LEFT JOIN Scrobble sc ON sc.song_id = s.id
                WHERE g.name = 'Rap' AND l.name = 'Spanish'
                GROUP BY s.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Check if a song is in the Spanish Rap category (effective genre=Rap AND effective language=Spanish)
     */
    public boolean isSongSpanishRap(int songId) {
        String sql = """
            SELECT COUNT(*) FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            INNER JOIN Genre g ON COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) = g.id
            INNER JOIN Language l ON COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) = l.id
            WHERE s.id = ? AND g.name = 'Rap' AND l.name = 'Spanish'
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public List<Map<String, Object>> getSongDetailsForIds(List<Integer> ids) {
        if (ids == null || ids.isEmpty()) return new ArrayList<>();
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        String sql = "SELECT id, name FROM Song WHERE id IN (" + placeholders + ")";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> song = new HashMap<>();
            song.put("id", rs.getInt("id"));
            song.put("name", rs.getString("name"));
            return song;
        }, ids.toArray());
    }
}