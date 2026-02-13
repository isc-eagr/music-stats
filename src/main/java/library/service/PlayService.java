package library.service;

import library.entity.Play;
import library.repository.PlayRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class PlayService {
    
    private final PlayRepository playRepository;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    
    public PlayService(PlayRepository playRepository, JdbcTemplate jdbcTemplate, PlatformTransactionManager txManager) {
        this.playRepository = playRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(txManager);
    }
    
    /**
     * Creates a normalized lookup key from artist, album, and song name.
     * Uses lowercase, trimming, and accent stripping for exact matching during import.
     * Does NOT remove parentheses or featured artists to avoid false matches.
     * E.g., "José" matches "Jose", but "7 Days" does NOT match "7 Days (feat. Fat Joe)"
     */
    private String createLookupKey(String artist, String album, String song) {
        String a = artist != null ? library.util.StringNormalizer.normalizeForImport(artist) : "";
        String al = album != null ? library.util.StringNormalizer.normalizeForImport(album) : "";
        String s = song != null ? library.util.StringNormalizer.normalizeForImport(song) : "";
        return a + "||" + al + "||" + s;
    }
    
    /**
     * Stream-import plays from a multipart file. Processes rows using a streaming iterator
     * and saves in chunks of `batchSize`. If dryRun is true, it will not write to the DB and
     * will return statistics only.
     * 
     * This avoids loading the whole CSV into memory.
     */
    public Map<String, Integer> importPlaysStream(MultipartFile file, String account, int batchSize, boolean dryRun) throws Exception {
        // Build song lookup map once (artist||album||song -> id)
        Map<String, Integer> songLookup = new HashMap<>();
        String sql = "SELECT s.id as id, a.name as artist, COALESCE(al.name,'') as album, s.name as song FROM song s "
                + "LEFT JOIN artist a ON s.artist_id = a.id "
                + "LEFT JOIN album al ON s.album_id = al.id";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> row : rows) {
            Number idNum = (Number) row.get("id");
            if (idNum == null) continue;
            int id = idNum.intValue();
            String artist = row.get("artist") != null ? row.get("artist").toString() : "";
            String album = row.get("album") != null ? row.get("album").toString() : "";
            String song = row.get("song") != null ? row.get("song").toString() : "";
            String key = createLookupKey(artist, album, song);
            songLookup.putIfAbsent(key, id);
        }

        int totalProcessed = 0;
        int totalMatched = 0;
        int totalUnmatched = 0;
        int totalErrors = 0;

        List<Play> batch = new ArrayList<>(batchSize);

        try (Reader reader = new InputStreamReader(file.getInputStream())) {
            CsvToBean<Play> csvToBean = new CsvToBeanBuilder<Play>(reader)
                    .withType(Play.class)
                    .withSkipLines(1)
                    .withIgnoreQuotations(false)
                    .build();
            Iterator<Play> it = csvToBean.iterator();

            while (true) {
                Play pl = null;
                try {
                    if (!it.hasNext()) break;
                    pl = it.next();
                } catch (Exception e) {
                    // Row-level parsing or introspection error: record and continue
                    totalErrors++;
                    System.err.println("Skipping CSV row due to parse error: " + e.getMessage());
                    // Attempt to clear internal exception and continue - CsvToBean may have captured exceptions
                    continue;
                }

                try {
                    pl.setAccount(account);
                    String key = createLookupKey(pl.getArtist(), pl.getAlbum(), pl.getSong());
                    Integer songId = songLookup.get(key);
                    if (songId != null) {
                        pl.setSongId(songId);
                        totalMatched++;
                    } else {
                        pl.setSongId(null);
                        totalUnmatched++;
                    }
                    batch.add(pl);
                    totalProcessed++;
                } catch (Exception e) {
                    totalErrors++;
                    System.err.println("Skipping play due to processing error: " + e.getMessage());
                    continue;
                }

                if (batch.size() >= batchSize) {
                    final List<Play> toSave = new ArrayList<>(batch);
                    if (!dryRun) {
                        transactionTemplate.execute(status -> {
                            playRepository.saveAll(toSave);
                            return null;
                        });
                    }
                    batch.clear();
                }
            }

            // Save remaining
            if (!batch.isEmpty()) {
                final List<Play> toSave = new ArrayList<>(batch);
                if (!dryRun) {
                    transactionTemplate.execute(status -> {
                        playRepository.saveAll(toSave);
                        return null;
                    });
                }
                batch.clear();
            }
        }

        Map<String, Integer> stats = new HashMap<>();
        stats.put("totalProcessed", totalProcessed);
        stats.put("totalMatched", totalMatched);
        stats.put("totalUnmatched", totalUnmatched);
        stats.put("totalErrors", totalErrors);
        return stats;
    }

    /**
     * Returns unmatched plays grouped by account/artist/album/song with counts, ordered by count desc.
     * If account is null or blank, returns across all accounts; otherwise filters by given account.
     * Each map contains keys: account, artist, album, song, cnt
     */
    public java.util.List<java.util.Map<String, Object>> getUnmatchedPlays(String account) {
        String baseSql = "SELECT COALESCE(account,'') as account, COALESCE(artist,'') as artist, COALESCE(album,'') as album, COALESCE(song,'') as song, COUNT(*) as cnt "
                + "FROM play WHERE song_id IS NULL ";
        java.util.List<java.util.Map<String, Object>> rows;
        if (account == null) {
            String sql = baseSql + "GROUP BY account, artist, album, song ORDER BY cnt DESC";
            rows = jdbcTemplate.queryForList(sql);
        } else if ("__BLANK__".equals(account)) {
            // Special marker: only blank/null accounts
            String sql = baseSql + "AND (account IS NULL OR account = '') GROUP BY account, artist, album, song ORDER BY cnt DESC";
            rows = jdbcTemplate.queryForList(sql);
        } else {
            String sql = baseSql + "AND account = ? GROUP BY account, artist, album, song ORDER BY cnt DESC";
            rows = jdbcTemplate.queryForList(sql, account);
        }
        return rows;
    }

    /**
     * Returns a list of distinct accounts present in play table (including empty string for nulls).
     */
    public java.util.List<String> getDistinctAccounts() {
        String sql = "SELECT DISTINCT COALESCE(account,'') as account FROM play ORDER BY account";
        java.util.List<java.util.Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        java.util.List<String> accounts = new java.util.ArrayList<>();
        for (java.util.Map<String, Object> r : rows) {
            Object a = r.get("account");
            accounts.add(a == null ? "" : a.toString());
        }
        return accounts;
    }
    
    /**
     * Assigns all matching unmatched plays to a song.
     * Matches by artist, album, and song name (case-insensitive) across ALL accounts.
     * Only updates plays that don't already have a song_id.
     * Also updates the play's artist, album, and song fields to match the canonical song data.
     * 
     * @param account The account name (ignored - matches all accounts)
     * @param artist The artist name from the play
     * @param album The album name from the play
     * @param song The song name from the play
     * @param songId The song ID to assign
     * @return The number of plays updated
     */
    public int assignPlaysToSong(String account, String artist, String album, String song, Integer songId) {
        // First, fetch the canonical names from the song
        String lookupSql = "SELECT s.name as song_name, ar.name as artist_name, COALESCE(al.name, '') as album_name " +
                           "FROM song s " +
                           "JOIN artist ar ON s.artist_id = ar.id " +
                           "LEFT JOIN album al ON s.album_id = al.id " +
                           "WHERE s.id = ?";
        
        Map<String, Object> songData = null;
        try {
            songData = jdbcTemplate.queryForMap(lookupSql, songId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            // Song not found
            return 0;
        }
        
        String canonicalArtist = (String) songData.get("artist_name");
        String canonicalAlbum = (String) songData.get("album_name");
        String canonicalSong = (String) songData.get("song_name");
        
        // Update plays with song_id AND canonical names
        String sql = "UPDATE play SET song_id = ?, artist = ?, album = ?, song = ? " +
                     "WHERE song_id IS NULL " +
                     "AND COALESCE(LOWER(artist),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(album),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(song),'') = LOWER(?)";
        
        return jdbcTemplate.update(sql, 
            songId,
            canonicalArtist,
            canonicalAlbum != null ? canonicalAlbum : "",
            canonicalSong,
            artist != null ? artist : "",
            album != null ? album : "",
            song != null ? song : ""
        );
    }
    
    /**
     * Returns the maximum lastfm_id for each account.
     * @return Map of account name to max lastfm_id
     */
    public Map<String, Integer> getMaxLastfmIdByAccount() {
        String sql = "SELECT COALESCE(account,'') as account, MAX(lastfm_id) as max_id FROM play GROUP BY account ORDER BY account";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        Map<String, Integer> result = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String account = row.get("account") != null ? row.get("account").toString() : "";
            Number maxId = (Number) row.get("max_id");
            result.put(account, maxId != null ? maxId.intValue() : 0);
        }
        return result;
    }
    
    /**
     * Import result containing stats and grouped unmatched plays.
     */
    public static class ImportResult {
        public Map<String, Integer> stats;
        public List<Map<String, Object>> unmatchedGrouped;
        public ValidationResult validation;
        
        public ImportResult(Map<String, Integer> stats, List<Map<String, Object>> unmatchedGrouped) {
            this.stats = stats;
            this.unmatchedGrouped = unmatchedGrouped;
        }
        
        public ImportResult(Map<String, Integer> stats, List<Map<String, Object>> unmatchedGrouped, ValidationResult validation) {
            this.stats = stats;
            this.unmatchedGrouped = unmatchedGrouped;
            this.validation = validation;
        }
    }
    
    /**
     * Validation result comparing Last.fm playcount to local play count.
     */
    public static class ValidationResult {
        public int lastfmPlaycount;
        public int localPlayCount;
        public boolean matches;
        public int difference;
        
        public ValidationResult(int lastfmPlaycount, int localPlayCount) {
            this.lastfmPlaycount = lastfmPlaycount;
            this.localPlayCount = localPlayCount;
            this.matches = lastfmPlaycount == localPlayCount;
            this.difference = lastfmPlaycount - localPlayCount;
        }
    }
    
    /**
     * Gets the total count of plays for a specific account.
     */
    public int getPlayCountByAccount(String account) {
        String sql = "SELECT COUNT(*) FROM play WHERE account = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, account);
        return count != null ? count : 0;
    }
    
    /**
     * Fetches the playcount from Last.fm user.getinfo API.
     */
    public int getLastfmPlaycount(String account, String apiKey) throws Exception {
        String url = String.format(
            "http://ws.audioscrobbler.com/2.0/?method=user.getinfo&user=%s&api_key=%s&format=json",
            account, apiKey
        );
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET()
            .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            throw new RuntimeException("Last.fm API returned status " + response.statusCode());
        }
        
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response.body());
        String playcountStr = root.path("user").path("playcount").asText("0");
        return Integer.parseInt(playcountStr);
    }
    
    /**
     * Validates that local play count matches Last.fm playcount.
     */
    public ValidationResult validatePlayCount(String account, String apiKey) throws Exception {
        int lastfmPlaycount = getLastfmPlaycount(account, apiKey);
        int localCount = getPlayCountByAccount(account);
        return new ValidationResult(lastfmPlaycount, localCount);
    }
    
    /**
     * Stream-import plays with full result including unmatched list.
     */
    public ImportResult importPlaysWithUnmatched(MultipartFile file, String account, int batchSize, boolean dryRun) throws Exception {
        // Build song lookup map once (artist||album||song -> id)
        Map<String, Integer> songLookup = new HashMap<>();
        String sql = "SELECT s.id as id, a.name as artist, COALESCE(al.name,'') as album, s.name as song FROM song s "
                + "LEFT JOIN artist a ON s.artist_id = a.id "
                + "LEFT JOIN album al ON s.album_id = al.id";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> row : rows) {
            Number idNum = (Number) row.get("id");
            if (idNum == null) continue;
            int id = idNum.intValue();
            String artist = row.get("artist") != null ? row.get("artist").toString() : "";
            String album = row.get("album") != null ? row.get("album").toString() : "";
            String song = row.get("song") != null ? row.get("song").toString() : "";
            String key = createLookupKey(artist, album, song);
            songLookup.putIfAbsent(key, id);
        }

        int totalProcessed = 0;
        int totalMatched = 0;
        int totalUnmatched = 0;
        int totalErrors = 0;
        
        // Track unmatched plays grouped by artist/album/song
        Map<String, int[]> unmatchedCounts = new HashMap<>(); // key -> [count]

        List<Play> batch = new ArrayList<>(batchSize);

        try (Reader reader = new InputStreamReader(file.getInputStream())) {
            CsvToBean<Play> csvToBean = new CsvToBeanBuilder<Play>(reader)
                    .withType(Play.class)
                    .withSkipLines(1)
                    .withIgnoreQuotations(false)
                    .build();
            Iterator<Play> it = csvToBean.iterator();

            while (true) {
                Play pl = null;
                try {
                    if (!it.hasNext()) break;
                    pl = it.next();
                } catch (Exception e) {
                    totalErrors++;
                    System.err.println("Skipping CSV row due to parse error: " + e.getMessage());
                    continue;
                }

                try {
                    pl.setAccount(account);
                    String key = createLookupKey(pl.getArtist(), pl.getAlbum(), pl.getSong());
                    Integer songId = songLookup.get(key);
                    if (songId != null) {
                        pl.setSongId(songId);
                        totalMatched++;
                    } else {
                        pl.setSongId(null);
                        totalUnmatched++;
                        
                        // Track for unmatched grouping
                        String unmatchedKey = (pl.getArtist() != null ? pl.getArtist() : "") + "||" +
                                              (pl.getAlbum() != null ? pl.getAlbum() : "") + "||" +
                                              (pl.getSong() != null ? pl.getSong() : "");
                        unmatchedCounts.computeIfAbsent(unmatchedKey, k -> new int[]{0})[0]++;
                    }
                    batch.add(pl);
                    totalProcessed++;
                } catch (Exception e) {
                    totalErrors++;
                    System.err.println("Skipping play due to processing error: " + e.getMessage());
                    continue;
                }

                if (batch.size() >= batchSize) {
                    final List<Play> toSave = new ArrayList<>(batch);
                    if (!dryRun) {
                        transactionTemplate.execute(status -> {
                            playRepository.saveAll(toSave);
                            return null;
                        });
                    }
                    batch.clear();
                }
            }

            // Save remaining
            if (!batch.isEmpty()) {
                final List<Play> toSave = new ArrayList<>(batch);
                if (!dryRun) {
                    transactionTemplate.execute(status -> {
                        playRepository.saveAll(toSave);
                        return null;
                    });
                }
                batch.clear();
            }
        }

        Map<String, Integer> stats = new HashMap<>();
        stats.put("totalProcessed", totalProcessed);
        stats.put("totalMatched", totalMatched);
        stats.put("totalUnmatched", totalUnmatched);
        stats.put("totalErrors", totalErrors);
        
        // Convert unmatched to list of maps sorted by count desc
        List<Map<String, Object>> unmatchedGrouped = new ArrayList<>();
        for (Map.Entry<String, int[]> entry : unmatchedCounts.entrySet()) {
            String[] parts = entry.getKey().split("\\|\\|", -1);
            Map<String, Object> row = new HashMap<>();
            row.put("artist", parts.length > 0 ? parts[0] : "");
            row.put("album", parts.length > 1 ? parts[1] : "");
            row.put("song", parts.length > 2 ? parts[2] : "");
            row.put("account", account);
            row.put("cnt", entry.getValue()[0]);
            unmatchedGrouped.add(row);
        }
        unmatchedGrouped.sort((a, b) -> ((Integer) b.get("cnt")).compareTo((Integer) a.get("cnt")));
        
        return new ImportResult(stats, unmatchedGrouped);
    }
    
    /**
     * Deletes unmatched plays that match the given account/artist/album/song.
     * Only deletes plays that don't have a song_id (unmatched).
     * 
     * @param account The account name from the play
     * @param artist The artist name from the play
     * @param album The album name from the play
     * @param song The song name from the play
     * @return The number of plays deleted
     */
    public int deleteUnmatchedPlays(String account, String artist, String album, String song) {
        String sql = "DELETE FROM play " +
                     "WHERE song_id IS NULL " +
                     "AND COALESCE(account,'') = ? " +
                     "AND COALESCE(artist,'') = ? " +
                     "AND COALESCE(album,'') = ? " +
                     "AND COALESCE(song,'') = ?";
        
        return jdbcTemplate.update(sql, 
            account != null ? account : "",
            artist != null ? artist : "",
            album != null ? album : "",
            song != null ? song : ""
        );
    }
    
    /**
     * Assigns plays to a song and updates the play text fields to canonical names.
     * Matches by artist, album, and song name (case-insensitive) across ALL accounts.
     * Only updates plays that don't already have a song_id.
     * 
     * @param playAccount The account name from the play (ignored - matches all accounts)
     * @param playArtist The artist name from the play
     * @param playAlbum The album name from the play
     * @param playSong The song name from the play
     * @param songId The song ID to assign
     * @param canonicalArtist The canonical artist name
     * @param canonicalAlbum The canonical album name (can be null)
     * @param canonicalSong The canonical song name
     * @return The number of plays updated
     */
    public int assignPlaysToSongWithCanonicalNames(String playAccount, String playArtist, 
            String playAlbum, String playSong, Integer songId,
            String canonicalArtist, String canonicalAlbum, String canonicalSong) {
        String sql = "UPDATE play SET song_id = ?, artist = ?, album = ?, song = ? " +
                     "WHERE song_id IS NULL " +
                     "AND COALESCE(LOWER(artist),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(album),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(song),'') = LOWER(?)";
        
        return jdbcTemplate.update(sql, 
            songId,
            canonicalArtist,
            canonicalAlbum != null ? canonicalAlbum : "",
            canonicalSong,
            playArtist != null ? playArtist : "",
            playAlbum != null ? playAlbum : "",
            playSong != null ? playSong : ""
        );
    }
    
    /**
     * Fetches recent plays from Last.fm API and imports them.
     * Uses the Last.fm API endpoint with the 'from' parameter set to maxLastfmId + 1.
     * Handles pagination by iterating through all pages.
     * 
     * @param account The account name (e.g., "vatito")
     * @param apiKey The Last.fm API key
     * @return ImportResult with stats and unmatched list
     */
    public ImportResult fetchAndImportPlaysFromLastfm(String account, String apiKey) throws Exception {
        // Get the max lastfm_id for this account
        Map<String, Integer> maxIds = getMaxLastfmIdByAccount();
        int maxLastfmId = maxIds.getOrDefault(account, 0);
        int fromTimestamp = maxLastfmId + 1;
        
        // Build song lookup map once
        Map<String, Integer> songLookup = new HashMap<>();
        String sql = "SELECT s.id as id, a.name as artist, COALESCE(al.name,'') as album, s.name as song FROM song s "
                + "LEFT JOIN artist a ON s.artist_id = a.id "
                + "LEFT JOIN album al ON s.album_id = al.id";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> row : rows) {
            Number idNum = (Number) row.get("id");
            if (idNum == null) continue;
            int id = idNum.intValue();
            String artist = row.get("artist") != null ? row.get("artist").toString() : "";
            String album = row.get("album") != null ? row.get("album").toString() : "";
            String song = row.get("song") != null ? row.get("song").toString() : "";
            String key = createLookupKey(artist, album, song);
            songLookup.putIfAbsent(key, id);
        }
        
        HttpClient client = HttpClient.newHttpClient();
        ObjectMapper mapper = new ObjectMapper();
        
        int totalProcessed = 0;
        int totalMatched = 0;
        int totalUnmatched = 0;
        int totalErrors = 0;
        Map<String, int[]> unmatchedCounts = new HashMap<>();
        List<Play> allPlays = new ArrayList<>();
        
        int currentPage = 1;
        int totalPages = 1;
        
        do {
            // Build the API URL with page parameter
            String url = String.format(
                "http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=%s&api_key=%s&format=json&from=%d&page=%d&limit=200",
                account, apiKey, fromTimestamp, currentPage
            );
            
            // Fetch from the API
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
            
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() != 200) {
                throw new RuntimeException("Last.fm API returned status " + response.statusCode() + " on page " + currentPage);
            }
            
            // Parse JSON response
            JsonNode root = mapper.readTree(response.body());
            JsonNode recenttracks = root.path("recenttracks");
            JsonNode tracks = recenttracks.path("track");
            
            // Get total pages from the first request
            if (currentPage == 1) {
                JsonNode attr = recenttracks.path("@attr");
                totalPages = attr.path("totalPages").asInt(1);
            }
            
            if (!tracks.isArray()) {
                // No tracks on this page, move on
                currentPage++;
                continue;
            }
            
            for (JsonNode track : tracks) {
                try {
                    // Skip "now playing" tracks that don't have a date
                    JsonNode dateNode = track.path("date");
                    if (dateNode.isMissingNode() || !dateNode.has("uts")) {
                        continue;
                    }
                    
                    String artistName = track.path("artist").path("#text").asText("");
                    String albumName = track.path("album").path("#text").asText("");
                    String songName = track.path("name").asText("");
                    String utsStr = dateNode.path("uts").asText("");
                    
                    if (utsStr.isEmpty()) {
                        totalErrors++;
                        continue;
                    }
                    
                    // Parse the UTS timestamp
                    long uts = Long.parseLong(utsStr);
                    
                    // Convert to Mexico City timezone
                    ZonedDateTime utcDate = Instant.ofEpochSecond(uts).atZone(ZoneId.of("UTC"));
                    ZonedDateTime mexicoDate = utcDate.withZoneSameInstant(ZoneId.of("America/Mexico_City"));
                    String formattedDate = mexicoDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
                    
                    Play pl = new Play();
                    pl.setLastfmId((int) uts); // Use the UTS timestamp as the lastfm_id
                    pl.setArtist(artistName);
                    pl.setAlbum(albumName);
                    pl.setSong(songName);
                    pl.setAccount(account);
                    // Set playDate directly without parsing (already formatted)
                    try {
                        java.lang.reflect.Field field = Play.class.getDeclaredField("playDate");
                        field.setAccessible(true);
                        field.set(pl, formattedDate);
                    } catch (Exception e) {
                        pl.setPlayDate(formattedDate);
                    }
                    
                    // Match to song
                    String key = createLookupKey(artistName, albumName, songName);
                    Integer songId = songLookup.get(key);
                    if (songId != null) {
                        pl.setSongId(songId);
                        totalMatched++;
                    } else {
                        pl.setSongId(null);
                        totalUnmatched++;
                        
                        String unmatchedKey = artistName + "||" + albumName + "||" + songName;
                        unmatchedCounts.computeIfAbsent(unmatchedKey, k -> new int[]{0})[0]++;
                    }
                    
                    allPlays.add(pl);
                    totalProcessed++;
                    
                } catch (Exception e) {
                    totalErrors++;
                    System.err.println("Error processing track: " + e.getMessage());
                }
            }
            
            currentPage++;
            
        } while (currentPage <= totalPages);
        
        // Save all plays
        if (!allPlays.isEmpty()) {
            final List<Play> toSave = new ArrayList<>(allPlays);
            transactionTemplate.execute(status -> {
                playRepository.saveAll(toSave);
                return null;
            });
        }
        
        Map<String, Integer> stats = new HashMap<>();
        stats.put("totalProcessed", totalProcessed);
        stats.put("totalMatched", totalMatched);
        stats.put("totalUnmatched", totalUnmatched);
        stats.put("totalErrors", totalErrors);
        stats.put("totalPages", totalPages);
        
        // Convert unmatched to list of maps sorted by count desc
        List<Map<String, Object>> unmatchedGrouped = new ArrayList<>();
        for (Map.Entry<String, int[]> entry : unmatchedCounts.entrySet()) {
            String[] parts = entry.getKey().split("\\|\\|", -1);
            Map<String, Object> row = new HashMap<>();
            row.put("artist", parts.length > 0 ? parts[0] : "");
            row.put("album", parts.length > 1 ? parts[1] : "");
            row.put("song", parts.length > 2 ? parts[2] : "");
            row.put("account", account);
            row.put("cnt", entry.getValue()[0]);
            unmatchedGrouped.add(row);
        }
        unmatchedGrouped.sort((a, b) -> ((Integer) b.get("cnt")).compareTo((Integer) a.get("cnt")));
        
        // Validate play count against Last.fm
        ValidationResult validation = null;
        try {
            validation = validatePlayCount(account, apiKey);
        } catch (Exception e) {
            System.err.println("Failed to validate play count: " + e.getMessage());
        }
        
        return new ImportResult(stats, unmatchedGrouped, validation);
    }
    
    /**
     * Deletes all plays from the last N days.
     * 
     * @param days Number of days to delete (e.g., 5, 10, 20, 30, 60, 90)
     * @return The number of plays deleted
     */
    public int deleteRecentPlays(int days) {
        String sql = "DELETE FROM play WHERE play_date > date('now', '-' || ? || ' days')";
        return jdbcTemplate.update(sql, days);
    }
    
    /**
     * Deletes all plays for a specific song.
     * This is a dangerous operation that removes all listening history for the song.
     * 
     * @param songId The ID of the song whose plays should be deleted
     * @return The number of plays deleted
     */
    public int deletePlaysForSong(Long songId) {
        String sql = "DELETE FROM play WHERE song_id = ?";
        return jdbcTemplate.update(sql, songId);
    }
    
    /**
     * Get the total number of unique days since the first play in the database.
     */
    public int getTotalDaysSinceFirstPlay() {
        String sql = "SELECT CAST(julianday('now') - julianday(MIN(DATE(play_date))) AS INTEGER) + 1 FROM Play WHERE play_date IS NOT NULL";
        try {
            Integer days = jdbcTemplate.queryForObject(sql, Integer.class);
            return days != null && days > 0 ? days : 1;
        } catch (Exception e) {
            return 1;
        }
    }
    
    /**
     * Get the total number of unique weeks since the first play in the database.
     */
    public int getTotalWeeksSinceFirstPlay() {
        try {
            // Calculate weeks between first play and now
            String sql = """
                SELECT CAST((julianday('now') - julianday(MIN(DATE(play_date)))) / 7 AS INTEGER) + 1 
                FROM Play WHERE play_date IS NOT NULL
                """;
            Integer weeks = jdbcTemplate.queryForObject(sql, Integer.class);
            return weeks != null && weeks > 0 ? weeks : 1;
        } catch (Exception e) {
            return 1;
        }
    }
    
    /**
     * Get the total number of unique months since the first play in the database.
     */
    public int getTotalMonthsSinceFirstPlay() {
        String sql = """
            SELECT 
                (CAST(strftime('%Y', 'now') AS INTEGER) - CAST(strftime('%Y', MIN(play_date)) AS INTEGER)) * 12 +
                (CAST(strftime('%m', 'now') AS INTEGER) - CAST(strftime('%m', MIN(play_date)) AS INTEGER)) + 1
            FROM Play WHERE play_date IS NOT NULL
            """;
        try {
            Integer months = jdbcTemplate.queryForObject(sql, Integer.class);
            return months != null && months > 0 ? months : 1;
        } catch (Exception e) {
            return 1;
        }
    }
    
    /**
     * Get the total number of unique years since the first play in the database.
     */
    public int getTotalYearsSinceFirstPlay() {
        String sql = """
            SELECT 
                CAST(strftime('%Y', 'now') AS INTEGER) - CAST(strftime('%Y', MIN(play_date)) AS INTEGER) + 1
            FROM Play WHERE play_date IS NOT NULL
            """;
        try {
            Integer years = jdbcTemplate.queryForObject(sql, Integer.class);
            return years != null && years > 0 ? years : 1;
        } catch (Exception e) {
            return 1;
        }
    }
}
