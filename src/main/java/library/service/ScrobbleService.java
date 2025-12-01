package library.service;

import library.entity.Scrobble;
import library.repository.ScrobbleRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class ScrobbleService {
    
    private final ScrobbleRepository scrobbleRepository;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    
    public ScrobbleService(ScrobbleRepository scrobbleRepository, JdbcTemplate jdbcTemplate, PlatformTransactionManager txManager) {
        this.scrobbleRepository = scrobbleRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(txManager);
    }
    
    /**
     * Creates a normalized lookup key from artist, album, and song name.
     * Uses lowercase, trimming, and accent stripping for better matching.
     * E.g., "José" matches "Jose", "María" matches "Maria"
     */
    private String createLookupKey(String artist, String album, String song) {
        String a = artist != null ? library.util.StringNormalizer.normalizeForSearch(artist) : "";
        String al = album != null ? library.util.StringNormalizer.normalizeForSearch(album) : "";
        String s = song != null ? library.util.StringNormalizer.normalizeForSearch(song) : "";
        return a + "||" + al + "||" + s;
    }
    
    /**
     * Stream-import scrobbles from a multipart file. Processes rows using a streaming iterator
     * and saves in chunks of `batchSize`. If dryRun is true, it will not write to the DB and
     * will return statistics only.
     * 
     * This avoids loading the whole CSV into memory.
     */
    public Map<String, Integer> importScrobblesStream(MultipartFile file, String account, int batchSize, boolean dryRun) throws Exception {
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

        List<Scrobble> batch = new ArrayList<>(batchSize);

        try (Reader reader = new InputStreamReader(file.getInputStream())) {
            CsvToBean<Scrobble> csvToBean = new CsvToBeanBuilder<Scrobble>(reader)
                    .withType(Scrobble.class)
                    .withSkipLines(1)
                    .withIgnoreQuotations(false)
                    .build();
            Iterator<Scrobble> it = csvToBean.iterator();

            while (true) {
                Scrobble sc = null;
                try {
                    if (!it.hasNext()) break;
                    sc = it.next();
                } catch (Exception e) {
                    // Row-level parsing or introspection error: record and continue
                    totalErrors++;
                    System.err.println("Skipping CSV row due to parse error: " + e.getMessage());
                    // Attempt to clear internal exception and continue - CsvToBean may have captured exceptions
                    continue;
                }

                try {
                    sc.setAccount(account);
                    String key = createLookupKey(sc.getArtist(), sc.getAlbum(), sc.getSong());
                    Integer songId = songLookup.get(key);
                    if (songId != null) {
                        sc.setSongId(songId);
                        totalMatched++;
                    } else {
                        sc.setSongId(null);
                        totalUnmatched++;
                    }
                    batch.add(sc);
                    totalProcessed++;
                } catch (Exception e) {
                    totalErrors++;
                    System.err.println("Skipping scrobble due to processing error: " + e.getMessage());
                    continue;
                }

                if (batch.size() >= batchSize) {
                    final List<Scrobble> toSave = new ArrayList<>(batch);
                    if (!dryRun) {
                        transactionTemplate.execute(status -> {
                            scrobbleRepository.saveAll(toSave);
                            return null;
                        });
                    }
                    batch.clear();
                }
            }

            // Save remaining
            if (!batch.isEmpty()) {
                final List<Scrobble> toSave = new ArrayList<>(batch);
                if (!dryRun) {
                    transactionTemplate.execute(status -> {
                        scrobbleRepository.saveAll(toSave);
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
     * Returns unmatched scrobbles grouped by account/artist/album/song with counts, ordered by count desc.
     * If account is null or blank, returns across all accounts; otherwise filters by given account.
     * Each map contains keys: account, artist, album, song, cnt
     */
    public java.util.List<java.util.Map<String, Object>> getUnmatchedScrobbles(String account) {
        String baseSql = "SELECT COALESCE(account,'') as account, COALESCE(artist,'') as artist, COALESCE(album,'') as album, COALESCE(song,'') as song, COUNT(*) as cnt "
                + "FROM scrobble WHERE song_id IS NULL ";
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
     * Returns a list of distinct accounts present in scrobble table (including empty string for nulls).
     */
    public java.util.List<String> getDistinctAccounts() {
        String sql = "SELECT DISTINCT COALESCE(account,'') as account FROM scrobble ORDER BY account";
        java.util.List<java.util.Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        java.util.List<String> accounts = new java.util.ArrayList<>();
        for (java.util.Map<String, Object> r : rows) {
            Object a = r.get("account");
            accounts.add(a == null ? "" : a.toString());
        }
        return accounts;
    }
    
    /**
     * Assigns all matching unmatched scrobbles to a song.
     * Matches by artist, album, and song name (case-insensitive) across ALL accounts.
     * Only updates scrobbles that don't already have a song_id.
     * 
     * @param account The account name (ignored - matches all accounts)
     * @param artist The artist name from the scrobble
     * @param album The album name from the scrobble
     * @param song The song name from the scrobble
     * @param songId The song ID to assign
     * @return The number of scrobbles updated
     */
    public int assignScrobblesToSong(String account, String artist, String album, String song, Integer songId) {
        String sql = "UPDATE scrobble SET song_id = ? " +
                     "WHERE song_id IS NULL " +
                     "AND COALESCE(LOWER(artist),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(album),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(song),'') = LOWER(?)";
        
        return jdbcTemplate.update(sql, 
            songId, 
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
        String sql = "SELECT COALESCE(account,'') as account, MAX(lastfm_id) as max_id FROM scrobble GROUP BY account ORDER BY account";
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
     * Import result containing stats and grouped unmatched scrobbles.
     */
    public static class ImportResult {
        public Map<String, Integer> stats;
        public List<Map<String, Object>> unmatchedGrouped;
        
        public ImportResult(Map<String, Integer> stats, List<Map<String, Object>> unmatchedGrouped) {
            this.stats = stats;
            this.unmatchedGrouped = unmatchedGrouped;
        }
    }
    
    /**
     * Stream-import scrobbles with full result including unmatched list.
     */
    public ImportResult importScrobblesWithUnmatched(MultipartFile file, String account, int batchSize, boolean dryRun) throws Exception {
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
        
        // Track unmatched scrobbles grouped by artist/album/song
        Map<String, int[]> unmatchedCounts = new HashMap<>(); // key -> [count]

        List<Scrobble> batch = new ArrayList<>(batchSize);

        try (Reader reader = new InputStreamReader(file.getInputStream())) {
            CsvToBean<Scrobble> csvToBean = new CsvToBeanBuilder<Scrobble>(reader)
                    .withType(Scrobble.class)
                    .withSkipLines(1)
                    .withIgnoreQuotations(false)
                    .build();
            Iterator<Scrobble> it = csvToBean.iterator();

            while (true) {
                Scrobble sc = null;
                try {
                    if (!it.hasNext()) break;
                    sc = it.next();
                } catch (Exception e) {
                    totalErrors++;
                    System.err.println("Skipping CSV row due to parse error: " + e.getMessage());
                    continue;
                }

                try {
                    sc.setAccount(account);
                    String key = createLookupKey(sc.getArtist(), sc.getAlbum(), sc.getSong());
                    Integer songId = songLookup.get(key);
                    if (songId != null) {
                        sc.setSongId(songId);
                        totalMatched++;
                    } else {
                        sc.setSongId(null);
                        totalUnmatched++;
                        
                        // Track for unmatched grouping
                        String unmatchedKey = (sc.getArtist() != null ? sc.getArtist() : "") + "||" +
                                              (sc.getAlbum() != null ? sc.getAlbum() : "") + "||" +
                                              (sc.getSong() != null ? sc.getSong() : "");
                        unmatchedCounts.computeIfAbsent(unmatchedKey, k -> new int[]{0})[0]++;
                    }
                    batch.add(sc);
                    totalProcessed++;
                } catch (Exception e) {
                    totalErrors++;
                    System.err.println("Skipping scrobble due to processing error: " + e.getMessage());
                    continue;
                }

                if (batch.size() >= batchSize) {
                    final List<Scrobble> toSave = new ArrayList<>(batch);
                    if (!dryRun) {
                        transactionTemplate.execute(status -> {
                            scrobbleRepository.saveAll(toSave);
                            return null;
                        });
                    }
                    batch.clear();
                }
            }

            // Save remaining
            if (!batch.isEmpty()) {
                final List<Scrobble> toSave = new ArrayList<>(batch);
                if (!dryRun) {
                    transactionTemplate.execute(status -> {
                        scrobbleRepository.saveAll(toSave);
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
     * Deletes unmatched scrobbles that match the given account/artist/album/song.
     * Only deletes scrobbles that don't have a song_id (unmatched).
     * 
     * @param account The account name from the scrobble
     * @param artist The artist name from the scrobble
     * @param album The album name from the scrobble
     * @param song The song name from the scrobble
     * @return The number of scrobbles deleted
     */
    public int deleteUnmatchedScrobbles(String account, String artist, String album, String song) {
        String sql = "DELETE FROM scrobble " +
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
     * Assigns scrobbles to a song and updates the scrobble text fields to canonical names.
     * Matches by artist, album, and song name (case-insensitive) across ALL accounts.
     * Only updates scrobbles that don't already have a song_id.
     * 
     * @param scrobbleAccount The account name from the scrobble (ignored - matches all accounts)
     * @param scrobbleArtist The artist name from the scrobble
     * @param scrobbleAlbum The album name from the scrobble
     * @param scrobbleSong The song name from the scrobble
     * @param songId The song ID to assign
     * @param canonicalArtist The canonical artist name
     * @param canonicalAlbum The canonical album name (can be null)
     * @param canonicalSong The canonical song name
     * @return The number of scrobbles updated
     */
    public int assignScrobblesToSongWithCanonicalNames(String scrobbleAccount, String scrobbleArtist, 
            String scrobbleAlbum, String scrobbleSong, Integer songId,
            String canonicalArtist, String canonicalAlbum, String canonicalSong) {
        String sql = "UPDATE scrobble SET song_id = ?, artist = ?, album = ?, song = ? " +
                     "WHERE song_id IS NULL " +
                     "AND COALESCE(LOWER(artist),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(album),'') = LOWER(?) " +
                     "AND COALESCE(LOWER(song),'') = LOWER(?)";
        
        return jdbcTemplate.update(sql, 
            songId,
            canonicalArtist,
            canonicalAlbum != null ? canonicalAlbum : "",
            canonicalSong,
            scrobbleArtist != null ? scrobbleArtist : "",
            scrobbleAlbum != null ? scrobbleAlbum : "",
            scrobbleSong != null ? scrobbleSong : ""
        );
    }
}
