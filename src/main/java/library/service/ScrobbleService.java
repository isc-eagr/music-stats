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
     * Populates song_id for scrobbles by matching artist, album, and song name.
     * Processes in batches of 2000 to handle large datasets efficiently.
     * 
     * @return Statistics about the matching process
     */
    public Map<String, Integer> populateSongIds() {
        Map<String, Integer> stats = new HashMap<>();
        int totalProcessed = 0;
        int totalMatched = 0;
        int totalUnmatched = 0;
        
        // Build a lookup map from the database using a native query that joins song->artist and song->album
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
            // prefer existing mapping if present; don't override
            songLookup.putIfAbsent(key, id);
        }
        
        // Process scrobbles in batches
        int batchSize = 2000;
        int pageNumber = 0;
        boolean hasMore = true;
        
        while (hasMore) {
            List<Scrobble> batch = getScrobbleBatch(pageNumber, batchSize);
            
            if (batch.isEmpty()) {
                hasMore = false;
            } else {
                // Execute each batch saving inside its own transaction so it commits every batch
                Integer batchMatched = transactionTemplate.execute(status -> processBatch(batch, songLookup));
                int matchedInBatch = batchMatched == null ? 0 : batchMatched;
                totalProcessed += batch.size();
                totalMatched += matchedInBatch;
                totalUnmatched += (batch.size() - matchedInBatch);
                
                pageNumber++;
                
                // Log progress every batch
                System.out.println(String.format(
                    "Processed %d scrobbles (Matched: %d, Unmatched: %d)",
                    totalProcessed, totalMatched, totalUnmatched
                ));
            }
        }
        
        stats.put("totalProcessed", totalProcessed);
        stats.put("totalMatched", totalMatched);
        stats.put("totalUnmatched", totalUnmatched);
        
        return stats;
    }
    
    /**
     * Gets a batch of scrobbles using offset-based pagination
     */
    private List<Scrobble> getScrobbleBatch(int pageNumber, int batchSize) {
        return scrobbleRepository.findAll(
            org.springframework.data.domain.PageRequest.of(pageNumber, batchSize)
        ).getContent();
    }
    
    /**
     * Processes a batch of scrobbles and saves them with matched song_ids.
     * Returns the number of matched scrobbles in the batch.
     */
    protected int processBatch(List<Scrobble> scrobbles, Map<String, Integer> songLookup) {
        int matched = 0;
        
        for (Scrobble scrobble : scrobbles) {
            String key = createLookupKey(scrobble.getArtist(), scrobble.getAlbum(), scrobble.getSong());
            Integer songId = songLookup.get(key);
            
            if (songId != null) {
                scrobble.setSongId(songId);
                matched++;
            } else {
                // Leave as null to indicate no match found (or set to 0 if you prefer)
                scrobble.setSongId(null);
            }
        }
        
        // Save the entire batch at once
        scrobbleRepository.saveAll(scrobbles);
        
        return matched;
    }
    
    /**
     * Creates a normalized lookup key from artist, album, and song name.
     * Uses lowercase and trimming for better matching.
     */
    private String createLookupKey(String artist, String album, String song) {
        String a = artist != null ? artist.toLowerCase().trim() : "";
        String al = album != null ? album.toLowerCase().trim() : "";
        String s = song != null ? song.toLowerCase().trim() : "";
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
}
