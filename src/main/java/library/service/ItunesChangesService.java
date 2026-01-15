package library.service;

import library.dto.*;
import library.entity.ItunesSnapshot;
import library.repository.ItunesSnapshotRepository;
import library.service.ItunesService.ItunesSong;
import library.util.StringNormalizer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for detecting changes in the iTunes library between snapshots.
 * Compares the current iTunes XML with the last saved snapshot to find:
 * - Songs with changed artist/album/title
 * - Songs that were added to iTunes
 * - Songs that were removed from iTunes
 */
@Service
public class ItunesChangesService {

    private final ItunesService itunesService;
    private final ItunesSnapshotRepository snapshotRepository;
    private final JdbcTemplate jdbcTemplate;

    public ItunesChangesService(ItunesService itunesService, 
                                 ItunesSnapshotRepository snapshotRepository,
                                 JdbcTemplate jdbcTemplate) {
        this.itunesService = itunesService;
        this.snapshotRepository = snapshotRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Detect all changes between the current iTunes XML and the saved snapshot.
     */
    public ItunesChangesResultDTO detectChanges() throws Exception {
        if (!itunesService.libraryExists()) {
            return new ItunesChangesResultDTO(null, false, 
                List.of(), List.of(), List.of());
        }

        boolean hasSnapshot = snapshotRepository.hasAnySnapshot();
        LocalDateTime lastSnapshotDate = hasSnapshot ? snapshotRepository.getLastSnapshotDate() : null;

        if (!hasSnapshot) {
            // First run - no changes to report, just show that snapshot needs to be saved
            return new ItunesChangesResultDTO(null, false,
                List.of(), List.of(), List.of());
        }

        // Get current iTunes songs
        List<ItunesSong> currentSongs = itunesService.getAllItunesSongs();
        Map<String, ItunesSong> currentByPersistentId = currentSongs.stream()
            .filter(s -> s.getPersistentId() != null)
            .collect(Collectors.toMap(ItunesSong::getPersistentId, s -> s, (a, b) -> a));

        // Get snapshot songs
        List<ItunesSnapshot> snapshotSongs = snapshotRepository.findAll();
        Map<String, ItunesSnapshot> snapshotByPersistentId = snapshotSongs.stream()
            .collect(Collectors.toMap(ItunesSnapshot::getPersistentId, s -> s, (a, b) -> a));

        // Build database lookup for checking if songs exist
        Set<String> dbSongKeys = buildDatabaseLookup();
        Map<String, Long> dbSongKeyToId = buildDatabaseLookupWithIds();

        List<ItunesChangedSongDTO> changedSongs = new ArrayList<>();
        List<ItunesAddedSongDTO> addedSongs = new ArrayList<>();
        List<ItunesRemovedSongDTO> removedSongs = new ArrayList<>();

        // Find changed and added songs
        for (ItunesSong current : currentSongs) {
            String persistentId = current.getPersistentId();
            if (persistentId == null) continue;

            ItunesSnapshot snapshot = snapshotByPersistentId.get(persistentId);
            
            if (snapshot == null) {
                // New song - added to iTunes
                String key = createSongLookupKey(current.getArtist(), current.getAlbum(), current.getName());
                boolean found = dbSongKeys.contains(key);
                Long songId = dbSongKeyToId.get(key);
                
                addedSongs.add(new ItunesAddedSongDTO(
                    persistentId,
                    current.getArtist(),
                    current.getAlbum(),
                    current.getName(),
                    current.getTrackNumber(),
                    current.getYear(),
                    found,
                    songId
                ));
            } else {
                // Check if artist, album, name, or length changed
                boolean artistChanged = !nullSafeEquals(snapshot.getArtist(), current.getArtist());
                boolean albumChanged = !nullSafeEquals(snapshot.getAlbum(), current.getAlbum());
                boolean nameChanged = !nullSafeEquals(snapshot.getName(), current.getName());
                boolean lengthChanged = !nullSafeEquals(snapshot.getTotalTime(), current.getTotalTime());

                if (artistChanged || albumChanged || nameChanged || lengthChanged) {
                    // Something changed - check if new values are in database
                    String key = createSongLookupKey(current.getArtist(), current.getAlbum(), current.getName());
                    boolean found = dbSongKeys.contains(key);
                    Long songId = dbSongKeyToId.get(key);

                    changedSongs.add(new ItunesChangedSongDTO(
                        persistentId,
                        snapshot.getArtist(),
                        snapshot.getAlbum(),
                        snapshot.getName(),
                        current.getArtist(),
                        current.getAlbum(),
                        current.getName(),
                        current.getTrackNumber(),
                        current.getYear(),
                        found,
                        songId,
                        snapshot.getTotalTime(),
                        current.getTotalTime()
                    ));
                }
            }
        }

        // Find removed songs
        for (ItunesSnapshot snapshot : snapshotSongs) {
            String persistentId = snapshot.getPersistentId();
            if (!currentByPersistentId.containsKey(persistentId)) {
                removedSongs.add(new ItunesRemovedSongDTO(
                    persistentId,
                    snapshot.getArtist(),
                    snapshot.getAlbum(),
                    snapshot.getName(),
                    snapshot.getTrackNumber(),
                    snapshot.getYear()
                ));
            }
        }

        // Sort results - changed songs: not found first, then by artist/name
        changedSongs.sort(Comparator.comparing(ItunesChangedSongDTO::isFoundInDatabase)
            .thenComparing((ItunesChangedSongDTO s) -> s.getNewArtist() != null ? s.getNewArtist() : "", String.CASE_INSENSITIVE_ORDER)
            .thenComparing((ItunesChangedSongDTO s) -> s.getNewName() != null ? s.getNewName() : "", String.CASE_INSENSITIVE_ORDER));

        addedSongs.sort(Comparator.comparing((ItunesAddedSongDTO s) -> s.getArtist() != null ? s.getArtist() : "", String.CASE_INSENSITIVE_ORDER)
            .thenComparing((ItunesAddedSongDTO s) -> s.getName() != null ? s.getName() : "", String.CASE_INSENSITIVE_ORDER));

        removedSongs.sort(Comparator.comparing((ItunesRemovedSongDTO s) -> s.getArtist() != null ? s.getArtist() : "", String.CASE_INSENSITIVE_ORDER)
            .thenComparing((ItunesRemovedSongDTO s) -> s.getName() != null ? s.getName() : "", String.CASE_INSENSITIVE_ORDER));

        return new ItunesChangesResultDTO(lastSnapshotDate, true, changedSongs, addedSongs, removedSongs);
    }

    /**
     * Save the current iTunes library state as a snapshot.
     * This replaces any existing snapshot.
     */
    @Transactional
    public int saveSnapshot() throws Exception {
        if (!itunesService.libraryExists()) {
            throw new IllegalStateException("iTunes library file not found");
        }

        List<ItunesSong> currentSongs = itunesService.getAllItunesSongs();
        LocalDateTime now = LocalDateTime.now();

        // Delete all existing snapshot entries
        snapshotRepository.deleteAllSnapshots();
        snapshotRepository.flush();

        // Save new snapshot entries in batches
        List<ItunesSnapshot> toSave = new ArrayList<>();
        for (ItunesSong song : currentSongs) {
            if (song.getPersistentId() == null) continue;
            
            ItunesSnapshot snapshot = new ItunesSnapshot(
                song.getPersistentId(),
                song.getTrackId(),
                song.getArtist(),
                song.getAlbum(),
                song.getName(),
                song.getTrackNumber(),
                song.getYear(),
                song.getTotalTime()
            );
            snapshot.setSnapshotDate(now);
            toSave.add(snapshot);
        }

        snapshotRepository.saveAll(toSave);
        return toSave.size();
    }

    /**
     * Check if a snapshot exists.
     */
    public boolean hasSnapshot() {
        return snapshotRepository.hasAnySnapshot();
    }

    /**
     * Get the last snapshot date.
     */
    public LocalDateTime getLastSnapshotDate() {
        return snapshotRepository.getLastSnapshotDate();
    }

    /**
     * Get the number of songs in the current snapshot.
     */
    public long getSnapshotCount() {
        return snapshotRepository.count();
    }

    // ============ Helper Methods ============

    /**
     * Compare two strings for exact equality, case-insensitive.
     * This preserves punctuation, parentheses, and special characters.
     * Only capitalization changes are ignored (e.g., "Dirty Harry" == "dirty harry").
     * But these are considered different:
     * - "Dirty Harry" vs "Dirty Harry (feat. Bootie Brown)"
     * - "José" vs "Jose"
     * - "Hello!" vs "Hello"
     */
    private boolean nullSafeEquals(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        // Case-insensitive comparison but preserve all other characters
        return a.equalsIgnoreCase(b);
    }
    
    /**
     * Compare two integers for exact equality.
     * Used for comparing song lengths and other numeric fields.
     */
    private boolean nullSafeEquals(Integer a, Integer b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

    private String createSongLookupKey(String artist, String album, String song) {
        String a = StringNormalizer.normalizeForSearch(artist != null ? artist : "");
        String al = StringNormalizer.normalizeForSearch(album != null ? album : "");
        String s = StringNormalizer.normalizeForSearch(song != null ? song : "");
        return a + "||" + al + "||" + s;
    }

    private Set<String> buildDatabaseLookup() {
        Set<String> keys = new HashSet<>();

        String sql = """
            SELECT ar.name as artist_name, al.name as album_name, s.name as song_name
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            """;

        jdbcTemplate.query(sql, rs -> {
            String artistName = rs.getString("artist_name");
            String albumName = rs.getString("album_name");
            String songName = rs.getString("song_name");
            String key = createSongLookupKey(artistName, albumName, songName);
            keys.add(key);
        });

        return keys;
    }

    private Map<String, Long> buildDatabaseLookupWithIds() {
        Map<String, Long> keyToId = new HashMap<>();

        String sql = """
            SELECT s.id as song_id, ar.name as artist_name, al.name as album_name, s.name as song_name
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            """;

        jdbcTemplate.query(sql, rs -> {
            Long songId = rs.getLong("song_id");
            String artistName = rs.getString("artist_name");
            String albumName = rs.getString("album_name");
            String songName = rs.getString("song_name");
            String key = createSongLookupKey(artistName, albumName, songName);
            keyToId.put(key, songId);
        });

        return keyToId;
    }
}
