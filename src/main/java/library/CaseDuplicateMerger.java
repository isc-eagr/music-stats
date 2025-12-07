package library;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Standalone script to merge duplicate artists and albums that differ only in capitalization.
 * 
 * Rules:
 * - Prefers entries where all words start with a capital letter (Title Case)
 * - For Artists: Reassigns all albums and songs to the correct artist, then deletes duplicates
 * - For Albums: Reassigns all songs to the correct album, then deletes duplicates
 * 
 * Usage: Run main() method directly from IDE or via:
 *   mvnw exec:java -Dexec.mainClass="library.CaseDuplicateMerger"
 */
public class CaseDuplicateMerger {
    
    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    
    private JdbcTemplate jdbcTemplate;
    private boolean dryRun = false;
    
    // Statistics
    private int artistDuplicatesFound = 0;
    private int artistsDeleted = 0;
    private int albumDuplicatesFound = 0;
    private int albumsDeleted = 0;
    private int songsReassignedToArtist = 0;
    private int albumsReassignedToArtist = 0;
    private int songsReassignedToAlbum = 0;
    
    public CaseDuplicateMerger() {
        // Setup database connection
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    public static void main(String[] args) {
        CaseDuplicateMerger merger = new CaseDuplicateMerger();
        
        // Parse command line arguments
        for (String arg : args) {
            if ("--dry-run".equalsIgnoreCase(arg)) {
                merger.dryRun = true;
            }
        }
        
        merger.run();
    }
    
    public void run() {
        System.out.println("========================================");
        System.out.println("Case-Sensitive Duplicate Merger");
        System.out.println("========================================");
        System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
        System.out.println();
        
        // First merge artists (this is important to do first)
        System.out.println(">>> Finding Artist Duplicates...");
        mergeArtistDuplicates();
        System.out.println();
        
        // Then merge albums
        System.out.println(">>> Finding Album Duplicates...");
        mergeAlbumDuplicates();
        System.out.println();
        
        printSummary();
    }
    
    /**
     * Find and merge duplicate artists that differ only in capitalization
     */
    private void mergeArtistDuplicates() {
        String sql = "SELECT id, name FROM Artist ORDER BY name";
        
        List<ArtistData> artists = jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistData artist = new ArtistData();
            artist.id = rs.getInt("id");
            artist.name = rs.getString("name");
            return artist;
        });
        
        // Group by lowercase name
        Map<String, List<ArtistData>> groups = artists.stream()
            .collect(Collectors.groupingBy(a -> a.name.toLowerCase()));
        
        // Process each group that has duplicates
        for (Map.Entry<String, List<ArtistData>> entry : groups.entrySet()) {
            List<ArtistData> duplicates = entry.getValue();
            
            if (duplicates.size() > 1) {
                artistDuplicatesFound += duplicates.size() - 1;
                
                // Find the correct artist (prefer title case)
                ArtistData correctArtist = findCorrectArtist(duplicates);
                List<ArtistData> incorrectArtists = duplicates.stream()
                    .filter(a -> a.id != correctArtist.id)
                    .collect(Collectors.toList());
                
                System.out.println("Found " + duplicates.size() + " versions of: " + correctArtist.name);
                System.out.println("  ✓ Keeping: " + correctArtist.name + " (ID: " + correctArtist.id + ")");
                
                for (ArtistData incorrect : incorrectArtists) {
                    System.out.println("  ✗ Merging: " + incorrect.name + " (ID: " + incorrect.id + ")");
                    
                    // Get counts before merging
                    int songCount = getSongCountForArtist(incorrect.id);
                    int albumCount = getAlbumCountForArtist(incorrect.id);
                    
                    System.out.println("    - " + albumCount + " albums, " + songCount + " songs to reassign");
                    
                    if (!dryRun) {
                        // Reassign all albums from incorrect to correct
                        reassignAlbumsToArtist(incorrect.id, correctArtist.id);
                        albumsReassignedToArtist += albumCount;
                        
                        // Reassign all songs from incorrect to correct
                        reassignSongsToArtist(incorrect.id, correctArtist.id);
                        songsReassignedToArtist += songCount;
                        
                        // Delete the incorrect artist
                        deleteArtist(incorrect.id);
                        artistsDeleted++;
                    }
                }
                System.out.println();
            }
        }
        
        if (artistDuplicatesFound == 0) {
            System.out.println("No artist duplicates found!");
        }
    }
    
    /**
     * Find and merge duplicate albums that differ only in capitalization
     */
    private void mergeAlbumDuplicates() {
        // Group albums by artist and lowercase name
        String sql = "SELECT id, name, artist_id FROM Album ORDER BY artist_id, name";
        
        List<AlbumData> albums = jdbcTemplate.query(sql, (rs, rowNum) -> {
            AlbumData album = new AlbumData();
            album.id = rs.getInt("id");
            album.name = rs.getString("name");
            album.artistId = rs.getInt("artist_id");
            return album;
        });
        
        // Group by artist_id and lowercase name
        Map<String, List<AlbumData>> groups = albums.stream()
            .collect(Collectors.groupingBy(a -> a.artistId + "||" + a.name.toLowerCase()));
        
        // Process each group that has duplicates
        for (Map.Entry<String, List<AlbumData>> entry : groups.entrySet()) {
            List<AlbumData> duplicates = entry.getValue();
            
            if (duplicates.size() > 1) {
                albumDuplicatesFound += duplicates.size() - 1;
                
                // Get artist name for display
                String artistName = getArtistName(duplicates.get(0).artistId);
                
                // Find the correct album (prefer title case)
                AlbumData correctAlbum = findCorrectAlbum(duplicates);
                List<AlbumData> incorrectAlbums = duplicates.stream()
                    .filter(a -> a.id != correctAlbum.id)
                    .collect(Collectors.toList());
                
                System.out.println("Found " + duplicates.size() + " versions of: " + artistName + " - " + correctAlbum.name);
                System.out.println("  ✓ Keeping: " + correctAlbum.name + " (ID: " + correctAlbum.id + ")");
                
                for (AlbumData incorrect : incorrectAlbums) {
                    System.out.println("  ✗ Merging: " + incorrect.name + " (ID: " + incorrect.id + ")");
                    
                    // Get counts before merging
                    int songCount = getSongCountForAlbum(incorrect.id);
                    System.out.println("    - " + songCount + " songs to reassign");
                    
                    if (!dryRun) {
                        // Reassign all songs from incorrect to correct album
                        reassignSongsToAlbum(incorrect.id, correctAlbum.id);
                        songsReassignedToAlbum += songCount;
                        
                        // Delete the incorrect album
                        deleteAlbum(incorrect.id);
                        albumsDeleted++;
                    }
                }
                System.out.println();
            }
        }
        
        if (albumDuplicatesFound == 0) {
            System.out.println("No album duplicates found!");
        }
    }
    
    /**
     * Find the correct artist - prefer title case where all words start with capital letter
     */
    private ArtistData findCorrectArtist(List<ArtistData> artists) {
        // First, prefer artists with image data (more complete)
        List<ArtistData> artistsWithImages = artists.stream()
            .filter(a -> hasArtistImage(a.id))
            .collect(Collectors.toList());
        
        List<ArtistData> candidates = artistsWithImages.isEmpty() ? artists : artistsWithImages;
        
        // Find the one with best capitalization
        return candidates.stream()
            .max(Comparator.comparing(a -> getCapitalizationScore(a.name)))
            .orElse(artists.get(0));
    }
    
    /**
     * Find the correct album - prefer title case where all words start with capital letter
     */
    private AlbumData findCorrectAlbum(List<AlbumData> albums) {
        // First, prefer albums with image data (more complete)
        List<AlbumData> albumsWithImages = albums.stream()
            .filter(a -> hasAlbumImage(a.id))
            .collect(Collectors.toList());
        
        List<AlbumData> candidates = albumsWithImages.isEmpty() ? albums : albumsWithImages;
        
        // Find the one with best capitalization
        return candidates.stream()
            .max(Comparator.comparing(a -> getCapitalizationScore(a.name)))
            .orElse(albums.get(0));
    }
    
    /**
     * Calculate capitalization score - higher score = better capitalization
     * Prefers Title Case (all words start with capital)
     */
    private int getCapitalizationScore(String name) {
        if (name == null || name.isEmpty()) return 0;
        
        // Split by spaces and common separators
        String[] words = name.split("[\\s\\-'\"]+");
        int score = 0;
        
        for (String word : words) {
            if (word.isEmpty()) continue;
            
            // Check if word starts with capital letter
            if (Character.isUpperCase(word.charAt(0))) {
                score += 10;
                
                // Bonus if rest of word is lowercase (proper title case)
                if (word.length() > 1 && word.substring(1).equals(word.substring(1).toLowerCase())) {
                    score += 5;
                }
            }
        }
        
        return score;
    }
    
    /**
     * Database helper methods
     */
    private int getSongCountForArtist(int artistId) {
        String sql = "SELECT COUNT(*) FROM Song WHERE artist_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    private int getAlbumCountForArtist(int artistId) {
        String sql = "SELECT COUNT(*) FROM Album WHERE artist_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, artistId);
        return count != null ? count : 0;
    }
    
    private int getSongCountForAlbum(int albumId) {
        String sql = "SELECT COUNT(*) FROM Song WHERE album_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, albumId);
        return count != null ? count : 0;
    }
    
    private String getArtistName(int artistId) {
        String sql = "SELECT name FROM Artist WHERE id = ?";
        return jdbcTemplate.queryForObject(sql, String.class, artistId);
    }
    
    private boolean hasArtistImage(int artistId) {
        String sql = "SELECT image IS NOT NULL AND LENGTH(image) > 0 FROM Artist WHERE id = ?";
        Boolean hasImage = jdbcTemplate.queryForObject(sql, Boolean.class, artistId);
        return Boolean.TRUE.equals(hasImage);
    }
    
    private boolean hasAlbumImage(int albumId) {
        String sql = "SELECT image IS NOT NULL AND LENGTH(image) > 0 FROM Album WHERE id = ?";
        Boolean hasImage = jdbcTemplate.queryForObject(sql, Boolean.class, albumId);
        return Boolean.TRUE.equals(hasImage);
    }
    
    private void reassignAlbumsToArtist(int fromArtistId, int toArtistId) {
        String sql = "UPDATE Album SET artist_id = ? WHERE artist_id = ?";
        jdbcTemplate.update(sql, toArtistId, fromArtistId);
    }
    
    private void reassignSongsToArtist(int fromArtistId, int toArtistId) {
        String sql = "UPDATE Song SET artist_id = ? WHERE artist_id = ?";
        jdbcTemplate.update(sql, toArtistId, fromArtistId);
    }
    
    private void reassignSongsToAlbum(int fromAlbumId, int toAlbumId) {
        String sql = "UPDATE Song SET album_id = ? WHERE album_id = ?";
        jdbcTemplate.update(sql, toAlbumId, fromAlbumId);
    }
    
    private void deleteArtist(int artistId) {
        String sql = "DELETE FROM Artist WHERE id = ?";
        jdbcTemplate.update(sql, artistId);
    }
    
    private void deleteAlbum(int albumId) {
        String sql = "DELETE FROM Album WHERE id = ?";
        jdbcTemplate.update(sql, albumId);
    }
    
    /**
     * Print summary statistics
     */
    private void printSummary() {
        System.out.println("========================================");
        System.out.println("Summary");
        System.out.println("========================================");
        System.out.println("Artist duplicates found: " + artistDuplicatesFound);
        System.out.println("Artists deleted: " + artistsDeleted);
        System.out.println("Albums reassigned to correct artist: " + albumsReassignedToArtist);
        System.out.println("Songs reassigned to correct artist: " + songsReassignedToArtist);
        System.out.println();
        System.out.println("Album duplicates found: " + albumDuplicatesFound);
        System.out.println("Albums deleted: " + albumsDeleted);
        System.out.println("Songs reassigned to correct album: " + songsReassignedToAlbum);
        System.out.println();
        
        if (dryRun) {
            System.out.println("*** DRY RUN - No changes were saved to database ***");
        } else {
            System.out.println("Done! All duplicates merged successfully.");
        }
    }
    
    /**
     * Data classes
     */
    private static class ArtistData {
        int id;
        String name;
    }
    
    private static class AlbumData {
        int id;
        String name;
        int artistId;
    }
}
