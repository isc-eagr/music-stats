package library;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standalone script to populate SongFeaturedArtist table by parsing song titles.
 * Searches for patterns like "Song Name (Feat. Artist 1 & Artist 2)" in song titles
 * and matches them to existing artists in the database.
 * 
 * Only creates records for exact matches - if a featured artist doesn't exist
 * in the Artist table, it will be skipped.
 * 
 * Usage: Run main() method directly from IDE or via:
 *   mvn exec:java -Dexec.mainClass="library.FeaturedArtistPopulator"
 */
public class FeaturedArtistPopulator {
    
    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    
    // Regex patterns to match featured artist variations
    private static final List<Pattern> FEAT_PATTERNS = Arrays.asList(
        Pattern.compile("\\((?:feat\\.?|featuring|ft\\.?)\\s+([^)]+)\\)", Pattern.CASE_INSENSITIVE),
        Pattern.compile("\\[(?:feat\\.?|featuring|ft\\.?)\\s+([^]]+)\\]", Pattern.CASE_INSENSITIVE),
        Pattern.compile("(?:feat\\.?|featuring|ft\\.?)\\s+([^(\\[\\-]+)$", Pattern.CASE_INSENSITIVE)
    );
    
    // Delimiters that separate multiple featured artists
    private static final Pattern ARTIST_DELIMITER = Pattern.compile("\\s*[&,]\\s*|\\s+and\\s+", Pattern.CASE_INSENSITIVE);
    
    private JdbcTemplate jdbcTemplate;
    
    public FeaturedArtistPopulator() {
        // Setup database connection
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    public static void main(String[] args) {
        FeaturedArtistPopulator populator = new FeaturedArtistPopulator();
        populator.run();
    }
    
    public void run() {
        System.out.println("========================================");
        System.out.println("Featured Artist Populator");
        System.out.println("========================================");
        System.out.println();
        
        // Step 1: Load all artists into a map for quick lookup
        Map<String, Integer> artistMap = loadArtistMap();
        System.out.println("Loaded " + artistMap.size() + " artists from database");
        System.out.println();
        
        // Step 2: Get all songs
        List<SongData> songs = loadSongs();
        System.out.println("Found " + songs.size() + " songs to process");
        System.out.println();
        
        // Step 3: Process each song and find featured artists
        int totalMatches = 0;
        int totalInserts = 0;
        int totalSkipped = 0;
        int songsProcessed = 0;
        
        for (SongData song : songs) {
            List<String> featuredArtistNames = extractFeaturedArtists(song.name);
            
            if (!featuredArtistNames.isEmpty()) {
                songsProcessed++;
                System.out.println("Song: " + song.name + " (ID: " + song.id + ")");
                System.out.println("  Primary Artist: " + song.artistName);
                
                for (String featuredName : featuredArtistNames) {
                    totalMatches++;
                    String normalizedName = normalizeName(featuredName);
                    
                    // Check if artist exists in database
                    Integer artistId = artistMap.get(normalizedName);
                    
                    if (artistId != null) {
                        // Check if this relationship already exists
                        if (!relationshipExists(song.id, artistId)) {
                            insertFeaturedArtist(song.id, artistId);
                            totalInserts++;
                            System.out.println("  ✓ Added: " + featuredName + " (ID: " + artistId + ")");
                        } else {
                            System.out.println("  ⚠ Already exists: " + featuredName + " (ID: " + artistId + ")");
                        }
                    } else {
                        totalSkipped++;
                        System.out.println("  ✗ Not found in database: " + featuredName);
                    }
                }
                System.out.println();
            }
        }
        
        // Summary
        System.out.println("========================================");
        System.out.println("Summary");
        System.out.println("========================================");
        System.out.println("Songs with featured artists: " + songsProcessed);
        System.out.println("Total featured artist mentions: " + totalMatches);
        System.out.println("Successfully inserted: " + totalInserts);
        System.out.println("Skipped (not found): " + totalSkipped);
        System.out.println("Skipped (already exist): " + (totalMatches - totalInserts - totalSkipped));
        System.out.println();
        System.out.println("Done!");
    }
    
    /**
     * Load all artists from database into a map (normalized name -> artist ID)
     */
    private Map<String, Integer> loadArtistMap() {
        Map<String, Integer> artistMap = new HashMap<>();
        
        String sql = "SELECT id, name FROM Artist";
        jdbcTemplate.query(sql, rs -> {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            String normalizedName = normalizeName(name);
            artistMap.put(normalizedName, id);
        });
        
        return artistMap;
    }
    
    /**
     * Load all songs with their primary artist names
     */
    private List<SongData> loadSongs() {
        String sql = """
            SELECT s.id, s.name, s.artist_id, a.name as artist_name
            FROM Song s
            INNER JOIN Artist a ON s.artist_id = a.id
            ORDER BY s.id
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            SongData song = new SongData();
            song.id = rs.getInt("id");
            song.name = rs.getString("name");
            song.artistName = rs.getString("artist_name");
            return song;
        });
    }
    
    /**
     * Extract featured artist names from a song title
     */
    private List<String> extractFeaturedArtists(String songTitle) {
        List<String> featuredArtists = new ArrayList<>();
        
        if (songTitle == null || songTitle.isEmpty()) {
            return featuredArtists;
        }
        
        // Try each pattern to find featured artists
        for (Pattern pattern : FEAT_PATTERNS) {
            Matcher matcher = pattern.matcher(songTitle);
            if (matcher.find()) {
                String featuredSection = matcher.group(1).trim();
                
                // Split by delimiters (& , and)
                String[] artists = ARTIST_DELIMITER.split(featuredSection);
                
                for (String artist : artists) {
                    String cleaned = cleanArtistName(artist.trim());
                    if (!cleaned.isEmpty()) {
                        featuredArtists.add(cleaned);
                    }
                }
                
                // Once we find a match, stop looking
                break;
            }
        }
        
        return featuredArtists;
    }
    
    /**
     * Clean up artist name by removing common suffixes/prefixes
     */
    private String cleanArtistName(String name) {
        if (name == null) {
            return "";
        }
        
        // Remove leading/trailing whitespace
        name = name.trim();
        
        // Remove any remaining parentheses or brackets
        name = name.replaceAll("[\\(\\)\\[\\]]", "");
        
        return name.trim();
    }
    
    /**
     * Normalize name for comparison (lowercase, trim)
     */
    private String normalizeName(String name) {
        if (name == null) {
            return "";
        }
        return name.toLowerCase().trim();
    }
    
    /**
     * Check if a song-featured artist relationship already exists
     */
    private boolean relationshipExists(int songId, int artistId) {
        String sql = "SELECT COUNT(*) FROM SongFeaturedArtist WHERE song_id = ? AND artist_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId, artistId);
        return count != null && count > 0;
    }
    
    /**
     * Insert a new featured artist relationship
     */
    private void insertFeaturedArtist(int songId, int artistId) {
        String sql = "INSERT INTO SongFeaturedArtist (song_id, artist_id, creation_date) VALUES (?, ?, CURRENT_TIMESTAMP)";
        jdbcTemplate.update(sql, songId, artistId);
    }
    
    /**
     * Simple data class to hold song information
     */
    private static class SongData {
        int id;
        String name;
        String artistName;
    }
}
