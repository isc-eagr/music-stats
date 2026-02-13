package library;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Standalone script to match unmatched plays to existing songs.
 * 
 * Round 1: Match by artist + album + song (exact match including songs without album)
 * Round 2: Match by artist + song only (and update the album field on the plays)
 * 
 * Run this class directly with: mvn exec:java -Dexec.mainClass="library.PlayMatcherScript"
 * Or run from your IDE.
 */
public class PlayMatcherScript {

    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    private static final String DB_URL = "jdbc:sqlite:" + DB_PATH;
    private static final String ACCOUNT = "robertlover";

    public static void main(String[] args) {
        System.out.println("=== Play Matcher Script ===");
        System.out.println("Database: " + DB_PATH);
        System.out.println("Account: " + ACCOUNT + " only");
        System.out.println();

        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            // Count unmatched before
            int unmatchedBefore = countUnmatchedPlays(conn);
            System.out.println("Unmatched plays before: " + unmatchedBefore);
            System.out.println();

            // Round 1: Match by artist + album + song
            System.out.println("=== ROUND 1: Matching by Artist + Album + Song ===");
            int round1Matched = matchByArtistAlbumSong(conn);
            System.out.println("Round 1 matched: " + round1Matched + " plays");
            System.out.println();

            // Round 2: Match by artist + song only for plays with NO album
            System.out.println("=== ROUND 2: Matching by Artist + Song (only plays with no album) ===");
            int round2Matched = matchByArtistSongOnly(conn);
            System.out.println("Round 2 matched: " + round2Matched + " plays");
            System.out.println();

            // Round 3: Fuzzy match by artist + song
            System.out.println("=== ROUND 3: Fuzzy Matching (ignoring accents, punctuation, feat, etc.) ===");
            int round3Matched = matchFuzzy(conn);
            System.out.println("Round 3 matched: " + round3Matched + " plays");
            System.out.println();

            // Count unmatched after
            int unmatchedAfter = countUnmatchedPlays(conn);
            System.out.println("=== SUMMARY ===");
            System.out.println("Unmatched before: " + unmatchedBefore);
            System.out.println("Unmatched after:  " + unmatchedAfter);
            System.out.println("Total matched:    " + (unmatchedBefore - unmatchedAfter));

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Count total unmatched plays for the configured account
     */
    private static int countUnmatchedPlays(Connection conn) throws SQLException {
        String sql = "SELECT COUNT(*) FROM play WHERE song_id IS NULL AND account = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, ACCOUNT);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return 0;
    }

    /**
     * Round 1: Match unmatched plays by artist + album + song.
     * This includes songs without an album (album_id IS NULL) where the play also has no album.
     */
    private static int matchByArtistAlbumSong(Connection conn) throws SQLException {
        // Build lookup map: normalized(artist||album||song) -> song_id
        Map<String, SongMatch> songLookup = new HashMap<>();
        
        String songSql = """
            SELECT s.id, s.album_id, ar.name as artist_name, COALESCE(al.name, '') as album_name, s.name as song_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN album al ON s.album_id = al.id
            """;
        
        try (PreparedStatement ps = conn.prepareStatement(songSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int songId = rs.getInt("id");
                Integer albumId = rs.getObject("album_id") != null ? rs.getInt("album_id") : null;
                String artistName = rs.getString("artist_name");
                String albumName = rs.getString("album_name"); // empty string if no album
                String songName = rs.getString("song_name");
                
                String key = createLookupKey(artistName, albumName, songName);
                songLookup.putIfAbsent(key, new SongMatch(songId, albumId, artistName, albumName, songName));
            }
        }
        
        System.out.println("  Loaded " + songLookup.size() + " songs into lookup map");

        // Get distinct unmatched play groups for the configured account
        String unmatchedSql = """
            SELECT DISTINCT 
                COALESCE(artist, '') as artist, 
                COALESCE(album, '') as album, 
                COALESCE(song, '') as song
            FROM play 
            WHERE song_id IS NULL AND account = ?
            """;
        
        List<String[]> unmatchedGroups = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(unmatchedSql)) {
            ps.setString(1, ACCOUNT);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    unmatchedGroups.add(new String[]{
                        rs.getString("artist"),
                        rs.getString("album"),
                        rs.getString("song")
                    });
                }
            }
        }
        
        System.out.println("  Found " + unmatchedGroups.size() + " distinct unmatched play groups");

        // Match and update for the configured account (sync all fields to canonical names)
        String updateSql = """
            UPDATE play SET song_id = ?, artist = ?, album = ?, song = ?
            WHERE song_id IS NULL
            AND account = ?
            AND COALESCE(artist, '') = ?
            AND COALESCE(album, '') = ?
            AND COALESCE(song, '') = ?
            """;
        
        int totalMatched = 0;
        int groupsMatched = 0;
        
        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            for (String[] group : unmatchedGroups) {
                String artist = group[0];
                String album = group[1];
                String song = group[2];
                
                String key = createLookupKey(artist, album, song);
                SongMatch match = songLookup.get(key);
                
                if (match != null) {
                    ps.setInt(1, match.songId);
                    ps.setString(2, match.artistName);  // Sync artist to canonical
                    ps.setString(3, match.albumName);   // Sync album to canonical
                    ps.setString(4, match.songName);    // Sync song to canonical
                    ps.setString(5, ACCOUNT);
                    ps.setString(6, artist);
                    ps.setString(7, album);
                    ps.setString(8, song);
                    
                    int updated = ps.executeUpdate();
                    if (updated > 0) {
                        totalMatched += updated;
                        groupsMatched++;
                        System.out.println("    Matched: \"" + artist + "\" - \"" + album + "\" - \"" + song + "\" -> song_id=" + match.songId + " (" + updated + " plays)");
                    }
                }
            }
        }
        
        System.out.println("  Round 1 matched " + groupsMatched + " groups");
        return totalMatched;
    }

    /**
     * Round 2: Match by artist + song only (ignoring album).
     * ONLY for plays that have NO album (empty/null album field).
     * When a match is found, update both song_id AND the album field to the canonical album name.
     */
    private static int matchByArtistSongOnly(Connection conn) throws SQLException {
        // Build lookup map: normalized(artist||song) -> song info (prioritize songs WITH albums)
        // Key: artist||song -> SongMatch
        Map<String, SongMatch> songLookup = new HashMap<>();
        
        // First, load songs WITHOUT albums
        String songWithoutAlbumSql = """
            SELECT s.id, ar.name as artist_name, s.name as song_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            WHERE s.album_id IS NULL
            """;
        
        try (PreparedStatement ps = conn.prepareStatement(songWithoutAlbumSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int songId = rs.getInt("id");
                String artistName = rs.getString("artist_name");
                String songName = rs.getString("song_name");
                
                String key = createLookupKeyArtistSong(artistName, songName);
                songLookup.putIfAbsent(key, new SongMatch(songId, null, artistName, "", songName));
            }
        }
        
        // Then, load songs WITH albums (these will override songs without albums)
        String songWithAlbumSql = """
            SELECT s.id, s.album_id, ar.name as artist_name, al.name as album_name, s.name as song_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            JOIN album al ON s.album_id = al.id
            """;
        
        try (PreparedStatement ps = conn.prepareStatement(songWithAlbumSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int songId = rs.getInt("id");
                Integer albumId = rs.getInt("album_id");
                String artistName = rs.getString("artist_name");
                String albumName = rs.getString("album_name");
                String songName = rs.getString("song_name");
                
                String key = createLookupKeyArtistSong(artistName, songName);
                // Override: songs with albums take priority
                songLookup.put(key, new SongMatch(songId, albumId, artistName, albumName, songName));
            }
        }
        
        System.out.println("  Loaded " + songLookup.size() + " unique artist+song combinations");

        // Get distinct unmatched play groups that have NO ALBUM (still unmatched after round 1)
        String unmatchedSql = """
            SELECT DISTINCT 
                COALESCE(artist, '') as artist, 
                COALESCE(song, '') as song
            FROM play 
            WHERE song_id IS NULL
            AND account = ?
            AND (album IS NULL OR TRIM(album) = '')
            """;
        
        List<String[]> unmatchedGroups = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(unmatchedSql)) {
            ps.setString(1, ACCOUNT);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    unmatchedGroups.add(new String[]{
                        rs.getString("artist"),
                        rs.getString("song")
                    });
                }
            }
        }
        
        System.out.println("  Found " + unmatchedGroups.size() + " distinct unmatched play groups (with no album)");

        // Match and update (sync all fields to canonical names) for the configured account
        String updateSql = """
            UPDATE play SET song_id = ?, artist = ?, album = ?, song = ?
            WHERE song_id IS NULL
            AND account = ?
            AND COALESCE(artist, '') = ?
            AND (album IS NULL OR TRIM(album) = '')
            AND COALESCE(song, '') = ?
            """;
        
        int totalMatched = 0;
        int groupsMatched = 0;
        
        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            for (String[] group : unmatchedGroups) {
                String playArtist = group[0];
                String playSong = group[1];
                
                String key = createLookupKeyArtistSong(playArtist, playSong);
                SongMatch match = songLookup.get(key);
                
                if (match != null) {
                    ps.setInt(1, match.songId);
                    ps.setString(2, match.artistName);  // Sync artist to canonical
                    ps.setString(3, match.albumName);   // Sync album to canonical
                    ps.setString(4, match.songName);    // Sync song to canonical
                    ps.setString(5, ACCOUNT);
                    ps.setString(6, playArtist);
                    ps.setString(7, playSong);
                    
                    int updated = ps.executeUpdate();
                    if (updated > 0) {
                        totalMatched += updated;
                        groupsMatched++;
                        String albumInfo = match.albumName.isEmpty() ? "(no album)" : "\"" + match.albumName + "\"";
                        System.out.println("    Matched: \"" + playArtist + "\" - \"" + playSong + "\" -> song_id=" + match.songId + ", album=" + albumInfo + " (" + updated + " plays)");
                    }
                }
            }
        }
        
        System.out.println("  Round 2 matched " + groupsMatched + " groups");
        return totalMatched;
    }

    /**
     * Round 3: Fuzzy match by artist + song with normalization.
     * Ignores accents, periods, parentheses, brackets, "feat", "featuring", etc.
     * Updates song_id and album field when a match is found.
     */
    private static int matchFuzzy(Connection conn) throws SQLException {
        // Build lookup map: fuzzy_normalized(artist||song) -> song info (prioritize songs WITH albums)
        Map<String, SongMatch> songLookup = new HashMap<>();
        
        // First, load songs WITHOUT albums
        String songWithoutAlbumSql = """
            SELECT s.id, ar.name as artist_name, s.name as song_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            WHERE s.album_id IS NULL
            """;
        
        try (PreparedStatement ps = conn.prepareStatement(songWithoutAlbumSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int songId = rs.getInt("id");
                String artistName = rs.getString("artist_name");
                String songName = rs.getString("song_name");
                
                String key = createFuzzyLookupKey(artistName, songName);
                songLookup.putIfAbsent(key, new SongMatch(songId, null, artistName, "", songName));
            }
        }
        
        // Then, load songs WITH albums (these will override songs without albums)
        String songWithAlbumSql = """
            SELECT s.id, s.album_id, ar.name as artist_name, al.name as album_name, s.name as song_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            JOIN album al ON s.album_id = al.id
            """;
        
        try (PreparedStatement ps = conn.prepareStatement(songWithAlbumSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int songId = rs.getInt("id");
                Integer albumId = rs.getInt("album_id");
                String artistName = rs.getString("artist_name");
                String albumName = rs.getString("album_name");
                String songName = rs.getString("song_name");
                
                String key = createFuzzyLookupKey(artistName, songName);
                // Override: songs with albums take priority
                songLookup.put(key, new SongMatch(songId, albumId, artistName, albumName, songName));
            }
        }
        
        System.out.println("  Loaded " + songLookup.size() + " unique fuzzy artist+song combinations");

        // Get distinct unmatched play groups (still unmatched after rounds 1 & 2)
        String unmatchedSql = """
            SELECT DISTINCT 
                COALESCE(artist, '') as artist, 
                COALESCE(album, '') as album, 
                COALESCE(song, '') as song
            FROM play 
            WHERE song_id IS NULL AND account = ?
            """;
        
        List<String[]> unmatchedGroups = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(unmatchedSql)) {
            ps.setString(1, ACCOUNT);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    unmatchedGroups.add(new String[]{
                        rs.getString("artist"),
                        rs.getString("album"),
                        rs.getString("song")
                    });
                }
            }
        }
        
        System.out.println("  Found " + unmatchedGroups.size() + " distinct unmatched play groups remaining");

        // Match and update (sync all fields to canonical names) for the configured account
        String updateSql = """
            UPDATE play SET song_id = ?, artist = ?, album = ?, song = ?
            WHERE song_id IS NULL
            AND account = ?
            AND COALESCE(artist, '') = ?
            AND COALESCE(album, '') = ?
            AND COALESCE(song, '') = ?
            """;
        
        int totalMatched = 0;
        int groupsMatched = 0;
        int skippedRemixMismatch = 0;
        
        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            for (String[] group : unmatchedGroups) {
                String playArtist = group[0];
                String playAlbum = group[1];
                String playSong = group[2];
                
                String key = createFuzzyLookupKey(playArtist, playSong);
                SongMatch match = songLookup.get(key);
                
                if (match != null) {
                    // Check remix mismatch: skip if one is a remix and the other is not
                    boolean playIsRemix = isRemix(playSong);
                    boolean songIsRemix = isRemix(match.songName);
                    if (playIsRemix != songIsRemix) {
                        skippedRemixMismatch++;
                        continue;
                    }
                    
                    ps.setInt(1, match.songId);
                    ps.setString(2, match.artistName);  // Sync artist to canonical
                    ps.setString(3, match.albumName);   // Sync album to canonical
                    ps.setString(4, match.songName);    // Sync song to canonical
                    ps.setString(5, ACCOUNT);
                    ps.setString(6, playArtist);
                    ps.setString(7, playAlbum);
                    ps.setString(8, playSong);
                    
                    int updated = ps.executeUpdate();
                    if (updated > 0) {
                        totalMatched += updated;
                        groupsMatched++;
                        String albumInfo = match.albumName.isEmpty() ? "(no album)" : "\"" + match.albumName + "\"";
                        System.out.println("    Fuzzy matched: \"" + playArtist + "\" - \"" + playSong + "\" -> song_id=" + match.songId + " (\"" + match.songName + "\"), album=" + albumInfo + " (" + updated + " plays)");
                    }
                }
            }
        }
        
        if (skippedRemixMismatch > 0) {
            System.out.println("  Skipped " + skippedRemixMismatch + " groups due to remix mismatch");
        }
        System.out.println("  Round 3 matched " + groupsMatched + " groups");
        return totalMatched;
    }

    /**
     * Create normalized lookup key for artist + album + song
     */
    private static String createLookupKey(String artist, String album, String song) {
        String a = normalizeForSearch(artist != null ? artist : "");
        String al = normalizeForSearch(album != null ? album : "");
        String s = normalizeForSearch(song != null ? song : "");
        return a + "||" + al + "||" + s;
    }

    /**
     * Create normalized lookup key for artist + song only
     */
    private static String createLookupKeyArtistSong(String artist, String song) {
        String a = normalizeForSearch(artist != null ? artist : "");
        String s = normalizeForSearch(song != null ? song : "");
        return a + "||" + s;
    }

    /**
     * Create fuzzy lookup key for artist + song.
     * Removes accents, punctuation, "feat", "featuring", parentheses content, brackets content, etc.
     */
    private static String createFuzzyLookupKey(String artist, String song) {
        String a = fuzzyNormalize(artist != null ? artist : "");
        String s = fuzzyNormalize(song != null ? song : "");
        return a + "||" + s;
    }

    /**
     * Fuzzy normalize a string:
     * - Lowercase and trim
     * - Strip accents
     * - Remove content in parentheses () and brackets []
     * - Remove "feat.", "feat", "featuring", "ft.", "ft"
     * - Remove punctuation: . , ! ? ' " - _ : ;
     * - Collapse multiple spaces
     */
    private static String fuzzyNormalize(String input) {
        if (input == null || input.isEmpty()) {
            return "";
        }
        
        String result = input.toLowerCase().trim();
        
        // Strip accents
        result = stripAccents(result);
        
        // Remove content in parentheses and brackets (including the brackets themselves)
        result = result.replaceAll("\\([^)]*\\)", "");
        result = result.replaceAll("\\[[^]]*\\]", "");
        
        // Remove featuring variations (with word boundaries)
        result = result.replaceAll("\\bfeaturing\\b", "");
        result = result.replaceAll("\\bfeat\\.?\\b", "");
        result = result.replaceAll("\\bft\\.?\\b", "");
        
        // Remove punctuation
        result = result.replaceAll("[.,'!?\"\\-_:;]", "");
        
        // Collapse multiple spaces and trim
        result = result.replaceAll("\\s+", " ").trim();
        
        return result;
    }

    /**
     * Check if a song name indicates it's a remix.
     * Looks for "remix", "rmx", "mix" (as standalone word) in the song name.
     */
    private static boolean isRemix(String songName) {
        if (songName == null || songName.isEmpty()) {
            return false;
        }
        String lower = songName.toLowerCase();
        // Check for remix, rmx, or standalone "mix" (not as part of another word)
        return lower.contains("remix") 
            || lower.contains("rmx")
            || lower.matches(".*\\bmix\\b.*");
    }

    /**
     * Normalize string for search: lowercase + strip accents + trim
     */
    private static String normalizeForSearch(String input) {
        if (input == null) {
            return "";
        }
        return stripAccents(input.toLowerCase().trim());
    }

    /**
     * Strip accents from string
     */
    private static String stripAccents(String input) {
        if (input == null) {
            return null;
        }
        // Handle special characters that don't decompose in NFD
        String result = input
            .replace('\u00F1', 'n').replace('\u00D1', 'N')  // ñ, Ñ
            .replace('\u00E7', 'c').replace('\u00C7', 'C'); // ç, Ç
        
        // Normalize to NFD and remove diacritical marks
        String normalized = Normalizer.normalize(result, Normalizer.Form.NFD);
        return normalized.replaceAll("\\p{M}", "");
    }

    /**
     * Helper class to hold song match information
     */
    private static class SongMatch {
        final int songId;
        final String artistName;
        final String albumName;
        final String songName;

        SongMatch(int songId, Integer albumId, String artistName, String albumName, String songName) {
            this.songId = songId;
            // albumId is passed for construction but not stored (used only in lookup key generation)
            this.artistName = artistName;
            this.albumName = albumName;
            this.songName = songName;
        }
    }
}
