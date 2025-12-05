package library;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Standalone script to sync scrobble artist, album, and song fields with their matched song data.
 * 
 * This script finds all scrobbles that have a song_id but where the artist, album, or song
 * fields don't match the canonical data from the song table. It then updates those scrobbles
 * to use the canonical values.
 * 
 * Run this class directly with: mvn exec:java -Dexec.mainClass="library.ScrobbleSyncScript"
 * Or run from your IDE.
 */
public class ScrobbleSyncScript {

    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    private static final String DB_URL = "jdbc:sqlite:" + DB_PATH;
    
    // Set to true to only report what would be changed without making changes
    private static final boolean DRY_RUN = false;

    public static void main(String[] args) {
        System.out.println("=== Scrobble Sync Script ===");
        System.out.println("Database: " + DB_PATH);
        System.out.println("Dry Run: " + DRY_RUN);
        System.out.println();

        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            // Count out-of-sync scrobbles before
            int outOfSyncBefore = countOutOfSyncScrobbles(conn);
            System.out.println("Out-of-sync scrobbles before: " + outOfSyncBefore);
            System.out.println();

            if (outOfSyncBefore == 0) {
                System.out.println("All scrobbles are already in sync. Nothing to do.");
                return;
            }

            // Sync the scrobbles
            System.out.println("=== Syncing Scrobbles ===");
            int synced = syncScrobbles(conn);
            System.out.println();

            // Count out-of-sync scrobbles after
            int outOfSyncAfter = countOutOfSyncScrobbles(conn);
            
            System.out.println("=== SUMMARY ===");
            System.out.println("Out-of-sync before: " + outOfSyncBefore);
            System.out.println("Scrobbles synced:   " + synced);
            System.out.println("Out-of-sync after:  " + outOfSyncAfter);

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Count scrobbles that have a song_id but where the artist, album, or song
     * fields don't match the canonical data.
     */
    private static int countOutOfSyncScrobbles(Connection conn) throws SQLException {
        String sql = """
            SELECT COUNT(*) FROM scrobble sc
            JOIN song s ON sc.song_id = s.id
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN album al ON s.album_id = al.id
            WHERE sc.song_id IS NOT NULL
            AND (
                sc.artist != ar.name
                OR sc.song != s.name
                OR sc.album != COALESCE(al.name, '')
            )
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }

    /**
     * Sync all out-of-sync scrobbles to use canonical artist, album, and song names.
     */
    private static int syncScrobbles(Connection conn) throws SQLException {
        // First, get a sample of what will be synced for logging
        String sampleSql = """
            SELECT 
                sc.id as scrobble_id,
                sc.artist as old_artist,
                sc.album as old_album,
                sc.song as old_song,
                ar.name as new_artist,
                COALESCE(al.name, '') as new_album,
                s.name as new_song,
                sc.song_id
            FROM scrobble sc
            JOIN song s ON sc.song_id = s.id
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN album al ON s.album_id = al.id
            WHERE sc.song_id IS NOT NULL
            AND (
                sc.artist != ar.name
                OR sc.song != s.name
                OR sc.album != COALESCE(al.name, '')
            )
            LIMIT 20
            """;
        
        System.out.println("Sample of out-of-sync scrobbles (first 20):");
        System.out.println("-".repeat(100));
        
        try (PreparedStatement ps = conn.prepareStatement(sampleSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int scrobbleId = rs.getInt("scrobble_id");
                String oldArtist = rs.getString("old_artist");
                String oldAlbum = rs.getString("old_album");
                String oldSong = rs.getString("old_song");
                String newArtist = rs.getString("new_artist");
                String newAlbum = rs.getString("new_album");
                String newSong = rs.getString("new_song");
                int songId = rs.getInt("song_id");
                
                System.out.printf("  Scrobble #%d (song_id=%d):%n", scrobbleId, songId);
                if (!oldArtist.equals(newArtist)) {
                    System.out.printf("    Artist: \"%s\" -> \"%s\"%n", oldArtist, newArtist);
                }
                if (!oldAlbum.equals(newAlbum)) {
                    System.out.printf("    Album:  \"%s\" -> \"%s\"%n", oldAlbum, newAlbum);
                }
                if (!oldSong.equals(newSong)) {
                    System.out.printf("    Song:   \"%s\" -> \"%s\"%n", oldSong, newSong);
                }
            }
        }
        System.out.println("-".repeat(100));
        System.out.println();

        if (DRY_RUN) {
            System.out.println("DRY RUN - No changes will be made.");
            return 0;
        }

        // Perform the sync update
        String updateSql = """
            UPDATE scrobble
            SET 
                artist = (SELECT ar.name FROM song s JOIN artist ar ON s.artist_id = ar.id WHERE s.id = scrobble.song_id),
                album = (SELECT COALESCE(al.name, '') FROM song s LEFT JOIN album al ON s.album_id = al.id WHERE s.id = scrobble.song_id),
                song = (SELECT s.name FROM song s WHERE s.id = scrobble.song_id)
            WHERE song_id IS NOT NULL
            AND (
                artist != (SELECT ar.name FROM song s JOIN artist ar ON s.artist_id = ar.id WHERE s.id = scrobble.song_id)
                OR song != (SELECT s.name FROM song s WHERE s.id = scrobble.song_id)
                OR album != (SELECT COALESCE(al.name, '') FROM song s LEFT JOIN album al ON s.album_id = al.id WHERE s.id = scrobble.song_id)
            )
            """;
        
        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            int updated = ps.executeUpdate();
            System.out.println("Synced " + updated + " scrobbles");
            return updated;
        }
    }
}
