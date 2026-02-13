package library;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Standalone script to sync play artist, album, and song fields with their matched song data.
 * 
 * This script finds all plays that have a song_id but where the artist, album, or song
 * fields don't match the canonical data from the song table. It then updates those plays
 * to use the canonical values.
 * 
 * Run this class directly with: mvn exec:java -Dexec.mainClass="library.PlaySyncScript"
 * Or run from your IDE.
 */
public class PlaySyncScript {

    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    private static final String DB_URL = "jdbc:sqlite:" + DB_PATH;
    
    // Set to true to only report what would be changed without making changes
    private static final boolean DRY_RUN = false;

    public static void main(String[] args) {
        System.out.println("=== Play Sync Script ===");
        System.out.println("Database: " + DB_PATH);
        System.out.println("Dry Run: " + DRY_RUN);
        System.out.println();

        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            // Count out-of-sync plays before
            int outOfSyncBefore = countOutOfSyncPlays(conn);
            System.out.println("Out-of-sync plays before: " + outOfSyncBefore);
            System.out.println();

            if (outOfSyncBefore == 0) {
                System.out.println("All plays are already in sync. Nothing to do.");
                return;
            }

            // Sync the plays
            System.out.println("=== Syncing Plays ===");
            int synced = syncPlays(conn);
            System.out.println();

            // Count out-of-sync plays after
            int outOfSyncAfter = countOutOfSyncPlays(conn);
            
            System.out.println("=== SUMMARY ===");
            System.out.println("Out-of-sync before: " + outOfSyncBefore);
            System.out.println("Plays synced:       " + synced);
            System.out.println("Out-of-sync after:  " + outOfSyncAfter);

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Count plays that have a song_id but where the artist, album, or song
     * fields don't match the canonical data.
     */
    private static int countOutOfSyncPlays(Connection conn) throws SQLException {
        String sql = """
            SELECT COUNT(*) FROM play p
            JOIN song s ON p.song_id = s.id
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN album al ON s.album_id = al.id
            WHERE p.song_id IS NOT NULL
            AND (
                p.artist != ar.name
                OR p.song != s.name
                OR p.album != COALESCE(al.name, '')
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
     * Sync all out-of-sync plays to use canonical artist, album, and song names.
     */
    private static int syncPlays(Connection conn) throws SQLException {
        // First, get a sample of what will be synced for logging
        String sampleSql = """
            SELECT 
                p.id as play_id,
                p.artist as old_artist,
                p.album as old_album,
                p.song as old_song,
                ar.name as new_artist,
                COALESCE(al.name, '') as new_album,
                s.name as new_song,
                p.song_id
            FROM play p
            JOIN song s ON p.song_id = s.id
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN album al ON s.album_id = al.id
            WHERE p.song_id IS NOT NULL
            AND (
                p.artist != ar.name
                OR p.song != s.name
                OR p.album != COALESCE(al.name, '')
            )
            LIMIT 20
            """;
        
        System.out.println("Sample of out-of-sync plays (first 20):");
        System.out.println("-".repeat(100));
        
        try (PreparedStatement ps = conn.prepareStatement(sampleSql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int playId = rs.getInt("play_id");
                String oldArtist = rs.getString("old_artist");
                String oldAlbum = rs.getString("old_album");
                String oldSong = rs.getString("old_song");
                String newArtist = rs.getString("new_artist");
                String newAlbum = rs.getString("new_album");
                String newSong = rs.getString("new_song");
                int songId = rs.getInt("song_id");
                
                System.out.printf("  Play #%d (song_id=%d):%n", playId, songId);
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
            UPDATE play
            SET 
                artist = (SELECT ar.name FROM song s JOIN artist ar ON s.artist_id = ar.id WHERE s.id = play.song_id),
                album = (SELECT COALESCE(al.name, '') FROM song s LEFT JOIN album al ON s.album_id = al.id WHERE s.id = play.song_id),
                song = (SELECT s.name FROM song s WHERE s.id = play.song_id)
            WHERE song_id IS NOT NULL
            AND (
                artist != (SELECT ar.name FROM song s JOIN artist ar ON s.artist_id = ar.id WHERE s.id = play.song_id)
                OR song != (SELECT s.name FROM song s WHERE s.id = play.song_id)
                OR album != (SELECT COALESCE(al.name, '') FROM song s LEFT JOIN album al ON s.album_id = al.id WHERE s.id = play.song_id)
            )
            """;
        
        try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
            int updated = ps.executeUpdate();
            System.out.println("Synced " + updated + " plays");
            return updated;
        }
    }
}
