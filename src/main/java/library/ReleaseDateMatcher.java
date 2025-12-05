package library;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

/**
 * Standalone utility to match songs from the database export against iTunes XML
 * and generate SQL UPDATE statements for release dates.
 * 
 * Usage: Run main() method directly or via:
 *   mvn exec:java -Dexec.mainClass="library.ReleaseDateMatcher"
 * 
 * Input files:
 *   - iTunes XML: C:\Users\ing_e\Music\iTunes\iTunes Music Library.xml
 *   - Database export: export.csv in project root (id, song_name, artist_name, album_name)
 * 
 * Output:
 *   - release_date_updates.sql in project root
 */
public class ReleaseDateMatcher {
    
    // iTunes XML location
    private static final String ITUNES_XML_PATH = "C:\\Users\\ing_e\\Music\\iTunes\\iTunes Music Library.xml";
    
    // CSV export from database (in project root)
    private static final String CSV_PATH = "export.csv";
    
    // Output SQL file
    private static final String OUTPUT_SQL_PATH = "release_date_updates.sql";
    
    public static void main(String[] args) {
        System.out.println("=== Release Date Matcher ===");
        System.out.println();
        
        try {
            // Step 1: Parse iTunes XML
            System.out.println("Parsing iTunes XML...");
            Map<String, String> itunesReleaseDates = parseItunesXml();
            System.out.println("Found " + itunesReleaseDates.size() + " tracks with release dates in iTunes");
            System.out.println();
            
            // Step 2: Parse database CSV
            System.out.println("Parsing database CSV...");
            List<SongRecord> dbSongs = parseCsv();
            System.out.println("Found " + dbSongs.size() + " songs in database export");
            System.out.println();
            
            // Step 3: Match and generate SQL
            System.out.println("Matching songs and generating SQL...");
            int matchCount = generateSql(dbSongs, itunesReleaseDates);
            System.out.println();
            System.out.println("=== Complete ===");
            System.out.println("Matched " + matchCount + " songs");
            System.out.println("SQL written to: " + OUTPUT_SQL_PATH);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Parse iTunes XML and build a map of lookup key -> release date (YYYY-MM-DD)
     */
    private static Map<String, String> parseItunesXml() throws IOException {
        Map<String, String> releaseDates = new HashMap<>();
        
        String xmlContent = Files.readString(Path.of(ITUNES_XML_PATH), StandardCharsets.UTF_8);
        
        // Pattern to match each track dict
        // iTunes XML structure: <key>Track ID</key><integer>...</integer> followed by track properties
        // We need to extract Name, Artist, Album, Release Date, Year from each track
        
        // Split by track entries - each track starts with a numeric key
        Pattern trackPattern = Pattern.compile("<key>(\\d+)</key>\\s*<dict>(.*?)</dict>", Pattern.DOTALL);
        Matcher trackMatcher = trackPattern.matcher(xmlContent);
        
        while (trackMatcher.find()) {
            String trackDict = trackMatcher.group(2);
            
            String name = extractStringValue(trackDict, "Name");
            String artist = extractStringValue(trackDict, "Artist");
            String album = extractStringValue(trackDict, "Album");
            String releaseDateRaw = extractDateValue(trackDict, "Release Date");
            String yearRaw = extractIntegerValue(trackDict, "Year");
            
            if (name == null || artist == null) {
                continue; // Skip tracks without name or artist
            }
            
            // Determine release date - prioritize Release Date over Year
            String releaseDate = null;
            if (releaseDateRaw != null) {
                // Format: 2016-03-18T12:00:00Z -> 2016-03-18
                releaseDate = releaseDateRaw.substring(0, 10);
            } else if (yearRaw != null) {
                // Use year as YYYY-01-01
                releaseDate = yearRaw + "-01-01";
            }
            
            if (releaseDate == null) {
                continue; // No date info
            }
            
            // Build lookup key with album
            String key = buildLookupKey(artist, album, name);
            releaseDates.put(key, releaseDate);
            
            // Also add key without album for matching songs with empty album
            if (album != null && !album.isEmpty()) {
                String keyNoAlbum = buildLookupKey(artist, "", name);
                // Only add if not already present (prefer more specific match)
                releaseDates.putIfAbsent(keyNoAlbum, releaseDate);
            }
        }
        
        return releaseDates;
    }
    
    /**
     * Extract a string value from iTunes dict XML
     */
    private static String extractStringValue(String dict, String key) {
        Pattern p = Pattern.compile("<key>" + Pattern.quote(key) + "</key>\\s*<string>(.*?)</string>", Pattern.DOTALL);
        Matcher m = p.matcher(dict);
        if (m.find()) {
            return decodeXmlEntities(m.group(1));
        }
        return null;
    }
    
    /**
     * Extract a date value from iTunes dict XML
     */
    private static String extractDateValue(String dict, String key) {
        Pattern p = Pattern.compile("<key>" + Pattern.quote(key) + "</key>\\s*<date>(.*?)</date>");
        Matcher m = p.matcher(dict);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }
    
    /**
     * Extract an integer value from iTunes dict XML
     */
    private static String extractIntegerValue(String dict, String key) {
        Pattern p = Pattern.compile("<key>" + Pattern.quote(key) + "</key>\\s*<integer>(.*?)</integer>");
        Matcher m = p.matcher(dict);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }
    
    /**
     * Decode XML entities
     */
    private static String decodeXmlEntities(String s) {
        if (s == null) return null;
        return s.replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                .replace("&#38;", "&");
    }
    
    /**
     * Build a normalized lookup key
     */
    private static String buildLookupKey(String artist, String album, String song) {
        String a = artist == null ? "" : artist.toLowerCase().trim();
        String b = album == null ? "" : album.toLowerCase().trim();
        String s = song == null ? "" : song.toLowerCase().trim();
        return a + "||" + b + "||" + s;
    }
    
    /**
     * Parse the database CSV export
     */
    private static List<SongRecord> parseCsv() throws IOException {
        List<SongRecord> songs = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(Path.of(CSV_PATH), StandardCharsets.UTF_8)) {
            String line;
            boolean firstLine = true;
            
            while ((line = reader.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue; // Skip header
                }
                
                // Parse CSV line (handle quoted fields with commas)
                String[] fields = parseCsvLine(line);
                if (fields.length >= 4) {
                    try {
                        int id = Integer.parseInt(fields[0].trim());
                        String songName = fields[1].trim();
                        String artistName = fields[2].trim();
                        String albumName = fields[3].trim();
                        songs.add(new SongRecord(id, songName, artistName, albumName));
                    } catch (NumberFormatException e) {
                        // Skip malformed lines
                    }
                }
            }
        }
        
        return songs;
    }
    
    /**
     * Parse a CSV line handling quoted fields
     */
    private static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // Escaped quote
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        
        return fields.toArray(new String[0]);
    }
    
    /**
     * Generate SQL UPDATE statements
     */
    private static int generateSql(List<SongRecord> dbSongs, Map<String, String> itunesReleaseDates) throws IOException {
        int matchCount = 0;
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(OUTPUT_SQL_PATH, StandardCharsets.UTF_8))) {
            writer.println("-- Release Date Updates generated from iTunes XML");
            writer.println("-- Generated: " + java.time.LocalDateTime.now());
            writer.println("-- Source: " + ITUNES_XML_PATH);
            writer.println();
            writer.println("BEGIN TRANSACTION;");
            writer.println();
            
            for (SongRecord song : dbSongs) {
                // Try matching with album first
                String key = buildLookupKey(song.artistName, song.albumName, song.songName);
                String releaseDate = itunesReleaseDates.get(key);
                
                // If no match and album is empty, try matching without album
                if (releaseDate == null && (song.albumName == null || song.albumName.isEmpty())) {
                    String keyNoAlbum = buildLookupKey(song.artistName, "", song.songName);
                    releaseDate = itunesReleaseDates.get(keyNoAlbum);
                }
                
                if (releaseDate != null) {
                    writer.printf("UPDATE Song SET release_date = '%s' WHERE id = %d;%n", 
                            releaseDate, song.id);
                    matchCount++;
                }
            }
            
            writer.println();
            writer.println("COMMIT;");
            writer.println();
            writer.println("-- Total updates: " + matchCount);
        }
        
        return matchCount;
    }
    
    /**
     * Simple record class for database songs
     */
    private static class SongRecord {
        final int id;
        final String songName;
        final String artistName;
        final String albumName;
        
        SongRecord(int id, String songName, String artistName, String albumName) {
            this.id = id;
            this.songName = songName;
            this.artistName = artistName;
            this.albumName = albumName;
        }
    }
}
