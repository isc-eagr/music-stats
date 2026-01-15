package library.service;

import org.springframework.stereotype.Service;
import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.*;

@Service
public class iTunesLibraryService {

    // Default iTunes library location on Windows
    private static final String DEFAULT_ITUNES_LIBRARY_PATH = 
        System.getProperty("user.home") + "/Music/iTunes/iTunes Music Library.xml";

    /**
     * Represents a track in the iTunes library
     */
    public static class iTunesTrack {
        public final String name;
        public final String artist;
        public final String album;

        public iTunesTrack(String name, String artist, String album) {
            this.name = name != null ? name : "";
            this.artist = artist != null ? artist : "";
            this.album = album != null ? album : "";
        }

        /**
         * Create a lookup key for matching (lowercase, trimmed)
         */
        public String getLookupKey() {
            return (name.toLowerCase().trim() + "||" + 
                    artist.toLowerCase().trim() + "||" + 
                    album.toLowerCase().trim());
        }
    }

    /**
     * Load all tracks from the iTunes library XML
     */
    public Map<String, iTunesTrack> loadLibrary() {
        return loadLibrary(DEFAULT_ITUNES_LIBRARY_PATH);
    }

    /**
     * Load all tracks from a specific iTunes library XML file
     */
    public Map<String, iTunesTrack> loadLibrary(String libraryPath) {
        Map<String, iTunesTrack> library = new HashMap<>();
        
        try {
            File xmlFile = new File(libraryPath);
            if (!xmlFile.exists()) {
                System.err.println("iTunes library not found at: " + libraryPath);
                return library;
            }

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // Disable external entities for security
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            // Navigate to the Tracks dict: plist > dict > key("Tracks") > dict
            Element plist = doc.getDocumentElement();
            NodeList plistChildren = plist.getChildNodes();
            
            Element mainDict = null;
            for (int i = 0; i < plistChildren.getLength(); i++) {
                Node node = plistChildren.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE && "dict".equals(node.getNodeName())) {
                    mainDict = (Element) node;
                    break;
                }
            }

            if (mainDict == null) {
                System.err.println("Could not find main dict in iTunes library");
                return library;
            }

            // Find the Tracks dict
            Element tracksDict = findDictForKey(mainDict, "Tracks");
            if (tracksDict == null) {
                System.err.println("Could not find Tracks dict in iTunes library");
                return library;
            }

            // Parse each track
            NodeList trackChildren = tracksDict.getChildNodes();
            String currentTrackId = null;
            
            for (int i = 0; i < trackChildren.getLength(); i++) {
                Node node = trackChildren.item(i);
                if (node.getNodeType() != Node.ELEMENT_NODE) continue;
                
                if ("key".equals(node.getNodeName())) {
                    currentTrackId = node.getTextContent();
                } else if ("dict".equals(node.getNodeName()) && currentTrackId != null) {
                    iTunesTrack track = parseTrackDict((Element) node);
                    if (track != null && !track.name.isEmpty()) {
                        library.put(track.getLookupKey(), track);
                    }
                    currentTrackId = null;
                }
            }

            System.out.println("Loaded " + library.size() + " tracks from iTunes library");
            
        } catch (Exception e) {
            System.err.println("Error loading iTunes library: " + e.getMessage());
            e.printStackTrace();
        }

        return library;
    }

    /**
     * Find the dict element that follows a specific key in a parent dict
     */
    private Element findDictForKey(Element parentDict, String keyName) {
        NodeList children = parentDict.getChildNodes();
        boolean foundKey = false;
        
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;
            
            if (foundKey && "dict".equals(node.getNodeName())) {
                return (Element) node;
            }
            
            if ("key".equals(node.getNodeName()) && keyName.equals(node.getTextContent())) {
                foundKey = true;
            }
        }
        
        return null;
    }

    /**
     * Parse a track dict element to extract Name, Artist, Album
     */
    private iTunesTrack parseTrackDict(Element trackDict) {
        String name = null;
        String artist = null;
        String album = null;

        NodeList children = trackDict.getChildNodes();
        String currentKey = null;

        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;

            if ("key".equals(node.getNodeName())) {
                currentKey = node.getTextContent();
            } else if ("string".equals(node.getNodeName()) && currentKey != null) {
                String value = node.getTextContent();
                switch (currentKey) {
                    case "Name" -> name = value;
                    case "Artist" -> artist = value;
                    case "Album" -> album = value;
                }
                currentKey = null;
            } else {
                // Other value types (integer, date, etc.) - skip
                currentKey = null;
            }
        }

        return new iTunesTrack(name, artist, album);
    }

    /**
     * Check if a song exists in the iTunes library
     */
    public boolean songExists(Map<String, iTunesTrack> library, String name, String artist, String album) {
        String lookupKey = (name != null ? name.toLowerCase().trim() : "") + "||" +
                          (artist != null ? artist.toLowerCase().trim() : "") + "||" +
                          (album != null ? album.toLowerCase().trim() : "");
        return library.containsKey(lookupKey);
    }

    /**
     * Find the best match for a song in the iTunes library.
     * Returns the matched track or null if no match found.
     */
    public iTunesTrack findMatch(Map<String, iTunesTrack> library, String name, String artist, String album) {
        String lookupKey = (name != null ? name.toLowerCase().trim() : "") + "||" +
                          (artist != null ? artist.toLowerCase().trim() : "") + "||" +
                          (album != null ? album.toLowerCase().trim() : "");
        return library.get(lookupKey);
    }

    /**
     * Check if the iTunes library file exists
     */
    public boolean libraryExists() {
        return new File(DEFAULT_ITUNES_LIBRARY_PATH).exists();
    }

    /**
     * Get the default library path
     */
    public String getDefaultLibraryPath() {
        return DEFAULT_ITUNES_LIBRARY_PATH;
    }

    /**
     * Represents iTunes track data (release date and length)
     */
    public static class iTunesTrackData {
        public final String releaseDate; // YYYY-MM-DD format
        public final Integer lengthSeconds; // song length in seconds
        public final String matchType; // "exact" or "partial" (without album)

        public iTunesTrackData(String releaseDate, Integer lengthSeconds, String matchType) {
            this.releaseDate = releaseDate;
            this.lengthSeconds = lengthSeconds;
            this.matchType = matchType;
        }
    }

    /**
     * Find release date and length for a specific song from iTunes XML.
     * Returns iTunesTrackData with release date (YYYY-MM-DD) and length in seconds, or null if not found.
     */
    public iTunesTrackData findTrackData(String songName, String artistName, String albumName) {
        return findTrackData(DEFAULT_ITUNES_LIBRARY_PATH, songName, artistName, albumName);
    }

    /**
     * Find release date and length for a specific song from iTunes XML.
     * Returns iTunesTrackData with release date (YYYY-MM-DD) and length in seconds, or null if not found.
     */
    public iTunesTrackData findTrackData(String libraryPath, String songName, String artistName, String albumName) {
        try {
            File xmlFile = new File(libraryPath);
            if (!xmlFile.exists()) {
                System.err.println("iTunes library not found at: " + libraryPath);
                return null;
            }

            // Read XML file as string for regex matching (similar to ReleaseDateMatcher)
            String xmlContent = java.nio.file.Files.readString(
                java.nio.file.Path.of(libraryPath), 
                java.nio.charset.StandardCharsets.UTF_8
            );

            // Pattern to match each track dict
            java.util.regex.Pattern trackPattern = java.util.regex.Pattern.compile(
                "<key>(\\d+)</key>\\s*<dict>(.*?)</dict>", 
                java.util.regex.Pattern.DOTALL
            );
            java.util.regex.Matcher trackMatcher = trackPattern.matcher(xmlContent);

            while (trackMatcher.find()) {
                String trackDict = trackMatcher.group(2);

                String name = extractStringValue(trackDict, "Name");
                String artist = extractStringValue(trackDict, "Artist");
                String album = extractStringValue(trackDict, "Album");

                if (name == null || artist == null) {
                    continue;
                }

                // Build lookup keys (normalized: lowercase, trimmed)
                String trackKey = buildLookupKey(artist, album, name);
                String searchKey = buildLookupKey(
                    artistName != null ? artistName : "",
                    albumName != null ? albumName : "",
                    songName != null ? songName : ""
                );

                // Also try without album for matching
                String trackKeyNoAlbum = buildLookupKey(artist, "", name);
                String searchKeyNoAlbum = buildLookupKey(
                    artistName != null ? artistName : "",
                    "",
                    songName != null ? songName : ""
                );

                // Determine match type
                String matchType = null;
                if (trackKey.equals(searchKey)) {
                    matchType = "exact";
                } else if (trackKeyNoAlbum.equals(searchKeyNoAlbum)) {
                    matchType = "partial";
                }

                if (matchType != null) {
                    // Found a match! Extract release date and length
                    String releaseDateRaw = extractDateValue(trackDict, "Release Date");
                    String yearRaw = extractIntegerValue(trackDict, "Year");
                    String totalTimeMs = extractIntegerValue(trackDict, "Total Time");

                    // Determine release date
                    String releaseDate = null;
                    if (releaseDateRaw != null && releaseDateRaw.length() >= 10) {
                        releaseDate = releaseDateRaw.substring(0, 10); // Format: 2016-03-18T12:00:00Z -> 2016-03-18
                    } else if (yearRaw != null) {
                        releaseDate = yearRaw + "-01-01";
                    }

                    // Convert total time from milliseconds to seconds
                    Integer lengthSeconds = null;
                    if (totalTimeMs != null) {
                        try {
                            lengthSeconds = Integer.parseInt(totalTimeMs) / 1000;
                        } catch (NumberFormatException e) {
                            System.err.println("Failed to parse Total Time: " + totalTimeMs);
                        }
                    }

                    return new iTunesTrackData(releaseDate, lengthSeconds, matchType);
                }
            }

        } catch (Exception e) {
            System.err.println("Error finding track data in iTunes library: " + e.getMessage());
            e.printStackTrace();
        }

        return null; // Not found
    }

    /**
     * Extract a string value from iTunes dict XML
     */
    private String extractStringValue(String dict, String key) {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
            "<key>" + java.util.regex.Pattern.quote(key) + "</key>\\s*<string>(.*?)</string>", 
            java.util.regex.Pattern.DOTALL
        );
        java.util.regex.Matcher m = p.matcher(dict);
        if (m.find()) {
            return decodeXmlEntities(m.group(1));
        }
        return null;
    }

    /**
     * Extract a date value from iTunes dict XML
     */
    private String extractDateValue(String dict, String key) {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
            "<key>" + java.util.regex.Pattern.quote(key) + "</key>\\s*<date>(.*?)</date>"
        );
        java.util.regex.Matcher m = p.matcher(dict);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    /**
     * Extract an integer value from iTunes dict XML
     */
    private String extractIntegerValue(String dict, String key) {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
            "<key>" + java.util.regex.Pattern.quote(key) + "</key>\\s*<integer>(.*?)</integer>"
        );
        java.util.regex.Matcher m = p.matcher(dict);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    /**
     * Decode XML entities
     */
    private String decodeXmlEntities(String s) {
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
    private String buildLookupKey(String artist, String album, String song) {
        String a = artist == null ? "" : artist.toLowerCase().trim();
        String b = album == null ? "" : album.toLowerCase().trim();
        String s = song == null ? "" : song.toLowerCase().trim();
        return a + "||" + b + "||" + s;
    }
}
