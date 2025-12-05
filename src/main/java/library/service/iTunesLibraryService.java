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
}
