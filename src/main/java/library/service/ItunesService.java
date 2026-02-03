package library.service;

import library.util.StringNormalizer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.InputStream;
import java.util.*;

/**
 * Service for parsing iTunes Library.xml files and finding songs that exist
 * in iTunes but not in the Music Stats database.
 * 
 * Caches the parsed iTunes data in memory and only re-parses when the file changes.
 */
@Service
public class ItunesService {

    private final JdbcTemplate jdbcTemplate;
    private final iTunesLibraryService iTunesLibraryService;

    // ============ Cache Fields ============
    private Set<String> cachedSongKeys = null;
    private Set<String> cachedAlbumKeys = null;
    private Set<String> cachedArtistKeys = null;
    private List<ItunesSong> cachedAllSongs = null;  // Full song list for iTunes Only page
    private long cachedFileLastModified = 0;
    private String cachedFilePath = null;

    public ItunesService(JdbcTemplate jdbcTemplate, iTunesLibraryService iTunesLibraryService) {
        this.jdbcTemplate = jdbcTemplate;
        this.iTunesLibraryService = iTunesLibraryService;
    }

    /**
     * Check if the cache is still valid (same file, not modified)
     */
    private boolean isCacheValid() {
        if (cachedSongKeys == null || cachedFilePath == null) {
            return false;
        }
        String currentPath = getDefaultLibraryPath();
        if (!cachedFilePath.equals(currentPath)) {
            return false;
        }
        File file = new File(currentPath);
        if (!file.exists()) {
            return false;
        }
        return file.lastModified() == cachedFileLastModified;
    }

    /**
     * Ensure the cache is populated and up-to-date.
     * Parses the iTunes library XML only if needed.
     */
    private synchronized void ensureCacheLoaded() throws Exception {
        if (isCacheValid()) {
            return;
        }

        String filePath = getDefaultLibraryPath();
        File file = new File(filePath);
        if (!file.exists()) {
            cachedSongKeys = new HashSet<>();
            cachedAlbumKeys = new HashSet<>();
            cachedArtistKeys = new HashSet<>();
            cachedAllSongs = new ArrayList<>();
            cachedFilePath = filePath;
            cachedFileLastModified = 0;
            return;
        }

        // Parse the library and build all caches at once
        List<ItunesSong> allSongs = parseItunesLibrary(new java.io.FileInputStream(file));
        
        Set<String> songKeys = new HashSet<>();
        Set<String> albumKeys = new HashSet<>();
        Set<String> artistKeys = new HashSet<>();
        
        for (ItunesSong song : allSongs) {
            // Song keys (artist||album||song) - using strict matching
            songKeys.add(createStrictSongLookupKey(song.getArtist(), song.getAlbum(), song.getName()));
            
            // Album keys - using strict matching
            if (song.getAlbum() != null && !song.getAlbum().isBlank()) {
                albumKeys.add(createStrictAlbumLookupKey(song.getArtist(), song.getAlbum()));
            }
            
            // Artist keys - using strict matching
            if (song.getArtist() != null && !song.getArtist().isBlank()) {
                artistKeys.add(normalizeForStrictMatch(song.getArtist()));
            }
        }
        
        // Update cache atomically
        cachedSongKeys = songKeys;
        cachedAlbumKeys = albumKeys;
        cachedArtistKeys = artistKeys;
        cachedAllSongs = allSongs;
        cachedFilePath = filePath;
        cachedFileLastModified = file.lastModified();
    }

    /**
     * Invalidate the cache, forcing a reload on next access.
     */
    public void invalidateCache() {
        cachedSongKeys = null;
        cachedAlbumKeys = null;
        cachedArtistKeys = null;
        cachedAllSongs = null;
        cachedFilePath = null;
        cachedFileLastModified = 0;
    }

    /**
     * Get the default iTunes library path from the existing iTunesLibraryService.
     */
    public String getDefaultLibraryPath() {
        return iTunesLibraryService.getDefaultLibraryPath();
    }

    /**
     * Check if the iTunes library file exists.
     */
    public boolean libraryExists() {
        return iTunesLibraryService.libraryExists();
    }

    /**
     * Find unmatched songs using the default iTunes library path.
     * Uses in-memory cache for performance.
     */
    public List<ItunesSong> findUnmatchedItunesSongs() throws Exception {
        ensureCacheLoaded();
        return findUnmatchedSongsFromCache();
    }

    /**
     * Get all songs from the iTunes library.
     * Used by the changes detection feature.
     */
    public List<ItunesSong> getAllItunesSongs() throws Exception {
        ensureCacheLoaded();
        return cachedAllSongs != null ? new ArrayList<>(cachedAllSongs) : new ArrayList<>();
    }

    /**
     * Find unmatched songs using the cached iTunes library data.
     * This method leverages the in-memory cache for maximum performance.
     * Uses STRICT matching for the iTunes Only page - only case and punctuation differences allowed.
     */
    private List<ItunesSong> findUnmatchedSongsFromCache() {
        if (cachedAllSongs == null || cachedAllSongs.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Build lookup set from database using strict matching
        Set<String> dbSongKeys = buildDatabaseLookupStrict();
        
        // Filter to only unmatched songs
        List<ItunesSong> unmatched = new ArrayList<>();
        for (ItunesSong song : cachedAllSongs) {
            String key = createStrictSongLookupKey(song.getArtist(), song.getAlbum(), song.getName());
            if (!dbSongKeys.contains(key)) {
                unmatched.add(song);
            }
        }
        
        // Sort by artist, then album, then track number/name
        unmatched.sort((a, b) -> {
            int cmp = nullSafeCompareIgnoreCase(a.getArtist(), b.getArtist());
            if (cmp != 0) return cmp;
            cmp = nullSafeCompareIgnoreCase(a.getAlbum(), b.getAlbum());
            if (cmp != 0) return cmp;
            // Within same album, try track number
            if (a.getTrackNumber() != null && b.getTrackNumber() != null) {
                cmp = a.getTrackNumber().compareTo(b.getTrackNumber());
                if (cmp != 0) return cmp;
            }
            return nullSafeCompareIgnoreCase(a.getName(), b.getName());
        });
        
        return unmatched;
    }

    /**
     * DTO for an iTunes song entry.
     * 
     * About Track ID vs Persistent ID:
     * - Track ID: A numeric ID assigned during a session. Can change when library is rebuilt.
     * - Persistent ID: A hexadecimal string that remains stable across library rebuilds.
     *   This is the true unique identifier for tracking changes over time.
     */
    public static class ItunesSong {
        private String persistentId;  // Stable hex identifier (e.g., "A1B2C3D4E5F6")
        private Integer trackId;      // Numeric ID that may change
        private String artist;
        private String albumArtist;   // Album Artist (may differ from Artist for compilations)
        private String album;
        private String name;
        private Integer trackNumber;
        private Integer year;
        private Integer totalTime;    // Total Time in milliseconds
        private String genre;         // Genre from iTunes

        public ItunesSong(String persistentId, Integer trackId, String artist, String albumArtist, String album, 
                          String name, Integer trackNumber, Integer year, Integer totalTime, String genre) {
            this.persistentId = persistentId;
            this.trackId = trackId;
            this.artist = artist;
            this.albumArtist = albumArtist;
            this.album = album;
            this.name = name;
            this.trackNumber = trackNumber;
            this.year = year;
            this.totalTime = totalTime;
            this.genre = genre;
        }

        public String getPersistentId() { return persistentId; }
        public Integer getTrackId() { return trackId; }
        public String getArtist() { return artist; }
        public String getAlbumArtist() { return albumArtist; }
        public String getAlbum() { return album; }
        public String getName() { return name; }
        public Integer getTrackNumber() { return trackNumber; }
        public Integer getYear() { return year; }
        public Integer getTotalTime() { return totalTime; }
        public String getGenre() { return genre; }
        
        // Get length in seconds (converted from milliseconds)
        public Integer getLengthSeconds() {
            return totalTime != null ? totalTime / 1000 : null;
        }
    }

    /**
     * Parse iTunes Library.xml file from file path and find all songs
     * that exist in iTunes but NOT in the database.
     * 
     * @param filePath Path to the Library.xml file
     * @return List of iTunes songs not found in the database
     */
    public List<ItunesSong> findUnmatchedItunesSongs(String filePath) throws Exception {
        java.io.File file = new java.io.File(filePath);
        if (!file.exists()) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }
        List<ItunesSong> allItunesSongs = parseItunesLibrary(new java.io.FileInputStream(file));
        return filterUnmatchedSongs(allItunesSongs);
    }

    /**
     * Parse iTunes Library.xml and extract all song entries
     */
    private List<ItunesSong> parseItunesLibrary(InputStream inputStream) throws Exception {
        List<ItunesSong> songs = new ArrayList<>();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // Disable DTD processing for security and to avoid network calls
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(inputStream);

        // iTunes Library.xml structure:
        // <plist>
        //   <dict>
        //     <key>Tracks</key>
        //     <dict>
        //       <key>123</key>  <!-- Track ID -->
        //       <dict>
        //         <key>Name</key><string>Song Name</string>
        //         <key>Artist</key><string>Artist Name</string>
        //         <key>Album</key><string>Album Name</string>
        //         ...
        //       </dict>
        //     </dict>
        //   </dict>
        // </plist>

        NodeList plistChildren = doc.getElementsByTagName("plist");
        if (plistChildren.getLength() == 0) {
            throw new IllegalArgumentException("Invalid iTunes Library.xml: no plist element found");
        }

        // Navigate to Tracks dict
        Element plist = (Element) plistChildren.item(0);
        Element mainDict = getFirstChildElement(plist, "dict");
        if (mainDict == null) {
            throw new IllegalArgumentException("Invalid iTunes Library.xml: no main dict found");
        }

        // Find the Tracks key and its associated dict
        Element tracksDict = null;
        NodeList mainDictChildren = mainDict.getChildNodes();
        boolean foundTracksKey = false;
        
        for (int i = 0; i < mainDictChildren.getLength(); i++) {
            Node node = mainDictChildren.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;
            
            Element elem = (Element) node;
            if ("key".equals(elem.getTagName()) && "Tracks".equals(elem.getTextContent())) {
                foundTracksKey = true;
                continue;
            }
            if (foundTracksKey && "dict".equals(elem.getTagName())) {
                tracksDict = elem;
                break;
            }
        }

        if (tracksDict == null) {
            throw new IllegalArgumentException("Invalid iTunes Library.xml: no Tracks dict found");
        }

        // Parse each track within the Tracks dict
        NodeList trackNodes = tracksDict.getChildNodes();
        Element currentTrackDict = null;
        
        for (int i = 0; i < trackNodes.getLength(); i++) {
            Node node = trackNodes.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;
            
            Element elem = (Element) node;
            if ("key".equals(elem.getTagName())) {
                // This is a track ID key, next dict is the track data
                continue;
            }
            if ("dict".equals(elem.getTagName())) {
                currentTrackDict = elem;
                ItunesSong song = parseTrackDict(currentTrackDict);
                if (song != null && song.getName() != null && !song.getName().isBlank()) {
                    songs.add(song);
                }
            }
        }

        return songs;
    }

    /**
     * Parse a single track dict element and extract song info
     */
    private ItunesSong parseTrackDict(Element trackDict) {
        String persistentId = null;
        Integer trackId = null;
        String name = null;
        String artist = null;
        String albumArtist = null;
        String album = null;
        Integer trackNumber = null;
        Integer year = null;
        Integer totalTime = null;
        String genre = null;
        Boolean isPodcast = false;
        Boolean isAudiobook = false;
        Boolean isPlaylistOnly = false;
        String kind = null;

        NodeList children = trackDict.getChildNodes();
        String currentKey = null;

        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;

            Element elem = (Element) node;
            String tagName = elem.getTagName();

            if ("key".equals(tagName)) {
                currentKey = elem.getTextContent();
            } else if (currentKey != null) {
                switch (currentKey) {
                    case "Persistent ID":
                        persistentId = elem.getTextContent();
                        break;
                    case "Track ID":
                        try {
                            trackId = Integer.parseInt(elem.getTextContent());
                        } catch (NumberFormatException e) {
                            trackId = null;
                        }
                        break;
                    case "Name":
                        name = elem.getTextContent();
                        break;
                    case "Artist":
                        artist = elem.getTextContent();
                        break;
                    case "Album Artist":
                        albumArtist = elem.getTextContent();
                        break;
                    case "Album":
                        album = elem.getTextContent();
                        break;
                    case "Playlist Only":
                        isPlaylistOnly = "true".equals(tagName);
                        break;
                    case "Track Number":
                        try {
                            trackNumber = Integer.parseInt(elem.getTextContent());
                        } catch (NumberFormatException e) {
                            trackNumber = null;
                        }
                        break;
                    case "Year":
                        try {
                            year = Integer.parseInt(elem.getTextContent());
                        } catch (NumberFormatException e) {
                            year = null;
                        }
                        break;
                    case "Total Time":
                        try {
                            totalTime = Integer.parseInt(elem.getTextContent());
                        } catch (NumberFormatException e) {
                            totalTime = null;
                        }
                        break;
                    case "Genre":
                        genre = elem.getTextContent();
                        break;
                    case "Podcast":
                        isPodcast = "true".equals(tagName) || "true".equalsIgnoreCase(elem.getTextContent());
                        break;
                    case "Kind":
                        kind = elem.getTextContent();
                        break;
                }
                currentKey = null;
            }
        }

        // Skip podcasts, audiobooks, and non-music items
        if (isPodcast || isAudiobook) {
            return null;
        }
        if (kind != null && (kind.toLowerCase().contains("video") || kind.toLowerCase().contains("podcast"))) {
            return null;
        }

        // Skip playlist-only items
        if (isPlaylistOnly) {
            return null;
        }

        // Only include items with at least a name and a persistent ID
        if (name == null || name.isBlank()) {
            return null;
        }
        if (persistentId == null || persistentId.isBlank()) {
            return null;
        }

        return new ItunesSong(persistentId, trackId, artist, albumArtist, album, name, trackNumber, year, totalTime, genre);
    }

    /**
     * Filter iTunes songs to only those not found in the database
     */
    private List<ItunesSong> filterUnmatchedSongs(List<ItunesSong> itunesSongs) {
        // Build lookup set from database: normalized key (artist||song) -> exists
        Set<String> dbSongKeys = buildDatabaseLookup();

        List<ItunesSong> unmatched = new ArrayList<>();

        for (ItunesSong song : itunesSongs) {
            String key = createSongLookupKey(song.getArtist(), song.getAlbum(), song.getName());
            if (!dbSongKeys.contains(key)) {
                unmatched.add(song);
            }
        }

        // Sort by artist, then album, then track number/name
        unmatched.sort((a, b) -> {
            int cmp = nullSafeCompareIgnoreCase(a.getArtist(), b.getArtist());
            if (cmp != 0) return cmp;
            cmp = nullSafeCompareIgnoreCase(a.getAlbum(), b.getAlbum());
            if (cmp != 0) return cmp;
            // Within same album, try track number
            if (a.getTrackNumber() != null && b.getTrackNumber() != null) {
                cmp = a.getTrackNumber().compareTo(b.getTrackNumber());
                if (cmp != 0) return cmp;
            }
            return nullSafeCompareIgnoreCase(a.getName(), b.getName());
        });

        return unmatched;
    }

    /**
     * Build a lookup set of all songs in the database using normalized artist||album||song keys
     */
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

    /**
     * Build a lookup set of all songs in the database using STRICT matching.
     * Only normalizes case and punctuation - no removal of parentheses, brackets, or featuring text.
     * Used specifically for the iTunes Only page.
     */
    private Set<String> buildDatabaseLookupStrict() {
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
            String key = createStrictSongLookupKey(artistName, albumName, songName);
            keys.add(key);
        });

        return keys;
    }

    /**
     * Create normalized lookup key for artist + song.
     * Uses case-insensitive matching with accent normalization.
     */
    private String createLookupKey(String artist, String song) {
        String a = StringNormalizer.normalizeForSearch(artist != null ? artist : "");
        String s = StringNormalizer.normalizeForSearch(song != null ? song : "");
        return a + "||" + s;
    }

    private int nullSafeCompareIgnoreCase(String a, String b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return a.compareToIgnoreCase(b);
    }

    private Element getFirstChildElement(Element parent, String tagName) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE && tagName.equals(node.getNodeName())) {
                return (Element) node;
            }
        }
        return null;
    }

    // ============ iTunes Presence Checking Methods ============

    /**
     * Check if a song exists in iTunes library (strict match by artist + album + song name).
     * Only case and punctuation differences are ignored.
     */
    public boolean songExistsInItunes(String artistName, String albumName, String songName) {
        if (!libraryExists()) return false;
        try {
            ensureCacheLoaded();
            String key = createStrictSongLookupKey(artistName, albumName, songName);
            return cachedSongKeys.contains(key);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Check if at least one song from an album exists in iTunes library.
     * Uses strict matching - only case and punctuation differences are ignored.
     */
    public boolean albumExistsInItunes(String artistName, String albumName) {
        if (!libraryExists()) return false;
        try {
            ensureCacheLoaded();
            String key = createStrictAlbumLookupKey(artistName, albumName);
            return cachedAlbumKeys.contains(key);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Check if at least one song from an artist exists in iTunes library.
     * Uses strict matching - only case and punctuation differences are ignored.
     */
    public boolean artistExistsInItunes(String artistName) {
        if (!libraryExists()) return false;
        try {
            ensureCacheLoaded();
            String key = normalizeForStrictMatch(artistName);
            return cachedArtistKeys.contains(key);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Create normalized lookup key for artist + album.
     */
    private String createAlbumLookupKey(String artist, String album) {
        String a = StringNormalizer.normalizeForSearch(artist != null ? artist : "");
        String al = StringNormalizer.normalizeForSearch(album != null ? album : "");
        return a + "||" + al;
    }

    /**
     * Create strict lookup key for artist + album.
     * Only normalizes case and punctuation - no removal of parentheses, brackets, or featuring text.
     */
    private String createStrictAlbumLookupKey(String artist, String album) {
        String a = normalizeForStrictMatch(artist);
        String al = normalizeForStrictMatch(album);
        return a + "||" + al;
    }

    /**
     * Create normalized lookup key for artist + album + song.
     * Uses case-insensitive matching with accent normalization.
     */
    private String createSongLookupKey(String artist, String album, String song) {
        String a = StringNormalizer.normalizeForSearch(artist != null ? artist : "");
        String al = StringNormalizer.normalizeForSearch(album != null ? album : "");
        String s = StringNormalizer.normalizeForSearch(song != null ? song : "");
        return a + "||" + al + "||" + s;
    }

    /**
     * Create strict lookup key for artist + album + song for iTunes Only page.
     * Only normalizes case and punctuation - no removal of parentheses, brackets, or featuring text.
     */
    private String createStrictSongLookupKey(String artist, String album, String song) {
        String a = normalizeForStrictMatch(artist);
        String al = normalizeForStrictMatch(album);
        String s = normalizeForStrictMatch(song);
        return a + "||" + al + "||" + s;
    }

    /**
     * Normalize for strict matching: lowercase + strip accents + trim + remove punctuation only.
     * Does NOT remove parentheses, brackets, or featuring text.
     */
    private String normalizeForStrictMatch(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }
        String result = StringNormalizer.stripAccents(input.toLowerCase().trim());
        // Remove only punctuation characters, keeping parentheses, brackets, etc.
        result = result.replaceAll("[\\\\.,'!?\"\\-_:;\\/&%]", "");
        // Collapse whitespace
        result = result.replaceAll("\\s+", " ").trim();
        return result;
    }

    // ============ Methods to get cached sets for filtering ============

    /**
     * Get the set of normalized artist names that exist in iTunes.
     * Useful for filtering artists by iTunes presence.
     */
    public Set<String> getItunesArtistKeys() {
        if (!libraryExists()) return new HashSet<>();
        try {
            ensureCacheLoaded();
            return cachedArtistKeys != null ? cachedArtistKeys : new HashSet<>();
        } catch (Exception e) {
            return new HashSet<>();
        }
    }

    /**
     * Get the set of normalized album keys (artist||album) that exist in iTunes.
     * Useful for filtering albums by iTunes presence.
     */
    public Set<String> getItunesAlbumKeys() {
        if (!libraryExists()) return new HashSet<>();
        try {
            ensureCacheLoaded();
            return cachedAlbumKeys != null ? cachedAlbumKeys : new HashSet<>();
        } catch (Exception e) {
            return new HashSet<>();
        }
    }

    /**
     * Get the set of normalized song keys (artist||song) that exist in iTunes.
     * Useful for filtering songs by iTunes presence.
     */
    public Set<String> getItunesSongKeys() {
        if (!libraryExists()) return new HashSet<>();
        try {
            ensureCacheLoaded();
            return cachedSongKeys != null ? cachedSongKeys : new HashSet<>();
        } catch (Exception e) {
            return new HashSet<>();
        }
    }
}
