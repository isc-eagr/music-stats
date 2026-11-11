package library.dto;

/**
 * DTO representing an iTunes song that has changed (artist, album, or title).
 * Includes both the old and new values, plus whether the new values
 * match a song in the Music Stats database.
 */
public class ItunesChangedSongDTO {

    private String persistentId;
    
    // Old values (from snapshot)
    private String oldArtist;
    private String oldAlbumArtist;
    private String oldAlbum;
    private String oldName;
    private String oldGenre;
    
    // New values (from current XML)
    private String newArtist;
    private String newAlbumArtist;
    private String newAlbum;
    private String newName;
    private String newGenre;
    private Integer trackNumber;
    private Integer year;
    
    // Length fields (in milliseconds from iTunes, but we'll also track seconds)
    private Integer oldTotalTime;  // Old length in milliseconds
    private Integer newTotalTime;  // New length in milliseconds
    
    // Change flags
    private boolean artistChanged;
    private boolean albumArtistChanged;
    private boolean albumChanged;
    private boolean nameChanged;
    private boolean lengthChanged;
    private boolean genreChanged;
    
    // Whether the new values match a song in the database
    private boolean foundInDatabase;
    
    // If found, the song ID in the database
    private Long databaseSongId;

    public ItunesChangedSongDTO() {
    }

    public ItunesChangedSongDTO(String persistentId,
                                 String oldArtist, String oldAlbumArtist, String oldAlbum, String oldName, String oldGenre,
                                 String newArtist, String newAlbumArtist, String newAlbum, String newName, String newGenre,
                                 Integer trackNumber, Integer year,
                                 boolean foundInDatabase, Long databaseSongId,
                                 Integer oldTotalTime, Integer newTotalTime) {
        this.persistentId = persistentId;
        this.oldArtist = oldArtist;
        this.oldAlbumArtist = oldAlbumArtist;
        this.oldAlbum = oldAlbum;
        this.oldName = oldName;
        this.oldGenre = oldGenre;
        this.newArtist = newArtist;
        this.newAlbumArtist = newAlbumArtist;
        this.newAlbum = newAlbum;
        this.newName = newName;
        this.newGenre = newGenre;
        this.trackNumber = trackNumber;
        this.year = year;
        this.foundInDatabase = foundInDatabase;
        this.databaseSongId = databaseSongId;
        this.oldTotalTime = oldTotalTime;
        this.newTotalTime = newTotalTime;
        
        // Compute change flags
        this.artistChanged = textChanged(oldArtist, newArtist);
        this.albumArtistChanged = textChanged(oldAlbumArtist, newAlbumArtist);
        this.albumChanged = textChanged(oldAlbum, newAlbum);
        this.nameChanged = textChanged(oldName, newName);
        this.lengthChanged = lengthChanged(oldTotalTime, newTotalTime);
        this.genreChanged = textChanged(oldGenre, newGenre);
    }

    /**
     * Case-only edits ("Dirty harry" -> "Dirty Harry") are not changes. Everything else is:
     * added/removed parentheses ("(feat. Bootie Brown)"), accents, punctuation, any special character.
     */
    public static boolean textChanged(String a, String b) {
        if (a == null || b == null) return a != b;
        return !a.equalsIgnoreCase(b);
    }

    /**
     * Makes spacing edits visible on the page, where the browser collapses repeated spaces.
     * Leading/trailing spaces, runs of 2+ spaces and odd whitespace (tabs, non-breaking spaces)
     * become a middle dot; a normal single space between words is left alone.
     * "The Show Must Go  On" -> "The Show Must Go··On".
     */
    public String showSpaces(String value) {
        if (value == null) return null;
        int n = value.length();
        StringBuilder out = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            char c = value.charAt(i);
            if (!isSpace(c)) {
                out.append(c);
                continue;
            }
            boolean normal = c == ' '
                && i > 0 && i < n - 1
                && !isSpace(value.charAt(i - 1)) && !isSpace(value.charAt(i + 1));
            out.append(normal ? ' ' : '·');
        }
        return out.toString();
    }

    private static boolean isSpace(char c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c);
    }

    /**
     * Lengths are in milliseconds. iTunes re-reads can round the same file differently
     * (311072 vs 311000), so differences under one second are not changes.
     */
    public static boolean lengthChanged(Integer a, Integer b) {
        if (a == null || b == null) return a != b;
        return Math.abs(a - b) >= 1000;
    }
    
    // Helper methods for formatted length display
    public String getOldLengthFormatted() {
        return formatLength(oldTotalTime);
    }
    
    public String getNewLengthFormatted() {
        return formatLength(newTotalTime);
    }
    
    private String formatLength(Integer totalTimeMs) {
        if (totalTimeMs == null || totalTimeMs == 0) return null;
        int totalSeconds = totalTimeMs / 1000;
        int minutes = totalSeconds / 60;
        int seconds = totalSeconds % 60;
        return String.format("%d:%02d", minutes, seconds);
    }

    // Getters
    public String getPersistentId() { return persistentId; }
    public String getOldArtist() { return oldArtist; }
    public String getOldAlbumArtist() { return oldAlbumArtist; }
    public String getOldAlbum() { return oldAlbum; }
    public String getOldName() { return oldName; }
    public String getOldGenre() { return oldGenre; }
    public String getNewArtist() { return newArtist; }
    public String getNewAlbumArtist() { return newAlbumArtist; }
    public String getNewAlbum() { return newAlbum; }
    public String getNewName() { return newName; }
    public String getNewGenre() { return newGenre; }
    public Integer getTrackNumber() { return trackNumber; }
    public Integer getYear() { return year; }
    public boolean isArtistChanged() { return artistChanged; }
    public boolean isAlbumArtistChanged() { return albumArtistChanged; }
    public boolean isAlbumChanged() { return albumChanged; }
    public boolean isNameChanged() { return nameChanged; }
    public boolean isLengthChanged() { return lengthChanged; }
    public boolean isGenreChanged() { return genreChanged; }
    public boolean isFoundInDatabase() { return foundInDatabase; }
    public Long getDatabaseSongId() { return databaseSongId; }
    public Integer getOldTotalTime() { return oldTotalTime; }
    public Integer getNewTotalTime() { return newTotalTime; }

    // Setters
    public void setTrackNumber(Integer trackNumber) { this.trackNumber = trackNumber; }
    public void setYear(Integer year) { this.year = year; }
}
