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
    private String oldAlbum;
    private String oldName;
    
    // New values (from current XML)
    private String newArtist;
    private String newAlbum;
    private String newName;
    private Integer trackNumber;
    private Integer year;
    
    // Length fields (in milliseconds from iTunes, but we'll also track seconds)
    private Integer oldTotalTime;  // Old length in milliseconds
    private Integer newTotalTime;  // New length in milliseconds
    
    // Change flags
    private boolean artistChanged;
    private boolean albumChanged;
    private boolean nameChanged;
    private boolean lengthChanged;
    
    // Whether the new values match a song in the database
    private boolean foundInDatabase;
    
    // If found, the song ID in the database
    private Long databaseSongId;

    public ItunesChangedSongDTO() {
    }

    public ItunesChangedSongDTO(String persistentId,
                                 String oldArtist, String oldAlbum, String oldName,
                                 String newArtist, String newAlbum, String newName,
                                 Integer trackNumber, Integer year,
                                 boolean foundInDatabase, Long databaseSongId,
                                 Integer oldTotalTime, Integer newTotalTime) {
        this.persistentId = persistentId;
        this.oldArtist = oldArtist;
        this.oldAlbum = oldAlbum;
        this.oldName = oldName;
        this.newArtist = newArtist;
        this.newAlbum = newAlbum;
        this.newName = newName;
        this.trackNumber = trackNumber;
        this.year = year;
        this.foundInDatabase = foundInDatabase;
        this.databaseSongId = databaseSongId;
        this.oldTotalTime = oldTotalTime;
        this.newTotalTime = newTotalTime;
        
        // Compute change flags
        this.artistChanged = !nullSafeEquals(oldArtist, newArtist);
        this.albumChanged = !nullSafeEquals(oldAlbum, newAlbum);
        this.nameChanged = !nullSafeEquals(oldName, newName);
        this.lengthChanged = !nullSafeEquals(oldTotalTime, newTotalTime);
    }

    private boolean nullSafeEquals(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }
    
    private boolean nullSafeEquals(Integer a, Integer b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
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
    public String getOldAlbum() { return oldAlbum; }
    public String getOldName() { return oldName; }
    public String getNewArtist() { return newArtist; }
    public String getNewAlbum() { return newAlbum; }
    public String getNewName() { return newName; }
    public Integer getTrackNumber() { return trackNumber; }
    public Integer getYear() { return year; }
    public boolean isArtistChanged() { return artistChanged; }
    public boolean isAlbumChanged() { return albumChanged; }
    public boolean isNameChanged() { return nameChanged; }
    public boolean isLengthChanged() { return lengthChanged; }
    public boolean isFoundInDatabase() { return foundInDatabase; }
    public Long getDatabaseSongId() { return databaseSongId; }
    public Integer getOldTotalTime() { return oldTotalTime; }
    public Integer getNewTotalTime() { return newTotalTime; }

    // Setters
    public void setPersistentId(String persistentId) { this.persistentId = persistentId; }
    public void setOldArtist(String oldArtist) { this.oldArtist = oldArtist; }
    public void setOldAlbum(String oldAlbum) { this.oldAlbum = oldAlbum; }
    public void setOldName(String oldName) { this.oldName = oldName; }
    public void setNewArtist(String newArtist) { this.newArtist = newArtist; }
    public void setNewAlbum(String newAlbum) { this.newAlbum = newAlbum; }
    public void setNewName(String newName) { this.newName = newName; }
    public void setTrackNumber(Integer trackNumber) { this.trackNumber = trackNumber; }
    public void setYear(Integer year) { this.year = year; }
    public void setArtistChanged(boolean artistChanged) { this.artistChanged = artistChanged; }
    public void setAlbumChanged(boolean albumChanged) { this.albumChanged = albumChanged; }
    public void setNameChanged(boolean nameChanged) { this.nameChanged = nameChanged; }
    public void setLengthChanged(boolean lengthChanged) { this.lengthChanged = lengthChanged; }
    public void setFoundInDatabase(boolean foundInDatabase) { this.foundInDatabase = foundInDatabase; }
    public void setDatabaseSongId(Long databaseSongId) { this.databaseSongId = databaseSongId; }
    public void setOldTotalTime(Integer oldTotalTime) { this.oldTotalTime = oldTotalTime; }
    public void setNewTotalTime(Integer newTotalTime) { this.newTotalTime = newTotalTime; }
}
