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
        this.artistChanged = !nullSafeEquals(oldArtist, newArtist);
        this.albumArtistChanged = !nullSafeEquals(oldAlbumArtist, newAlbumArtist);
        this.albumChanged = !nullSafeEquals(oldAlbum, newAlbum);
        this.nameChanged = !nullSafeEquals(oldName, newName);
        this.lengthChanged = !nullSafeEquals(oldTotalTime, newTotalTime);
        this.genreChanged = !nullSafeEquals(oldGenre, newGenre);
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
    public void setPersistentId(String persistentId) { this.persistentId = persistentId; }
    public void setOldArtist(String oldArtist) { this.oldArtist = oldArtist; }
    public void setOldAlbumArtist(String oldAlbumArtist) { this.oldAlbumArtist = oldAlbumArtist; }
    public void setOldAlbum(String oldAlbum) { this.oldAlbum = oldAlbum; }
    public void setOldName(String oldName) { this.oldName = oldName; }
    public void setOldGenre(String oldGenre) { this.oldGenre = oldGenre; }
    public void setNewArtist(String newArtist) { this.newArtist = newArtist; }
    public void setNewAlbumArtist(String newAlbumArtist) { this.newAlbumArtist = newAlbumArtist; }
    public void setNewAlbum(String newAlbum) { this.newAlbum = newAlbum; }
    public void setNewName(String newName) { this.newName = newName; }
    public void setNewGenre(String newGenre) { this.newGenre = newGenre; }
    public void setTrackNumber(Integer trackNumber) { this.trackNumber = trackNumber; }
    public void setYear(Integer year) { this.year = year; }
    public void setArtistChanged(boolean artistChanged) { this.artistChanged = artistChanged; }
    public void setAlbumArtistChanged(boolean albumArtistChanged) { this.albumArtistChanged = albumArtistChanged; }
    public void setAlbumChanged(boolean albumChanged) { this.albumChanged = albumChanged; }
    public void setNameChanged(boolean nameChanged) { this.nameChanged = nameChanged; }
    public void setLengthChanged(boolean lengthChanged) { this.lengthChanged = lengthChanged; }
    public void setGenreChanged(boolean genreChanged) { this.genreChanged = genreChanged; }
    public void setFoundInDatabase(boolean foundInDatabase) { this.foundInDatabase = foundInDatabase; }
    public void setDatabaseSongId(Long databaseSongId) { this.databaseSongId = databaseSongId; }
    public void setOldTotalTime(Integer oldTotalTime) { this.oldTotalTime = oldTotalTime; }
    public void setNewTotalTime(Integer newTotalTime) { this.newTotalTime = newTotalTime; }
}
