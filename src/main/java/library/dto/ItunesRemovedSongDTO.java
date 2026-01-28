package library.dto;

/**
 * DTO representing an iTunes song that was removed (in snapshot but not in current XML).
 */
public class ItunesRemovedSongDTO {

    private String persistentId;
    private String artist;
    private String album;
    private String name;
    private Integer trackNumber;
    private Integer year;
    private boolean foundInDatabase;
    private Long databaseSongId;

    public ItunesRemovedSongDTO() {
    }

    public ItunesRemovedSongDTO(String persistentId, String artist, String album, String name,
                                 Integer trackNumber, Integer year, boolean foundInDatabase, Long databaseSongId) {
        this.persistentId = persistentId;
        this.artist = artist;
        this.album = album;
        this.name = name;
        this.trackNumber = trackNumber;
        this.year = year;
        this.foundInDatabase = foundInDatabase;
        this.databaseSongId = databaseSongId;
    }

    // Getters
    public String getPersistentId() { return persistentId; }
    public String getArtist() { return artist; }
    public String getAlbum() { return album; }
    public String getName() { return name; }
    public Integer getTrackNumber() { return trackNumber; }
    public Integer getYear() { return year; }
    public boolean isFoundInDatabase() { return foundInDatabase; }
    public Long getDatabaseSongId() { return databaseSongId; }

    // Setters
    public void setPersistentId(String persistentId) { this.persistentId = persistentId; }
    public void setArtist(String artist) { this.artist = artist; }
    public void setAlbum(String album) { this.album = album; }
    public void setName(String name) { this.name = name; }
    public void setTrackNumber(Integer trackNumber) { this.trackNumber = trackNumber; }
    public void setYear(Integer year) { this.year = year; }
    public void setFoundInDatabase(boolean foundInDatabase) { this.foundInDatabase = foundInDatabase; }
    public void setDatabaseSongId(Long databaseSongId) { this.databaseSongId = databaseSongId; }
}
