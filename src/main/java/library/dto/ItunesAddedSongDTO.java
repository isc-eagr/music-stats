package library.dto;

/**
 * DTO representing an iTunes song that was added (new in the XML, not in snapshot).
 */
public class ItunesAddedSongDTO {

    private String persistentId;
    private String artist;
    private String album;
    private String name;
    private Integer trackNumber;
    private Integer year;
    private String genre;
    
    // Whether this song already exists in the Music Stats database
    private boolean foundInDatabase;
    private Long databaseSongId;

    public ItunesAddedSongDTO() {
    }

    public ItunesAddedSongDTO(String persistentId, String artist, String album, String name,
                               Integer trackNumber, Integer year, String genre,
                               boolean foundInDatabase, Long databaseSongId) {
        this.persistentId = persistentId;
        this.artist = artist;
        this.album = album;
        this.name = name;
        this.trackNumber = trackNumber;
        this.year = year;
        this.genre = genre;
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
    public String getGenre() { return genre; }
    public boolean isFoundInDatabase() { return foundInDatabase; }
    public Long getDatabaseSongId() { return databaseSongId; }

    // Setters
    public void setPersistentId(String persistentId) { this.persistentId = persistentId; }
    public void setArtist(String artist) { this.artist = artist; }
    public void setAlbum(String album) { this.album = album; }
    public void setName(String name) { this.name = name; }
    public void setTrackNumber(Integer trackNumber) { this.trackNumber = trackNumber; }
    public void setYear(Integer year) { this.year = year; }
    public void setGenre(String genre) { this.genre = genre; }
    public void setFoundInDatabase(boolean foundInDatabase) { this.foundInDatabase = foundInDatabase; }
    public void setDatabaseSongId(Long databaseSongId) { this.databaseSongId = databaseSongId; }
}
