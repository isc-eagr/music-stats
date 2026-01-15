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

    public ItunesRemovedSongDTO() {
    }

    public ItunesRemovedSongDTO(String persistentId, String artist, String album, String name,
                                 Integer trackNumber, Integer year) {
        this.persistentId = persistentId;
        this.artist = artist;
        this.album = album;
        this.name = name;
        this.trackNumber = trackNumber;
        this.year = year;
    }

    // Getters
    public String getPersistentId() { return persistentId; }
    public String getArtist() { return artist; }
    public String getAlbum() { return album; }
    public String getName() { return name; }
    public Integer getTrackNumber() { return trackNumber; }
    public Integer getYear() { return year; }

    // Setters
    public void setPersistentId(String persistentId) { this.persistentId = persistentId; }
    public void setArtist(String artist) { this.artist = artist; }
    public void setAlbum(String album) { this.album = album; }
    public void setName(String name) { this.name = name; }
    public void setTrackNumber(Integer trackNumber) { this.trackNumber = trackNumber; }
    public void setYear(Integer year) { this.year = year; }
}
