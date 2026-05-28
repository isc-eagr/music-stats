package library.dto;

public class LinkedSongDTO {
    private Integer id;
    private String name;
    private String artistName;
    private Integer artistId;
    private String albumName;
    private Integer albumId;
    private Integer lengthSeconds;
    private String lengthFormatted;
    private Integer playCount;
    private Boolean cleanest;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public Integer getArtistId() {
        return artistId;
    }

    public void setArtistId(Integer artistId) {
        this.artistId = artistId;
    }

    public String getAlbumName() {
        return albumName;
    }

    public void setAlbumName(String albumName) {
        this.albumName = albumName;
    }

    public Integer getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }

    public Integer getLengthSeconds() {
        return lengthSeconds;
    }

    public void setLengthSeconds(Integer lengthSeconds) {
        this.lengthSeconds = lengthSeconds;
    }

    public String getLengthFormatted() {
        return lengthFormatted;
    }

    public void setLengthFormatted(String lengthFormatted) {
        this.lengthFormatted = lengthFormatted;
    }

    public Integer getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
    }

    public Boolean getCleanest() {
        return cleanest;
    }

    public void setCleanest(Boolean cleanest) {
        this.cleanest = cleanest;
    }
}
