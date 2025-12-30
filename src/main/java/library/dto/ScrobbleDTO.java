package library.dto;

public class ScrobbleDTO {
    private Integer id;
    private String scrobbleDate;
    private String songName;
    private Integer songId;
    private String albumName;
    private Integer albumId;
    private String artistName;
    private Integer artistId;
    private String account;
    private boolean fromGroup;
    private boolean fromFeatured;

    // Getters and Setters
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getScrobbleDate() {
        return scrobbleDate;
    }

    public void setScrobbleDate(String scrobbleDate) {
        this.scrobbleDate = scrobbleDate;
    }

    public String getSongName() {
        return songName;
    }

    public void setSongName(String songName) {
        this.songName = songName;
    }

    public Integer getSongId() {
        return songId;
    }

    public void setSongId(Integer songId) {
        this.songId = songId;
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

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public boolean isFromGroup() {
        return fromGroup;
    }

    public void setFromGroup(boolean fromGroup) {
        this.fromGroup = fromGroup;
    }

    public boolean isFromFeatured() {
        return fromFeatured;
    }

    public void setFromFeatured(boolean fromFeatured) {
        this.fromFeatured = fromFeatured;
    }
}
