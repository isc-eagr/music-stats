package library.dto;

public class GenderCardDTO {
    private Integer id;
    private String name;
    private Integer playCount;
    private Integer vatitoPlayCount;
    private Integer robertloverPlayCount;
    private Long timeListened;
    private String timeListenedFormatted;
    private Integer artistCount;
    private Integer albumCount;
    private Integer songCount;

    // Getters and Setters
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

    public Integer getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
    }

    public Integer getVatitoPlayCount() {
        return vatitoPlayCount;
    }

    public void setVatitoPlayCount(Integer vatitoPlayCount) {
        this.vatitoPlayCount = vatitoPlayCount;
    }

    public Integer getRobertloverPlayCount() {
        return robertloverPlayCount;
    }

    public void setRobertloverPlayCount(Integer robertloverPlayCount) {
        this.robertloverPlayCount = robertloverPlayCount;
    }

    public Long getTimeListened() {
        return timeListened;
    }

    public void setTimeListened(Long timeListened) {
        this.timeListened = timeListened;
    }

    public String getTimeListenedFormatted() {
        return timeListenedFormatted;
    }

    public void setTimeListenedFormatted(String timeListenedFormatted) {
        this.timeListenedFormatted = timeListenedFormatted;
    }

    public Integer getArtistCount() {
        return artistCount;
    }

    public void setArtistCount(Integer artistCount) {
        this.artistCount = artistCount;
    }

    public Integer getAlbumCount() {
        return albumCount;
    }

    public void setAlbumCount(Integer albumCount) {
        this.albumCount = albumCount;
    }

    public Integer getSongCount() {
        return songCount;
    }

    public void setSongCount(Integer songCount) {
        this.songCount = songCount;
    }

    // Top/winning artist, album, song for this category
    private Integer topArtistId;
    private String topArtistName;
    private Integer topArtistGenderId;
    private Integer topAlbumId;
    private String topAlbumName;
    private String topAlbumArtistName;
    private Integer topSongId;
    private String topSongName;
    private String topSongArtistName;

    public Integer getTopArtistId() {
        return topArtistId;
    }

    public void setTopArtistId(Integer topArtistId) {
        this.topArtistId = topArtistId;
    }

    public String getTopArtistName() {
        return topArtistName;
    }

    public void setTopArtistName(String topArtistName) {
        this.topArtistName = topArtistName;
    }

    public Integer getTopArtistGenderId() {
        return topArtistGenderId;
    }

    public void setTopArtistGenderId(Integer topArtistGenderId) {
        this.topArtistGenderId = topArtistGenderId;
    }

    public Integer getTopAlbumId() {
        return topAlbumId;
    }

    public void setTopAlbumId(Integer topAlbumId) {
        this.topAlbumId = topAlbumId;
    }

    public String getTopAlbumName() {
        return topAlbumName;
    }

    public void setTopAlbumName(String topAlbumName) {
        this.topAlbumName = topAlbumName;
    }

    public String getTopAlbumArtistName() {
        return topAlbumArtistName;
    }

    public void setTopAlbumArtistName(String topAlbumArtistName) {
        this.topAlbumArtistName = topAlbumArtistName;
    }

    public Integer getTopSongId() {
        return topSongId;
    }

    public void setTopSongId(Integer topSongId) {
        this.topSongId = topSongId;
    }

    public String getTopSongName() {
        return topSongName;
    }

    public void setTopSongName(String topSongName) {
        this.topSongName = topSongName;
    }

    public String getTopSongArtistName() {
        return topSongArtistName;
    }

    public void setTopSongArtistName(String topSongArtistName) {
        this.topSongArtistName = topSongArtistName;
    }
}
