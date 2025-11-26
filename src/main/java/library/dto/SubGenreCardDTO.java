package library.dto;

public class SubGenreCardDTO {
    private Integer id;
    private String name;
    private Integer parentGenreId;
    private String parentGenreName;
    private boolean hasImage;
    private Integer playCount;
    private Long timeListened;
    private String timeListenedFormatted;
    private Integer artistCount;
    private Integer albumCount;
    private Integer songCount;
    private Integer maleCount;
    private Integer femaleCount;
    private Integer maleArtistCount;
    private Integer femaleArtistCount;
    private Integer maleAlbumCount;
    private Integer femaleAlbumCount;
    private Integer malePlayCount;
    private Integer femalePlayCount;
    private Long maleTimeListened;
    private Long femaleTimeListened;

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

    public Integer getParentGenreId() {
        return parentGenreId;
    }

    public void setParentGenreId(Integer parentGenreId) {
        this.parentGenreId = parentGenreId;
    }

    public String getParentGenreName() {
        return parentGenreName;
    }

    public void setParentGenreName(String parentGenreName) {
        this.parentGenreName = parentGenreName;
    }

    public boolean isHasImage() {
        return hasImage;
    }

    public boolean getHasImage() {
        return hasImage;
    }

    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }

    public Integer getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
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

    public Integer getMaleCount() {
        return maleCount;
    }

    public void setMaleCount(Integer maleCount) {
        this.maleCount = maleCount;
    }

    public Integer getFemaleCount() {
        return femaleCount;
    }

    public void setFemaleCount(Integer femaleCount) {
        this.femaleCount = femaleCount;
    }

    public Integer getMaleArtistCount() {
        return maleArtistCount;
    }

    public void setMaleArtistCount(Integer maleArtistCount) {
        this.maleArtistCount = maleArtistCount;
    }

    public Integer getFemaleArtistCount() {
        return femaleArtistCount;
    }

    public void setFemaleArtistCount(Integer femaleArtistCount) {
        this.femaleArtistCount = femaleArtistCount;
    }

    public Integer getMaleAlbumCount() {
        return maleAlbumCount;
    }

    public void setMaleAlbumCount(Integer maleAlbumCount) {
        this.maleAlbumCount = maleAlbumCount;
    }

    public Integer getFemaleAlbumCount() {
        return femaleAlbumCount;
    }

    public void setFemaleAlbumCount(Integer femaleAlbumCount) {
        this.femaleAlbumCount = femaleAlbumCount;
    }

    public Integer getMalePlayCount() {
        return malePlayCount;
    }

    public void setMalePlayCount(Integer malePlayCount) {
        this.malePlayCount = malePlayCount;
    }

    public Integer getFemalePlayCount() {
        return femalePlayCount;
    }

    public void setFemalePlayCount(Integer femalePlayCount) {
        this.femalePlayCount = femalePlayCount;
    }

    public Long getMaleTimeListened() {
        return maleTimeListened;
    }

    public void setMaleTimeListened(Long maleTimeListened) {
        this.maleTimeListened = maleTimeListened;
    }

    public Long getFemaleTimeListened() {
        return femaleTimeListened;
    }

    public void setFemaleTimeListened(Long femaleTimeListened) {
        this.femaleTimeListened = femaleTimeListened;
    }
}
