package library.dto;

public class SubGenreCardDTO {
    private Integer id;
    private String name;
    private Integer parentGenreId;
    private String parentGenreName;
    private boolean hasImage;
    private Integer playCount;
    private Integer vatitoPlayCount;
    private Integer robertloverPlayCount;
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
    private Integer otherCount;
    private Integer otherArtistCount;
    private Integer otherAlbumCount;
    private Integer otherPlayCount;
    private Long otherTimeListened;

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

    public Integer getOtherCount() {
        return otherCount;
    }

    public void setOtherCount(Integer otherCount) {
        this.otherCount = otherCount;
    }

    public Integer getOtherArtistCount() {
        return otherArtistCount;
    }

    public void setOtherArtistCount(Integer otherArtistCount) {
        this.otherArtistCount = otherArtistCount;
    }

    public Integer getOtherAlbumCount() {
        return otherAlbumCount;
    }

    public void setOtherAlbumCount(Integer otherAlbumCount) {
        this.otherAlbumCount = otherAlbumCount;
    }

    public Integer getOtherPlayCount() {
        return otherPlayCount;
    }

    public void setOtherPlayCount(Integer otherPlayCount) {
        this.otherPlayCount = otherPlayCount;
    }

    public Long getOtherTimeListened() {
        return otherTimeListened;
    }

    public void setOtherTimeListened(Long otherTimeListened) {
        this.otherTimeListened = otherTimeListened;
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

    // Computed male percentages (for sorting and display)
    // Computed male percentages (for sorting and display)
    // These include "other" in the denominator to match UI display
    public Double getMaleArtistPercentage() {
        int total = (maleArtistCount != null ? maleArtistCount : 0) +
                    (femaleArtistCount != null ? femaleArtistCount : 0) +
                    (otherArtistCount != null ? otherArtistCount : 0);
        return total > 0 ? (maleArtistCount != null ? maleArtistCount : 0) * 100.0 / total : null;
    }

    public Double getMaleAlbumPercentage() {
        int total = (maleAlbumCount != null ? maleAlbumCount : 0) +
                    (femaleAlbumCount != null ? femaleAlbumCount : 0) +
                    (otherAlbumCount != null ? otherAlbumCount : 0);
        return total > 0 ? (maleAlbumCount != null ? maleAlbumCount : 0) * 100.0 / total : null;
    }

    public Double getMaleSongPercentage() {
        int total = (maleCount != null ? maleCount : 0) +
                    (femaleCount != null ? femaleCount : 0) +
                    (otherCount != null ? otherCount : 0);
        return total > 0 ? (maleCount != null ? maleCount : 0) * 100.0 / total : null;
    }

    public Double getMalePlayPercentage() {
        int total = (malePlayCount != null ? malePlayCount : 0) +
                    (femalePlayCount != null ? femalePlayCount : 0) +
                    (otherPlayCount != null ? otherPlayCount : 0);
        return total > 0 ? (malePlayCount != null ? malePlayCount : 0) * 100.0 / total : null;
    }

    public Double getMaleTimePercentage() {
        long total = (maleTimeListened != null ? maleTimeListened : 0L) +
                     (femaleTimeListened != null ? femaleTimeListened : 0L) +
                     (otherTimeListened != null ? otherTimeListened : 0L);
        return total > 0 ? (maleTimeListened != null ? maleTimeListened : 0L) * 100.0 / total : null;
    }
}
