package library.dto;

public class TimeframeCardDTO {
    // Period identification
    private String periodKey;           // Sortable key: "2024", "2024-01", "2024-W12", "2024-Winter"
    private String periodDisplayName;   // Display name: "2024", "January 2024", "Week 12, 2024", "Winter 2024"
    private String periodType;          // Type: days, weeks, months, seasons, years, decades
    private String listenedDateFrom;    // Start date for filtering (YYYY-MM-DD)
    private String listenedDateTo;      // End date for filtering (YYYY-MM-DD)
    
    // Core stats (same as GenreCardDTO)
    private Integer playCount;
    private Long timeListened;
    private String timeListenedFormatted;
    private Integer artistCount;
    private Integer albumCount;
    private Integer songCount;
    
    // Gender breakdown for songs (maleCount = male songs, femaleCount = female songs)
    private Integer maleCount;
    private Integer femaleCount;
    
    // Gender breakdown for artists
    private Integer maleArtistCount;
    private Integer femaleArtistCount;
    
    // Gender breakdown for albums
    private Integer maleAlbumCount;
    private Integer femaleAlbumCount;
    
    // Gender breakdown for plays
    private Integer malePlayCount;
    private Integer femalePlayCount;
    
    // Gender breakdown for listening time
    private Long maleTimeListened;
    private Long femaleTimeListened;
    
    // Other gender breakdown
    private Integer otherCount;
    private Integer otherArtistCount;
    private Integer otherAlbumCount;
    private Integer otherPlayCount;
    private Long otherTimeListened;
    
    // Winning attributes (the one with most plays in this period)
    private Integer winningGenderId;
    private String winningGenderName;
    private Integer winningGenreId;
    private String winningGenreName;
    private Integer winningEthnicityId;
    private String winningEthnicityName;
    private Integer winningLanguageId;
    private String winningLanguageName;
    private String winningCountry;
    
    // Computed male percentages (for filtering and display)
    public Double getMaleArtistPercentage() {
        int total = (maleArtistCount != null ? maleArtistCount : 0) + (femaleArtistCount != null ? femaleArtistCount : 0);
        return total > 0 ? (maleArtistCount != null ? maleArtistCount : 0) * 100.0 / total : null;
    }
    
    public Double getMaleAlbumPercentage() {
        int total = (maleAlbumCount != null ? maleAlbumCount : 0) + (femaleAlbumCount != null ? femaleAlbumCount : 0);
        return total > 0 ? (maleAlbumCount != null ? maleAlbumCount : 0) * 100.0 / total : null;
    }
    
    public Double getMaleSongPercentage() {
        int total = (maleCount != null ? maleCount : 0) + (femaleCount != null ? femaleCount : 0);
        return total > 0 ? (maleCount != null ? maleCount : 0) * 100.0 / total : null;
    }
    
    public Double getMalePlayPercentage() {
        int total = (malePlayCount != null ? malePlayCount : 0) + (femalePlayCount != null ? femalePlayCount : 0);
        return total > 0 ? (malePlayCount != null ? malePlayCount : 0) * 100.0 / total : null;
    }
    
    public Double getMaleTimePercentage() {
        long total = (maleTimeListened != null ? maleTimeListened : 0L) + (femaleTimeListened != null ? femaleTimeListened : 0L);
        return total > 0 ? (maleTimeListened != null ? maleTimeListened : 0L) * 100.0 / total : null;
    }

    // Getters and Setters
    public String getPeriodKey() {
        return periodKey;
    }

    public void setPeriodKey(String periodKey) {
        this.periodKey = periodKey;
    }

    public String getPeriodDisplayName() {
        return periodDisplayName;
    }

    public void setPeriodDisplayName(String periodDisplayName) {
        this.periodDisplayName = periodDisplayName;
    }

    public String getPeriodType() {
        return periodType;
    }

    public void setPeriodType(String periodType) {
        this.periodType = periodType;
    }

    public String getListenedDateFrom() {
        return listenedDateFrom;
    }

    public void setListenedDateFrom(String listenedDateFrom) {
        this.listenedDateFrom = listenedDateFrom;
    }

    public String getListenedDateTo() {
        return listenedDateTo;
    }

    public void setListenedDateTo(String listenedDateTo) {
        this.listenedDateTo = listenedDateTo;
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

    public Integer getWinningGenderId() {
        return winningGenderId;
    }

    public void setWinningGenderId(Integer winningGenderId) {
        this.winningGenderId = winningGenderId;
    }

    public String getWinningGenderName() {
        return winningGenderName;
    }

    public void setWinningGenderName(String winningGenderName) {
        this.winningGenderName = winningGenderName;
    }

    public Integer getWinningGenreId() {
        return winningGenreId;
    }

    public void setWinningGenreId(Integer winningGenreId) {
        this.winningGenreId = winningGenreId;
    }

    public String getWinningGenreName() {
        return winningGenreName;
    }

    public void setWinningGenreName(String winningGenreName) {
        this.winningGenreName = winningGenreName;
    }

    public Integer getWinningEthnicityId() {
        return winningEthnicityId;
    }

    public void setWinningEthnicityId(Integer winningEthnicityId) {
        this.winningEthnicityId = winningEthnicityId;
    }

    public String getWinningEthnicityName() {
        return winningEthnicityName;
    }

    public void setWinningEthnicityName(String winningEthnicityName) {
        this.winningEthnicityName = winningEthnicityName;
    }

    public Integer getWinningLanguageId() {
        return winningLanguageId;
    }

    public void setWinningLanguageId(Integer winningLanguageId) {
        this.winningLanguageId = winningLanguageId;
    }

    public String getWinningLanguageName() {
        return winningLanguageName;
    }

    public void setWinningLanguageName(String winningLanguageName) {
        this.winningLanguageName = winningLanguageName;
    }

    public String getWinningCountry() {
        return winningCountry;
    }

    public void setWinningCountry(String winningCountry) {
        this.winningCountry = winningCountry;
    }
}
