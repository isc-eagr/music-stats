package library.dto;

public class AlbumCardDTO {
    private Integer id;
    private String name;
    private String artistName;
    private Integer artistId;
    private Integer genreId;
    private String genreName;
    private Integer subgenreId;
    private String subgenreName;
    private Integer languageId;
    private String languageName;
    private Integer ethnicityId;
    private String ethnicityName;
    private String releaseYear;
    private String releaseDate;
    private String firstListenedDate;
    private String lastListenedDate;
    private Integer songCount;
    private Integer playCount;
    private Integer vatitoPlayCount;
    private Integer robertloverPlayCount;
    private Long timeListened; // in seconds
    private String timeListenedFormatted;
    private boolean hasImage;
    private String genderName;
    private String country;
    private Boolean organized;
    private Long albumLength; // in seconds (sum of song lengths)
    private String albumLengthFormatted;
    private Boolean inItunes; // Whether album exists in iTunes library
    private String birthDate; // Artist birth date
    private String deathDate; // Artist death date
    private Integer imageCount; // Count of primary + gallery images
    private Integer seasonalChartPeak; // Best position on seasonal chart
    private Integer weeklyChartPeak; // Best position on weekly chart
    private Integer weeklyChartWeeks; // Total weeks spent on weekly chart
    private Integer yearlyChartPeak; // Best position on yearly chart
    private String weeklyChartPeakStartDate; // Start date of the week when weekly peak was first reached
    private String seasonalChartPeakPeriod; // Period key when seasonal peak was first reached
    private String yearlyChartPeakPeriod; // Period key when yearly peak was first reached

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

    public String getGenreName() {
        return genreName;
    }

    public void setGenreName(String genreName) {
        this.genreName = genreName;
    }

    public Integer getGenreId() {
        return genreId;
    }

    public void setGenreId(Integer genreId) {
        this.genreId = genreId;
    }

    public String getSubgenreName() {
        return subgenreName;
    }

    public void setSubgenreName(String subgenreName) {
        this.subgenreName = subgenreName;
    }

    public Integer getSubgenreId() {
        return subgenreId;
    }

    public void setSubgenreId(Integer subgenreId) {
        this.subgenreId = subgenreId;
    }

    public String getLanguageName() {
        return languageName;
    }

    public void setLanguageName(String languageName) {
        this.languageName = languageName;
    }

    public Integer getLanguageId() {
        return languageId;
    }

    public void setLanguageId(Integer languageId) {
        this.languageId = languageId;
    }

    public Integer getEthnicityId() {
        return ethnicityId;
    }

    public void setEthnicityId(Integer ethnicityId) {
        this.ethnicityId = ethnicityId;
    }

    public String getEthnicityName() {
        return ethnicityName;
    }

    public void setEthnicityName(String ethnicityName) {
        this.ethnicityName = ethnicityName;
    }

    public String getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(String releaseYear) {
        this.releaseYear = releaseYear;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
    }

    public String getFirstListenedDate() {
        return firstListenedDate;
    }

    public void setFirstListenedDate(String firstListenedDate) {
        this.firstListenedDate = firstListenedDate;
    }

    public String getLastListenedDate() {
        return lastListenedDate;
    }

    public void setLastListenedDate(String lastListenedDate) {
        this.lastListenedDate = lastListenedDate;
    }

    public Integer getSongCount() {
        return songCount;
    }

    public void setSongCount(Integer songCount) {
        this.songCount = songCount;
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

    public boolean isHasImage() {
        return hasImage;
    }

    public boolean getHasImage() {
        return hasImage;
    }

    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }

    public String getGenderName() {
        return genderName;
    }

    public void setGenderName(String genderName) {
        this.genderName = genderName;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Boolean getOrganized() {
        return organized;
    }

    public void setOrganized(Boolean organized) {
        this.organized = organized;
    }

    public Long getAlbumLength() {
        return albumLength;
    }

    public void setAlbumLength(Long albumLength) {
        this.albumLength = albumLength;
    }

    public String getAlbumLengthFormatted() {
        return albumLengthFormatted;
    }

    public void setAlbumLengthFormatted(String albumLengthFormatted) {
        this.albumLengthFormatted = albumLengthFormatted;
    }

    public Boolean getInItunes() {
        return inItunes;
    }

    public void setInItunes(Boolean inItunes) {
        this.inItunes = inItunes;
    }

    public String getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(String birthDate) {
        this.birthDate = birthDate;
    }

    public String getDeathDate() {
        return deathDate;
    }

    public void setDeathDate(String deathDate) {
        this.deathDate = deathDate;
    }

    public Integer getImageCount() {
        return imageCount;
    }

    public void setImageCount(Integer imageCount) {
        this.imageCount = imageCount;
    }

    public Integer getSeasonalChartPeak() {
        return seasonalChartPeak;
    }

    public void setSeasonalChartPeak(Integer seasonalChartPeak) {
        this.seasonalChartPeak = seasonalChartPeak;
    }

    public Integer getWeeklyChartPeak() {
        return weeklyChartPeak;
    }

    public void setWeeklyChartPeak(Integer weeklyChartPeak) {
        this.weeklyChartPeak = weeklyChartPeak;
    }

    public Integer getWeeklyChartWeeks() {
        return weeklyChartWeeks;
    }

    public void setWeeklyChartWeeks(Integer weeklyChartWeeks) {
        this.weeklyChartWeeks = weeklyChartWeeks;
    }

    public Integer getYearlyChartPeak() {
        return yearlyChartPeak;
    }

    public void setYearlyChartPeak(Integer yearlyChartPeak) {
        this.yearlyChartPeak = yearlyChartPeak;
    }

    public String getWeeklyChartPeakStartDate() {
        return weeklyChartPeakStartDate;
    }

    public void setWeeklyChartPeakStartDate(String weeklyChartPeakStartDate) {
        this.weeklyChartPeakStartDate = weeklyChartPeakStartDate;
    }

    public String getSeasonalChartPeakPeriod() {
        return seasonalChartPeakPeriod;
    }

    public void setSeasonalChartPeakPeriod(String seasonalChartPeakPeriod) {
        this.seasonalChartPeakPeriod = seasonalChartPeakPeriod;
    }

    public String getYearlyChartPeakPeriod() {
        return yearlyChartPeakPeriod;
    }

    public void setYearlyChartPeakPeriod(String yearlyChartPeakPeriod) {
        this.yearlyChartPeakPeriod = yearlyChartPeakPeriod;
    }
}
