package library.dto;

public class SongCardDTO {
    private Integer id;
    private String name;
    private String artistName;
    private Integer artistId;
    private String albumName;
    private Integer albumId;
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
    private Integer lengthSeconds;
    private String lengthFormatted;
    private Integer playCount;
    private Integer vatitoPlayCount;
    private Integer robertloverPlayCount;
    private Long timeListened; // in seconds
    private String timeListenedFormatted;
    private boolean hasImage;
    private String genderName;
    private String country;
    private Boolean organized;

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
}
