package library.dto;

/**
 * DTO for displaying a "run" at #1 on the weekly chart.
 * Each run represents consecutive (or non-consecutive) weeks an item spent at #1.
 * Used on the Weekly Number Ones page.
 */
public class NumberOneRunDTO {

    private Integer id;           // song_id or album_id
    private String name;          // song or album name
    private String artistName;
    private Integer artistId;
    private Integer genderId;
    private boolean hasImage;
    private String type;          // "song" or "album"
    private Integer albumId;      // for songs: the album id
    private boolean albumHasImage;

    private int runWeeks;         // number of consecutive weeks in this run
    private String runStartDate;  // formatted start date of the run (dd/MM/yyyy)
    private String runEndDate;    // formatted end date of the run (dd/MM/yyyy)
    private int cumulativeWeeks;  // total weeks at #1 up to and including this run
    private int totalWeeks;       // grand total weeks at #1 across ALL runs (ever)

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

    public Integer getGenderId() {
        return genderId;
    }

    public void setGenderId(Integer genderId) {
        this.genderId = genderId;
    }

    public boolean isHasImage() {
        return hasImage;
    }

    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }

    public boolean isAlbumHasImage() {
        return albumHasImage;
    }

    public void setAlbumHasImage(boolean albumHasImage) {
        this.albumHasImage = albumHasImage;
    }

    public int getRunWeeks() {
        return runWeeks;
    }

    public void setRunWeeks(int runWeeks) {
        this.runWeeks = runWeeks;
    }

    public String getRunStartDate() {
        return runStartDate;
    }

    public void setRunStartDate(String runStartDate) {
        this.runStartDate = runStartDate;
    }

    public String getRunEndDate() {
        return runEndDate;
    }

    public void setRunEndDate(String runEndDate) {
        this.runEndDate = runEndDate;
    }

    public int getCumulativeWeeks() {
        return cumulativeWeeks;
    }

    public void setCumulativeWeeks(int cumulativeWeeks) {
        this.cumulativeWeeks = cumulativeWeeks;
    }

    public int getTotalWeeks() {
        return totalWeeks;
    }

    public void setTotalWeeks(int totalWeeks) {
        this.totalWeeks = totalWeeks;
    }
}
