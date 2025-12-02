package library.dto;

/**
 * DTO for displaying chart history entries (songs/albums that have charted).
 * Used in artist, album, and song detail pages.
 */
public class ChartHistoryDTO {
    
    private Integer id;           // song_id or album_id
    private String name;          // song or album name
    private String artistName;    // for songs in artist view, or album artist
    private Integer peakPosition;
    private Integer weeksAtPeak;
    private Integer totalWeeks;
    private String chartType;     // "song" or "album"
    private String releaseDate;   // Release date of the song/album (formatted)
    private String peakDate;      // First date the song/album reached its peak position (formatted)
    private String debutDate;     // First date the song/album appeared on the chart (formatted)
    private String peakWeek;      // Period key for peak date (e.g., "2024-W48") for linking
    private String debutWeek;     // Period key for debut date (e.g., "2024-W48") for linking
    
    public ChartHistoryDTO() {}
    
    public ChartHistoryDTO(Integer id, String name, String artistName, Integer peakPosition, Integer weeksAtPeak, Integer totalWeeks, String chartType) {
        this.id = id;
        this.name = name;
        this.artistName = artistName;
        this.peakPosition = peakPosition;
        this.weeksAtPeak = weeksAtPeak;
        this.totalWeeks = totalWeeks;
        this.chartType = chartType;
    }
    
    public ChartHistoryDTO(Integer id, String name, String artistName, Integer peakPosition, Integer weeksAtPeak, Integer totalWeeks, String chartType, String releaseDate, String peakDate, String debutDate, String peakWeek, String debutWeek) {
        this.id = id;
        this.name = name;
        this.artistName = artistName;
        this.peakPosition = peakPosition;
        this.weeksAtPeak = weeksAtPeak;
        this.totalWeeks = totalWeeks;
        this.chartType = chartType;
        this.releaseDate = releaseDate;
        this.peakDate = peakDate;
        this.debutDate = debutDate;
        this.peakWeek = peakWeek;
        this.debutWeek = debutWeek;
    }

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

    public Integer getPeakPosition() {
        return peakPosition;
    }

    public void setPeakPosition(Integer peakPosition) {
        this.peakPosition = peakPosition;
    }

    public Integer getWeeksAtPeak() {
        return weeksAtPeak;
    }

    public void setWeeksAtPeak(Integer weeksAtPeak) {
        this.weeksAtPeak = weeksAtPeak;
    }

    public Integer getTotalWeeks() {
        return totalWeeks;
    }

    public void setTotalWeeks(Integer totalWeeks) {
        this.totalWeeks = totalWeeks;
    }

    public String getChartType() {
        return chartType;
    }

    public void setChartType(String chartType) {
        this.chartType = chartType;
    }
    
    public String getReleaseDate() {
        return releaseDate;
    }
    
    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
    }
    
    public String getPeakDate() {
        return peakDate;
    }
    
    public void setPeakDate(String peakDate) {
        this.peakDate = peakDate;
    }
    
    public String getDebutDate() {
        return debutDate;
    }
    
    public void setDebutDate(String debutDate) {
        this.debutDate = debutDate;
    }
    
    public String getPeakWeek() {
        return peakWeek;
    }
    
    public void setPeakWeek(String peakWeek) {
        this.peakWeek = peakWeek;
    }
    
    public String getDebutWeek() {
        return debutWeek;
    }
    
    public void setDebutWeek(String debutWeek) {
        this.debutWeek = debutWeek;
    }
    
    /**
     * Check if this entry peaked at #1.
     */
    public boolean isPeakNumberOne() {
        return peakPosition != null && peakPosition == 1;
    }
}
