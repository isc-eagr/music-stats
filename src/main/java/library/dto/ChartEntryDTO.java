package library.dto;

/**
 * DTO for displaying a chart entry with calculated statistics.
 * Used in the weekly charts table display.
 */
public class ChartEntryDTO {
    
    // Core data from ChartEntry
    private Integer position;
    private Integer songId;
    private Integer albumId;
    private String songName;
    private String artistName;
    private Integer artistId;
    private Integer playCount;
    private boolean hasImage;
    
    // Calculated stats from comparing with previous charts
    private Integer lastWeekPosition;      // null = new entry, -1 = re-entry
    private Integer lastWeekPlayCount;
    private Integer peakPosition;
    private Integer timesAtPeak;
    private Integer weeksOnChart;
    
    // Getters and setters
    public Integer getPosition() {
        return position;
    }
    
    public void setPosition(Integer position) {
        this.position = position;
    }
    
    public Integer getSongId() {
        return songId;
    }
    
    public void setSongId(Integer songId) {
        this.songId = songId;
    }
    
    public Integer getAlbumId() {
        return albumId;
    }
    
    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }
    
    public String getSongName() {
        return songName;
    }
    
    public void setSongName(String songName) {
        this.songName = songName;
    }
    
    // For album charts
    private String albumName;
    
    public String getAlbumName() {
        return albumName;
    }
    
    public void setAlbumName(String albumName) {
        this.albumName = albumName;
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
    
    public Integer getPlayCount() {
        return playCount;
    }
    
    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
    }
    
    public boolean isHasImage() {
        return hasImage;
    }
    
    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }
    
    public Integer getLastWeekPosition() {
        return lastWeekPosition;
    }
    
    public void setLastWeekPosition(Integer lastWeekPosition) {
        this.lastWeekPosition = lastWeekPosition;
    }
    
    public Integer getLastWeekPlayCount() {
        return lastWeekPlayCount;
    }
    
    public void setLastWeekPlayCount(Integer lastWeekPlayCount) {
        this.lastWeekPlayCount = lastWeekPlayCount;
    }
    
    public Integer getPeakPosition() {
        return peakPosition;
    }
    
    public void setPeakPosition(Integer peakPosition) {
        this.peakPosition = peakPosition;
    }
    
    public Integer getTimesAtPeak() {
        return timesAtPeak;
    }
    
    public void setTimesAtPeak(Integer timesAtPeak) {
        this.timesAtPeak = timesAtPeak;
    }
    
    public Integer getWeeksOnChart() {
        return weeksOnChart;
    }
    
    public void setWeeksOnChart(Integer weeksOnChart) {
        this.weeksOnChart = weeksOnChart;
    }
    
    // Computed properties
    
    /**
     * Returns true if this is a new entry (first time on chart).
     */
    public boolean isNewEntry() {
        return lastWeekPosition == null && weeksOnChart != null && weeksOnChart == 1;
    }
    
    /**
     * Returns true if this is a re-entry (was on chart before, dropped off, now back).
     */
    public boolean isReEntry() {
        return lastWeekPosition != null && lastWeekPosition == -1;
    }
    
    /**
     * Returns the position change from last week.
     * Positive = moved up, Negative = moved down, 0 = same position.
     * Returns null if new entry or re-entry.
     */
    public Integer getPositionChange() {
        if (lastWeekPosition == null || lastWeekPosition < 0) {
            return null;
        }
        return lastWeekPosition - position; // Higher last week position = moved up
    }
    
    /**
     * Returns formatted last week display string.
     * "NEW" for new entries, "RE" for re-entries, or the position number.
     */
    public String getLastWeekDisplay() {
        if (isNewEntry()) {
            return "NEW";
        } else if (isReEntry()) {
            return "RE";
        } else if (lastWeekPosition != null) {
            return String.valueOf(lastWeekPosition);
        }
        return "-";
    }
    
    /**
     * Returns CSS class for position change indicator.
     */
    public String getPositionChangeClass() {
        Integer change = getPositionChange();
        if (change == null) {
            if (isNewEntry()) return "new-entry";
            if (isReEntry()) return "re-entry";
            return "";
        }
        if (change > 0) return "position-up";
        if (change < 0) return "position-down";
        return "position-same";
    }
}
