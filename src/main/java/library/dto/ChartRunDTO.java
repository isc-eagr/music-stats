package library.dto;

import java.util.List;

/**
 * DTO for displaying a song's chart run history.
 * Shows position for each week and aggregate stats.
 */
public class ChartRunDTO {
    
    private Integer songId;
    private String songName;
    private String artistName;
    
    // List of positions per week ("x" for absent weeks, numbers for charting weeks)
    // Ordered from oldest to newest
    private List<ChartRunWeek> weeks;
    
    // Aggregate stats
    private Integer weeksAtTop1;
    private Integer weeksAtTop5;
    private Integer weeksAtTop10;
    private Integer weeksAtTop20;
    private Integer totalWeeksOnChart;
    private Integer peakPosition;
    
    public Integer getSongId() {
        return songId;
    }
    
    public void setSongId(Integer songId) {
        this.songId = songId;
    }
    
    public String getSongName() {
        return songName;
    }
    
    public void setSongName(String songName) {
        this.songName = songName;
    }
    
    public String getArtistName() {
        return artistName;
    }
    
    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }
    
    public List<ChartRunWeek> getWeeks() {
        return weeks;
    }
    
    public void setWeeks(List<ChartRunWeek> weeks) {
        this.weeks = weeks;
    }
    
    public Integer getWeeksAtTop1() {
        return weeksAtTop1;
    }
    
    public void setWeeksAtTop1(Integer weeksAtTop1) {
        this.weeksAtTop1 = weeksAtTop1;
    }
    
    public Integer getWeeksAtTop5() {
        return weeksAtTop5;
    }
    
    public void setWeeksAtTop5(Integer weeksAtTop5) {
        this.weeksAtTop5 = weeksAtTop5;
    }
    
    public Integer getWeeksAtTop10() {
        return weeksAtTop10;
    }
    
    public void setWeeksAtTop10(Integer weeksAtTop10) {
        this.weeksAtTop10 = weeksAtTop10;
    }
    
    public Integer getWeeksAtTop20() {
        return weeksAtTop20;
    }
    
    public void setWeeksAtTop20(Integer weeksAtTop20) {
        this.weeksAtTop20 = weeksAtTop20;
    }
    
    public Integer getTotalWeeksOnChart() {
        return totalWeeksOnChart;
    }
    
    public void setTotalWeeksOnChart(Integer totalWeeksOnChart) {
        this.totalWeeksOnChart = totalWeeksOnChart;
    }
    
    public Integer getPeakPosition() {
        return peakPosition;
    }
    
    public void setPeakPosition(Integer peakPosition) {
        this.peakPosition = peakPosition;
    }
    
    /**
     * Represents a single week in the chart run.
     */
    public static class ChartRunWeek {
        private String periodKey;
        private String display;    // Position number or "x" for absent
        private boolean onChart;
        private Integer position;  // null if not on chart
        private boolean isCurrent; // true if this is the current week being viewed
        private String dateRange;  // Human-readable date range, e.g., "Nov 6 - Nov 12, 2025"
        
        public ChartRunWeek() {}
        
        public ChartRunWeek(String periodKey, Integer position, boolean isCurrent) {
            this.periodKey = periodKey;
            this.position = position;
            this.onChart = position != null;
            this.display = position != null ? String.valueOf(position) : "x";
            this.isCurrent = isCurrent;
        }
        
        public ChartRunWeek(String periodKey, Integer position, boolean isCurrent, String dateRange) {
            this(periodKey, position, isCurrent);
            this.dateRange = dateRange;
        }
        
        public String getPeriodKey() {
            return periodKey;
        }
        
        public void setPeriodKey(String periodKey) {
            this.periodKey = periodKey;
        }
        
        public String getDisplay() {
            return display;
        }
        
        public void setDisplay(String display) {
            this.display = display;
        }
        
        public boolean isOnChart() {
            return onChart;
        }
        
        public void setOnChart(boolean onChart) {
            this.onChart = onChart;
        }
        
        public Integer getPosition() {
            return position;
        }
        
        public void setPosition(Integer position) {
            this.position = position;
        }
        
        public boolean isCurrent() {
            return isCurrent;
        }
        
        public void setCurrent(boolean current) {
            isCurrent = current;
        }
        
        public String getDateRange() {
            return dateRange;
        }
        
        public void setDateRange(String dateRange) {
            this.dateRange = dateRange;
        }
    }
}
