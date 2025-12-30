package library.dto;

/**
 * DTO for compact weekly chart statistics used in detail page chips.
 * Contains total weeks on chart, peak position, and weeks at peak.
 */
public class WeeklyChartStatsDTO {

    private Integer totalWeeks;
    private Integer peakPosition;
    private Integer weeksAtPeak;

    public WeeklyChartStatsDTO() {}

    public WeeklyChartStatsDTO(Integer totalWeeks, Integer peakPosition, Integer weeksAtPeak) {
        this.totalWeeks = totalWeeks;
        this.peakPosition = peakPosition;
        this.weeksAtPeak = weeksAtPeak;
    }

    public Integer getTotalWeeks() {
        return totalWeeks;
    }

    public void setTotalWeeks(Integer totalWeeks) {
        this.totalWeeks = totalWeeks;
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

    /**
     * Returns true if the item has charted at least once.
     */
    public boolean hasCharted() {
        return totalWeeks != null && totalWeeks > 0;
    }
}

