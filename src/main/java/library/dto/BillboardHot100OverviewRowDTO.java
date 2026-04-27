package library.dto;

public class BillboardHot100OverviewRowDTO {

    private boolean matched;
    private Integer songId;
    private Integer resolvedArtistId;
    private String songTitle;
    private String artistName;
    private String firstWeek;
    private String lastWeek;
    private String peakWeek;
    private int weeksOnChart;
    private int peakPosition;
    private int weeksAtPeak;
    private String genderClass;

    public boolean isMatched() {
        return matched;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

    public Integer getSongId() {
        return songId;
    }

    public void setSongId(Integer songId) {
        this.songId = songId;
    }

    public Integer getResolvedArtistId() {
        return resolvedArtistId;
    }

    public void setResolvedArtistId(Integer resolvedArtistId) {
        this.resolvedArtistId = resolvedArtistId;
    }

    public String getSongTitle() {
        return songTitle;
    }

    public void setSongTitle(String songTitle) {
        this.songTitle = songTitle;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public String getFirstWeek() {
        return firstWeek;
    }

    public void setFirstWeek(String firstWeek) {
        this.firstWeek = firstWeek;
    }

    public String getLastWeek() {
        return lastWeek;
    }

    public void setLastWeek(String lastWeek) {
        this.lastWeek = lastWeek;
    }

    public String getPeakWeek() {
        return peakWeek;
    }

    public void setPeakWeek(String peakWeek) {
        this.peakWeek = peakWeek;
    }

    public int getWeeksOnChart() {
        return weeksOnChart;
    }

    public void setWeeksOnChart(int weeksOnChart) {
        this.weeksOnChart = weeksOnChart;
    }

    public int getPeakPosition() {
        return peakPosition;
    }

    public void setPeakPosition(int peakPosition) {
        this.peakPosition = peakPosition;
    }

    public int getWeeksAtPeak() {
        return weeksAtPeak;
    }

    public void setWeeksAtPeak(int weeksAtPeak) {
        this.weeksAtPeak = weeksAtPeak;
    }

    public String getGenderClass() {
        return genderClass;
    }

    public void setGenderClass(String genderClass) {
        this.genderClass = genderClass;
    }
}