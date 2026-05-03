package library.dto;

public class BillboardHot100OverviewRowDTO {

    private boolean matched;
    private Integer songId;
    private Integer resolvedArtistId;
    private String songTitle;
    private String artistName;
    private String firstWeek;
    private Integer debutPosition;
    private String lastWeek;
    private String peakWeek;
    private int weeksOnChart;
    private int peakPosition;
    private int weeksAtPeak;
    private String genderClass;
    private int weeksAtTop1;
    private int weeksAtTop5;
    private int weeksAtTop10;
    private int weeksAtTop20;
    private int weeksAtTop50;
    private int weeksAtTop100;

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

    public Integer getDebutPosition() {
        return debutPosition;
    }

    public void setDebutPosition(Integer debutPosition) {
        this.debutPosition = debutPosition;
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

    public int getWeeksAtTop1() { return weeksAtTop1; }
    public void setWeeksAtTop1(int weeksAtTop1) { this.weeksAtTop1 = weeksAtTop1; }

    public int getWeeksAtTop5() { return weeksAtTop5; }
    public void setWeeksAtTop5(int weeksAtTop5) { this.weeksAtTop5 = weeksAtTop5; }

    public int getWeeksAtTop10() { return weeksAtTop10; }
    public void setWeeksAtTop10(int weeksAtTop10) { this.weeksAtTop10 = weeksAtTop10; }

    public int getWeeksAtTop20() { return weeksAtTop20; }
    public void setWeeksAtTop20(int weeksAtTop20) { this.weeksAtTop20 = weeksAtTop20; }

    public int getWeeksAtTop50() { return weeksAtTop50; }
    public void setWeeksAtTop50(int weeksAtTop50) { this.weeksAtTop50 = weeksAtTop50; }

    public int getWeeksAtTop100() { return weeksAtTop100; }
    public void setWeeksAtTop100(int weeksAtTop100) { this.weeksAtTop100 = weeksAtTop100; }
}