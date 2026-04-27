package library.dto;

public class PcOverviewRowDTO {

    private boolean matched;
    private Integer songId;
    private Integer resolvedArtistId;
    private String songTitle;
    private String artistName;
    private String firstWeek;
    private String lastWeek;
    private String peakWeek;
    private int daysOnCountdown;
    private int peakPosition;
    private int daysAtPeak;
    private int rawVariantCount;
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

    public int getDaysOnCountdown() {
        return daysOnCountdown;
    }

    public void setDaysOnCountdown(int daysOnCountdown) {
        this.daysOnCountdown = daysOnCountdown;
    }

    public int getPeakPosition() {
        return peakPosition;
    }

    public void setPeakPosition(int peakPosition) {
        this.peakPosition = peakPosition;
    }

    public int getDaysAtPeak() {
        return daysAtPeak;
    }

    public void setDaysAtPeak(int daysAtPeak) {
        this.daysAtPeak = daysAtPeak;
    }

    public int getRawVariantCount() {
        return rawVariantCount;
    }

    public void setRawVariantCount(int rawVariantCount) {
        this.rawVariantCount = rawVariantCount;
    }

    public String getGenderClass() {
        return genderClass;
    }

    public void setGenderClass(String genderClass) {
        this.genderClass = genderClass;
    }
}