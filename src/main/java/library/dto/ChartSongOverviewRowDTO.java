package library.dto;

import java.util.List;

public class ChartSongOverviewRowDTO {

    private Integer songId;
    private Integer albumId;
    private Integer artistId;
    private String songTitle;
    private String albumName;
    private String artistName;
    private boolean hasImage;
    private boolean albumHasImage;
    private boolean artistHasImage;
    private String firstAppearanceLabel;
    private String firstAppearanceKey;
    private String firstAppearanceSortValue;
    private String lastAppearanceLabel;
    private String lastAppearanceKey;
    private String lastAppearanceSortValue;
    private String peakAppearanceLabel;
    private String peakAppearanceKey;
    private String peakAppearanceSortValue;
    private Integer debutPosition;
    private int totalChartSpan;
    private int peakPosition;
    private int spanAtPeak;
    private int spanAtTop1;
    private int spanAtTop5;
    private int spanAtTop10;
    private int[] spanAtTopThresholds;
    private String genderClass;
    private List<String> linkedSongTitles;

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

    public Integer getArtistId() {
        return artistId;
    }

    public void setArtistId(Integer artistId) {
        this.artistId = artistId;
    }

    public String getSongTitle() {
        return songTitle;
    }

    public void setSongTitle(String songTitle) {
        this.songTitle = songTitle;
    }

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

    public boolean isHasImage() {
        return hasImage;
    }

    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }

    public boolean isAlbumHasImage() {
        return albumHasImage;
    }

    public void setAlbumHasImage(boolean albumHasImage) {
        this.albumHasImage = albumHasImage;
    }

    public boolean isArtistHasImage() {
        return artistHasImage;
    }

    public void setArtistHasImage(boolean artistHasImage) {
        this.artistHasImage = artistHasImage;
    }

    public String getFirstAppearanceLabel() {
        return firstAppearanceLabel;
    }

    public void setFirstAppearanceLabel(String firstAppearanceLabel) {
        this.firstAppearanceLabel = firstAppearanceLabel;
    }

    public String getFirstAppearanceKey() {
        return firstAppearanceKey;
    }

    public void setFirstAppearanceKey(String firstAppearanceKey) {
        this.firstAppearanceKey = firstAppearanceKey;
    }

    public String getFirstAppearanceSortValue() {
        return firstAppearanceSortValue;
    }

    public void setFirstAppearanceSortValue(String firstAppearanceSortValue) {
        this.firstAppearanceSortValue = firstAppearanceSortValue;
    }

    public String getLastAppearanceLabel() {
        return lastAppearanceLabel;
    }

    public void setLastAppearanceLabel(String lastAppearanceLabel) {
        this.lastAppearanceLabel = lastAppearanceLabel;
    }

    public String getLastAppearanceKey() {
        return lastAppearanceKey;
    }

    public void setLastAppearanceKey(String lastAppearanceKey) {
        this.lastAppearanceKey = lastAppearanceKey;
    }

    public String getLastAppearanceSortValue() {
        return lastAppearanceSortValue;
    }

    public void setLastAppearanceSortValue(String lastAppearanceSortValue) {
        this.lastAppearanceSortValue = lastAppearanceSortValue;
    }

    public String getPeakAppearanceLabel() {
        return peakAppearanceLabel;
    }

    public void setPeakAppearanceLabel(String peakAppearanceLabel) {
        this.peakAppearanceLabel = peakAppearanceLabel;
    }

    public String getPeakAppearanceKey() {
        return peakAppearanceKey;
    }

    public void setPeakAppearanceKey(String peakAppearanceKey) {
        this.peakAppearanceKey = peakAppearanceKey;
    }

    public String getPeakAppearanceSortValue() {
        return peakAppearanceSortValue;
    }

    public void setPeakAppearanceSortValue(String peakAppearanceSortValue) {
        this.peakAppearanceSortValue = peakAppearanceSortValue;
    }

    public Integer getDebutPosition() {
        return debutPosition;
    }

    public void setDebutPosition(Integer debutPosition) {
        this.debutPosition = debutPosition;
    }

    public int getTotalChartSpan() {
        return totalChartSpan;
    }

    public void setTotalChartSpan(int totalChartSpan) {
        this.totalChartSpan = totalChartSpan;
    }

    public int getPeakPosition() {
        return peakPosition;
    }

    public void setPeakPosition(int peakPosition) {
        this.peakPosition = peakPosition;
    }

    public int getSpanAtPeak() {
        return spanAtPeak;
    }

    public void setSpanAtPeak(int spanAtPeak) {
        this.spanAtPeak = spanAtPeak;
    }

    public int getSpanAtTop1() {
        return spanAtTop1;
    }

    public void setSpanAtTop1(int spanAtTop1) {
        this.spanAtTop1 = spanAtTop1;
    }

    public int getSpanAtTop5() {
        return spanAtTop5;
    }

    public void setSpanAtTop5(int spanAtTop5) {
        this.spanAtTop5 = spanAtTop5;
    }

    public int getSpanAtTop10() {
        return spanAtTop10;
    }

    public void setSpanAtTop10(int spanAtTop10) {
        this.spanAtTop10 = spanAtTop10;
    }

    public int[] getSpanAtTopThresholds() {
        return spanAtTopThresholds;
    }

    public void setSpanAtTopThresholds(int[] spanAtTopThresholds) {
        this.spanAtTopThresholds = spanAtTopThresholds;
    }

    public String getGenderClass() {
        return genderClass;
    }

    public void setGenderClass(String genderClass) {
        this.genderClass = genderClass;
    }

    public List<String> getLinkedSongTitles() {
        return linkedSongTitles;
    }

    public void setLinkedSongTitles(List<String> linkedSongTitles) {
        this.linkedSongTitles = linkedSongTitles;
    }
}