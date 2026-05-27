package library.dto;

import java.util.List;

public class ChartAlbumOverviewRowDTO {

    private Integer albumId;
    private Integer resolvedArtistId;
    private String albumName;
    private String artistName;
    private boolean hasImage;
    private boolean artistHasImage;
    private int chartedSongsCount;
    private int totalChartSpan;
    private Integer highestPeak;
    private int numberOneSongsCount;
    private int totalSpanAtNumberOne;
    private int spanAtPeak;
    private String firstDebutDate;
    private String firstDebutKey;
    private String firstDebutSortValue;
    private Integer debutPosition;
    private String peakAppearanceDate;
    private String peakAppearanceKey;
    private String peakAppearanceSortValue;
    private String lastAppearanceDate;
    private String lastAppearanceKey;
    private String lastAppearanceSortValue;
    private int[] spanAtTopThresholds;
    private List<String> numberOneSongTitles;
    private String genderClass;

    public Integer getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }

    public Integer getResolvedArtistId() {
        return resolvedArtistId;
    }

    public void setResolvedArtistId(Integer resolvedArtistId) {
        this.resolvedArtistId = resolvedArtistId;
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

    public boolean isArtistHasImage() {
        return artistHasImage;
    }

    public void setArtistHasImage(boolean artistHasImage) {
        this.artistHasImage = artistHasImage;
    }

    public int getChartedSongsCount() {
        return chartedSongsCount;
    }

    public void setChartedSongsCount(int chartedSongsCount) {
        this.chartedSongsCount = chartedSongsCount;
    }

    public int getTotalChartSpan() {
        return totalChartSpan;
    }

    public void setTotalChartSpan(int totalChartSpan) {
        this.totalChartSpan = totalChartSpan;
    }

    public Integer getHighestPeak() {
        return highestPeak;
    }

    public void setHighestPeak(Integer highestPeak) {
        this.highestPeak = highestPeak;
    }

    public int getNumberOneSongsCount() {
        return numberOneSongsCount;
    }

    public void setNumberOneSongsCount(int numberOneSongsCount) {
        this.numberOneSongsCount = numberOneSongsCount;
    }

    public int getTotalSpanAtNumberOne() {
        return totalSpanAtNumberOne;
    }

    public void setTotalSpanAtNumberOne(int totalSpanAtNumberOne) {
        this.totalSpanAtNumberOne = totalSpanAtNumberOne;
    }

    public int getSpanAtPeak() {
        return spanAtPeak;
    }

    public void setSpanAtPeak(int spanAtPeak) {
        this.spanAtPeak = spanAtPeak;
    }

    public String getFirstDebutDate() {
        return firstDebutDate;
    }

    public void setFirstDebutDate(String firstDebutDate) {
        this.firstDebutDate = firstDebutDate;
    }

    public String getFirstDebutKey() {
        return firstDebutKey;
    }

    public void setFirstDebutKey(String firstDebutKey) {
        this.firstDebutKey = firstDebutKey;
    }

    public String getFirstDebutSortValue() {
        return firstDebutSortValue;
    }

    public void setFirstDebutSortValue(String firstDebutSortValue) {
        this.firstDebutSortValue = firstDebutSortValue;
    }

    public Integer getDebutPosition() {
        return debutPosition;
    }

    public void setDebutPosition(Integer debutPosition) {
        this.debutPosition = debutPosition;
    }

    public String getPeakAppearanceDate() {
        return peakAppearanceDate;
    }

    public void setPeakAppearanceDate(String peakAppearanceDate) {
        this.peakAppearanceDate = peakAppearanceDate;
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

    public String getLastAppearanceDate() {
        return lastAppearanceDate;
    }

    public void setLastAppearanceDate(String lastAppearanceDate) {
        this.lastAppearanceDate = lastAppearanceDate;
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

    public int[] getSpanAtTopThresholds() {
        return spanAtTopThresholds;
    }

    public void setSpanAtTopThresholds(int[] spanAtTopThresholds) {
        this.spanAtTopThresholds = spanAtTopThresholds;
    }

    public List<String> getNumberOneSongTitles() {
        return numberOneSongTitles;
    }

    public void setNumberOneSongTitles(List<String> numberOneSongTitles) {
        this.numberOneSongTitles = numberOneSongTitles;
    }

    public String getGenderClass() {
        return genderClass;
    }

    public void setGenderClass(String genderClass) {
        this.genderClass = genderClass;
    }
}