package library.dto;

public class ChartArtistOverviewRowDTO {

    private boolean matched;
    private Integer resolvedArtistId;
    private String artistName;
    private int chartedSongsCount;
    private int totalChartSpan;
    private Integer highestPeak;
    private int numberOneSongsCount;
    private int totalSpanAtNumberOne;
    private String firstDebutDate;
    private String firstDebutKey;
    private String firstDebutSortValue;
    private String lastAppearanceDate;
    private String lastAppearanceKey;
    private String lastAppearanceSortValue;
    private String genderClass;

    public boolean isMatched() {
        return matched;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

    public Integer getResolvedArtistId() {
        return resolvedArtistId;
    }

    public void setResolvedArtistId(Integer resolvedArtistId) {
        this.resolvedArtistId = resolvedArtistId;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
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

    public String getGenderClass() {
        return genderClass;
    }

    public void setGenderClass(String genderClass) {
        this.genderClass = genderClass;
    }
}