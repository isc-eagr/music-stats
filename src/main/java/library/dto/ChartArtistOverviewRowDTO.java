package library.dto;

public class ChartArtistOverviewRowDTO {

    private boolean matched;
    private Integer resolvedArtistId;
    private String artistName;
    private boolean hasImage;
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
    private int chartedAlbumsCount;
    private int albumTotalChartSpan;
    private Integer albumHighestPeak;
    private int numberOneAlbumsCount;
    private int albumTotalSpanAtNumberOne;
    private int[] topSongCounts;
    private int[] topSongWeeks;
    private int[] topAlbumCounts;
    private int[] topAlbumWeeks;
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

    public boolean isHasImage() {
        return hasImage;
    }

    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
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

    public int getChartedAlbumsCount() {
        return chartedAlbumsCount;
    }

    public void setChartedAlbumsCount(int chartedAlbumsCount) {
        this.chartedAlbumsCount = chartedAlbumsCount;
    }

    public int getAlbumTotalChartSpan() {
        return albumTotalChartSpan;
    }

    public void setAlbumTotalChartSpan(int albumTotalChartSpan) {
        this.albumTotalChartSpan = albumTotalChartSpan;
    }

    public Integer getAlbumHighestPeak() {
        return albumHighestPeak;
    }

    public void setAlbumHighestPeak(Integer albumHighestPeak) {
        this.albumHighestPeak = albumHighestPeak;
    }

    public int getNumberOneAlbumsCount() {
        return numberOneAlbumsCount;
    }

    public void setNumberOneAlbumsCount(int numberOneAlbumsCount) {
        this.numberOneAlbumsCount = numberOneAlbumsCount;
    }

    public int getAlbumTotalSpanAtNumberOne() {
        return albumTotalSpanAtNumberOne;
    }

    public void setAlbumTotalSpanAtNumberOne(int albumTotalSpanAtNumberOne) {
        this.albumTotalSpanAtNumberOne = albumTotalSpanAtNumberOne;
    }

    public int[] getTopSongCounts() {
        return topSongCounts;
    }

    public void setTopSongCounts(int[] topSongCounts) {
        this.topSongCounts = topSongCounts;
    }

    public int[] getTopSongWeeks() {
        return topSongWeeks;
    }

    public void setTopSongWeeks(int[] topSongWeeks) {
        this.topSongWeeks = topSongWeeks;
    }

    public int[] getTopAlbumCounts() {
        return topAlbumCounts;
    }

    public void setTopAlbumCounts(int[] topAlbumCounts) {
        this.topAlbumCounts = topAlbumCounts;
    }

    public int[] getTopAlbumWeeks() {
        return topAlbumWeeks;
    }

    public void setTopAlbumWeeks(int[] topAlbumWeeks) {
        this.topAlbumWeeks = topAlbumWeeks;
    }

    public String getGenderClass() {
        return genderClass;
    }

    public void setGenderClass(String genderClass) {
        this.genderClass = genderClass;
    }
}