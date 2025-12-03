package library.dto;

/**
 * DTO for displaying entries in the "Most Hits" chart.
 * Represents an artist with the count of unique songs that reached a specific position threshold.
 */
public class MostHitsEntryDTO {
    
    private int rank;
    private Integer artistId;
    private String artistName;
    private int songsCount;
    private int totalWeeks;
    private boolean hasImage;
    
    public int getRank() {
        return rank;
    }
    
    public void setRank(int rank) {
        this.rank = rank;
    }
    
    public Integer getArtistId() {
        return artistId;
    }
    
    public void setArtistId(Integer artistId) {
        this.artistId = artistId;
    }
    
    public String getArtistName() {
        return artistName;
    }
    
    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }
    
    public int getSongsCount() {
        return songsCount;
    }
    
    public void setSongsCount(int songsCount) {
        this.songsCount = songsCount;
    }
    
    public int getTotalWeeks() {
        return totalWeeks;
    }
    
    public void setTotalWeeks(int totalWeeks) {
        this.totalWeeks = totalWeeks;
    }
    
    public boolean isHasImage() {
        return hasImage;
    }
    
    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }
}
