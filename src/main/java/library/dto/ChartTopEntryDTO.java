package library.dto;

/**
 * DTO for holding #1 chart entry information (song or album) for a period.
 * Used for batch fetching chart top entries to eliminate N+1 queries.
 */
public class ChartTopEntryDTO {
    
    private String periodKey;
    private String numberOneSongName;
    private String numberOneAlbumName;
    private Integer numberOneSongGenderId;
    private Integer numberOneAlbumGenderId;
    
    public ChartTopEntryDTO() {}
    
    public ChartTopEntryDTO(String periodKey) {
        this.periodKey = periodKey;
    }
    
    public String getPeriodKey() {
        return periodKey;
    }
    
    public void setPeriodKey(String periodKey) {
        this.periodKey = periodKey;
    }
    
    public String getNumberOneSongName() {
        return numberOneSongName;
    }
    
    public void setNumberOneSongName(String numberOneSongName) {
        this.numberOneSongName = numberOneSongName;
    }
    
    public String getNumberOneAlbumName() {
        return numberOneAlbumName;
    }
    
    public void setNumberOneAlbumName(String numberOneAlbumName) {
        this.numberOneAlbumName = numberOneAlbumName;
    }
    
    public Integer getNumberOneSongGenderId() {
        return numberOneSongGenderId;
    }
    
    public void setNumberOneSongGenderId(Integer numberOneSongGenderId) {
        this.numberOneSongGenderId = numberOneSongGenderId;
    }
    
    public Integer getNumberOneAlbumGenderId() {
        return numberOneAlbumGenderId;
    }
    
    public void setNumberOneAlbumGenderId(Integer numberOneAlbumGenderId) {
        this.numberOneAlbumGenderId = numberOneAlbumGenderId;
    }
}
