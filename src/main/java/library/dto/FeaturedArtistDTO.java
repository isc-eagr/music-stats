package library.dto;

/**
 * DTO for featured artist display in song detail page
 */
public class FeaturedArtistDTO {
    
    private Integer artistId;
    private String artistName;
    
    public FeaturedArtistDTO() {
    }
    
    public FeaturedArtistDTO(Integer artistId, String artistName) {
        this.artistId = artistId;
        this.artistName = artistName;
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
}
