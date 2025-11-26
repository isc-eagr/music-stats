package library.dto;

/**
 * DTO for displaying featured artists in detail pages.
 * Extends ArtistCardDTO with an additional count field for showing
 * how many times an artist appears as a featured artist.
 */
public class FeaturedArtistCardDTO extends ArtistCardDTO {
    
    private int featureCount;
    
    public FeaturedArtistCardDTO() {
        super();
        this.featureCount = 1;
    }
    
    public int getFeatureCount() {
        return featureCount;
    }
    
    public void setFeatureCount(int featureCount) {
        this.featureCount = featureCount;
    }
}
