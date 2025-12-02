package library.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * DTO for displaying featured artists in detail pages.
 * Extends ArtistCardDTO with an additional count field for showing
 * how many times an artist appears as a featured artist, and a list
 * of song names where the collaboration happened.
 */
public class FeaturedArtistCardDTO extends ArtistCardDTO {
    
    private int featureCount;
    private List<String> songNames;
    
    public FeaturedArtistCardDTO() {
        super();
        this.featureCount = 1;
        this.songNames = new ArrayList<>();
    }
    
    public int getFeatureCount() {
        return featureCount;
    }
    
    public void setFeatureCount(int featureCount) {
        this.featureCount = featureCount;
    }
    
    public List<String> getSongNames() {
        return songNames;
    }
    
    public void setSongNames(List<String> songNames) {
        this.songNames = songNames;
    }
    
    /**
     * Get song names as a comma-separated string for display/tooltip
     */
    public String getSongNamesFormatted() {
        if (songNames == null || songNames.isEmpty()) {
            return "";
        }
        return String.join(", ", songNames);
    }
}
