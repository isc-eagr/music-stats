package library.dto;

/**
 * DTO for displaying entries in the "Most Weeks at Position" chart.
 * Represents either a song or album with its weeks count at a specific position range.
 */
public class MostWeeksEntryDTO {
    
    private int rank;
    private Integer id;
    private String name;
    private String artistName;
    private Integer artistId;
    private int weeksCount;
    private boolean hasImage;
    private String type; // "song" or "album"
    private Integer genderId;
    private Integer peakPosition;
    private Integer albumId;
    private boolean albumHasImage;

    public int getRank() {
        return rank;
    }
    
    public void setRank(int rank) {
        this.rank = rank;
    }
    
    public Integer getId() {
        return id;
    }
    
    public void setId(Integer id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getArtistName() {
        return artistName;
    }
    
    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }
    
    public Integer getArtistId() {
        return artistId;
    }
    
    public void setArtistId(Integer artistId) {
        this.artistId = artistId;
    }
    
    public int getWeeksCount() {
        return weeksCount;
    }
    
    public void setWeeksCount(int weeksCount) {
        this.weeksCount = weeksCount;
    }
    
    public boolean isHasImage() {
        return hasImage;
    }
    
    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public Integer getGenderId() {
        return genderId;
    }
    
    public void setGenderId(Integer genderId) {
        this.genderId = genderId;
    }

    public Integer getPeakPosition() {
        return peakPosition;
    }

    public void setPeakPosition(Integer peakPosition) {
        this.peakPosition = peakPosition;
    }

    public Integer getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }

    public boolean isAlbumHasImage() {
        return albumHasImage;
    }

    public void setAlbumHasImage(boolean albumHasImage) {
        this.albumHasImage = albumHasImage;
    }
}
