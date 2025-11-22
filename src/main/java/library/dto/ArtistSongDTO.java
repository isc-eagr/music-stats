package library.dto;

public class ArtistSongDTO {
    private Integer id;
    private String name;
    private Integer albumId;
    private String albumName;
    private Integer length; // in seconds
    private String lengthFormatted; // mm:ss format
    private Integer vatitoPlays;
    private Integer robertloverPlays;
    private Integer totalPlays;
    private String totalListeningTime; // formatted as dd:hh:mm:ss (smart display)
    private String firstListenedDate; // formatted date
    private String lastListenedDate; // formatted date
    
    public ArtistSongDTO() {}
    
    // Getters and setters
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
    
    public Integer getAlbumId() {
        return albumId;
    }
    
    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }
    
    public String getAlbumName() {
        return albumName;
    }
    
    public void setAlbumName(String albumName) {
        this.albumName = albumName;
    }
    
    public Integer getLength() {
        return length;
    }
    
    public void setLength(Integer length) {
        this.length = length;
        // Auto-format when setting length
        if (length != null && length > 0) {
            int minutes = length / 60;
            int seconds = length % 60;
            this.lengthFormatted = String.format("%d:%02d", minutes, seconds);
        } else {
            this.lengthFormatted = "-";
        }
    }
    
    public String getLengthFormatted() {
        return lengthFormatted;
    }
    
    public void setLengthFormatted(String lengthFormatted) {
        this.lengthFormatted = lengthFormatted;
    }
    
    public Integer getVatitoPlays() {
        return vatitoPlays;
    }
    
    public void setVatitoPlays(Integer vatitoPlays) {
        this.vatitoPlays = vatitoPlays;
    }
    
    public Integer getRobertloverPlays() {
        return robertloverPlays;
    }
    
    public void setRobertloverPlays(Integer robertloverPlays) {
        this.robertloverPlays = robertloverPlays;
    }
    
    public Integer getTotalPlays() {
        return totalPlays;
    }
    
    public void setTotalPlays(Integer totalPlays) {
        this.totalPlays = totalPlays;
    }
    
    public String getTotalListeningTime() {
        return totalListeningTime;
    }
    
    public void setTotalListeningTime(String totalListeningTime) {
        this.totalListeningTime = totalListeningTime;
    }
    
    public String getFirstListenedDate() {
        return firstListenedDate;
    }
    
    public void setFirstListenedDate(String firstListenedDate) {
        this.firstListenedDate = firstListenedDate;
    }
    
    public String getLastListenedDate() {
        return lastListenedDate;
    }
    
    public void setLastListenedDate(String lastListenedDate) {
        this.lastListenedDate = lastListenedDate;
    }
    
    // Helper method to calculate and format total listening time
    public void calculateTotalListeningTime() {
        if (length != null && totalPlays != null && length > 0 && totalPlays > 0) {
            long totalSeconds = (long) length * totalPlays;
            
            long days = totalSeconds / 86400;
            long hours = (totalSeconds % 86400) / 3600;
            long minutes = (totalSeconds % 3600) / 60;
            long seconds = totalSeconds % 60;
            
            // Smart formatting
            if (days > 0) {
                this.totalListeningTime = String.format("%dd:%02d:%02d:%02d", days, hours, minutes, seconds);
            } else if (hours > 0) {
                this.totalListeningTime = String.format("%d:%02d:%02d", hours, minutes, seconds);
            } else {
                this.totalListeningTime = String.format("%d:%02d", minutes, seconds);
            }
        } else {
            this.totalListeningTime = "-";
        }
    }
}
