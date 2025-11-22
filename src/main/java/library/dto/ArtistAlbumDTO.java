package library.dto;

public class ArtistAlbumDTO {
    private Integer id;
    private String name;
    private Integer songCount;
    private String totalLength; // formatted total album length
    private Integer vatitoPlays;
    private Integer robertloverPlays;
    private Integer totalPlays;
    private String totalListeningTime; // formatted as dd:hh:mm:ss (smart display)
    private String firstListenedDate; // formatted date
    private String lastListenedDate; // formatted date
    
    public ArtistAlbumDTO() {}
    
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
    
    public Integer getSongCount() {
        return songCount;
    }
    
    public void setSongCount(Integer songCount) {
        this.songCount = songCount;
    }
    
    public String getTotalLength() {
        return totalLength;
    }
    
    public void setTotalLength(String totalLength) {
        this.totalLength = totalLength;
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
    
    // Helper method to calculate total listening time
    public void calculateTotalListeningTime() {
        if (totalLength != null && totalPlays != null && totalPlays > 0) {
            // Parse total length string (assuming format like "45:30")
            try {
                String[] parts = totalLength.split(":");
                if (parts.length == 2) {
                    int minutes = Integer.parseInt(parts[0]);
                    int seconds = Integer.parseInt(parts[1]);
                    int totalSeconds = (minutes * 60 + seconds) * totalPlays;
                    
                    long days = totalSeconds / 86400;
                    long hours = (totalSeconds % 86400) / 3600;
                    long mins = (totalSeconds % 3600) / 60;
                    long secs = totalSeconds % 60;
                    
                    if (days > 0) {
                        this.totalListeningTime = String.format("%dd:%02d:%02d:%02d", days, hours, mins, secs);
                    } else if (hours > 0) {
                        this.totalListeningTime = String.format("%d:%02d:%02d", hours, mins, secs);
                    } else {
                        this.totalListeningTime = String.format("%d:%02d", mins, secs);
                    }
                } else {
                    this.totalListeningTime = "-";
                }
            } catch (Exception e) {
                this.totalListeningTime = "-";
            }
        } else {
            this.totalListeningTime = "-";
        }
    }
}
