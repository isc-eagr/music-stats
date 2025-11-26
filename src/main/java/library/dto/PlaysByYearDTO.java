package library.dto;

public class PlaysByYearDTO {
    private String year;
    private Long playCount;

    public PlaysByYearDTO() {}

    public PlaysByYearDTO(String year, Long playCount) {
        this.year = year;
        this.playCount = playCount;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Long getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Long playCount) {
        this.playCount = playCount;
    }
}
