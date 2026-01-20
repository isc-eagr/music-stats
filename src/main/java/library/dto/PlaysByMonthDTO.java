package library.dto;

public class PlaysByMonthDTO {
    private String year;
    private String month; // 01-12
    private Long playCount;

    public PlaysByMonthDTO() {}

    public PlaysByMonthDTO(String year, String month, Long playCount) {
        this.year = year;
        this.month = month;
        this.playCount = playCount;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public Long getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Long playCount) {
        this.playCount = playCount;
    }
    
    // Helper to get display label like "2024-01"
    public String getLabel() {
        return year + "-" + month;
    }
}
