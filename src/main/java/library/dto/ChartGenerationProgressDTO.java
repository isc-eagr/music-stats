package library.dto;

/**
 * DTO for tracking progress of bulk chart generation.
 */
public class ChartGenerationProgressDTO {
    
    private int totalWeeks;
    private int completedWeeks;
    private String currentWeek;
    private boolean isComplete;
    private String error;
    
    public ChartGenerationProgressDTO() {
    }
    
    public ChartGenerationProgressDTO(int totalWeeks, int completedWeeks, String currentWeek, boolean isComplete) {
        this.totalWeeks = totalWeeks;
        this.completedWeeks = completedWeeks;
        this.currentWeek = currentWeek;
        this.isComplete = isComplete;
    }
    
    public static ChartGenerationProgressDTO completed(int totalWeeks) {
        return new ChartGenerationProgressDTO(totalWeeks, totalWeeks, null, true);
    }
    
    public static ChartGenerationProgressDTO error(String errorMessage) {
        ChartGenerationProgressDTO dto = new ChartGenerationProgressDTO();
        dto.setError(errorMessage);
        dto.setComplete(true);
        return dto;
    }
    
    public int getTotalWeeks() {
        return totalWeeks;
    }
    
    public void setTotalWeeks(int totalWeeks) {
        this.totalWeeks = totalWeeks;
    }
    
    public int getCompletedWeeks() {
        return completedWeeks;
    }
    
    public void setCompletedWeeks(int completedWeeks) {
        this.completedWeeks = completedWeeks;
    }
    
    public String getCurrentWeek() {
        return currentWeek;
    }
    
    public void setCurrentWeek(String currentWeek) {
        this.currentWeek = currentWeek;
    }
    
    public boolean isComplete() {
        return isComplete;
    }
    
    public void setComplete(boolean complete) {
        isComplete = complete;
    }
    
    public String getError() {
        return error;
    }
    
    public void setError(String error) {
        this.error = error;
    }
    
    /**
     * Get the progress as a percentage (0-100).
     */
    public int getPercentage() {
        if (totalWeeks == 0) return 100;
        return (int) Math.round((completedWeeks * 100.0) / totalWeeks);
    }
}
