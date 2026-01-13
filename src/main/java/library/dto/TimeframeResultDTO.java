package library.dto;

import java.util.List;

/**
 * Result wrapper for timeframe queries that returns both the paginated results
 * and the total count in a single query, eliminating the need for a separate count query.
 */
public class TimeframeResultDTO {
    
    private final List<TimeframeCardDTO> timeframes;
    private final long totalCount;
    
    public TimeframeResultDTO(List<TimeframeCardDTO> timeframes, long totalCount) {
        this.timeframes = timeframes;
        this.totalCount = totalCount;
    }
    
    public List<TimeframeCardDTO> getTimeframes() {
        return timeframes;
    }
    
    public long getTotalCount() {
        return totalCount;
    }
    
    public int getTotalPages(int perPage) {
        return (int) Math.ceil((double) totalCount / perPage);
    }
}
