package library.dto;

import java.util.List;

/**
 * Represents a snapshot in time when the top-3 most-played configuration was stable.
 * A new snapshot is created whenever any of the 3 positions changes.
 */
public class TopPlayedSnapshotDTO {
    private int rank;                                   // sequential snapshot number
    private String startDate;                           // when this top-3 config started (dd/MM/yyyy)
    private String endDate;                             // when it changed/ended (dd/MM/yyyy or "Present")
    private int daysInConfig;                           // duration of this specific snapshot
    private List<TopPlayedSnapshotItemDTO> items;       // up to 3 items, ordered by position
    private boolean current;                            // true if this is the currently active snapshot

    public int getRank() { return rank; }
    public void setRank(int rank) { this.rank = rank; }

    public String getStartDate() { return startDate; }
    public void setStartDate(String startDate) { this.startDate = startDate; }

    public String getEndDate() { return endDate; }
    public void setEndDate(String endDate) { this.endDate = endDate; }

    public int getDaysInConfig() { return daysInConfig; }
    public void setDaysInConfig(int daysInConfig) { this.daysInConfig = daysInConfig; }

    public List<TopPlayedSnapshotItemDTO> getItems() { return items; }
    public void setItems(List<TopPlayedSnapshotItemDTO> items) { this.items = items; }

    public boolean isCurrent() { return current; }
    public void setCurrent(boolean current) { this.current = current; }
}
