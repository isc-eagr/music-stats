package library.dto;

import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO containing all detected iTunes changes since the last snapshot.
 */
public class ItunesChangesResultDTO {

    private LocalDateTime lastSnapshotDate;
    private boolean hasSnapshot;
    
    private List<ItunesChangedSongDTO> changedSongs;
    private List<ItunesAddedSongDTO> addedSongs;
    private List<ItunesRemovedSongDTO> removedSongs;
    
    // Counts for convenience
    private int changedCount;
    private int addedCount;
    private int removedCount;
    
    // Count of changed songs NOT found in database (problematic ones)
    private int changedNotFoundCount;

    public ItunesChangesResultDTO() {
    }

    public ItunesChangesResultDTO(LocalDateTime lastSnapshotDate, boolean hasSnapshot,
                                   List<ItunesChangedSongDTO> changedSongs,
                                   List<ItunesAddedSongDTO> addedSongs,
                                   List<ItunesRemovedSongDTO> removedSongs) {
        this.lastSnapshotDate = lastSnapshotDate;
        this.hasSnapshot = hasSnapshot;
        this.changedSongs = changedSongs;
        this.addedSongs = addedSongs;
        this.removedSongs = removedSongs;
        this.changedCount = changedSongs != null ? changedSongs.size() : 0;
        this.addedCount = addedSongs != null ? addedSongs.size() : 0;
        this.removedCount = removedSongs != null ? removedSongs.size() : 0;
        
        // Count problematic changed songs (not found in DB)
        this.changedNotFoundCount = changedSongs != null 
            ? (int) changedSongs.stream().filter(s -> !s.isFoundInDatabase()).count() 
            : 0;
    }

    public boolean hasChanges() {
        return changedCount > 0 || addedCount > 0 || removedCount > 0;
    }

    // Getters
    public LocalDateTime getLastSnapshotDate() { return lastSnapshotDate; }
    public boolean isHasSnapshot() { return hasSnapshot; }
    public List<ItunesChangedSongDTO> getChangedSongs() { return changedSongs; }
    public List<ItunesAddedSongDTO> getAddedSongs() { return addedSongs; }
    public List<ItunesRemovedSongDTO> getRemovedSongs() { return removedSongs; }
    public int getChangedCount() { return changedCount; }
    public int getAddedCount() { return addedCount; }
    public int getRemovedCount() { return removedCount; }
    public int getChangedNotFoundCount() { return changedNotFoundCount; }

    // Setters
    public void setLastSnapshotDate(LocalDateTime lastSnapshotDate) { this.lastSnapshotDate = lastSnapshotDate; }
    public void setHasSnapshot(boolean hasSnapshot) { this.hasSnapshot = hasSnapshot; }
    public void setChangedSongs(List<ItunesChangedSongDTO> changedSongs) { this.changedSongs = changedSongs; }
    public void setAddedSongs(List<ItunesAddedSongDTO> addedSongs) { this.addedSongs = addedSongs; }
    public void setRemovedSongs(List<ItunesRemovedSongDTO> removedSongs) { this.removedSongs = removedSongs; }
    public void setChangedCount(int changedCount) { this.changedCount = changedCount; }
    public void setAddedCount(int addedCount) { this.addedCount = addedCount; }
    public void setRemovedCount(int removedCount) { this.removedCount = removedCount; }
    public void setChangedNotFoundCount(int changedNotFoundCount) { this.changedNotFoundCount = changedNotFoundCount; }
}
