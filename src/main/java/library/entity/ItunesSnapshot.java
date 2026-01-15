package library.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * Entity representing a snapshot of an iTunes library song.
 * Used to detect changes between library parses.
 * 
 * The Persistent ID is the stable unique identifier - it doesn't change
 * when the library is rebuilt, unlike Track ID which can change.
 */
@Entity
@Table(name = "ItunesSnapshot")
public class ItunesSnapshot {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    /**
     * iTunes Persistent ID - a hex string that uniquely identifies a track
     * and remains stable across library rebuilds.
     */
    @Column(name = "persistent_id", nullable = false, unique = true, length = 20)
    private String persistentId;

    /**
     * iTunes Track ID - a numeric ID that may change when library is rebuilt.
     */
    @Column(name = "track_id")
    private Integer trackId;

    @Column(name = "artist", length = 500)
    private String artist;

    @Column(name = "album", length = 500)
    private String album;

    @Column(name = "name", nullable = false, length = 500)
    private String name;

    @Column(name = "track_number")
    private Integer trackNumber;

    @Column(name = "year")
    private Integer year;

    @Column(name = "total_time")
    private Integer totalTime;

    /**
     * When this snapshot was taken (when the library was last parsed and saved).
     */
    @Column(name = "snapshot_date")
    private LocalDateTime snapshotDate;

    @Column(name = "creation_date")
    private LocalDateTime creationDate;

    @Column(name = "update_date")
    private LocalDateTime updateDate;

    public ItunesSnapshot() {
    }

    public ItunesSnapshot(String persistentId, Integer trackId, String artist, String album, 
                          String name, Integer trackNumber, Integer year, Integer totalTime) {
        this.persistentId = persistentId;
        this.trackId = trackId;
        this.artist = artist;
        this.album = album;
        this.name = name;
        this.trackNumber = trackNumber;
        this.year = year;
        this.totalTime = totalTime;
        this.snapshotDate = LocalDateTime.now();
        this.creationDate = LocalDateTime.now();
        this.updateDate = LocalDateTime.now();
    }

    @PrePersist
    protected void onCreate() {
        if (creationDate == null) creationDate = LocalDateTime.now();
        if (updateDate == null) updateDate = LocalDateTime.now();
        if (snapshotDate == null) snapshotDate = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updateDate = LocalDateTime.now();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getPersistentId() { return persistentId; }
    public void setPersistentId(String persistentId) { this.persistentId = persistentId; }

    public Integer getTrackId() { return trackId; }
    public void setTrackId(Integer trackId) { this.trackId = trackId; }

    public String getArtist() { return artist; }
    public void setArtist(String artist) { this.artist = artist; }

    public String getAlbum() { return album; }
    public void setAlbum(String album) { this.album = album; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Integer getTrackNumber() { return trackNumber; }
    public void setTrackNumber(Integer trackNumber) { this.trackNumber = trackNumber; }

    public Integer getYear() { return year; }
    public void setYear(Integer year) { this.year = year; }

    public Integer getTotalTime() { return totalTime; }
    public void setTotalTime(Integer totalTime) { this.totalTime = totalTime; }

    public LocalDateTime getSnapshotDate() { return snapshotDate; }
    public void setSnapshotDate(LocalDateTime snapshotDate) { this.snapshotDate = snapshotDate; }

    public LocalDateTime getCreationDate() { return creationDate; }
    public void setCreationDate(LocalDateTime creationDate) { this.creationDate = creationDate; }

    public LocalDateTime getUpdateDate() { return updateDate; }
    public void setUpdateDate(LocalDateTime updateDate) { this.updateDate = updateDate; }
}
