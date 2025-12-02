package library.entity;

import jakarta.persistence.*;

/**
 * Entity representing an individual entry in a chart.
 * Each entry links to either a song or album and stores
 * the position and play count for that chart period.
 */
@Entity
@Table(name = "ChartEntry")
public class ChartEntry {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;
    
    @Column(name = "chart_id", nullable = false)
    private Integer chartId;
    
    @Column(name = "position", nullable = false)
    private Integer position;
    
    @Column(name = "song_id")
    private Integer songId;
    
    @Column(name = "album_id")
    private Integer albumId;
    
    @Column(name = "play_count", nullable = false)
    private Integer playCount;
    
    // Transient fields for display (populated by queries)
    @Transient
    private String songName;
    
    @Transient
    private String artistName;
    
    @Transient
    private String albumName;
    
    @Transient
    private boolean hasImage;
    
    // Default constructor
    public ChartEntry() {
    }
    
    // Constructor for song chart entry
    public static ChartEntry forSong(Integer chartId, Integer position, Integer songId, Integer playCount) {
        ChartEntry entry = new ChartEntry();
        entry.chartId = chartId;
        entry.position = position;
        entry.songId = songId;
        entry.playCount = playCount;
        return entry;
    }
    
    // Constructor for album chart entry
    public static ChartEntry forAlbum(Integer chartId, Integer position, Integer albumId, Integer playCount) {
        ChartEntry entry = new ChartEntry();
        entry.chartId = chartId;
        entry.position = position;
        entry.albumId = albumId;
        entry.playCount = playCount;
        return entry;
    }
    
    // Getters and setters
    public Integer getId() {
        return id;
    }
    
    public void setId(Integer id) {
        this.id = id;
    }
    
    public Integer getChartId() {
        return chartId;
    }
    
    public void setChartId(Integer chartId) {
        this.chartId = chartId;
    }
    
    public Integer getPosition() {
        return position;
    }
    
    public void setPosition(Integer position) {
        this.position = position;
    }
    
    public Integer getSongId() {
        return songId;
    }
    
    public void setSongId(Integer songId) {
        this.songId = songId;
    }
    
    public Integer getAlbumId() {
        return albumId;
    }
    
    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }
    
    public Integer getPlayCount() {
        return playCount;
    }
    
    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
    }
    
    public String getSongName() {
        return songName;
    }
    
    public void setSongName(String songName) {
        this.songName = songName;
    }
    
    public String getArtistName() {
        return artistName;
    }
    
    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }
    
    public String getAlbumName() {
        return albumName;
    }
    
    public void setAlbumName(String albumName) {
        this.albumName = albumName;
    }
    
    public boolean isHasImage() {
        return hasImage;
    }
    
    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }
}
