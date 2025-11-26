package library.entity;

import jakarta.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "SubGenre")
public class SubGenre {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;
    
    @Column(name = "name", length = 100, nullable = false)
    private String name;
    
    @Column(name = "parent_genre_id", nullable = false)
    private Integer parentGenreId;
    
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "image")
    private byte[] image;
    
    @Column(name = "creation_date")
    private Timestamp creationDate;
    
    @Column(name = "update_date")
    private Timestamp updateDate;
    
    // Transient fields for statistics and display
    @Transient
    private String parentGenreName;
    
    @Transient
    private Integer playCount;
    
    @Transient
    private Long timeListened;
    
    @Transient
    private String timeListenedFormatted;
    
    @Transient
    private Integer artistCount;
    
    @Transient
    private Integer albumCount;
    
    @Transient
    private Integer songCount;
    
    @Transient
    private Integer maleCount;
    
    @Transient
    private Integer femaleCount;

    // Getters and Setters
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

    public Integer getParentGenreId() {
        return parentGenreId;
    }

    public void setParentGenreId(Integer parentGenreId) {
        this.parentGenreId = parentGenreId;
    }

    public byte[] getImage() {
        return image;
    }

    public void setImage(byte[] image) {
        this.image = image;
    }

    public Timestamp getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Timestamp creationDate) {
        this.creationDate = creationDate;
    }

    public Timestamp getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Timestamp updateDate) {
        this.updateDate = updateDate;
    }

    public String getParentGenreName() {
        return parentGenreName;
    }

    public void setParentGenreName(String parentGenreName) {
        this.parentGenreName = parentGenreName;
    }

    public Integer getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
    }

    public Long getTimeListened() {
        return timeListened;
    }

    public void setTimeListened(Long timeListened) {
        this.timeListened = timeListened;
    }

    public String getTimeListenedFormatted() {
        return timeListenedFormatted;
    }

    public void setTimeListenedFormatted(String timeListenedFormatted) {
        this.timeListenedFormatted = timeListenedFormatted;
    }

    public Integer getArtistCount() {
        return artistCount;
    }

    public void setArtistCount(Integer artistCount) {
        this.artistCount = artistCount;
    }

    public Integer getAlbumCount() {
        return albumCount;
    }

    public void setAlbumCount(Integer albumCount) {
        this.albumCount = albumCount;
    }

    public Integer getSongCount() {
        return songCount;
    }

    public void setSongCount(Integer songCount) {
        this.songCount = songCount;
    }

    public Integer getMaleCount() {
        return maleCount;
    }

    public void setMaleCount(Integer maleCount) {
        this.maleCount = maleCount;
    }

    public Integer getFemaleCount() {
        return femaleCount;
    }

    public void setFemaleCount(Integer femaleCount) {
        this.femaleCount = femaleCount;
    }
}
