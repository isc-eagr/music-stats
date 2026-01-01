package library.entity;

import jakarta.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "SongImage")
public class SongImage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "song_id", nullable = false)
    private Integer songId;

    @Column(name = "image", nullable = false)
    private byte[] image;

    @Column(name = "display_order")
    private Integer displayOrder;

    @Column(name = "creation_date")
    private Timestamp creationDate;

    // Getters and Setters
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getSongId() {
        return songId;
    }

    public void setSongId(Integer songId) {
        this.songId = songId;
    }

    public byte[] getImage() {
        return image;
    }

    public void setImage(byte[] image) {
        this.image = image;
    }

    public Integer getDisplayOrder() {
        return displayOrder;
    }

    public void setDisplayOrder(Integer displayOrder) {
        this.displayOrder = displayOrder;
    }

    public Timestamp getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Timestamp creationDate) {
        this.creationDate = creationDate;
    }
}

