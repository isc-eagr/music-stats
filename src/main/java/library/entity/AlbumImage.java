package library.entity;

import jakarta.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "AlbumImage")
public class AlbumImage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "album_id", nullable = false)
    private Integer albumId;

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

    public Integer getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
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

