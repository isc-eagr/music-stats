package library.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "ArtistImageTheme")
public class ArtistImageTheme {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "theme_id", nullable = false)
    private Integer themeId;

    @Column(name = "artist_id", nullable = false)
    private Integer artistId;

    /**
     * FK to ArtistImage.id.
     * NULL means the artist's default (main) image is the one assigned to this theme.
     */
    @Column(name = "artist_image_id")
    private Integer artistImageId;

    @Column(name = "creation_date")
    private String creationDate;

    // ---- Getters / Setters ----

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public Integer getThemeId() { return themeId; }
    public void setThemeId(Integer themeId) { this.themeId = themeId; }

    public Integer getArtistId() { return artistId; }
    public void setArtistId(Integer artistId) { this.artistId = artistId; }

    public Integer getArtistImageId() { return artistImageId; }
    public void setArtistImageId(Integer artistImageId) { this.artistImageId = artistImageId; }

    public String getCreationDate() { return creationDate; }
    public void setCreationDate(String creationDate) { this.creationDate = creationDate; }
}
