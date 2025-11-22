package library.entity;

import jakarta.persistence.*;
import java.sql.Date;
import java.sql.Timestamp;

@Entity
@Table(name = "Song")
public class SongNew {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;
    
    @Column(name = "artist_id", nullable = false)
    private Integer artistId;
    
    @Column(name = "album_id")
    private Integer albumId;
    
    @Column(name = "name", length = 500, nullable = false)
    private String name;
    
    @Column(name = "status", length = 50)
    private String status;
    
    @Column(name = "length_seconds")
    private Integer lengthSeconds;
    
    @Column(name = "is_single")
    private Boolean isSingle;
    
    @Column(name = "override_genre_id")
    private Integer overrideGenreId;
    
    @Column(name = "override_subgenre_id")
    private Integer overrideSubgenreId;
    
    @Column(name = "override_language_id")
    private Integer overrideLanguageId;
    
    @Column(name = "override_gender_id")
    private Integer overrideGenderId;
    
    @Column(name = "override_ethnicity_id")
    private Integer overrideEthnicityId;
    
    @Column(name = "release_date")
    private Date releaseDate;
    
    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "single_cover")
    private byte[] singleCover;
    
    @Column(name = "creation_date")
    private Timestamp creationDate;
    
    @Column(name = "update_date")
    private Timestamp updateDate;
    
    // Transient fields for inherited values from Album and Artist
    @Transient
    private Integer albumGenreId;
    
    @Transient
    private Integer albumSubgenreId;
    
    @Transient
    private Integer albumLanguageId;
    
    @Transient
    private Integer artistGenreId;
    
    @Transient
    private Integer artistSubgenreId;
    
    @Transient
    private Integer artistLanguageId;
    
    @Transient
    private Integer artistGenderId;
    
    @Transient
    private Integer artistEthnicityId;
    
    // Methods to get effective (resolved) values with inheritance
    @Transient
    public Integer getEffectiveGenreId() {
        if (overrideGenreId != null) return overrideGenreId;
        if (albumGenreId != null) return albumGenreId;
        return artistGenreId;
    }
    
    @Transient
    public Integer getEffectiveSubgenreId() {
        if (overrideSubgenreId != null) return overrideSubgenreId;
        if (albumSubgenreId != null) return albumSubgenreId;
        return artistSubgenreId;
    }
    
    @Transient
    public Integer getEffectiveLanguageId() {
        if (overrideLanguageId != null) return overrideLanguageId;
        if (albumLanguageId != null) return albumLanguageId;
        return artistLanguageId;
    }
    
    @Transient
    public Integer getEffectiveGenderId() {
        if (overrideGenderId != null) return overrideGenderId;
        return artistGenderId;
    }
    
    @Transient
    public Integer getEffectiveEthnicityId() {
        if (overrideEthnicityId != null) return overrideEthnicityId;
        return artistEthnicityId;
    }
    
    // Transient field for formatted date display
    @Transient
    public String getReleaseDateFormatted() {
        if (releaseDate == null) return null;
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd MMM yyyy");
        return sdf.format(releaseDate);
    }
    
    // Transient field for input field (dd/MM/yyyy format)
    @Transient
    public String getReleaseDateInput() {
        if (releaseDate == null) return null;
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd/MM/yyyy");
        return sdf.format(releaseDate);
    }

    // Getters and Setters
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getArtistId() {
        return artistId;
    }

    public void setArtistId(Integer artistId) {
        this.artistId = artistId;
    }

    public Integer getAlbumId() {
        return albumId;
    }

    public void setAlbumId(Integer albumId) {
        this.albumId = albumId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getLengthSeconds() {
        return lengthSeconds;
    }

    public void setLengthSeconds(Integer lengthSeconds) {
        this.lengthSeconds = lengthSeconds;
    }

    public Boolean getIsSingle() {
        return isSingle;
    }

    public void setIsSingle(Boolean isSingle) {
        this.isSingle = isSingle;
    }

    public Integer getOverrideGenreId() {
        return overrideGenreId;
    }

    public void setOverrideGenreId(Integer overrideGenreId) {
        this.overrideGenreId = overrideGenreId;
    }

    public Integer getOverrideSubgenreId() {
        return overrideSubgenreId;
    }

    public void setOverrideSubgenreId(Integer overrideSubgenreId) {
        this.overrideSubgenreId = overrideSubgenreId;
    }

    public Integer getOverrideLanguageId() {
        return overrideLanguageId;
    }

    public void setOverrideLanguageId(Integer overrideLanguageId) {
        this.overrideLanguageId = overrideLanguageId;
    }

    public Integer getOverrideGenderId() {
        return overrideGenderId;
    }

    public void setOverrideGenderId(Integer overrideGenderId) {
        this.overrideGenderId = overrideGenderId;
    }

    public Integer getOverrideEthnicityId() {
        return overrideEthnicityId;
    }

    public void setOverrideEthnicityId(Integer overrideEthnicityId) {
        this.overrideEthnicityId = overrideEthnicityId;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public byte[] getSingleCover() {
        return singleCover;
    }

    public void setSingleCover(byte[] singleCover) {
        this.singleCover = singleCover;
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
    
    // Getters and setters for transient inherited fields
    public Integer getAlbumGenreId() {
        return albumGenreId;
    }
    
    public void setAlbumGenreId(Integer albumGenreId) {
        this.albumGenreId = albumGenreId;
    }
    
    public Integer getAlbumSubgenreId() {
        return albumSubgenreId;
    }
    
    public void setAlbumSubgenreId(Integer albumSubgenreId) {
        this.albumSubgenreId = albumSubgenreId;
    }
    
    public Integer getAlbumLanguageId() {
        return albumLanguageId;
    }
    
    public void setAlbumLanguageId(Integer albumLanguageId) {
        this.albumLanguageId = albumLanguageId;
    }
    
    public Integer getArtistGenreId() {
        return artistGenreId;
    }
    
    public void setArtistGenreId(Integer artistGenreId) {
        this.artistGenreId = artistGenreId;
    }
    
    public Integer getArtistSubgenreId() {
        return artistSubgenreId;
    }
    
    public void setArtistSubgenreId(Integer artistSubgenreId) {
        this.artistSubgenreId = artistSubgenreId;
    }
    
    public Integer getArtistLanguageId() {
        return artistLanguageId;
    }
    
    public void setArtistLanguageId(Integer artistLanguageId) {
        this.artistLanguageId = artistLanguageId;
    }
    
    public Integer getArtistGenderId() {
        return artistGenderId;
    }
    
    public void setArtistGenderId(Integer artistGenderId) {
        this.artistGenderId = artistGenderId;
    }
    
    public Integer getArtistEthnicityId() {
        return artistEthnicityId;
    }
    
    public void setArtistEthnicityId(Integer artistEthnicityId) {
        this.artistEthnicityId = artistEthnicityId;
    }
}
