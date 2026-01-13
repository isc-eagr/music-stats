package library.entity;

import jakarta.persistence.*;
import java.sql.Date;
import java.sql.Timestamp;

@Entity
@Table(name = "Album")
public class Album {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;
    
    @Column(name = "artist_id", nullable = false)
    private Integer artistId;
    
    @Column(name = "name", length = 500, nullable = false)
    private String name;
    
    @Column(name = "release_date")
    private Date releaseDate;
    
    @Column(name = "number_of_songs")
    private Integer numberOfSongs;
    
    @Column(name = "override_genre_id")
    private Integer overrideGenreId;
    
    @Column(name = "override_subgenre_id")
    private Integer overrideSubgenreId;
    
    @Column(name = "override_language_id")
    private Integer overrideLanguageId;
    
    @Column(name = "organized")
    private Boolean organized;
    
    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "image")
    private byte[] image;
    
    @Column(name = "creation_date")
    private Timestamp creationDate;
    
    @Column(name = "update_date")
    private Timestamp updateDate;
    
    // Transient fields for display
    @Transient
    private String artistName;
    
    @Transient
    private String genreName;
    
    @Transient
    private String subgenreName;
    
    @Transient
    private String languageName;
    
    @Transient
    private Integer songCount;
    
    // Transient fields for inherited values from Artist
    @Transient
    private Integer artistGenreId;
    
    @Transient
    private Integer artistSubgenreId;
    
    @Transient
    private Integer artistLanguageId;
    
    // Methods to get effective (resolved) values with inheritance
    @Transient
    public Integer getEffectiveGenreId() {
        if (overrideGenreId != null) return overrideGenreId;
        return artistGenreId;
    }
    
    @Transient
    public Integer getEffectiveSubgenreId() {
        if (overrideSubgenreId != null) return overrideSubgenreId;
        return artistSubgenreId;
    }
    
    @Transient
    public Integer getEffectiveLanguageId() {
        if (overrideLanguageId != null) return overrideLanguageId;
        return artistLanguageId;
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
    
    @Transient
    private Boolean inItunes;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public Integer getNumberOfSongs() {
        return numberOfSongs;
    }

    public void setNumberOfSongs(Integer numberOfSongs) {
        this.numberOfSongs = numberOfSongs;
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

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public String getGenreName() {
        return genreName;
    }

    public void setGenreName(String genreName) {
        this.genreName = genreName;
    }

    public String getSubgenreName() {
        return subgenreName;
    }

    public void setSubgenreName(String subgenreName) {
        this.subgenreName = subgenreName;
    }

    public String getLanguageName() {
        return languageName;
    }

    public void setLanguageName(String languageName) {
        this.languageName = languageName;
    }

    public Integer getSongCount() {
        return songCount;
    }

    public void setSongCount(Integer songCount) {
        this.songCount = songCount;
    }
    
    // Getters and setters for inherited fields
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

    public Boolean getOrganized() {
        return organized;
    }

    public void setOrganized(Boolean organized) {
        this.organized = organized;
    }
    
    public Boolean getInItunes() {
        return inItunes;
    }
    
    public void setInItunes(Boolean inItunes) {
        this.inItunes = inItunes;
    }
}