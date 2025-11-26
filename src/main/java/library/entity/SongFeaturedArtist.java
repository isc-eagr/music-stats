package library.entity;

import jakarta.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;

@Entity
@Table(name = "SongFeaturedArtist")
@IdClass(SongFeaturedArtist.SongFeaturedArtistId.class)
public class SongFeaturedArtist {
    
    @Id
    @Column(name = "song_id", nullable = false)
    private Integer songId;
    
    @Id
    @Column(name = "artist_id", nullable = false)
    private Integer artistId;
    
    @Column(name = "creation_date")
    private Timestamp creationDate;
    
    // Transient field for artist name display
    @Transient
    private String artistName;
    
    public SongFeaturedArtist() {
    }
    
    public SongFeaturedArtist(Integer songId, Integer artistId) {
        this.songId = songId;
        this.artistId = artistId;
    }
    
    // Getters and Setters
    public Integer getSongId() {
        return songId;
    }
    
    public void setSongId(Integer songId) {
        this.songId = songId;
    }
    
    public Integer getArtistId() {
        return artistId;
    }
    
    public void setArtistId(Integer artistId) {
        this.artistId = artistId;
    }
    
    public Timestamp getCreationDate() {
        return creationDate;
    }
    
    public void setCreationDate(Timestamp creationDate) {
        this.creationDate = creationDate;
    }
    
    public String getArtistName() {
        return artistName;
    }
    
    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }
    
    // Composite key class
    public static class SongFeaturedArtistId implements Serializable {
        private Integer songId;
        private Integer artistId;
        
        public SongFeaturedArtistId() {
        }
        
        public SongFeaturedArtistId(Integer songId, Integer artistId) {
            this.songId = songId;
            this.artistId = artistId;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SongFeaturedArtistId that = (SongFeaturedArtistId) o;
            return Objects.equals(songId, that.songId) && Objects.equals(artistId, that.artistId);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(songId, artistId);
        }
    }
}
