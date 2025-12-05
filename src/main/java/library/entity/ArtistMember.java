package library.entity;

import jakarta.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * Entity representing an artist's membership in a group.
 * E.g., Beyoncé (member) is part of Destiny's Child (group).
 */
@Entity
@Table(name = "ArtistMember")
@IdClass(ArtistMember.ArtistMemberId.class)
public class ArtistMember {
    
    @Id
    @Column(name = "group_artist_id", nullable = false)
    private Integer groupArtistId;
    
    @Id
    @Column(name = "member_artist_id", nullable = false)
    private Integer memberArtistId;
    
    @Column(name = "creation_date")
    private Timestamp creationDate;
    
    // Transient fields for display
    @Transient
    private String groupArtistName;
    
    @Transient
    private String memberArtistName;
    
    public ArtistMember() {
    }
    
    public ArtistMember(Integer groupArtistId, Integer memberArtistId) {
        this.groupArtistId = groupArtistId;
        this.memberArtistId = memberArtistId;
    }
    
    // Getters and Setters
    public Integer getGroupArtistId() {
        return groupArtistId;
    }
    
    public void setGroupArtistId(Integer groupArtistId) {
        this.groupArtistId = groupArtistId;
    }
    
    public Integer getMemberArtistId() {
        return memberArtistId;
    }
    
    public void setMemberArtistId(Integer memberArtistId) {
        this.memberArtistId = memberArtistId;
    }
    
    public Timestamp getCreationDate() {
        return creationDate;
    }
    
    public void setCreationDate(Timestamp creationDate) {
        this.creationDate = creationDate;
    }
    
    public String getGroupArtistName() {
        return groupArtistName;
    }
    
    public void setGroupArtistName(String groupArtistName) {
        this.groupArtistName = groupArtistName;
    }
    
    public String getMemberArtistName() {
        return memberArtistName;
    }
    
    public void setMemberArtistName(String memberArtistName) {
        this.memberArtistName = memberArtistName;
    }
    
    // Composite key class
    public static class ArtistMemberId implements Serializable {
        private Integer groupArtistId;
        private Integer memberArtistId;
        
        public ArtistMemberId() {
        }
        
        public ArtistMemberId(Integer groupArtistId, Integer memberArtistId) {
            this.groupArtistId = groupArtistId;
            this.memberArtistId = memberArtistId;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ArtistMemberId that = (ArtistMemberId) o;
            return Objects.equals(groupArtistId, that.groupArtistId) && Objects.equals(memberArtistId, that.memberArtistId);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(groupArtistId, memberArtistId);
        }
    }
}
