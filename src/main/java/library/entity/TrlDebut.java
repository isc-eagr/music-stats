package library.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "trl_debut")
public class TrlDebut {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "days_on_countdown", nullable = false)
    private Integer daysOnCountdown;

    @Transient
    private String debutDate;

    @Transient
    private Integer debutPosition;

    @Column(name = "song_title", nullable = false, length = 500)
    private String songTitle;

    @Column(name = "artist_name", nullable = false, length = 500)
    private String artistName;

    @Column(name = "song_id")
    private Integer songId;

    @Column(name = "retired", nullable = false)
    private boolean retired;

    @Transient
    private Integer peakPosition;

    @Transient
    private Integer daysAtPeak;

    @Transient
    private String peakDate;

    @Transient
    private Integer actualDays;

    @Transient
    private String lastAppearanceDate;

    // Transient: populated by service from joined Song
    @Transient
    private String songPath;

    @Transient
    private String artistPath;

    @Transient
    private String genderClass; // "gender-male" or "gender-female" or null

    @Transient
    private boolean hasImage;

    @Transient
    private Integer albumId;

    @Transient
    private boolean albumHasImage;

    @Transient
    private boolean artistHasImage;

    @Transient
    private Integer resolvedArtistId; // artist_id from Song join

    @Transient
    private int daysAtTop1;

    @Transient
    private int daysAtTop5;

    @Transient
    private int daysAtTop10;

    // ---- Getters / Setters ----

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public Integer getDaysOnCountdown() { return daysOnCountdown; }
    public void setDaysOnCountdown(Integer daysOnCountdown) { this.daysOnCountdown = daysOnCountdown; }

    public String getDebutDate() { return debutDate; }
    public void setDebutDate(String debutDate) { this.debutDate = debutDate; }

    public Integer getDebutPosition() { return debutPosition; }
    public void setDebutPosition(Integer debutPosition) { this.debutPosition = debutPosition; }

    public String getSongTitle() { return songTitle; }
    public void setSongTitle(String songTitle) { this.songTitle = songTitle; }

    public String getArtistName() { return artistName; }
    public void setArtistName(String artistName) { this.artistName = artistName; }

    public Integer getSongId() { return songId; }
    public void setSongId(Integer songId) { this.songId = songId; }

    public boolean isRetired() { return retired; }
    public void setRetired(boolean retired) { this.retired = retired; }

    public Integer getPeakPosition() { return peakPosition; }
    public void setPeakPosition(Integer peakPosition) { this.peakPosition = peakPosition; }

    public Integer getDaysAtPeak() { return daysAtPeak; }
    public void setDaysAtPeak(Integer daysAtPeak) { this.daysAtPeak = daysAtPeak; }

    public String getPeakDate() { return peakDate; }
    public void setPeakDate(String peakDate) { this.peakDate = peakDate; }

    public Integer getActualDays() { return actualDays; }
    public void setActualDays(Integer actualDays) { this.actualDays = actualDays; }

    public String getSongPath() { return songPath; }
    public void setSongPath(String songPath) { this.songPath = songPath; }

    public String getArtistPath() { return artistPath; }
    public void setArtistPath(String artistPath) { this.artistPath = artistPath; }

    public String getGenderClass() { return genderClass; }
    public void setGenderClass(String genderClass) { this.genderClass = genderClass; }

    public boolean isHasImage() { return hasImage; }
    public void setHasImage(boolean hasImage) { this.hasImage = hasImage; }

    public Integer getAlbumId() { return albumId; }
    public void setAlbumId(Integer albumId) { this.albumId = albumId; }

    public boolean isAlbumHasImage() { return albumHasImage; }
    public void setAlbumHasImage(boolean albumHasImage) { this.albumHasImage = albumHasImage; }

    public boolean isArtistHasImage() { return artistHasImage; }
    public void setArtistHasImage(boolean artistHasImage) { this.artistHasImage = artistHasImage; }

    public String getLastAppearanceDate() { return lastAppearanceDate; }
    public void setLastAppearanceDate(String lastAppearanceDate) { this.lastAppearanceDate = lastAppearanceDate; }

    public Integer getResolvedArtistId() { return resolvedArtistId; }
    public void setResolvedArtistId(Integer resolvedArtistId) { this.resolvedArtistId = resolvedArtistId; }

    public int getDaysAtTop1() { return daysAtTop1; }
    public void setDaysAtTop1(int daysAtTop1) { this.daysAtTop1 = daysAtTop1; }

    public int getDaysAtTop5() { return daysAtTop5; }
    public void setDaysAtTop5(int daysAtTop5) { this.daysAtTop5 = daysAtTop5; }

    public int getDaysAtTop10() { return daysAtTop10; }
    public void setDaysAtTop10(int daysAtTop10) { this.daysAtTop10 = daysAtTop10; }
}
