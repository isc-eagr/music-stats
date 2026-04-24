package library.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "pc_debut")
public class PcDebut {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "days_on_countdown", nullable = false)
    private Integer daysOnCountdown;

    @Column(name = "song_title", nullable = false, length = 500)
    private String songTitle;

    @Column(name = "artist_name", nullable = false, length = 500)
    private String artistName;

    @Column(name = "song_id")
    private Integer songId;

    @Column(name = "retired", nullable = false)
    private boolean retired;

    @Transient
    private String debutDate;

    @Transient
    private Integer debutPosition;

    @Transient
    private Integer peakPosition;

    @Transient
    private Integer daysAtPeak;

    @Transient
    private Integer actualDays;

    @Transient
    private String lastAppearanceDate;

    @Transient
    private String songPath;

    @Transient
    private String artistPath;

    @Transient
    private String genderClass;

    @Transient
    private Integer resolvedArtistId;

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public Integer getDaysOnCountdown() { return daysOnCountdown; }
    public void setDaysOnCountdown(Integer daysOnCountdown) { this.daysOnCountdown = daysOnCountdown; }

    public String getSongTitle() { return songTitle; }
    public void setSongTitle(String songTitle) { this.songTitle = songTitle; }

    public String getArtistName() { return artistName; }
    public void setArtistName(String artistName) { this.artistName = artistName; }

    public Integer getSongId() { return songId; }
    public void setSongId(Integer songId) { this.songId = songId; }

    public boolean isRetired() { return retired; }
    public void setRetired(boolean retired) { this.retired = retired; }

    public String getDebutDate() { return debutDate; }
    public void setDebutDate(String debutDate) { this.debutDate = debutDate; }

    public Integer getDebutPosition() { return debutPosition; }
    public void setDebutPosition(Integer debutPosition) { this.debutPosition = debutPosition; }

    public Integer getPeakPosition() { return peakPosition; }
    public void setPeakPosition(Integer peakPosition) { this.peakPosition = peakPosition; }

    public Integer getDaysAtPeak() { return daysAtPeak; }
    public void setDaysAtPeak(Integer daysAtPeak) { this.daysAtPeak = daysAtPeak; }

    public Integer getActualDays() { return actualDays; }
    public void setActualDays(Integer actualDays) { this.actualDays = actualDays; }

    public String getLastAppearanceDate() { return lastAppearanceDate; }
    public void setLastAppearanceDate(String lastAppearanceDate) { this.lastAppearanceDate = lastAppearanceDate; }

    public String getSongPath() { return songPath; }
    public void setSongPath(String songPath) { this.songPath = songPath; }

    public String getArtistPath() { return artistPath; }
    public void setArtistPath(String artistPath) { this.artistPath = artistPath; }

    public String getGenderClass() { return genderClass; }
    public void setGenderClass(String genderClass) { this.genderClass = genderClass; }

    public Integer getResolvedArtistId() { return resolvedArtistId; }
    public void setResolvedArtistId(Integer resolvedArtistId) { this.resolvedArtistId = resolvedArtistId; }
}
