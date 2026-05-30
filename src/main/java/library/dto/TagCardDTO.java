package library.dto;

public class TagCardDTO {
    private Integer id;
    private String name;
    private int artistCount;
    private int albumCount;
    private int songCount;
    private int maleArtistCount;
    private int femaleArtistCount;
    private int otherArtistCount;
    private int maleAlbumCount;
    private int femaleAlbumCount;
    private int otherAlbumCount;
    private int maleSongCount;
    private int femaleSongCount;
    private int otherSongCount;
    private Integer topArtistId;
    private String topArtistName;
    private Integer topArtistGenderId;
    private Integer topAlbumId;
    private String topAlbumName;
    private String topAlbumArtistName;
    private Integer topAlbumGenderId;
    private Integer topSongId;
    private String topSongName;
    private String topSongArtistName;
    private Integer topSongGenderId;

    public TagCardDTO() {
    }

    public TagCardDTO(Integer id, String name, int artistCount, int albumCount, int songCount) {
        this.id = id;
        this.name = name;
        this.artistCount = artistCount;
        this.albumCount = albumCount;
        this.songCount = songCount;
    }

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

    public int getArtistCount() {
        return artistCount;
    }

    public void setArtistCount(int artistCount) {
        this.artistCount = artistCount;
    }

    public int getAlbumCount() {
        return albumCount;
    }

    public void setAlbumCount(int albumCount) {
        this.albumCount = albumCount;
    }

    public int getSongCount() {
        return songCount;
    }

    public void setSongCount(int songCount) {
        this.songCount = songCount;
    }

    public int getMaleArtistCount() {
        return maleArtistCount;
    }

    public void setMaleArtistCount(int maleArtistCount) {
        this.maleArtistCount = maleArtistCount;
    }

    public int getFemaleArtistCount() {
        return femaleArtistCount;
    }

    public void setFemaleArtistCount(int femaleArtistCount) {
        this.femaleArtistCount = femaleArtistCount;
    }

    public int getOtherArtistCount() {
        return otherArtistCount;
    }

    public void setOtherArtistCount(int otherArtistCount) {
        this.otherArtistCount = otherArtistCount;
    }

    public int getMaleAlbumCount() {
        return maleAlbumCount;
    }

    public void setMaleAlbumCount(int maleAlbumCount) {
        this.maleAlbumCount = maleAlbumCount;
    }

    public int getFemaleAlbumCount() {
        return femaleAlbumCount;
    }

    public void setFemaleAlbumCount(int femaleAlbumCount) {
        this.femaleAlbumCount = femaleAlbumCount;
    }

    public int getOtherAlbumCount() {
        return otherAlbumCount;
    }

    public void setOtherAlbumCount(int otherAlbumCount) {
        this.otherAlbumCount = otherAlbumCount;
    }

    public int getMaleSongCount() {
        return maleSongCount;
    }

    public void setMaleSongCount(int maleSongCount) {
        this.maleSongCount = maleSongCount;
    }

    public int getFemaleSongCount() {
        return femaleSongCount;
    }

    public void setFemaleSongCount(int femaleSongCount) {
        this.femaleSongCount = femaleSongCount;
    }

    public int getOtherSongCount() {
        return otherSongCount;
    }

    public void setOtherSongCount(int otherSongCount) {
        this.otherSongCount = otherSongCount;
    }

    public Integer getTopArtistId() {
        return topArtistId;
    }

    public void setTopArtistId(Integer topArtistId) {
        this.topArtistId = topArtistId;
    }

    public String getTopArtistName() {
        return topArtistName;
    }

    public void setTopArtistName(String topArtistName) {
        this.topArtistName = topArtistName;
    }

    public Integer getTopArtistGenderId() {
        return topArtistGenderId;
    }

    public void setTopArtistGenderId(Integer topArtistGenderId) {
        this.topArtistGenderId = topArtistGenderId;
    }

    public Integer getTopAlbumId() {
        return topAlbumId;
    }

    public void setTopAlbumId(Integer topAlbumId) {
        this.topAlbumId = topAlbumId;
    }

    public String getTopAlbumName() {
        return topAlbumName;
    }

    public void setTopAlbumName(String topAlbumName) {
        this.topAlbumName = topAlbumName;
    }

    public String getTopAlbumArtistName() {
        return topAlbumArtistName;
    }

    public void setTopAlbumArtistName(String topAlbumArtistName) {
        this.topAlbumArtistName = topAlbumArtistName;
    }

    public Integer getTopAlbumGenderId() {
        return topAlbumGenderId;
    }

    public void setTopAlbumGenderId(Integer topAlbumGenderId) {
        this.topAlbumGenderId = topAlbumGenderId;
    }

    public Integer getTopSongId() {
        return topSongId;
    }

    public void setTopSongId(Integer topSongId) {
        this.topSongId = topSongId;
    }

    public String getTopSongName() {
        return topSongName;
    }

    public void setTopSongName(String topSongName) {
        this.topSongName = topSongName;
    }

    public String getTopSongArtistName() {
        return topSongArtistName;
    }

    public void setTopSongArtistName(String topSongArtistName) {
        this.topSongArtistName = topSongArtistName;
    }

    public Integer getTopSongGenderId() {
        return topSongGenderId;
    }

    public void setTopSongGenderId(Integer topSongGenderId) {
        this.topSongGenderId = topSongGenderId;
    }
}
