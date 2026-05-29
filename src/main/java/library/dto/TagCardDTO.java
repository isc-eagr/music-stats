package library.dto;

public class TagCardDTO {
    private Integer id;
    private String name;
    private int artistCount;
    private int albumCount;
    private int songCount;

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
}
