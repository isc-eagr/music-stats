package library.dto;

public class GroupAccountCountAlbumDTO {
    private String artist;
    private String album;
    private String account;
    private int count;

    public String getArtist() { return artist; }
    public void setArtist(String artist) { this.artist = artist; }

    public String getAlbum() { return album; }
    public void setAlbum(String album) { this.album = album; }

    public String getAccount() { return account; }
    public void setAccount(String account) { this.account = account; }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }
}
