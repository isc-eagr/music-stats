package library.dto;

public class TrlChartEntryGroupDTO {
    private String artistName;
    private String songTitle;
    private int appearances;

    public TrlChartEntryGroupDTO() {}

    public TrlChartEntryGroupDTO(String artistName, String songTitle, int appearances) {
        this.artistName = artistName;
        this.songTitle = songTitle;
        this.appearances = appearances;
    }

    public String getArtistName() { return artistName; }
    public void setArtistName(String artistName) { this.artistName = artistName; }

    public String getSongTitle() { return songTitle; }
    public void setSongTitle(String songTitle) { this.songTitle = songTitle; }

    public int getAppearances() { return appearances; }
    public void setAppearances(int appearances) { this.appearances = appearances; }
}
