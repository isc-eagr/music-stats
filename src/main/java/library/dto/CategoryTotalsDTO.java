package library.dto;

public class CategoryTotalsDTO {
    private int totalPlays;
    private int totalPlaytime;
    private int numberOfArtists;
    private int numberOfAlbums;
    private int numberOfSongs;
    private int daysCategoryWasPlayed;
    private int weeksCategoryWasPlayed;
    private int monthsCategoryWasPlayed;
    private int averageSongLength;
    private String playsByAccount;

    public int getTotalPlays() { return totalPlays; }
    public void setTotalPlays(int totalPlays) { this.totalPlays = totalPlays; }

    public int getTotalPlaytime() { return totalPlaytime; }
    public void setTotalPlaytime(int totalPlaytime) { this.totalPlaytime = totalPlaytime; }

    public int getNumberOfArtists() { return numberOfArtists; }
    public void setNumberOfArtists(int numberOfArtists) { this.numberOfArtists = numberOfArtists; }

    public int getNumberOfAlbums() { return numberOfAlbums; }
    public void setNumberOfAlbums(int numberOfAlbums) { this.numberOfAlbums = numberOfAlbums; }

    public int getNumberOfSongs() { return numberOfSongs; }
    public void setNumberOfSongs(int numberOfSongs) { this.numberOfSongs = numberOfSongs; }

    public int getDaysCategoryWasPlayed() { return daysCategoryWasPlayed; }
    public void setDaysCategoryWasPlayed(int daysCategoryWasPlayed) { this.daysCategoryWasPlayed = daysCategoryWasPlayed; }

    public int getWeeksCategoryWasPlayed() { return weeksCategoryWasPlayed; }
    public void setWeeksCategoryWasPlayed(int weeksCategoryWasPlayed) { this.weeksCategoryWasPlayed = weeksCategoryWasPlayed; }

    public int getMonthsCategoryWasPlayed() { return monthsCategoryWasPlayed; }
    public void setMonthsCategoryWasPlayed(int monthsCategoryWasPlayed) { this.monthsCategoryWasPlayed = monthsCategoryWasPlayed; }

    public int getAverageSongLength() { return averageSongLength; }
    public void setAverageSongLength(int averageSongLength) { this.averageSongLength = averageSongLength; }

    public String getPlaysByAccount() { return playsByAccount; }
    public void setPlaysByAccount(String playsByAccount) { this.playsByAccount = playsByAccount; }
}