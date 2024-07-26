package library.dto;

import java.util.List;

public class AlbumPageDTO {
	
	List<AlbumSongsQueryDTO> songs;
	String firstSongPlayed;
	String lastSongPlayed;
	int totalPlays;
	String totalPlaytime;
	String averageSongLength;
	double averagePlaysPerSong;
	int numberOfSongs;
	String albumLength;
	int daysAlbumWasPlayed;
	int weeksAlbumWasPlayed;
	int monthsAlbumWasPlayed;
	List<MilestoneDTO> milestones;
	String playsByAccount;
	
	
	public List<AlbumSongsQueryDTO> getSongs() {
		return songs;
	}
	public void setSongs(List<AlbumSongsQueryDTO> songs) {
		this.songs = songs;
	}
	public String getFirstSongPlayed() {
		return firstSongPlayed;
	}
	public void setFirstSongPlayed(String firstSongPlayed) {
		this.firstSongPlayed = firstSongPlayed;
	}
	public String getLastSongPlayed() {
		return lastSongPlayed;
	}
	public void setLastSongPlayed(String lastSongPlayed) {
		this.lastSongPlayed = lastSongPlayed;
	}
	public int getTotalPlays() {
		return totalPlays;
	}
	public void setTotalPlays(int totalPlays) {
		this.totalPlays = totalPlays;
	}
	public String getTotalPlaytime() {
		return totalPlaytime;
	}
	public void setTotalPlaytime(String totalPlaytime) {
		this.totalPlaytime = totalPlaytime;
	}
	public String getAverageSongLength() {
		return averageSongLength;
	}
	public void setAverageSongLength(String averageSongLength) {
		this.averageSongLength = averageSongLength;
	}
	public double getAveragePlaysPerSong() {
		return averagePlaysPerSong;
	}
	public void setAveragePlaysPerSong(double averagePlaysPerSong) {
		this.averagePlaysPerSong = averagePlaysPerSong;
	}
	public int getNumberOfSongs() {
		return numberOfSongs;
	}
	public void setNumberOfSongs(int numberOfSongs) {
		this.numberOfSongs = numberOfSongs;
	}
	public String getAlbumLength() {
		return albumLength;
	}
	public void setAlbumLength(String albumLength) {
		this.albumLength = albumLength;
	}
	
	public int getDaysAlbumWasPlayed() {
		return daysAlbumWasPlayed;
	}
	public void setDaysAlbumWasPlayed(int daysAlbumWasPlayed) {
		this.daysAlbumWasPlayed = daysAlbumWasPlayed;
	}
	
	public int getWeeksAlbumWasPlayed() {
		return weeksAlbumWasPlayed;
	}
	public void setWeeksAlbumWasPlayed(int weeksAlbumWasPlayed) {
		this.weeksAlbumWasPlayed = weeksAlbumWasPlayed;
	}
	public int getMonthsAlbumWasPlayed() {
		return monthsAlbumWasPlayed;
	}
	public void setMonthsAlbumWasPlayed(int monthsAlbumWasPlayed) {
		this.monthsAlbumWasPlayed = monthsAlbumWasPlayed;
	}
	public List<MilestoneDTO> getMilestones() {
		return milestones;
	}
	public void setMilestones(List<MilestoneDTO> milestones) {
		this.milestones = milestones;
	}
	public String getPlaysByAccount() {
		return playsByAccount;
	}
	public void setPlaysByAccount(String playsByAccount) {
		this.playsByAccount = playsByAccount;
	}
	public AlbumPageDTO(List<AlbumSongsQueryDTO> songs, String firstSongPlayed, String lastSongPlayed, int totalPlays,
			String totalPlaytime, String averageSongLength, double averagePlaysPerSong, int numberOfSongs,
			String albumLength, int daysAlbumWasPlayed, int weeksAlbumWasPlayed, int monthsAlbumWasPlayed,
			List<MilestoneDTO> milestones, String playsByAccount) {
		super();
		this.songs = songs;
		this.firstSongPlayed = firstSongPlayed;
		this.lastSongPlayed = lastSongPlayed;
		this.totalPlays = totalPlays;
		this.totalPlaytime = totalPlaytime;
		this.averageSongLength = averageSongLength;
		this.averagePlaysPerSong = averagePlaysPerSong;
		this.numberOfSongs = numberOfSongs;
		this.albumLength = albumLength;
		this.daysAlbumWasPlayed = daysAlbumWasPlayed;
		this.weeksAlbumWasPlayed = weeksAlbumWasPlayed;
		this.monthsAlbumWasPlayed = monthsAlbumWasPlayed;
		this.milestones = milestones;
		this.playsByAccount = playsByAccount;
	}
	

}
