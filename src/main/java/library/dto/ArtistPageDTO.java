package library.dto;

import java.util.List;

public class ArtistPageDTO {
	
	List<ArtistSongsQueryDTO> songs;
	List<ArtistAlbumsQueryDTO> albums;
	String firstSongPlayed;
	String lastSongPlayed;
	int totalPlays;
	String totalPlaytime;
	String averageSongLength;
	int averagePlaysPerSong;
	int numberOfSongs;
	int daysArtistWasPlayed;
	int weeksArtistWasPlayed;
	int monthsArtistWasPlayed;
	
	public List<ArtistSongsQueryDTO> getSongs() {
		return songs;
	}
	public void setSongs(List<ArtistSongsQueryDTO> songs) {
		this.songs = songs;
	}
	public List<ArtistAlbumsQueryDTO> getAlbums() {
		return albums;
	}
	public void setAlbums(List<ArtistAlbumsQueryDTO> albums) {
		this.albums = albums;
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
	public int getAveragePlaysPerSong() {
		return averagePlaysPerSong;
	}
	public void setAveragePlaysPerSong(int averagePlaysPerSong) {
		this.averagePlaysPerSong = averagePlaysPerSong;
	}
	public int getNumberOfSongs() {
		return numberOfSongs;
	}
	public void setNumberOfSongs(int numberOfSongs) {
		this.numberOfSongs = numberOfSongs;
	}
	public int getDaysArtistWasPlayed() {
		return daysArtistWasPlayed;
	}
	public void setDaysArtistWasPlayed(int daysArtistWasPlayed) {
		this.daysArtistWasPlayed = daysArtistWasPlayed;
	}
	public int getWeeksArtistWasPlayed() {
		return weeksArtistWasPlayed;
	}
	public void setWeeksArtistWasPlayed(int weeksArtistWasPlayed) {
		this.weeksArtistWasPlayed = weeksArtistWasPlayed;
	}
	public int getMonthsArtistWasPlayed() {
		return monthsArtistWasPlayed;
	}
	public void setMonthsArtistWasPlayed(int monthsArtistWasPlayed) {
		this.monthsArtistWasPlayed = monthsArtistWasPlayed;
	}
	public ArtistPageDTO(List<ArtistSongsQueryDTO> songs, List<ArtistAlbumsQueryDTO> albums, String firstSongPlayed,
			String lastSongPlayed, int totalPlays, String totalPlaytime, String averageSongLength,
			int averagePlaysPerSong, int numberOfSongs, int daysArtistWasPlayed, int weeksArtistWasPlayed,
			int monthsArtistWasPlayed) {
		super();
		this.songs = songs;
		this.albums = albums;
		this.firstSongPlayed = firstSongPlayed;
		this.lastSongPlayed = lastSongPlayed;
		this.totalPlays = totalPlays;
		this.totalPlaytime = totalPlaytime;
		this.averageSongLength = averageSongLength;
		this.averagePlaysPerSong = averagePlaysPerSong;
		this.numberOfSongs = numberOfSongs;
		this.daysArtistWasPlayed = daysArtistWasPlayed;
		this.weeksArtistWasPlayed = weeksArtistWasPlayed;
		this.monthsArtistWasPlayed = monthsArtistWasPlayed;
	}
	
}
