package library.dto;

import library.util.Utils;

public class ArtistAlbumsQueryDTO {
	
	String artist;
	String album;
	String genre;
	String race;
	String sex;
	String language;
	int releaseYear;
	int albumLength;
	int totalPlays;
	int totalPlaytime;
	String firstPlay;
	String lastPlay;
	int numberOfTracks;
	int averageSongLength;
	double averagePlaysPerSong;
	int daysAlbumWasPlayed;
	int weeksAlbumWasPlayed;
	int monthsAlbumWasPlayed;
	String playsByAccount;


	public String getArtist() {
		return artist;
	}

	public void setArtist(String artist) {
		this.artist = artist;
	}

	public String getAlbum() {
		return album;
	}

	public void setAlbum(String album) {
		this.album = album;
	}
	
	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public String getRace() {
		return race;
	}

	public void setRace(String race) {
		this.race = race;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public int getAlbumLength() {
		return albumLength;
	}

	public void setAlbumLength(int albumLength) {
		this.albumLength = albumLength;
	}

	public int getTotalPlays() {
		return totalPlays;
	}

	public void setTotalPlays(int totalPlays) {
		this.totalPlays = totalPlays;
	}

	public String getFirstPlay() {
		return firstPlay;
	}

	public void setFirstPlay(String firstPlay) {
		this.firstPlay = firstPlay;
	}

	public String getLastPlay() {
		return lastPlay;
	}

	public void setLastPlay(String lastPlay) {
		this.lastPlay = lastPlay;
	}
	
	public int getTotalPlaytime() {
		return totalPlaytime;
	}

	public void setTotalPlaytime(int totalPlaytime) {
		this.totalPlaytime = totalPlaytime;
	}
	
	public int getNumberOfTracks() {
		return numberOfTracks;
	}

	public void setNumberOfTracks(int numberOfTracks) {
		this.numberOfTracks = numberOfTracks;
	}
	
	public int getAverageSongLength() {
		return averageSongLength;
	}

	public void setAverageSongLength(int averageSongLength) {
		this.averageSongLength = averageSongLength;
	}

	public double getAveragePlaysPerSong() {
		return averagePlaysPerSong;
	}

	public void setAveragePlaysPerSong(double averagePlaysPerSong) {
		this.averagePlaysPerSong = averagePlaysPerSong;
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

	public int getReleaseYear() {
		return releaseYear;
	}

	public void setReleaseYear(int releaseYear) {
		this.releaseYear = releaseYear;
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
	
	public String getPlaysByAccount() {
		return playsByAccount;
	}

	public void setPlaysByAccount(String playsByAccount) {
		this.playsByAccount = playsByAccount;
	}

	public String getAlbumLengthString() {
		return Utils.secondsToStringColon(albumLength);
	}
	
	public String getTotalPlaytimeString() {
		return Utils.secondsToString(totalPlaytime);
	}
	
	public String getAverageSongLengthString() {
		return Utils.secondsToStringColon(averageSongLength);
	}

	public ArtistAlbumsQueryDTO(String artist, String album, int albumLength, int totalPlays, String firstPlay,
			String lastPlay) {
		super();
		this.artist = artist;
		this.album = album;
		this.albumLength = albumLength;
		this.totalPlays = totalPlays;
		this.firstPlay = firstPlay;
		this.lastPlay = lastPlay;
	}

	public ArtistAlbumsQueryDTO() {

	}
	

}
