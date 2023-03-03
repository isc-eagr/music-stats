package library.dto;

import library.util.Utils;

public class ArtistAlbumsQueryDTO {
	
	String artist;
	String album;
	int albumLength;
	int totalPlays;
	int totalPlaytime;
	String firstPlay;
	String lastPlay;
	int numberOfTracks;
	int averageSongLength;
	int averagePlaysPerSong;
	int daysAlbumWasPlayed;
	int weeksAlbumWasPlayed;
	int monthsAlbumWasPlayed;


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

	public int getAveragePlaysPerSong() {
		return averagePlaysPerSong;
	}


	public void setAveragePlaysPerSong(int averagePlaysPerSong) {
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


	public void setWeeksAlbumWasPlayed(int weeksAlbumWasPlayed) {
		this.weeksAlbumWasPlayed = weeksAlbumWasPlayed;
	}


	public int getMonthsAlbumWasPlayed() {
		return monthsAlbumWasPlayed;
	}


	public void setMonthsAlbumWasPlayed(int monthsAlbumWasPlayed) {
		this.monthsAlbumWasPlayed = monthsAlbumWasPlayed;
	}


	public String getAlbumLengthString() {
		return Utils.secondsToStringColon(albumLength);
	}
	
	public String getTotalPlaytimeString() {
		return Utils.secondsToString(totalPlaytime);
	}
	
	public String getAverageSongLengthString() {
		return Utils.secondsToString(averageSongLength);
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
