package library.dto;

import library.util.Utils;

public class GenrePageDTO {
	
	int totalPlays;
	int totalPlaytime;
	int averageSongLength;
	double averagePlaysPerSong;
	int numberOfSongs;
	int numberOfAlbums;
	int numberOfArtists;
	String firstPlay;
	String lastPlay;
	int daysGenreWasPlayed;
	int weeksGenreWasPlayed;
	int monthsGenreWasPlayed;
	String mostPlayedArtist;
	String mostPlayedAlbum;
	String mostPlayedSong;
	String genre;
	
	public int getTotalPlays() {
		return totalPlays;
	}


	public void setTotalPlays(int totalPlays) {
		this.totalPlays = totalPlays;
	}


	public int getTotalPlaytime() {
		return totalPlaytime;
	}


	public void setTotalPlaytime(int totalPlaytime) {
		this.totalPlaytime = totalPlaytime;
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


	public int getNumberOfSongs() {
		return numberOfSongs;
	}


	public void setNumberOfSongs(int numberOfSongs) {
		this.numberOfSongs = numberOfSongs;
	}

	public int getNumberOfAlbums() {
		return numberOfAlbums;
	}


	public void setNumberOfAlbums(int numberOfAlbums) {
		this.numberOfAlbums = numberOfAlbums;
	}


	public int getNumberOfArtists() {
		return numberOfArtists;
	}


	public void setNumberOfArtists(int numberOfArtists) {
		this.numberOfArtists = numberOfArtists;
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


	public int getDaysGenreWasPlayed() {
		return daysGenreWasPlayed;
	}


	public void setDaysGenreWasPlayed(int daysGenreWasPlayed) {
		this.daysGenreWasPlayed = daysGenreWasPlayed;
	}


	public int getWeeksGenreWasPlayed() {
		return weeksGenreWasPlayed;
	}


	public void setWeeksGenreWasPlayed(int weeksGenreWasPlayed) {
		this.weeksGenreWasPlayed = weeksGenreWasPlayed;
	}


	public int getMonthsGenreWasPlayed() {
		return monthsGenreWasPlayed;
	}


	public void setMonthsGenreWasPlayed(int monthsGenreWasPlayed) {
		this.monthsGenreWasPlayed = monthsGenreWasPlayed;
	}


	public String getMostPlayedArtist() {
		return mostPlayedArtist;
	}


	public void setMostPlayedArtist(String mostPlayedArtist) {
		this.mostPlayedArtist = mostPlayedArtist;
	}


	public String getMostPlayedAlbum() {
		return mostPlayedAlbum;
	}


	public void setMostPlayedAlbum(String mostPlayedAlbum) {
		this.mostPlayedAlbum = mostPlayedAlbum;
	}


	public String getMostPlayedSong() {
		return mostPlayedSong;
	}


	public void setMostPlayedSong(String mostPlayedSong) {
		this.mostPlayedSong = mostPlayedSong;
	}
	
	public String getGenre() {
		return genre;
	}


	public void setGenre(String genre) {
		this.genre = genre;
	}


	public String getTotalPlaytimeString() {
		return Utils.secondsToString(totalPlaytime);
	}

	public String getAverageSongLengthString() {
		return Utils.secondsToStringColon(averageSongLength);
	}
	
	

	public GenrePageDTO(int totalPlays, int totalPlaytime, int averageSongLength, double averagePlaysPerSong,
			int numberOfSongs, int numberOfAlbums, int numberOfArtists, String firstPlay, String lastPlay,
			int daysGenreWasPlayed, int weeksGenreWasPlayed, int monthsGenreWasPlayed, String mostPlayedArtist,
			String mostPlayedAlbum, String mostPlayedSong, String genre) {
		super();
		this.totalPlays = totalPlays;
		this.totalPlaytime = totalPlaytime;
		this.averageSongLength = averageSongLength;
		this.averagePlaysPerSong = averagePlaysPerSong;
		this.numberOfSongs = numberOfSongs;
		this.numberOfAlbums = numberOfAlbums;
		this.numberOfArtists = numberOfArtists;
		this.firstPlay = firstPlay;
		this.lastPlay = lastPlay;
		this.daysGenreWasPlayed = daysGenreWasPlayed;
		this.weeksGenreWasPlayed = weeksGenreWasPlayed;
		this.monthsGenreWasPlayed = monthsGenreWasPlayed;
		this.mostPlayedArtist = mostPlayedArtist;
		this.mostPlayedAlbum = mostPlayedAlbum;
		this.mostPlayedSong = mostPlayedSong;
		this.genre = genre;
	}


	public GenrePageDTO() {

	}
	

}
