package library.dto;

import library.util.Utils;

public class AlbumSongsQueryDTO {
	
	String artist;
	String song;
	String album;
	Integer releaseYear;
	String genre;
	String race;
	String sex;
	String language;
	int year;
	int trackLength;
	int totalPlays;
	String firstPlay;
	String lastPlay;
	int numberOfTracks;
	int daysSongWasPlayed;
	int weeksSongWasPlayed;
	int monthsSongWasPlayed;


	public String getArtist() {
		return artist;
	}


	public void setArtist(String artist) {
		this.artist = artist;
	}

	public String getSong() {
		return song;
	}


	public void setSong(String song) {
		this.song = song;
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


	public int getYear() {
		return year;
	}


	public void setYear(int year) {
		this.year = year;
	}


	public String getAlbum() {
		return album;
	}


	public void setAlbum(String album) {
		this.album = album;
	}
	
	
	public Integer getReleaseYear() {
		return releaseYear;
	}


	public void setReleaseYear(Integer releaseYear) {
		this.releaseYear = releaseYear;
	}


	public int getTrackLength() {
		return trackLength;
	}


	public void setTrackLength(int trackLength) {
		this.trackLength = trackLength;
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
		return trackLength*totalPlays;
	}


	public int getNumberOfTracks() {
		return numberOfTracks;
	}

	public void setNumberOfTracks(int numberOfTracks) {
		this.numberOfTracks = numberOfTracks;
	}
	
	public int getDaysSongWasPlayed() {
		return daysSongWasPlayed;
	}

	public void setDaysSongWasPlayed(int daysSongWasPlayed) {
		this.daysSongWasPlayed = daysSongWasPlayed;
	}

	public int getWeeksSongWasPlayed() {
		return weeksSongWasPlayed;
	}


	public void setWeeksSongWasPlayed(int weeksSongWasPlayed) {
		this.weeksSongWasPlayed = weeksSongWasPlayed;
	}


	public int getMonthsSongWasPlayed() {
		return monthsSongWasPlayed;
	}


	public void setMonthsSongWasPlayed(int monthsSongWasPlayed) {
		this.monthsSongWasPlayed = monthsSongWasPlayed;
	}


	public String getTrackLengthString() {
		return Utils.secondsToStringColon(trackLength);
	}
	
	public String getTotalPlaytimeString() {
		return Utils.secondsToString(trackLength*totalPlays);
	}


	public AlbumSongsQueryDTO(String artist, String song, String album, int trackLength, int totalPlays, String firstPlay,
			String lastPlay) {
		super();
		this.artist = artist;
		this.song = song;
		this.album = album;
		this.trackLength = trackLength;
		this.totalPlays = totalPlays;
		this.firstPlay = firstPlay;
		this.lastPlay = lastPlay;
	}


	public AlbumSongsQueryDTO() {

	}
	

}
