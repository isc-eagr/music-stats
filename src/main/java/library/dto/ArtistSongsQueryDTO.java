package library.dto;

import library.util.Utils;

public class ArtistSongsQueryDTO {
	
	String artist;
	String song;
	String album;
	int releaseYear;
	int trackLength;
	int totalPlays;
	String firstPlay;
	String lastPlay;
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
	public String getAlbum() {
		return album;
	}
	public void setAlbum(String album) {
		this.album = album;
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
	public int getReleaseYear() {
		return releaseYear;
	}
	public void setReleaseYear(int releaseYear) {
		this.releaseYear = releaseYear;
	}
	public long getTotalPlaytime() {
		return trackLength*totalPlays;
	}
	public String getTrackLengthString() {
		return Utils.secondsToStringColon(trackLength);
	}
	public String getTotalPlaytimeString() {
		return Utils.secondsToString(trackLength*totalPlays);
	}
	public ArtistSongsQueryDTO(String artist, String song, String album, int trackLength, int totalPlays, String firstPlay,
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
	public ArtistSongsQueryDTO() {

	}
	
	

}
