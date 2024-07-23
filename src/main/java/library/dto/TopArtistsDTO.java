package library.dto;

import library.util.Utils;

public class TopArtistsDTO {
	
	private String artist;
	
	private String genre;
	
	private String race;
	
	private String sex;
	
	private String language;
	
	private int count;
	
	private String firstPlay;
	
	private String lastPlay;
	
	private long playtime;
	
	private int averageLength;
	
	private double averagePlays;
	
	private int numberOfAlbums;
	
	private int numberOfSongs;
	
	private long playDays;
	
	private long playWeeks;
	
	private long playMonths;
	
	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}

	public String getArtist() {
		return artist;
	}

	public void setArtist(String artist) {
		this.artist = artist;
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

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
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

	public long getPlaytime() {
		return playtime;
	}

	public void setPlaytime(long playtime) {
		this.playtime = playtime;
	}

	public int getAverageLength() {
		return averageLength;
	}

	public void setAverageLength(int averageLength) {
		this.averageLength = averageLength;
	}

	public double getAveragePlays() {
		return averagePlays;
	}

	public void setAveragePlays(double averagePlays) {
		this.averagePlays = averagePlays;
	}

	public int getNumberOfAlbums() {
		return numberOfAlbums;
	}

	public void setNumberOfAlbums(int numberOfAlbums) {
		this.numberOfAlbums = numberOfAlbums;
	}

	public int getNumberOfSongs() {
		return numberOfSongs;
	}

	public void setNumberOfSongs(int numberOfSongs) {
		this.numberOfSongs = numberOfSongs;
	}
	
	public long getPlayDays() {
		return playDays;
	}

	public void setPlayDays(long playDays) {
		this.playDays = playDays;
	}

	public long getPlayWeeks() {
		return playWeeks;
	}

	public void setPlayWeeks(long playWeeks) {
		this.playWeeks = playWeeks;
	}

	public long getPlayMonths() {
		return playMonths;
	}

	public void setPlayMonths(long playMonths) {
		this.playMonths = playMonths;
	}

	public String getAverageLengthString() {
		return Utils.secondsToStringColon(averageLength);
	}
	
}
