package library.dto;

import library.util.Utils;

public class TopArtistsDTO {
	
	private String artist;
	
	private String genre;
	
	private String race;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private long playtime;
	
	private int averageLength;
	
	private double averagePlays;
	
	private int numberOfAlbums;
	
	private int numberOfSongs;
	
	private int playDays;
	
	private int playWeeks;
	
	private int playMonths;
	
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

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
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
	
	public int getPlayDays() {
		return playDays;
	}

	public void setPlayDays(int playDays) {
		this.playDays = playDays;
	}

	public int getPlayWeeks() {
		return playWeeks;
	}

	public void setPlayWeeks(int playWeeks) {
		this.playWeeks = playWeeks;
	}

	public int getPlayMonths() {
		return playMonths;
	}

	public void setPlayMonths(int playMonths) {
		this.playMonths = playMonths;
	}

	public String getAverageLengthString() {
		return Utils.secondsToStringColon(averageLength);
	}
	
}
