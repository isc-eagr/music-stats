package library.dto;

import library.util.Utils;

public class TopAlbumsDTO {
	
	private String artist;
	
	private String album;
	
	private String genre;
	
	private String race;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private String year;
	
	private long playtime;
	
	private int length;
	
	private int averageLength;
	
	private double averagePlays;
	
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

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public long getPlaytime() {
		return playtime;
	}

	public void setPlaytime(long playtime) {
		this.playtime = playtime;
	}
	
	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
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
	
	public String getLengthString() {
		return Utils.secondsToStringColon(length);
	}
	

}
