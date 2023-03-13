package library.dto;

import library.util.Utils;

public class TopAlbumsDTO {
	
	private String artist;
	
	private String album;
	
	private String genre;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private String year;
	
	private long playtime;
	
	private int averageLength;
	
	private double averagePlays;
	
	private int numberOfSongs;
	
	private int scrobbleDays;
	
	private int scrobbleWeeks;
	
	private int scrobbleMonths;

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
	
	public int getScrobbleDays() {
		return scrobbleDays;
	}

	public void setScrobbleDays(int scrobbleDays) {
		this.scrobbleDays = scrobbleDays;
	}

	public int getScrobbleWeeks() {
		return scrobbleWeeks;
	}

	public void setScrobbleWeeks(int scrobbleWeeks) {
		this.scrobbleWeeks = scrobbleWeeks;
	}

	public int getScrobbleMonths() {
		return scrobbleMonths;
	}

	public void setScrobbleMonths(int scrobbleMonths) {
		this.scrobbleMonths = scrobbleMonths;
	}

	public String getAverageLengthString() {
		return Utils.secondsToStringColon(averageLength);
	}
	

}
