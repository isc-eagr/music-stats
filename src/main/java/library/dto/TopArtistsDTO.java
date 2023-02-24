package library.dto;

import library.util.Utils;

public class TopArtistsDTO {
	
	private String artist;
	
	private String genre;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private long playtime;
	
	private int averageSongLength;
	
	private int averagePlaysPerSong;
	
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
	
	public String getAverageSongLengthString() {
		return Utils.secondsToStringColon(averageSongLength);
	}
	
}
