package library.dto;

import library.util.Utils;

public class TopSongsDTO {
	
	private String artist;
	
	private String song;
	
	private String album;
	
	private String genre;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private String year;
	
	private long playtime;
	
	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}

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
	
	
}
