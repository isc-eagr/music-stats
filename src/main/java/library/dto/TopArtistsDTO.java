package library.dto;

import library.util.Utils;

public class TopArtistsDTO {
	
	private String artist;
	
	private String genre;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private long playtime;
	
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

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public long getPlaytime() {
		return playtime;
	}

	public void setPlaytime(long playtime) {
		this.playtime = playtime;
	}

	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}
	
}
