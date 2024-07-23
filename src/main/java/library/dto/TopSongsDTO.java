package library.dto;

import library.util.Utils;

public class TopSongsDTO {
	
	private int id;
	
	private String artist;
	
	private String song;
	
	private String album;
	
	private String genre;
	
	private String race;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private String firstPlay;
	
	private String lastPlay;
	
	private String year;
	
	private String cloudStatus;
	
	private int length;
	
	private long playtime;
	
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

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}
	
	public String getCloudStatus() {
		return cloudStatus;
	}

	public void setCloudStatus(String cloudStatus) {
		this.cloudStatus = cloudStatus;
	}

	public long getPlaytime() {
		return playtime;
	}
	
	public void setPlaytime(long playtime) {
		this.playtime = playtime;
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

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}
	
	public String getLengthString() {
		return Utils.secondsToStringColon(length);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
}
