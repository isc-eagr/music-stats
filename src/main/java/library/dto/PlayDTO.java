package library.dto;

import java.util.Objects;

public class PlayDTO {
	
	private String artist;
	private String album;
	private String song;
	private int trackLength;
	private String playDate;
	private String genre;
	private String race;
	private int year;
	private String language;
	private String sex;
	private String week;
	private String cloudStatus;
	private String mainOrFeature;
	private int id;
	
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
	public String getSong() {
		return song;
	}
	public void setSong(String song) {
		this.song = song;
	}
	public int getTrackLength() {
		return trackLength;
	}
	public void setTrackLength(int trackLength) {
		this.trackLength = trackLength;
	}
	
	public String getPlayDate() {
		return playDate;
	}
	public void setPlayDate(String playDate) {
		this.playDate = playDate;
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
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public String getWeek() {
		return week;
	}
	public void setWeek(String week) {
		this.week = week;
	}
	
	public String getCloudStatus() {
		return cloudStatus;
	}
	public void setCloudStatus(String cloudStatus) {
		this.cloudStatus = cloudStatus;
	}
	public String getMainOrFeature() {
		return mainOrFeature;
	}
	public void setMainOrFeature(String mainOrFeature) {
		this.mainOrFeature = mainOrFeature;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	@Override
	public String toString() {
		return "PlayDTO [artist=" + artist + ", album=" + album + ", song=" + song + ", trackLength=" + trackLength
				+ ", playDate=" + playDate + ", genre=" + genre + ", race=" + race + ", year=" + year + ", language="
				+ language + ", sex=" + sex + ", week=" + week + "]";
	}
	@Override
	public int hashCode() {
		return Objects.hash(album, artist, genre, language, race, sex, song, trackLength);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PlayDTO other = (PlayDTO) obj;
		return Objects.equals(album, other.album) && Objects.equals(artist, other.artist)
				&& Objects.equals(genre, other.genre) && Objects.equals(language, other.language)
				&& Objects.equals(race, other.race)
				&& Objects.equals(sex, other.sex) && Objects.equals(song, other.song)
				&& trackLength == other.trackLength;
	}
	

}
