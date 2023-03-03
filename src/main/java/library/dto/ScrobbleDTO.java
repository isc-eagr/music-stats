package library.dto;

import java.util.Objects;

public class ScrobbleDTO {
	
	private String artist;
	private String album;
	private String song;
	private int trackLength;
	private String scrobbleDate;
	private String genre;
	private int year;
	private String language;
	private String sex;
	private String week;
	
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
	public String getScrobbleDate() {
		return scrobbleDate;
	}
	public void setScrobbleDate(String scrobbleDate) {
		this.scrobbleDate = scrobbleDate;
	}
	public String getGenre() {
		return genre;
	}
	public void setGenre(String genre) {
		this.genre = genre;
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
	@Override
	public String toString() {
		return "ArtistScrobblesDTO [artist=" + artist + ", album=" + album + ", song=" + song + ", trackLength="
				+ trackLength + ", genre=" + genre + ", year=" + year + ", language=" + language + ", sex=" + sex + "]";
	}
	@Override
	public int hashCode() {
		return Objects.hash(album, artist, genre, language, sex, song, trackLength, year);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ScrobbleDTO other = (ScrobbleDTO) obj;
		return Objects.equals(album, other.album) && Objects.equals(artist, other.artist)
				&& Objects.equals(genre, other.genre) && Objects.equals(language, other.language)
				&& Objects.equals(sex, other.sex) && Objects.equals(song, other.song)
				&& trackLength == other.trackLength && year == other.year;
	}
	


}
