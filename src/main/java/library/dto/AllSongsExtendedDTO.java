package library.dto;

public class AllSongsExtendedDTO {
	
	String artist;
	String song;
	String album;
	String genre;
	String sex;
	String language;
	int plays;
	int duration;
	int playtime;
	
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
	public int getPlays() {
		return plays;
	}
	public void setPlays(int plays) {
		this.plays = plays;
	}
	public int getDuration() {
		return duration;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public int getPlaytime() {
		return playtime;
	}
	public void setPlaytime(int playtime) {
		this.playtime = playtime;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	
}
