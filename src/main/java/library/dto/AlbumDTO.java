package library.dto;

import java.util.List;

public class AlbumDTO {
	
	List<AlbumSongsQueryDTO> songs;
	String firstSongPlayed;
	String lastSongPlayed;
	int totalPlays;
	String totalPlaytime;
	String averageSongLength;
	int averagePlaysPerSong;
	int numberOfSongs;
	String albumLength;
	
	
	public List<AlbumSongsQueryDTO> getSongs() {
		return songs;
	}
	public void setSongs(List<AlbumSongsQueryDTO> songs) {
		this.songs = songs;
	}
	public String getFirstSongPlayed() {
		return firstSongPlayed;
	}
	public void setFirstSongPlayed(String firstSongPlayed) {
		this.firstSongPlayed = firstSongPlayed;
	}
	public String getLastSongPlayed() {
		return lastSongPlayed;
	}
	public void setLastSongPlayed(String lastSongPlayed) {
		this.lastSongPlayed = lastSongPlayed;
	}
	public int getTotalPlays() {
		return totalPlays;
	}
	public void setTotalPlays(int totalPlays) {
		this.totalPlays = totalPlays;
	}
	public String getTotalPlaytime() {
		return totalPlaytime;
	}
	public void setTotalPlaytime(String totalPlaytime) {
		this.totalPlaytime = totalPlaytime;
	}
	public String getAverageSongLength() {
		return averageSongLength;
	}
	public void setAverageSongLength(String averageSongLength) {
		this.averageSongLength = averageSongLength;
	}
	public int getAveragePlaysPerSong() {
		return averagePlaysPerSong;
	}
	public void setAveragePlaysPerSong(int averagePlaysPerSong) {
		this.averagePlaysPerSong = averagePlaysPerSong;
	}
	public int getNumberOfSongs() {
		return numberOfSongs;
	}
	public void setNumberOfSongs(int numberOfSongs) {
		this.numberOfSongs = numberOfSongs;
	}
	public String getAlbumLength() {
		return albumLength;
	}
	public void setAlbumLength(String albumLength) {
		this.albumLength = albumLength;
	}
	public AlbumDTO(List<AlbumSongsQueryDTO> songs, String firstSongPlayed, String lastSongPlayed, int totalPlays,
			String totalPlaytime, String averageSongLength, int averagePlaysPerSong, int numberOfSongs, String albumLength) {
		super();
		this.songs = songs;
		this.firstSongPlayed = firstSongPlayed;
		this.lastSongPlayed = lastSongPlayed;
		this.totalPlays = totalPlays;
		this.totalPlaytime = totalPlaytime;
		this.averageSongLength = averageSongLength;
		this.averagePlaysPerSong = averagePlaysPerSong;
		this.numberOfSongs = numberOfSongs;
		this.albumLength = albumLength;
	}
	

}
