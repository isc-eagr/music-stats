package library.dto;

import java.util.List;

import library.util.Utils;

public class TimeUnitDetailDTO {
	
	private int totalPlays;
	private long totalPlaytime;
	private String mostPlayedArtist;
	private String mostPlayedAlbum;
	private String mostPlayedSong;
	private int uniqueArtistsPlayed;
	private int uniqueAlbumsPlayed;
	private int uniqueSongsPlayed;
	private double percentageofUnitWhereMusicWasPlayed;
	private List<AlbumSongsQueryDTO> mostPlayedSongs;
	
	public int getTotalPlays() {
		return totalPlays;
	}
	public void setTotalPlays(int totalPlays) {
		this.totalPlays = totalPlays;
	}
	public long getTotalPlaytime() {
		return totalPlaytime;
	}
	public void setTotalPlaytime(long totalPlaytime) {
		this.totalPlaytime = totalPlaytime;
	}
	public String getMostPlayedArtist() {
		return mostPlayedArtist;
	}
	public void setMostPlayedArtist(String mostPlayedArtist) {
		this.mostPlayedArtist = mostPlayedArtist;
	}
	public String getMostPlayedAlbum() {
		return mostPlayedAlbum;
	}
	public void setMostPlayedAlbum(String mostPlayedAlbum) {
		this.mostPlayedAlbum = mostPlayedAlbum;
	}
	public String getMostPlayedSong() {
		return mostPlayedSong;
	}
	public void setMostPlayedSong(String mostPlayedSong) {
		this.mostPlayedSong = mostPlayedSong;
	}
	
	public int getUniqueArtistsPlayed() {
		return uniqueArtistsPlayed;
	}
	public void setUniqueArtistsPlayed(int uniqueArtistsPlayed) {
		this.uniqueArtistsPlayed = uniqueArtistsPlayed;
	}
	public int getUniqueAlbumsPlayed() {
		return uniqueAlbumsPlayed;
	}
	public void setUniqueAlbumsPlayed(int uniqueAlbumsPlayed) {
		this.uniqueAlbumsPlayed = uniqueAlbumsPlayed;
	}
	public int getUniqueSongsPlayed() {
		return uniqueSongsPlayed;
	}
	public void setUniqueSongsPlayed(int uniqueSongsPlayed) {
		this.uniqueSongsPlayed = uniqueSongsPlayed;
	}
	public List<AlbumSongsQueryDTO> getMostPlayedSongs() {
		return mostPlayedSongs;
	}
	public void setMostPlayedSongs(List<AlbumSongsQueryDTO> mostPlayedSongs) {
		this.mostPlayedSongs = mostPlayedSongs;
	}
	public String getTotalPlaytimeString() {
		return Utils.secondsToString(totalPlaytime);
	}
	public double getPercentageofUnitWhereMusicWasPlayed() {
		return percentageofUnitWhereMusicWasPlayed;
	}
	public void setPercentageofUnitWhereMusicWasPlayed(double percentageofUnitWhereMusicWasPlayed) {
		this.percentageofUnitWhereMusicWasPlayed = percentageofUnitWhereMusicWasPlayed;
	}
	
}
