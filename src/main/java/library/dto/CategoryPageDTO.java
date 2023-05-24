package library.dto;

import java.util.List;

import library.util.Utils;

public class CategoryPageDTO {
	
	int totalPlays;
	int totalPlaytime;
	int averageSongLength;
	double averagePlaysPerSong;
	int numberOfSongs;
	int numberOfAlbums;
	int numberOfArtists;
	String firstPlay;
	String lastPlay;
	int daysCategoryWasPlayed;
	int weeksCategoryWasPlayed;
	int monthsCategoryWasPlayed;
	String mostPlayedArtist;
	String mostPlayedAlbum;
	String mostPlayedSong;
	String categoryValue;
	private List<AlbumSongsQueryDTO> mostPlayedSongs;
	private List<ArtistAlbumsQueryDTO> mostPlayedAlbums;
	
	public int getTotalPlays() {
		return totalPlays;
	}


	public void setTotalPlays(int totalPlays) {
		this.totalPlays = totalPlays;
	}


	public int getTotalPlaytime() {
		return totalPlaytime;
	}


	public void setTotalPlaytime(int totalPlaytime) {
		this.totalPlaytime = totalPlaytime;
	}


	public int getAverageSongLength() {
		return averageSongLength;
	}


	public void setAverageSongLength(int averageSongLength) {
		this.averageSongLength = averageSongLength;
	}


	public double getAveragePlaysPerSong() {
		return averagePlaysPerSong;
	}


	public void setAveragePlaysPerSong(double averagePlaysPerSong) {
		this.averagePlaysPerSong = averagePlaysPerSong;
	}


	public int getNumberOfSongs() {
		return numberOfSongs;
	}


	public void setNumberOfSongs(int numberOfSongs) {
		this.numberOfSongs = numberOfSongs;
	}

	public int getNumberOfAlbums() {
		return numberOfAlbums;
	}


	public void setNumberOfAlbums(int numberOfAlbums) {
		this.numberOfAlbums = numberOfAlbums;
	}


	public int getNumberOfArtists() {
		return numberOfArtists;
	}


	public void setNumberOfArtists(int numberOfArtists) {
		this.numberOfArtists = numberOfArtists;
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
	
	public String getTotalPlaytimeString() {
		return Utils.secondsToString(totalPlaytime);
	}

	public String getAverageSongLengthString() {
		return Utils.secondsToStringColon(averageSongLength);
	}
	
	public int getDaysCategoryWasPlayed() {
		return daysCategoryWasPlayed;
	}


	public void setDaysCategoryWasPlayed(int daysCategoryWasPlayed) {
		this.daysCategoryWasPlayed = daysCategoryWasPlayed;
	}


	public int getWeeksCategoryWasPlayed() {
		return weeksCategoryWasPlayed;
	}


	public void setWeeksCategoryWasPlayed(int weeksCategoryWasPlayed) {
		this.weeksCategoryWasPlayed = weeksCategoryWasPlayed;
	}


	public int getMonthsCategoryWasPlayed() {
		return monthsCategoryWasPlayed;
	}


	public void setMonthsCategoryWasPlayed(int monthsCategoryWasPlayed) {
		this.monthsCategoryWasPlayed = monthsCategoryWasPlayed;
	}


	public String getCategoryValue() {
		return categoryValue;
	}


	public void setCategoryValue(String categoryValue) {
		this.categoryValue = categoryValue;
	}


	public CategoryPageDTO() {

	}
	
	public List<AlbumSongsQueryDTO> getMostPlayedSongs() {
		return mostPlayedSongs;
	}


	public void setMostPlayedSongs(List<AlbumSongsQueryDTO> mostPlayedSongs) {
		this.mostPlayedSongs = mostPlayedSongs;
	}
	

	public List<ArtistAlbumsQueryDTO> getMostPlayedAlbums() {
		return mostPlayedAlbums;
	}


	public void setMostPlayedAlbums(List<ArtistAlbumsQueryDTO> mostPlayedAlbums) {
		this.mostPlayedAlbums = mostPlayedAlbums;
	}


	public CategoryPageDTO(int totalPlays, int totalPlaytime, int averageSongLength, double averagePlaysPerSong,
			int numberOfSongs, int numberOfAlbums, int numberOfArtists, String firstPlay, String lastPlay,
			int daysCategoryWasPlayed, int weeksCategoryWasPlayed, int monthsCategoryWasPlayed, String mostPlayedArtist,
			String mostPlayedAlbum, String mostPlayedSong, String categoryValue) {
		super();
		this.totalPlays = totalPlays;
		this.totalPlaytime = totalPlaytime;
		this.averageSongLength = averageSongLength;
		this.averagePlaysPerSong = averagePlaysPerSong;
		this.numberOfSongs = numberOfSongs;
		this.numberOfAlbums = numberOfAlbums;
		this.numberOfArtists = numberOfArtists;
		this.firstPlay = firstPlay;
		this.lastPlay = lastPlay;
		this.daysCategoryWasPlayed = daysCategoryWasPlayed;
		this.weeksCategoryWasPlayed = weeksCategoryWasPlayed;
		this.monthsCategoryWasPlayed = monthsCategoryWasPlayed;
		this.mostPlayedArtist = mostPlayedArtist;
		this.mostPlayedAlbum = mostPlayedAlbum;
		this.mostPlayedSong = mostPlayedSong;
		this.categoryValue = categoryValue;
	}
	

}
