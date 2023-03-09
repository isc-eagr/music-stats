package library.dto;

import library.util.Utils;

public class TopGenresDTO {
	
	private String genre;
	
	private String count;
	
	private long playtime;
	
	private int averagePlays;
	
	private int averageLength;
	
	private int numberOfArtists;
	
	private int numberOfAlbums;
	
	private int numberOfSongs;
	
	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
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

	public int getAveragePlays() {
		return averagePlays;
	}

	public void setAveragePlays(int averagePlays) {
		this.averagePlays = averagePlays;
	}

	public int getAverageLength() {
		return averageLength;
	}

	public void setAverageLength(int averageLength) {
		this.averageLength = averageLength;
	}

	public int getNumberOfArtists() {
		return numberOfArtists;
	}

	public void setNumberOfArtists(int numberOfArtists) {
		this.numberOfArtists = numberOfArtists;
	}

	public int getNumberOfAlbums() {
		return numberOfAlbums;
	}

	public void setNumberOfAlbums(int numberOfAlbums) {
		this.numberOfAlbums = numberOfAlbums;
	}

	public int getNumberOfSongs() {
		return numberOfSongs;
	}

	public void setNumberOfSongs(int numberOfSongs) {
		this.numberOfSongs = numberOfSongs;
	}
	public String averageLengthString() {
		return Utils.secondsToStringColon(averageLength);
	}
	

}
