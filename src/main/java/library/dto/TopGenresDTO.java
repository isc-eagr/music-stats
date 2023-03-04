package library.dto;

import library.util.Utils;

public class TopGenresDTO {
	
	private String genre;
	
	private String count;
	
	private long playtime;
	
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
	
}
