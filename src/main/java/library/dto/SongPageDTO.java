package library.dto;

import java.util.List;

import library.util.Utils;

public class SongPageDTO {
	
	String artist;
	String song;
	String album;
	int trackLength;
	int totalPlays;
	String firstPlay;
	String lastPlay;
	int daysSongWasPlayed;
	int weeksSongWasPlayed;
	int monthsSongWasPlayed;
	String chartLabels;
	String chartValues;
	List<MilestoneDTO> milestones;
	String playsByAccount;

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
	
	public int getTrackLength() {
		return trackLength;
	}


	public void setTrackLength(int trackLength) {
		this.trackLength = trackLength;
	}


	public int getTotalPlays() {
		return totalPlays;
	}


	public void setTotalPlays(int totalPlays) {
		this.totalPlays = totalPlays;
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
	
	public int getTotalPlaytime() {
		return trackLength*totalPlays;
	}
	
	public int getDaysSongWasPlayed() {
		return daysSongWasPlayed;
	}

	public void setDaysSongWasPlayed(int daysSongWasPlayed) {
		this.daysSongWasPlayed = daysSongWasPlayed;
	}
	
	public int getWeeksSongWasPlayed() {
		return weeksSongWasPlayed;
	}


	public void setWeeksSongWasPlayed(int weeksSongWasPlayed) {
		this.weeksSongWasPlayed = weeksSongWasPlayed;
	}


	public int getMonthsSongWasPlayed() {
		return monthsSongWasPlayed;
	}

	public void setMonthsSongWasPlayed(int monthsSongWasPlayed) {
		this.monthsSongWasPlayed = monthsSongWasPlayed;
	}
	
	public String getChartLabels() {
		return chartLabels;
	}

	public void setChartLabels(String chartLabels) {
		this.chartLabels = chartLabels;
	}

	public String getChartValues() {
		return chartValues;
	}

	public void setChartValues(String chartValues) {
		this.chartValues = chartValues;
	}

	public String getTrackLengthString() {
		return Utils.secondsToStringColon(trackLength);
	}
	
	public String getTotalPlaytimeString() {
		return Utils.secondsToString(trackLength*totalPlays);
	}
	
	public List<MilestoneDTO> getMilestones() {
		return milestones;
	}

	public void setMilestones(List<MilestoneDTO> milestones) {
		this.milestones = milestones;
	}
	
	public String getPlaysByAccount() {
		return playsByAccount;
	}

	public void setPlaysByAccount(String playsByAccount) {
		this.playsByAccount = playsByAccount;
	}


	public SongPageDTO() {

	}
	

}
