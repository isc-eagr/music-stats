package library.dto;

import library.util.Utils;

public class TimeUnitStatsDTO {
	
	private String genre;
	private String displayDateGenre;
	private String queryDateGenre;
	private int durationGenre;
	private int countGenre;
	private String sex;
	private String dateSex;
	private int durationSex;
	private int countSex;
	private String race;
	private String dateRace;
	private int durationRace;
	private int countRace;
	private int totalDuration;
	private int totalCount;
	
	public String getTotalDurationText() {
		return Utils.secondsToString(this.totalDuration);
	}
	
	public String getDurationGenreText() {
		return Utils.secondsToString(this.durationGenre);
	}

	public String getDurationSexText() {
		return Utils.secondsToString(this.durationSex);
	}
	
	public String getDurationRaceText() {
		return Utils.secondsToString(this.durationRace);
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public String getDisplayDateGenre() {
		return displayDateGenre;
	}

	public void setDisplayDateGenre(String displayDateGenre) {
		this.displayDateGenre = displayDateGenre;
	}
	
	public String getQueryDateGenre() {
		return queryDateGenre;
	}

	public void setQueryDateGenre(String queryDateGenre) {
		this.queryDateGenre = queryDateGenre;
	}

	public int getDurationGenre() {
		return durationGenre;
	}

	public void setDurationGenre(int durationGenre) {
		this.durationGenre = durationGenre;
	}

	public int getCountGenre() {
		return countGenre;
	}

	public void setCountGenre(int countGenre) {
		this.countGenre = countGenre;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getDateSex() {
		return dateSex;
	}

	public void setDateSex(String dateSex) {
		this.dateSex = dateSex;
	}
	
	public int getDurationSex() {
		return durationSex;
	}

	public void setDurationSex(int durationSex) {
		this.durationSex = durationSex;
	}

	public int getCountSex() {
		return countSex;
	}

	public void setCountSex(int countSex) {
		this.countSex = countSex;
	}
	
	public String getRace() {
		return race;
	}

	public void setRace(String race) {
		this.race = race;
	}

	public String getDateRace() {
		return dateRace;
	}

	public void setDateRace(String dateRace) {
		this.dateRace = dateRace;
	}

	public int getDurationRace() {
		return durationRace;
	}

	public void setDurationRace(int durationRace) {
		this.durationRace = durationRace;
	}

	public int getCountRace() {
		return countRace;
	}

	public void setCountRace(int countRace) {
		this.countRace = countRace;
	}

	public int getTotalDuration() {
		return totalDuration;
	}

	public void setTotalDuration(int totalDuration) {
		this.totalDuration = totalDuration;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

}
