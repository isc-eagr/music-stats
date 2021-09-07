package library.dto;

import library.util.Utils;

public class WhatWonDTO {
	
	private String genre;
	private String dateGenre;
	private int durationGenre;
	private int countGenre;
	private String sex;
	private String dateSex;
	private int durationSex;
	private int countSex;
	
	public String getGenre() {
		return genre;
	}
	public void setGenre(String genre) {
		this.genre = genre;
	}
	public String getDateGenre() {
		return dateGenre;
	}
	public void setDateGenre(String dateGenre) {
		this.dateGenre = dateGenre;
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
	public String getDurationGenreText() {
		return Utils.secondsToStringHours(this.durationGenre);
	}

	public String getDurationSexText() {
		return Utils.secondsToStringHours(this.durationSex);
	}
	
	
}
