package library.dto;

import library.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WhatWonDTO {
	
	private String genre;
	private String dateGenre;
	private int durationGenre;
	private int countGenre;
	private String sex;
	private String dateSex;
	private int durationSex;
	private int countSex;
	
	public String getDurationGenreText() {
		return Utils.secondsToStringHours(this.durationGenre);
	}

	public String getDurationSexText() {
		return Utils.secondsToStringHours(this.durationSex);
	}
	
}
