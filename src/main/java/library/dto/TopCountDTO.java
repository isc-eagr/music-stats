package library.dto;

import library.util.Utils;

public class TopCountDTO {
	
	private String element;
	
	private int count;
	
	private int countMale;
	
	private double percentageCount;
	
	private double percentageCountMale;
	
	private int plays;
	
	private int playsMale;
	
	private double percentagePlays;
	
	private double percentagePlaysMale;
	
	private long playtime;
	
	private long playtimeMale;
	
	private double percentagePlaytime;
	
	private double percentagePlaytimeMale;
	
	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}
	
	public String getPlaytimeStringMale() {
		return Utils.secondsToString(playtimeMale);
	}

	public String getElement() {
		return element;
	}

	public void setElement(String element) {
		this.element = element;
	}

	public long getPlaytime() {
		return playtime;
	}

	public void setPlaytime(long playtime) {
		this.playtime = playtime;
	}
	
	public double getPercentagePlaytime() {
		return percentagePlaytime;
	}

	public void setPercentagePlaytime(double percentagePlaytime) {
		this.percentagePlaytime = percentagePlaytime;
	}

	public int getPlays() {
		return plays;
	}

	public void setPlays(int plays) {
		this.plays = plays;
	}

	public double getPercentagePlays() {
		return percentagePlays;
	}

	public void setPercentagePlays(double percentagePlays) {
		this.percentagePlays = percentagePlays;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getPercentageCount() {
		return percentageCount;
	}

	public void setPercentageCount(double percentageCount) {
		this.percentageCount = percentageCount;
	}

	public int getCountMale() {
		return countMale;
	}

	public void setCountMale(int countMale) {
		this.countMale = countMale;
	}

	public double getPercentageCountMale() {
		return percentageCountMale;
	}

	public void setPercentageCountMale(double percentageCountMale) {
		this.percentageCountMale = percentageCountMale;
	}

	public int getPlaysMale() {
		return playsMale;
	}

	public void setPlaysMale(int playsMale) {
		this.playsMale = playsMale;
	}

	public double getPercentagePlaysMale() {
		return percentagePlaysMale;
	}

	public void setPercentagePlaysMale(double percentagePlaysMale) {
		this.percentagePlaysMale = percentagePlaysMale;
	}

	public long getPlaytimeMale() {
		return playtimeMale;
	}

	public void setPlaytimeMale(long playtimeMale) {
		this.playtimeMale = playtimeMale;
	}

	public double getPercentagePlaytimeMale() {
		return percentagePlaytimeMale;
	}

	public void setPercentagePlaytimeMale(double percentagePlaytimeMale) {
		this.percentagePlaytimeMale = percentagePlaytimeMale;
	}

	
	public TopCountDTO(String element, int count, int countMale, double percentageCount, double percentageCountMale,
			int plays, int playsMale, double percentagePlays, double percentagePlaysMale, long playtime,
			long playtimeMale, double percentagePlaytime, double percentagePlaytimeMale) {
		super();
		this.element = element;
		this.count = count;
		this.countMale = countMale;
		this.percentageCount = percentageCount;
		this.percentageCountMale = percentageCountMale;
		this.plays = plays;
		this.playsMale = playsMale;
		this.percentagePlays = percentagePlays;
		this.percentagePlaysMale = percentagePlaysMale;
		this.playtime = playtime;
		this.playtimeMale = playtimeMale;
		this.percentagePlaytime = percentagePlaytime;
		this.percentagePlaytimeMale = percentagePlaytimeMale;
	}

	public TopCountDTO(String element, int count, double percentageCount, int plays, double percentagePlays,
			long playtime, double percentagePlaytime) {
		super();
		this.element = element;
		this.count = count;
		this.percentageCount = percentageCount;
		this.plays = plays;
		this.percentagePlays = percentagePlays;
		this.playtime = playtime;
		this.percentagePlaytime = percentagePlaytime;
	}
	
	


}
