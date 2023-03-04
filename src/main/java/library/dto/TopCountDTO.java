package library.dto;

import library.util.Utils;

public class TopCountDTO {
	
	private String element;
	
	private int count;
	
	private double percentageCount;
	
	private int plays;
	
	private double percentagePlays;
	
	private long playtime;
	
	private double percentagePlaytime;
	
	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
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
