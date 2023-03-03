package library.dto;

import library.util.Utils;

public class TopCountDTO {
	
	private String element;
	
	private int count;
	
	private double percentage;
	
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

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getPercentage() {
		return percentage;
	}

	public void setPercentage(double percentage) {
		this.percentage = percentage;
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

	public TopCountDTO(String element, int count, double percentage, long playtime, double percentagePlaytime) {
		super();
		this.element = element;
		this.count = count;
		this.percentage = percentage;
		this.playtime = playtime;
		this.percentagePlaytime = percentagePlaytime;
	}

}
