package library.dto;

public class MilestoneDTO {
	
	int plays;
	String date;
	long days;
	public int getPlays() {
		return plays;
	}
	public void setPlays(int plays) {
		this.plays = plays;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public long getDays() {
		return days;
	}
	public void setDays(long days) {
		this.days = days;
	}
	public MilestoneDTO(int plays, String date, long days) {
		super();
		this.plays = plays;
		this.date = date;
		this.days = days;
	}

}
