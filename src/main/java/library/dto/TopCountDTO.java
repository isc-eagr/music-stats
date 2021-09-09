package library.dto;

import library.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopCountDTO {
	
	private String element;
	
	private int count;
	
	private double percentage;
	
	private long playtime;
	
	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}
	
}
