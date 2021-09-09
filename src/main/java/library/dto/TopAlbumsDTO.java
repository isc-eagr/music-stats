package library.dto;

import library.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopAlbumsDTO {
	
	private String artist;
	
	private String album;
	
	private String genre;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private String year;
	
	private long playtime;

	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}

}
