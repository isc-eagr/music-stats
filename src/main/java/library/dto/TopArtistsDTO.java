package library.dto;

import library.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopArtistsDTO {
	
	private String artist;
	
	private String genre;
	
	private String sex;
	
	private String language;
	
	private String count;
	
	private long playtime;
	
	public String getPlaytimeString() {
		return Utils.secondsToString(playtime);
	}
	
}
