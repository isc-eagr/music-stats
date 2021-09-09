package library.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SongsInLastfmButNotItunesDTO {
	
	private String artist;
	
	private String song;
	
	private String album;
	
	private int count;

}
