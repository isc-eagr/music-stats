package library.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SongsInItunesButNotLastfmDTO {
	
	private String artist;
	
	private String song;
	
	private String album;
}
