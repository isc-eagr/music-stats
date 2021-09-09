package library.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AllSongsExtendedDTO {
	
	String artist;
	String song;
	String album;
	String genre;
	String sex;
	String language;
	int plays;
	int duration;
	int playtime;
	
}
