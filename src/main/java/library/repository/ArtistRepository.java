package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.ScrobbleDTO;

@Repository
public class ArtistRepository{

	private JdbcTemplate template;

	public ArtistRepository(JdbcTemplate template) {
		this.template = template;

	}

    private static final String ARTIST_SCROBBLES_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
			""";
    
    private static final String ALBUM_SCROBBLES_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
            and LOWER(IFNULL(so.album,'(single)'))=LOWER(?)
			""";
    
    private static final String SONG_SCROBBLES_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
            and LOWER(IFNULL(so.album,'(single)'))=LOWER(?)
            and LOWER(so.song) = LOWER(?)
			""";
    
	public List<ScrobbleDTO> artistScrobbles(String artist) {
		return template.query(ARTIST_SCROBBLES_QUERY, new BeanPropertyRowMapper<>(ScrobbleDTO.class), artist);
	}
	
	public List<ScrobbleDTO> albumScrobbles(String artist, String album) {
		return template.query(ALBUM_SCROBBLES_QUERY, new BeanPropertyRowMapper<>(ScrobbleDTO.class), artist, album);
	}
	
	public List<ScrobbleDTO> songScrobbles(String artist, String album, String song) {
		return template.query(SONG_SCROBBLES_QUERY, new BeanPropertyRowMapper<>(ScrobbleDTO.class), artist, album, song);
	}
	
}
