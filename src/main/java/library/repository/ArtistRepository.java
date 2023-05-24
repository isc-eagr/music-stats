package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.PlayDTO;

@Repository
public class ArtistRepository{

	private JdbcTemplate template;

	public ArtistRepository(JdbcTemplate template) {
		this.template = template;
	}

    private static final String ARTIST_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, 
    		sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, 
    		YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
			""";
    
    private static final String ALBUM_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, 
    		sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, 
    		YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
            and LOWER(IFNULL(so.album,'(single)'))=LOWER(?)
			""";
    
    private static final String SONG_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, 
    		 		sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
            and LOWER(IFNULL(so.album,'(single)'))=LOWER(?)
            and LOWER(so.song) = LOWER(?)
			""";
    
    private static final String CATEGORY_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, 
    		 		sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, so.race, 
    		 		YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(so.%s)=LOWER(?)
			""";
    
    private static final String CATEGORY_PLAYS_ALL = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, 
    		 		sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, so.race, 
    		 		YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id 
            where date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ? 
			""";
    
	public List<PlayDTO> artistPlays(String artist) {
		return template.query(ARTIST_PLAYS_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), artist);
	}
	
	public List<PlayDTO> albumPlays(String artist, String album) {
		return template.query(ALBUM_PLAYS_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), artist, album);
	}
	
	public List<PlayDTO> songPlays(String artist, String album, String song) {
		return template.query(SONG_PLAYS_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), artist, album, song);
	}
	
	public List<PlayDTO> categoryPlays(String[] categories, String[] values) {
		String query = CATEGORY_PLAYS_QUERY;
		if(categories.length > 1) {
			for(int i=1 ; i < categories.length ; i++) {
				query += "and LOWER(so.%s)=LOWER(?) ";
			}
		}
		
		query += " and date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ?";
		
		return template.query(String.format(query, (Object[])categories), new BeanPropertyRowMapper<>(PlayDTO.class), (Object[])values);
	}
	
	public List<PlayDTO> categoryPlaysAll(String start, String end) {
		return template.query(String.format(CATEGORY_PLAYS_ALL), new BeanPropertyRowMapper<>(PlayDTO.class), start, end);
	}
	
}
