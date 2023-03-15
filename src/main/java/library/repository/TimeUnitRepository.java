package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.PlayDTO;

@Repository
public class TimeUnitRepository{

	private JdbcTemplate template;

	public TimeUnitRepository(JdbcTemplate template) {
		this.template = template;

	}
	
	private static final String DAY_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where date(sc.scrobble_date)=date(?)
			""";
    
    private static final String WEEK_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where YEARWEEK(sc.scrobble_date,1)=?
			""";
    
    private static final String MONTH_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where date_format(sc.scrobble_date,'%Y-%M')=?
			""";
    
    private static final String SEASON_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where (case when MONTH(sc.scrobble_date) between 3 and 5 then CONCAT(YEAR(sc.scrobble_date),'Spring')
			         when MONTH(sc.scrobble_date) between 6 and 8 then CONCAT(YEAR(sc.scrobble_date),'Summer')
			         when MONTH(sc.scrobble_date) between 9 and 11 then CONCAT(YEAR(sc.scrobble_date),'Fall')
			         when MONTH(sc.scrobble_date) = 12 then CONCAT(YEAR(sc.scrobble_date)+1,'Winter')
			         when MONTH(sc.scrobble_date) between 1 and 2 then CONCAT(YEAR(sc.scrobble_date),'Winter')
			end)=?
			""";
    
    private static final String YEAR_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where date_format(sc.scrobble_date,'%Y')=?
			""";
    
    private static final String DECADE_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where concat(convert(year(scrobble_date),CHAR(3)),'0','s')=?
			""";
    
    public List<PlayDTO> getPlays(String day) {
		return template.query(DAY_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), day);
	}
        
	public List<PlayDTO> dayPlays(String day) {
		return template.query(DAY_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), day);
	}
	
	public List<PlayDTO> weekPlays(String yearweek) {
		return template.query(WEEK_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), yearweek);
	}
	
	public List<PlayDTO> monthPlays(String month) {
		return template.query(MONTH_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), month);
	}
	
	public List<PlayDTO> seasonPlays(String season) {
		return template.query(SEASON_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), season);
	}
	
	public List<PlayDTO> yearPlays(String year) {
		return template.query(YEAR_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), year);
	}
	
	public List<PlayDTO> decadePlays(String decade) {
		return template.query(DECADE_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), decade);
	}
}
