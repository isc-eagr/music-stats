package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.AllSongsExtendedDTO;
import library.dto.SongsInItunesButNotLastfmDTO;
import library.dto.TopAlbumsDTO;
import library.dto.TopArtistsDTO;
import library.dto.TopSongsDTO;
import library.dto.TimeUnitStatsDTO;

@Repository
public class SongRepositoryImpl{

	private JdbcTemplate template;

	public SongRepositoryImpl(JdbcTemplate template) {
		this.template = template;

	}
	
	private static final String ALL_SONGS_EXTENDED_QUERY = """
			select so.artist, so.song, so.album, so.genre, so.sex, so.language, count(*)  plays, duration, sum(duration) playtime 
			from song so inner join scrobble sc on so.id=sc.song_id 
			where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' 
			group by so.artist,so.song,so.album
			""";
	
	private static final String TOP_ALBUMS_QUERY = """
			select sc.artist, sc.album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime 
			from scrobble sc inner join song so on so.id=sc.song_id 
			where sc.album is not null and sc.album <> '' 
			group by sc.artist, sc.album 
			order by count desc 
			limit %s
			""";
	
	private static final String TOP_SONGS_QUERY = """
			select sc.artist, sc.song, sc.album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime 
				from scrobble sc inner join song so on so.id=sc.song_id 
				group by sc.artist, sc.song 
				order by count desc 
				limit %s
				""";
	
	private static final String TOP_ARTISTS_QUERY = """
				select sc.artist, so.genre, so.sex, so.language, count(*) count, sum(duration) playtime 
				from scrobble sc inner join song so on so.id=sc.song_id 
				group by sc.artist 
				order by count desc 
				limit %s
				""";
	
	private static final String SONGS_ITUNES_BUT_NOT_LASTFM = """
				select so.artist, so.song, so.album, so.genre, so.sex, so.language, count(*)  plays, duration, sum(duration) playtime 
				from song so left join scrobble sc on so.id=sc.song_id 
			 	where sc.artist is null 
				group by so.artist,so.song,so.album 
			 	order by so.artist, so.song, so.album
				""";
	
	private static final String DAY_UNIT="date(sc.scrobble_date)";
	private static final String DAY_SORT_UNIT="date(sc.scrobble_date)";
	
	private static final String MONTH_UNIT="date_format(sc.scrobble_date,'%Y-%M')";
	private static final String MONTH_SORT_UNIT="date_format(sc.scrobble_date,'%Y-%m')";
	
	private static final String YEAR_UNIT="date_format(sc.scrobble_date,'%Y')";
	private static final String YEAR_SORT_UNIT="date_format(sc.scrobble_date,'%Y')";
	
	private static final String DECADE_UNIT="concat(convert(year(scrobble_date),CHAR(3)),'0','s')";
	private static final String DECADE_SORT_UNIT="concat(convert(year(scrobble_date),CHAR(3)),'0','s')";
	
	private static final String TIME_UNIT_QUERY = """
			select * from 
				(select a.genre, a.date date_genre, max(a.duration) duration_genre, sum(a.duration) total_duration, a.count count_genre, sum(a.count) total_count
					from (select so.genre, %s date, sum(duration) duration, count(*) count 
						from song so inner join scrobble sc on so.id = sc.song_id 
						where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' 
						group by so.genre, %s 
						order by %s desc, sum(duration) desc) a 
					group by a.date) genre 
				inner join 
				(select a.sex,a.date date_sex, max(a.duration) duration_sex, a.count count_sex 
		            from (select so.sex, %s date, sum(duration) duration, count(*) count 
						from song so inner join scrobble sc on so.id = sc.song_id 
						where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' 
						group by so.sex, %s 
						order by %s desc, sum(duration) desc) a 
					group by a.date) sex 
			on genre.date_genre = sex.date_sex
			""";
	
	private static final String WEEK_QUERY = """
			select * from (select a.genre, MIN(minDate) date_genre, max(a.duration) duration_genre, sum(a.duration) total_duration, a.count count_genre, sum(a.count) total_count 
			from (select so.genre, YEARWEEK(sc.scrobble_date,1) date, sum(duration) duration, count(*) count, MIN(DATE(sc.scrobble_date)) minDate 
			from song so inner join scrobble sc on so.id = sc.song_id 
			where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' 
			group by so.genre, YEARWEEK(sc.scrobble_date,1) 
			order by YEARWEEK(sc.scrobble_date,1) desc, sum(duration) desc) a 
			group by a.date) genre 
			inner join 
			(select a.sex, MIN(minDate) date_sex, max(a.duration) duration_sex, a.count count_sex 
            from (select so.sex, YEARWEEK(sc.scrobble_date,1) date, sum(duration) duration, count(*) count, MIN(DATE(sc.scrobble_date)) minDate 
			from song so inner join scrobble sc on so.id = sc.song_id 
			where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' 
			group by so.sex, YEARWEEK(sc.scrobble_date,1) 
			order by YEARWEEK(sc.scrobble_date,1) desc, sum(duration) desc) a 
			group by a.date) sex 
			on genre.date_genre = sex.date_sex 
			""";
	
		
	public List<AllSongsExtendedDTO> getAllSongsExtended(String start, String end) {
		return template.query(ALL_SONGS_EXTENDED_QUERY.formatted(start,end), new BeanPropertyRowMapper<>(AllSongsExtendedDTO.class));
	}
	
	public List<TopAlbumsDTO> getTopAlbums(int limit) {
		return template.query(TOP_ALBUMS_QUERY.formatted(limit), new BeanPropertyRowMapper<>(TopAlbumsDTO.class));
	}
	
	public List<TopSongsDTO> getTopSongs(int limit) {
		return template.query(TOP_SONGS_QUERY.formatted(limit), new BeanPropertyRowMapper<>(TopSongsDTO.class));
	}
	
	public List<TopArtistsDTO> getTopArtists(int limit) {
		return template.query(TOP_ARTISTS_QUERY.formatted(limit), new BeanPropertyRowMapper<>(TopArtistsDTO.class));
	}
	
	public List<SongsInItunesButNotLastfmDTO> songsItunesButNotLastfm() {
		return template.query(SONGS_ITUNES_BUT_NOT_LASTFM, new BeanPropertyRowMapper<>(SongsInItunesButNotLastfmDTO.class));
	}
	
	public List<TimeUnitStatsDTO> timeUnitStats(String start, String end, String unit) {

		if(unit.equals("week"))
			return template.query(WEEK_QUERY.formatted(start, end, start, end), new BeanPropertyRowMapper<>(TimeUnitStatsDTO.class));
		else {
			String unitForQuery;
			String sortForQuery;
			
			switch(unit) {
				case "day": unitForQuery = DAY_UNIT; sortForQuery = DAY_SORT_UNIT; break;
				case "month": unitForQuery = MONTH_UNIT; sortForQuery = MONTH_SORT_UNIT; break;
				case "year": unitForQuery = YEAR_UNIT; sortForQuery = YEAR_SORT_UNIT; break;
				case "decade": unitForQuery = DECADE_UNIT; sortForQuery = DECADE_SORT_UNIT; break;
				default: unitForQuery = null; sortForQuery = null; break;
			};
			
			
			return template.query(TIME_UNIT_QUERY.formatted(unitForQuery, start, end, unitForQuery, sortForQuery, unitForQuery, start, end, unitForQuery, sortForQuery), new BeanPropertyRowMapper<>(TimeUnitStatsDTO.class));
		}

	}
}
