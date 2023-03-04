package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.AllSongsExtendedDTO;
import library.dto.SongsInItunesButNotLastfmDTO;
import library.dto.TopAlbumsDTO;
import library.dto.TopArtistsDTO;
import library.dto.TopGenresDTO;
import library.dto.TopSongsDTO;
import library.dto.TimeUnitStatsDTO;

@Repository
public class SongRepositoryImpl{

	private JdbcTemplate template;

	public SongRepositoryImpl(JdbcTemplate template) {
		this.template = template;

	}
	
	private static final String ALL_SONGS_EXTENDED_QUERY = """
			select so.artist, so.song, IFNULL(so.album,'(single)') album, so.genre, so.sex, so.language, count(*)  plays, duration, sum(duration) playtime 
			from song so inner join scrobble sc on so.id=sc.song_id 
			where date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ? 
			group by so.artist,so.song,so.album
			""";
	
	private static final String TOP_ALBUMS_QUERY = """
			select sc.artist, IFNULL(sc.album,'(single)') album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime 
			from scrobble sc inner join song so on so.id=sc.song_id 
			where sc.album is not null and sc.album <> '' 
			group by sc.artist, sc.album 
			order by count desc 
			limit ?
			""";
	
	private static final String TOP_SONGS_QUERY = """
				select sc.artist, sc.song, IFNULL(so.album,'(single)') album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime 
				from scrobble sc inner join song so on so.id=sc.song_id 
				group by sc.artist, sc.song 
				order by count desc 
				limit ?
				""";
	
	private static final String TOP_ARTISTS_QUERY = """
				select sc.artist, so.genre, so.sex, so.language, count(*) count, sum(duration) playtime 
				from scrobble sc inner join song so on so.id=sc.song_id 
				group by sc.artist 
				order by count desc 
				limit ?
				""";
	
	private static final String TOP_GENRES_QUERY = """
			select so.genre, count(*) count, sum(duration) playtime 
			from scrobble sc inner join song so on so.id=sc.song_id 
			group by so.genre 
			order by count desc 
			limit ?
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
				(select a.genre, a.date display_date_genre, a.date query_date_genre, max(a.duration) duration_genre, sum(a.duration) total_duration, a.count count_genre, sum(a.count) total_count
					from (select so.genre, %s date, sum(duration) duration, count(*) count 
						from song so inner join scrobble sc on so.id = sc.song_id 
						where date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ? 
						group by so.genre, %s 
						order by %s desc, sum(duration) desc) a 
					group by a.date) genre 
				inner join 
				(select a.sex,a.date display_date_sex, max(a.duration) duration_sex, a.count count_sex 
		            from (select so.sex, %s date, sum(duration) duration, count(*) count 
						from song so inner join scrobble sc on so.id = sc.song_id 
						where date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ? 
						group by so.sex, %s 
						order by %s desc, sum(duration) desc) a 
					group by a.date) sex 
			on genre.display_date_genre = sex.display_date_sex
			""";
	
	private static final String WEEK_QUERY = """
			select * from (select a.genre, MIN(minDate) display_date_genre, max(a.duration) duration_genre, sum(a.duration) total_duration, a.count count_genre, sum(a.count) total_count, a.yearweek query_date_genre
			from (select so.genre, YEARWEEK(sc.scrobble_date,1) yearweek, sum(duration) duration, count(*) count, MIN(DATE(sc.scrobble_date)) minDate 
			from song so inner join scrobble sc on so.id = sc.song_id 
			where date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ? 
			group by so.genre, YEARWEEK(sc.scrobble_date,1) 
			order by YEARWEEK(sc.scrobble_date,1) desc, sum(duration) desc) a 
			group by a.yearweek) genre 
			inner join 
			(select a.sex, MIN(minDate) display_date_sex, max(a.duration) duration_sex, a.count count_sex 
            from (select so.sex, YEARWEEK(sc.scrobble_date,1) yearweek, sum(duration) duration, count(*) count, MIN(DATE(sc.scrobble_date)) minDate 
			from song so inner join scrobble sc on so.id = sc.song_id 
			where date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ? 
			group by so.sex, YEARWEEK(sc.scrobble_date,1) 
			order by YEARWEEK(sc.scrobble_date,1) desc, sum(duration) desc) a 
			group by a.yearweek) sex 
			on genre.display_date_genre = sex.display_date_sex 
			""";
	
	
	public List<AllSongsExtendedDTO> getAllSongsExtended(String start, String end) {
		return template.query(ALL_SONGS_EXTENDED_QUERY, new BeanPropertyRowMapper<>(AllSongsExtendedDTO.class), start, end);
	}
	
	public List<TopAlbumsDTO> getTopAlbums(int limit) {
		return template.query(TOP_ALBUMS_QUERY, new BeanPropertyRowMapper<>(TopAlbumsDTO.class),limit);
	}
	
	public List<TopSongsDTO> getTopSongs(int limit) {
		return template.query(TOP_SONGS_QUERY, new BeanPropertyRowMapper<>(TopSongsDTO.class),limit);
	}
	
	public List<TopArtistsDTO> getTopArtists(int limit) {
		return template.query(TOP_ARTISTS_QUERY, new BeanPropertyRowMapper<>(TopArtistsDTO.class),limit);
	}
	public List<TopGenresDTO> getTopGenres(int limit) {
		return template.query(TOP_GENRES_QUERY, new BeanPropertyRowMapper<>(TopGenresDTO.class),limit);
	}
	
	public List<SongsInItunesButNotLastfmDTO> songsItunesButNotLastfm() {
		return template.query(SONGS_ITUNES_BUT_NOT_LASTFM, new BeanPropertyRowMapper<>(SongsInItunesButNotLastfmDTO.class));
	}
	
	public List<TimeUnitStatsDTO> timeUnitStats(String start, String end, String unit) {

		if(unit.equals("week"))
			return template.query(WEEK_QUERY, new BeanPropertyRowMapper<>(TimeUnitStatsDTO.class), start, end, start, end);
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
			
			
			return template.query(TIME_UNIT_QUERY.formatted(unitForQuery, unitForQuery, sortForQuery, unitForQuery, unitForQuery, sortForQuery), new BeanPropertyRowMapper<>(TimeUnitStatsDTO.class), start, end, start, end);
		}

	}
	
}
