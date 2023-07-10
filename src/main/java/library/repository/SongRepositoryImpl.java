package library.repository;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Order;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.Filter;
import library.dto.SongsLocalButNotLastfmDTO;
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
	
	
	private static final String TOP_ALBUMS_QUERY = """
			select * from 
			(select artist, album, avg(duration) average_length, avg(count) average_plays, sum(count) count, 
							sum(duration*count) playtime, sum(duration) length, count(distinct album,song) number_of_songs,
				            genre, sex, language, year, race, min(first_play) first_play, max(last_play) last_play
							from
							(select so.artist, IFNULL(so.album,'(single)') album, so.duration, so.song, count(*) count, 
							so.genre, so.sex, so.language, so.year, so.race, min(sc.scrobble_date) first_play, max(sc.scrobble_date) last_play
							from scrobble sc inner join song so on so.id=sc.song_id 
			                where sc.album is not null and sc.album <> ''
							group by so.artist, album, so.song) local
				            group by artist, album) loc1
			inner join 
			        (select sc.artist, IFNULL(sc.album,'(single)') album,
			                count(distinct date(sc.scrobble_date)) play_days, 
			                count(distinct YEARWEEK(sc.scrobble_date,1)) play_weeks, 
			                count(distinct date_format(sc.scrobble_date,'%Y-%M')) play_months
			                from scrobble sc
			                where sc.album is not null and sc.album <> ''
			                group by sc.artist, sc.album) loc2 
			on loc1.artist=loc2.artist and loc1.album=loc2.album
			order by count desc
			limit ?
			""";

	private static final String TOP_SONGS_BASE_QUERY = """
				select * from 
				(select sc.artist, sc.song, IFNULL(so.album,'(single)') album, so.duration length,  
								so.genre, so.sex, so.language, so.year, so.race, min(sc.scrobble_date) first_play, 
								max(sc.scrobble_date) last_play, count(*) count, sum(duration) playtime,
                                count(distinct date(sc.scrobble_date)) play_days, 
				                count(distinct YEARWEEK(sc.scrobble_date,1)) play_weeks, 
				                count(distinct date_format(sc.scrobble_date,'%Y-%M')) play_months
								from scrobble sc inner join song so on so.id=sc.song_id 
								group by sc.artist, sc.album, sc.song) loc1
				where 1=1 
				""";
	
	private static final String TOP_SONGS_COUNT = """
			select count(*) from 
			(select sc.artist, sc.song, IFNULL(so.album,'(single)') album, so.duration length,  
							so.genre, so.sex, so.language, so.year, so.race, min(sc.scrobble_date) first_play, 
							max(sc.scrobble_date) last_play, count(*) count, sum(duration) playtime,
                            count(distinct date(sc.scrobble_date)) play_days, 
			                count(distinct YEARWEEK(sc.scrobble_date,1)) play_weeks, 
			                count(distinct date_format(sc.scrobble_date,'%Y-%M')) play_months
							from scrobble sc inner join song so on so.id=sc.song_id 
							group by sc.artist, sc.album, sc.song ) loc1
			where 1=1  
			""";
	
	//TODO this is counting (single)s, find a way to exclude them
	private static final String TOP_ARTISTS_QUERY = """
				select * from 
				(select artist, avg(duration) average_length, avg(count) average_plays, sum(count) count, 
								sum(duration*count) playtime, count(distinct album) number_of_albums, count(distinct album,song) number_of_songs,
					            genre, sex, language, race, min(first_play) first_play, max(last_play) last_play
								from
								(select so.artist, IFNULL(so.album,'(single)') album, so.duration, so.song, count(*) count, 
								so.genre, so.sex, so.language, so.race, min(sc.scrobble_date) first_play, max(sc.scrobble_date) last_play
								from scrobble sc inner join song so on so.id=sc.song_id 
								group by so.artist, album, so.song) local
					            group by artist) loc1
				inner join 
				        (select sc.artist, 
				                count(distinct date(sc.scrobble_date)) play_days, 
				                count(distinct YEARWEEK(sc.scrobble_date,1)) play_weeks, 
				                count(distinct date_format(sc.scrobble_date,'%Y-%M')) play_months
				                from scrobble sc
				                group by sc.artist) loc2 
				on loc1.artist=loc2.artist
				order by count desc
				limit ?
				""";
	//TODO the album<>'(single)' part could be problematic, validate
	private static final String TOP_GENRES_QUERY = """
			select genre, avg(duration) average_length, avg(count) average_plays, sum(count) count, 
			sum(duration*count) playtime, count(distinct artist) number_of_artists, count(distinct artist,album<>'(single)') number_of_albums, count(distinct artist,album,song) number_of_songs
			from
			(select so.genre, so.artist, IFNULL(so.album,'(single)') album, so.duration, so.song, count(*) count
			from scrobble sc inner join song so on so.id=sc.song_id 
			group by so.genre, so.artist, album, so.song) local
            group by genre
            order by count desc
			limit ?
			""";
	
	private static final String SONGS_LOCAL_BUT_NOT_LASTFM = """
				select so.artist, so.song, so.album, so.source 
				from song so left join scrobble sc on so.id=sc.song_id 
			 	where sc.artist is null 
				group by so.artist,so.song,so.album 
			 	order by so.artist, so.song, so.album
				""";
	
	private static final String DAY_UNIT="date(sc.scrobble_date)";
	private static final String DAY_SORT_UNIT="date(sc.scrobble_date)";
	
	private static final String MONTH_UNIT="date_format(sc.scrobble_date,'%Y-%M')";
	private static final String MONTH_SORT_UNIT="date_format(sc.scrobble_date,'%Y-%m')";
	
	private static final String SEASON_UNIT="""
			case when MONTH(sc.scrobble_date) between 3 and 5 then CONCAT(YEAR(sc.scrobble_date),'Spring')
			         when MONTH(sc.scrobble_date) between 6 and 8 then CONCAT(YEAR(sc.scrobble_date),'Summer')
			         when MONTH(sc.scrobble_date) between 9 and 11 then CONCAT(YEAR(sc.scrobble_date),'Fall')
			         when MONTH(sc.scrobble_date) = 12 then CONCAT(YEAR(sc.scrobble_date)+1,'Winter')
			         when MONTH(sc.scrobble_date) between 1 and 2 then CONCAT(YEAR(sc.scrobble_date),'Winter')
			end
			""";
	private static final String SEASON_SORT_UNIT="date_format(sc.scrobble_date,'%Y-%m')";
	
	private static final String YEAR_UNIT="date_format(sc.scrobble_date,'%Y')";
	private static final String YEAR_SORT_UNIT="date_format(sc.scrobble_date,'%Y')";
	
	private static final String DECADE_UNIT="concat(convert(year(scrobble_date),CHAR(3)),'0','s')";
	private static final String DECADE_SORT_UNIT="concat(convert(year(scrobble_date),CHAR(3)),'0','s')";
	
	private static final String TIME_UNIT_QUERY = """
			select * from 
				(select a.genre, a.date display_date_genre, a.date query_date_genre, max(a.duration) duration_genre, 
				sum(a.duration) total_duration, a.count count_genre, sum(a.count) total_count
					from (select so.genre, %s date, sum(duration) duration, count(*) count 
						from song so inner join scrobble sc on so.id = sc.song_id 
						group by so.genre, %s 
						order by %s desc, sum(duration) desc) a 
					group by a.date) genre 
				inner join 
				(select a.sex,a.date display_date_sex, max(a.duration) duration_sex, a.count count_sex 
		            from (select so.sex, %s date, sum(duration) duration, count(*) count 
						from song so inner join scrobble sc on so.id = sc.song_id 
						group by so.sex, %s 
						order by %s desc, sum(duration) desc) a 
					group by a.date) sex on genre.display_date_genre = sex.display_date_sex 
				inner join
				(select a.race,a.date display_date_race, max(a.duration) duration_race, a.count count_race 
		            from (select so.race, %s date, sum(duration) duration, count(*) count 
						from song so inner join scrobble sc on so.id = sc.song_id 
						group by so.race, %s 
						order by %s desc, sum(duration) desc) a 
					group by a.date) race
			on sex.display_date_sex = race.display_date_race
			""";
	
	private static final String WEEK_QUERY = """
			select * from (select a.genre, MIN(minDate) display_date_genre, max(a.duration) duration_genre, sum(a.duration) total_duration, a.count count_genre, sum(a.count) total_count, a.yearweek query_date_genre
			from (select so.genre, YEARWEEK(sc.scrobble_date,1) yearweek, sum(duration) duration, count(*) count, MIN(DATE(sc.scrobble_date)) minDate 
			from song so inner join scrobble sc on so.id = sc.song_id 
			group by so.genre, YEARWEEK(sc.scrobble_date,1) 
			order by YEARWEEK(sc.scrobble_date,1) desc, sum(duration) desc) a 
			group by a.yearweek) genre 
			inner join 
			(select a.sex, MIN(minDate) display_date_sex, max(a.duration) duration_sex, a.count count_sex 
            from (select so.sex, YEARWEEK(sc.scrobble_date,1) yearweek, sum(duration) duration, count(*) count, MIN(DATE(sc.scrobble_date)) minDate 
			from song so inner join scrobble sc on so.id = sc.song_id 
			group by so.sex, YEARWEEK(sc.scrobble_date,1) 
			order by YEARWEEK(sc.scrobble_date,1) desc, sum(duration) desc) a 
			group by a.yearweek) sex 
			on genre.display_date_genre = sex.display_date_sex 
			inner join 
			(select a.race, MIN(minDate) display_date_race, max(a.duration) duration_race, a.count count_race 
            from (select so.race, YEARWEEK(sc.scrobble_date,1) yearweek, sum(duration) duration, count(*) count, MIN(DATE(sc.scrobble_date)) minDate 
			from song so inner join scrobble sc on so.id = sc.song_id 
			group by so.race, YEARWEEK(sc.scrobble_date,1) 
			order by YEARWEEK(sc.scrobble_date,1) desc, sum(duration) desc) a 
			group by a.yearweek) race 
			on sex.display_date_sex = race.display_date_race
			""";
	
	private static final String ALL_SEXES = """
			SELECT DISTINCT(sex) from song order by sex asc;
			""";
	
	private static final String ALL_GENRES = """
			SELECT DISTINCT(genre) from song order by genre asc;
			""";
	
	private static final String ALL_RACES = """
			SELECT DISTINCT(race) from song order by race asc;
			""";
	
	private static final String ALL_LANGUAGES = """
			SELECT DISTINCT(language) from song order by language asc;
			""";
		
	
	public List<TopAlbumsDTO> getTopAlbums(int limit) {
		return template.query(TOP_ALBUMS_QUERY, new BeanPropertyRowMapper<>(TopAlbumsDTO.class),limit);
	}
	
	public Page<TopSongsDTO> getTopSongs(Pageable page, Filter filter) {
		List<Object> params = new ArrayList<>();
		
		String topSongsBuiltQuery = TOP_SONGS_BASE_QUERY;
		String topSongsCountBuiltQuery = TOP_SONGS_COUNT;
		
		if(filter.getArtist()!=null && !filter.getArtist().isBlank()) {topSongsBuiltQuery += " and artist like ? "; params.add(filter.getArtist());topSongsCountBuiltQuery+=" and artist like ?";}
		if(filter.getSong()!=null && !filter.getSong().isBlank()) {topSongsBuiltQuery += " and song like ? "; params.add(filter.getSong());topSongsCountBuiltQuery+=" and song like ?";}
		if(filter.getAlbum()!=null && !filter.getAlbum().isBlank()) {topSongsBuiltQuery += " and album like ? "; params.add(filter.getAlbum());topSongsCountBuiltQuery+=" and album like ?";}
		if(filter.getSex()!=null && !filter.getSex().isBlank()) {topSongsBuiltQuery += " and sex=? "; params.add(filter.getSex());topSongsCountBuiltQuery+=" and sex=?";}
		if(filter.getGenre()!=null && !filter.getGenre().isBlank()) {topSongsBuiltQuery += " and genre=? "; params.add(filter.getGenre());topSongsCountBuiltQuery+=" and genre=?";}
		if(filter.getRace()!=null && !filter.getRace().isBlank()) {topSongsBuiltQuery += " and race=? "; params.add(filter.getRace());topSongsCountBuiltQuery+=" and race=?";}
		if(filter.getYear()>0) {topSongsBuiltQuery += " and year=? "; params.add(filter.getYear());topSongsCountBuiltQuery+=" and year=?";}
		if(filter.getLanguage()!=null && !filter.getLanguage().isBlank()) {topSongsBuiltQuery += " and language=? "; params.add(filter.getLanguage());topSongsCountBuiltQuery+=" and language=?";}
		
		int total = template.queryForObject(topSongsCountBuiltQuery, Integer.class, params.toArray());
		
		Order order = page.getSort().toList().get(0);		
		List<TopSongsDTO> songs = template.query(
				topSongsBuiltQuery+" order by "+order.getProperty()+" "+order.getDirection()+" limit "+page.getPageSize()+" offset "+page.getOffset()
				, new BeanPropertyRowMapper<>(TopSongsDTO.class), params.toArray());
		
		return new PageImpl<TopSongsDTO>(songs, page, total);
	}
	
	public List<TopArtistsDTO> getTopArtists(int limit) {
		return template.query(TOP_ARTISTS_QUERY, new BeanPropertyRowMapper<>(TopArtistsDTO.class),limit);
	}
	public List<TopGenresDTO> getTopGenres(int limit) {
		return template.query(TOP_GENRES_QUERY, new BeanPropertyRowMapper<>(TopGenresDTO.class),limit);
	}
	
	public List<SongsLocalButNotLastfmDTO> songsLocalButNotLastfm() {
		return template.query(SONGS_LOCAL_BUT_NOT_LASTFM, new BeanPropertyRowMapper<>(SongsLocalButNotLastfmDTO.class));
	}
	
	public List<TimeUnitStatsDTO> timeUnitStats(String unit) {

		if(unit.equals("week"))
			return template.query(WEEK_QUERY, new BeanPropertyRowMapper<>(TimeUnitStatsDTO.class));
		else {
			String unitForQuery;
			String sortForQuery;
			
			switch(unit) {
				case "day": unitForQuery = DAY_UNIT; sortForQuery = DAY_SORT_UNIT; break;
				case "month": unitForQuery = MONTH_UNIT; sortForQuery = MONTH_SORT_UNIT; break;
				case "season": unitForQuery = SEASON_UNIT; sortForQuery = SEASON_SORT_UNIT; break;
				case "year": unitForQuery = YEAR_UNIT; sortForQuery = YEAR_SORT_UNIT; break;
				case "decade": unitForQuery = DECADE_UNIT; sortForQuery = DECADE_SORT_UNIT; break;
				default: unitForQuery = null; sortForQuery = null; break;
			};
			
			
			return template.query(TIME_UNIT_QUERY.formatted(unitForQuery, unitForQuery, sortForQuery, unitForQuery, unitForQuery, sortForQuery, unitForQuery, unitForQuery, sortForQuery), 
					new BeanPropertyRowMapper<>(TimeUnitStatsDTO.class));
		}

	}
	
	public List<String> getAllSexes() {
		return template.queryForList(ALL_SEXES, String.class);
	}
	
	public List<String> getAllGenres() {
		return template.queryForList(ALL_GENRES, String.class);
	}
	
	public List<String> getAllRaces() {
		return template.queryForList(ALL_RACES, String.class);
	}
	
	public List<String> getAllLanguages() {
		return template.queryForList(ALL_LANGUAGES, String.class);
	}
	
}
