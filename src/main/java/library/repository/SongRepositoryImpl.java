package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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
	
	
	private static final String TOP_ALBUMS_QUERY = """
			select * from 
			(select artist, album, avg(duration) average_length, avg(count) average_plays, sum(count) count, 
							sum(duration*count) playtime, count(distinct album,song) number_of_songs,
				            genre, sex, language, year
							from
							(select so.artist, IFNULL(so.album,'(single)') album, so.duration, so.song, count(*) count, 
							so.genre, so.sex, so.language, so.year
							from scrobble sc inner join song so on so.id=sc.song_id 
			                where sc.album is not null and sc.album <> ''
							group by so.artist, album, so.song) local
				            group by artist, album) loc1
			inner join 
			        (select sc.artist, IFNULL(sc.album,'(single)') album,
			                count(distinct date(sc.scrobble_date)) scrobble_days, 
			                count(distinct YEARWEEK(sc.scrobble_date,1)) scrobble_weeks, 
			                count(distinct date_format(sc.scrobble_date,'%Y-%M')) scrobble_months
			                from scrobble sc
			                where sc.album is not null and sc.album <> ''
			                group by sc.artist, sc.album) loc2 
			on loc1.artist=loc2.artist and loc1.album=loc2.album
			order by count desc
			limit ?
			""";
	//Taking way too long, optimize TODO
	/*private static final String TOP_SONGS_QUERY = """
				select * from 
				(select sc.artist, sc.song, IFNULL(so.album,'(single)') album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime 
								from scrobble sc inner join song so on so.id=sc.song_id 
								group by sc.artist, sc.song ) loc1
				inner join 
				        (select sc.artist, sc.album, sc.song,
				                count(distinct date(sc.scrobble_date)) scrobble_days, 
				                count(distinct YEARWEEK(sc.scrobble_date,1)) scrobble_weeks, 
				                count(distinct date_format(sc.scrobble_date,'%Y-%M')) scrobble_months
				                from scrobble sc
				                group by sc.artist, sc.album, sc.song) loc2 
				on (loc1.artist=loc2.artist and loc1.album=loc2.album and loc1.song=loc2.song)
				""";*/
	
	private static final String TOP_SONGS_QUERY = """
			select sc.artist, sc.song, IFNULL(so.album,'(single)') album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime 
			from scrobble sc inner join song so on so.id=sc.song_id 
			group by sc.artist, sc.song 
			order by count desc 
			limit ?
			""";
	
	//TODO this is counting (single)s, find a way to exclude them
	private static final String TOP_ARTISTS_QUERY = """
				select * from 
				(select artist, avg(duration) average_length, avg(count) average_plays, sum(count) count, 
								sum(duration*count) playtime, count(distinct album) number_of_albums, count(distinct album,song) number_of_songs,
					            genre, sex, language
								from
								(select so.artist, IFNULL(so.album,'(single)') album, so.duration, so.song, count(*) count, 
								so.genre, so.sex, so.language
								from scrobble sc inner join song so on so.id=sc.song_id 
								group by so.artist, album, so.song) local
					            group by artist) loc1
				inner join 
				        (select sc.artist, 
				                count(distinct date(sc.scrobble_date)) scrobble_days, 
				                count(distinct YEARWEEK(sc.scrobble_date,1)) scrobble_weeks, 
				                count(distinct date_format(sc.scrobble_date,'%Y-%M')) scrobble_months
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
	
	
	
	public List<TopAlbumsDTO> getTopAlbums(int limit) {
		return template.query(TOP_ALBUMS_QUERY, new BeanPropertyRowMapper<>(TopAlbumsDTO.class),limit);
	}
	
	public List<TopSongsDTO> getTopSongs(int limit) {
		return template.query(TOP_SONGS_QUERY, new BeanPropertyRowMapper<>(TopSongsDTO.class), limit);
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
				case "season": unitForQuery = SEASON_UNIT; sortForQuery = SEASON_SORT_UNIT; break;
				case "year": unitForQuery = YEAR_UNIT; sortForQuery = YEAR_SORT_UNIT; break;
				case "decade": unitForQuery = DECADE_UNIT; sortForQuery = DECADE_SORT_UNIT; break;
				default: unitForQuery = null; sortForQuery = null; break;
			};
			
			
			return template.query(TIME_UNIT_QUERY.formatted(unitForQuery, unitForQuery, sortForQuery, unitForQuery, unitForQuery, sortForQuery), new BeanPropertyRowMapper<>(TimeUnitStatsDTO.class), start, end, start, end);
		}

	}
	
}
