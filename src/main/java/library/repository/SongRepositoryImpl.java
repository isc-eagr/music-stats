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
import library.dto.WhatWonDTO;

//import library.dto.CriterionData;

@Repository
public class SongRepositoryImpl{

	private JdbcTemplate template;

	public SongRepositoryImpl(JdbcTemplate template) {
		this.template = template;

	}
	
	private static final String ALL_SONGS_EXTENDED_QUERY = "select so.artist, so.song, so.album, so.genre, so.sex, so.language, count(*)  plays, duration, sum(duration) playtime "
			+ "from song so inner join scrobble sc on so.id=sc.song_id "
			+ "where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' "
			+ "group by so.artist,so.song,so.album";
	
	private static final String TOP_ALBUMS_QUERY = "select sc.artist, sc.album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime "
			+ "from scrobble sc inner join song so on so.id=sc.song_id "
			+ "where sc.album is not null and sc.album <> '' "
			+ "group by sc.artist, sc.album "
			+ "order by count desc "
			+ "limit %s";
	
	private static final String TOP_SONGS_QUERY = "select sc.artist, sc.song, sc.album, so.genre, so.sex, so.language, so.year, count(*) count, sum(duration) playtime "
			+ "	from scrobble sc inner join song so on so.id=sc.song_id "
			+ "	group by sc.artist, sc.song "
			+ "	order by count desc "
			+ "	limit %s";
	
	private static final String TOP_ARTISTS_QUERY = "select sc.artist, so.genre, so.sex, so.language, count(*) count, sum(duration) playtime "
			+ "	from scrobble sc inner join song so on so.id=sc.song_id "
			+ "	group by sc.artist "
			+ "	order by count desc "
			+ "	limit %s";
	
	private static final String SONGS_ITUNES_BUT_NOT_LASTFM = "select so.artist, so.song, so.album, so.genre, so.sex, so.language, count(*)  plays, duration, sum(duration) playtime "
			+ "	from song so left join scrobble sc on so.id=sc.song_id "
			+ " where sc.artist is null "
			+ "	group by so.artist,so.song,so.album "
			+ " order by so.artist, so.song, so.album";
	
	private static final String WHAT_WON_EACH_DAY = "select * from (select a.genre,a.date date_genre, max(a.duration) duration_genre,a.count count_genre from (select so.genre, date(sc.scrobble_date) date, sum(duration) duration, count(*) count "
			+ "from song so inner join scrobble sc on so.id = sc.song_id "
			+ "where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' "
			+ "group by so.genre, date(sc.scrobble_date) "
			+ "order by date(sc.scrobble_date) desc, sum(duration) desc) a "
			+ "group by a.date) genre "
			+ "inner join "
			+ "(select a.sex,a.date date_sex, max(a.duration) duration_sex,a.count count_sex from (select so.sex, date(sc.scrobble_date) date, sum(duration) duration, count(*) count "
			+ "from song so inner join scrobble sc on so.id = sc.song_id "
			+ "where date(sc.scrobble_date) >= '%s' and date(sc.scrobble_date) <= '%s' "
			+ "group by so.sex, date(sc.scrobble_date) "
			+ "order by date(sc.scrobble_date) desc, sum(duration) desc) a "
			+ "group by a.date) sex "
			+ "on genre.date_genre = sex.date_sex";
	
	public List<AllSongsExtendedDTO> getAllSongsExtended(String start, String end) {
		return template.query(String.format(ALL_SONGS_EXTENDED_QUERY,start,end), new BeanPropertyRowMapper<>(AllSongsExtendedDTO.class));
	}
	
	public List<TopAlbumsDTO> getTopAlbums(int limit) {
		return template.query(String.format(TOP_ALBUMS_QUERY,limit), new BeanPropertyRowMapper<>(TopAlbumsDTO.class));
	}
	
	public List<TopSongsDTO> getTopSongs(int limit) {
		return template.query(String.format(TOP_SONGS_QUERY,limit), new BeanPropertyRowMapper<>(TopSongsDTO.class));
	}
	
	public List<TopArtistsDTO> getTopArtists(int limit) {
		return template.query(String.format(TOP_ARTISTS_QUERY,limit), new BeanPropertyRowMapper<>(TopArtistsDTO.class));
	}
	
	public List<SongsInItunesButNotLastfmDTO> songsItunesButNotLastfm() {
		return template.query(SONGS_ITUNES_BUT_NOT_LASTFM, new BeanPropertyRowMapper<>(SongsInItunesButNotLastfmDTO.class));
	}
	
	public List<WhatWonDTO> whatWonEachDay(String start, String end) {
		return template.query(String.format(WHAT_WON_EACH_DAY, start, end, start, end), new BeanPropertyRowMapper<>(WhatWonDTO.class));
	}
}
