package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.PlayDTO;
import library.dto.SongsInLastfmButNotLocalDTO;
import library.entity.Song;

@Repository
public class ScrobbleRepositoryImpl{

	private JdbcTemplate template;

	public ScrobbleRepositoryImpl(JdbcTemplate template) {
		this.template = template;

	}

    private static final String SONGS_LASTFM_BUT_NOT_LOCAL = """
    		select sc.artist, sc.song, sc.album, count(*) count 
			from scrobble sc left join song so 
			on so.id = sc.song_id 
			where so.artist is null 
			group by sc.artist, sc.song, sc.album 
			order by count desc, sc.artist asc, sc.album asc, sc.song asc
			""";			           
	
	private static final String UPDATE_SONG_IDS_QUERY = """
			update scrobble 
			set song_id = ? 
			where artist = ? 
			and song = ? 
			and album = ?
			""";
	
	private static final String SONGS_FROM_ARTIST = """
			select sc.artist, sc.album, sc.song, count(*) plays
			from scrobble sc left outer join song so on sc.song_id=so.id
			where sc.artist = ? 
            and so.id is null 
            group by sc.artist, sc.album, sc.song
            order by sc.album, sc.scrobble_date
			""";
	
	private static final String SONGS_FROM_ALBUM = """
			select sc.artist, sc.album, sc.song, count(*) plays
			from scrobble sc left outer join song so on sc.song_id=so.id
			where sc.artist = ? 
			and IFNULL(sc.album,'') = ? 
            and so.id is null 
            group by sc.artist, sc.album, sc.song
            order by sc.scrobble_date
			""";
	
	private static final String GET_PLAYS_BY_DATE_RANGE_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, 
			 		so.duration track_length, sc.scrobble_date play_date, so.genre, so.race, 
			 		so.year, so.language, so.sex, YEARWEEK(sc.scrobble_date,1) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where date(sc.scrobble_date) >= ? and date(sc.scrobble_date) <= ? 
			""";
	
	
	public List<SongsInLastfmButNotLocalDTO> songsInLastFmButNotLocal() {
		return template.query(SONGS_LASTFM_BUT_NOT_LOCAL, new BeanPropertyRowMapper<>(SongsInLastfmButNotLocalDTO.class));
	}
	
	public int updateSongIds(int songId, String artist, String song, String album) {
        return template.update(UPDATE_SONG_IDS_QUERY,songId,artist,song,album);
    }
	
	public List<Song> unmatchedSongsFromArtist(String artist) {
		return template.query(SONGS_FROM_ARTIST, new BeanPropertyRowMapper<>(Song.class), artist);
	}
	
	public List<Song> unmatchedSongsFromAlbum(String artist, String album) {
		return template.query(SONGS_FROM_ALBUM, new BeanPropertyRowMapper<>(Song.class), artist, album);
	}
	
	public List<PlayDTO> getPlaysByDateRange(String start, String end) {
		return template.query(GET_PLAYS_BY_DATE_RANGE_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), start, end);
	}
	
}
