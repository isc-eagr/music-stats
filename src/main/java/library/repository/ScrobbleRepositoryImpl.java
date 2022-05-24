package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.SongsInLastfmButNotItunesDTO;

@Repository
public class ScrobbleRepositoryImpl{

	private JdbcTemplate template;

	public ScrobbleRepositoryImpl(JdbcTemplate template) {
		this.template = template;

	}

    private static final String SONGS_LASTFM_BUT_NOT_ITUNES = """
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
	

	
	public List<SongsInLastfmButNotItunesDTO> songsInLastFmButNotItunes() {
		return template.query(SONGS_LASTFM_BUT_NOT_ITUNES, new BeanPropertyRowMapper<>(SongsInLastfmButNotItunesDTO.class));
	}
	
	public int updateSongIds(int songId, String artist, String song, String album) {
        return template.update(UPDATE_SONG_IDS_QUERY,songId,artist,song,album);
    }
	
}
