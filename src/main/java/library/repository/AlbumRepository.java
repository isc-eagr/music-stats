package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.AlbumSongsQueryDTO;

@Repository
public class AlbumRepository{

	private JdbcTemplate template;

	public AlbumRepository(JdbcTemplate template) {
		this.template = template;

	}

    private static final String ALBUM_SONGS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, count(*) total_plays, min(sc.scrobble_date) first_play,max(sc.scrobble_date) last_play
    		from song so inner join scrobble sc on so.id=sc.song_id
    		where LOWER(sc.artist)=LOWER(?)
            and LOWER(IFNULL(so.album,'(single)'))=LOWER(?)
    		group by so.artist, so.song, so.album
    		order by total_plays desc, so.song asc
			""";
    
	
		
	public List<AlbumSongsQueryDTO> albumSongs(String artist, String album) {
		return template.query(ALBUM_SONGS_QUERY, new BeanPropertyRowMapper<>(AlbumSongsQueryDTO.class), artist, album);
	}

}
