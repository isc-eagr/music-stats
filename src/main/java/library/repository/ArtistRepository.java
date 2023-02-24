package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.ArtistAlbumsQueryDTO;
import library.dto.ArtistSongsQueryDTO;

@Repository
public class ArtistRepository{

	private JdbcTemplate template;

	public ArtistRepository(JdbcTemplate template) {
		this.template = template;

	}

    private static final String ARTIST_SONGS_QUERY = """
    		select so.artist, so.song, so.album, so.duration track_length, count(*) total_plays, min(sc.scrobble_date) first_play,max(sc.scrobble_date) last_play
    		from song so inner join scrobble sc on so.id=sc.song_id
    		where LOWER(sc.artist)=LOWER(?)
    		group by so.artist, so.song, so.album
    		order by total_plays desc, so.song asc
			""";
    
    private static final String ARTIST_ALBUMS_QUERY = """
    		select interna.*, 
    		 		(select count(duration) from song where artist = interna.artist and IFNULL(album,'')=interna.album) number_of_tracks,
    		 		(select sum(duration) from song where artist = interna.artist and IFNULL(album,'')=interna.album) album_length,
                (select sum(duration) from song inner join scrobble on song.id = scrobble.song_id where song.artist = interna.artist AND IFNULL(song.album,'') = interna.album) total_playtime
                from (select so.artist artist, IFNULL(so.album,'') album, count(*) total_plays, min(sc.scrobble_date) first_play,max(sc.scrobble_date) last_play
    		from song so inner join scrobble sc on so.id=sc.song_id
    		where LOWER(sc.artist)=LOWER(?)
    		group by so.artist, so.album
    		order by total_plays desc, so.album asc) interna
			""";
    
    private static final String AVERAGE_SONG_PLAYS_PER_ARTIST = """
    		select avg(total_plays) from (select so.artist, so.song, so.album, count(*) total_plays
    		from song so inner join scrobble sc on so.id=sc.song_id
    		where LOWER(sc.artist)=LOWER(?)
    		group by so.artist, so.song, so.album) temp
    		""";
	
		
	public List<ArtistSongsQueryDTO> songsByArtist(String artist) {
		return template.query(ARTIST_SONGS_QUERY, new BeanPropertyRowMapper<>(ArtistSongsQueryDTO.class), artist);
	}
	
	public List<ArtistAlbumsQueryDTO> albumsByArtist(String artist) {
		return template.query(ARTIST_ALBUMS_QUERY, new BeanPropertyRowMapper<>(ArtistAlbumsQueryDTO.class), artist);
	}
	
	public int averageSongDurationByArtist(String artist) {
		return template.queryForObject(AVERAGE_SONG_PLAYS_PER_ARTIST, Integer.class,artist);
	}
}
