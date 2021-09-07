package library.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import library.entity.Scrobble;

@Repository
public interface ScrobbleRepository extends JpaRepository<Scrobble, Long> {
	
	List<Scrobble> findByArtistAndSongAndAlbum (String artist, String song, String album);

}