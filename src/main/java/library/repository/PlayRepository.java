package library.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import library.entity.Play;

@Repository
public interface PlayRepository extends JpaRepository<Play, Integer> {
	
	List<Play> findByArtistAndSongAndAlbum (String artist, String song, String album);

}
