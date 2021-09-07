package library.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import library.entity.Song;

@Repository
public interface SongRepository extends JpaRepository<Song, Long> {
	
	public List<Song> findByCreated(String created);
	
}
