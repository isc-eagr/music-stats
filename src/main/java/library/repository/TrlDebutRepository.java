package library.repository;

import library.entity.TrlDebut;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TrlDebutRepository extends JpaRepository<TrlDebut, Integer> {

    @Query(value = "SELECT * FROM trl_debut ORDER BY artist_name ASC, song_title ASC", nativeQuery = true)
    List<TrlDebut> findAllOrdered();

    @Query(value = "SELECT * FROM trl_debut ORDER BY days_on_countdown DESC, artist_name ASC", nativeQuery = true)
    List<TrlDebut> findAllByDaysDesc();
}
