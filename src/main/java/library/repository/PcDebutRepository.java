package library.repository;

import library.entity.PcDebut;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PcDebutRepository extends JpaRepository<PcDebut, Integer> {

    @Query(value = "SELECT * FROM pc_debut ORDER BY artist_name ASC, song_title ASC", nativeQuery = true)
    List<PcDebut> findAllOrdered();

    @Query(value = "SELECT * FROM pc_debut ORDER BY days_on_countdown DESC, artist_name ASC", nativeQuery = true)
    List<PcDebut> findAllByDaysDesc();
}
