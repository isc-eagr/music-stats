package library.repository;

import library.entity.Artist;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ArtistRepositoryNew extends JpaRepository<Artist, Integer>, ArtistRepositoryCustom {
    
    @Query(value = """
            SELECT a.id, a.name, a.gender_id, a.country, a.ethnicity_id, 
                   a.genre_id, a.subgenre_id, a.language_id, 
                   a.creation_date, a.update_date
            FROM Artist a
            WHERE a.id = :id
            """, nativeQuery = true)
    Optional<Object[]> findByIdWithoutImage(@Param("id") Integer id);
}