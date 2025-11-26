package library.repository;

import library.entity.SubGenre;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Repository
public interface SubGenreRepository extends JpaRepository<SubGenre, Integer> {
    
    Optional<SubGenre> findByNameAndParentGenreId(String name, Integer parentGenreId);
    
    List<SubGenre> findByParentGenreId(Integer parentGenreId);
    
    @Query(value = "SELECT image FROM SubGenre WHERE id = :id", nativeQuery = true)
    byte[] findImageById(@Param("id") Integer id);
    
    @Modifying
    @Transactional
    @Query(value = "UPDATE SubGenre SET image = :image WHERE id = :id", nativeQuery = true)
    void updateImage(@Param("id") Integer id, @Param("image") byte[] image);
    
    @Modifying
    @Transactional
    @Query(value = "UPDATE SubGenre SET parent_genre_id = :parentGenreId WHERE id = :id", nativeQuery = true)
    void updateParentGenre(@Param("id") Integer id, @Param("parentGenreId") Integer parentGenreId);
    
    boolean existsByNameAndParentGenreId(String name, Integer parentGenreId);
}
