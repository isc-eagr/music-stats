package library.repository;

import library.entity.Ethnicity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface EthnicityRepository extends JpaRepository<Ethnicity, Integer> {
    
    Optional<Ethnicity> findByName(String name);
    
    @Query(value = "SELECT image FROM Ethnicity WHERE id = :id", nativeQuery = true)
    byte[] findImageById(@Param("id") Integer id);
    
    @Modifying
    @Transactional
    @Query(value = "UPDATE Ethnicity SET image = :image WHERE id = :id", nativeQuery = true)
    void updateImage(@Param("id") Integer id, @Param("image") byte[] image);
    
    boolean existsByName(String name);
}
