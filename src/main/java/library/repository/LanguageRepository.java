package library.repository;

import library.entity.Language;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface LanguageRepository extends JpaRepository<Language, Integer> {
    
    Optional<Language> findByName(String name);
    
    @Query(value = "SELECT image FROM Language WHERE id = :id", nativeQuery = true)
    byte[] findImageById(@Param("id") Integer id);
    
    @Modifying
    @Transactional
    @Query(value = "UPDATE Language SET image = :image WHERE id = :id", nativeQuery = true)
    void updateImage(@Param("id") Integer id, @Param("image") byte[] image);
    
    boolean existsByName(String name);
}
