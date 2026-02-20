package library.repository;

import library.entity.ArtistTheme;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Repository
public interface ArtistThemeRepository extends JpaRepository<ArtistTheme, Integer> {

    List<ArtistTheme> findAllByOrderByNameAsc();

    Optional<ArtistTheme> findByIsActiveTrue();

    @Modifying
    @Transactional
    @Query("UPDATE ArtistTheme t SET t.isActive = false WHERE t.isActive = true")
    void deactivateAll();
}
