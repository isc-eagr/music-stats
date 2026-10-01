package library.repository;

import library.entity.ArtistImageTheme;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ArtistImageThemeRepository extends JpaRepository<ArtistImageTheme, Integer> {

    Optional<ArtistImageTheme> findByThemeIdAndArtistId(Integer themeId, Integer artistId);

    /**
     * Finds any assignment for a given artist + specific image, regardless of theme.
     * Used to detect and clear conflicts when the same image is moved to another theme.
     */
    Optional<ArtistImageTheme> findByArtistIdAndArtistImageId(Integer artistId, Integer artistImageId);

    List<ArtistImageTheme> findByArtistId(Integer artistId);
}
