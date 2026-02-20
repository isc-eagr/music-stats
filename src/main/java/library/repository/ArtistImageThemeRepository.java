package library.repository;

import library.entity.ArtistImageTheme;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
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

    List<ArtistImageTheme> findByThemeId(Integer themeId);

    List<ArtistImageTheme> findByArtistId(Integer artistId);

    void deleteByThemeId(Integer themeId);

    /**
     * Finds the assignment for the currently active theme for a specific artist.
     * Returns empty if no theme is active or if the artist has no assignment for it.
     */
    @Query("SELECT ait FROM ArtistImageTheme ait " +
           "JOIN ArtistTheme t ON t.id = ait.themeId " +
           "WHERE t.isActive = true AND ait.artistId = :artistId")
    Optional<ArtistImageTheme> findActiveThemeAssignmentForArtist(@Param("artistId") Integer artistId);
}
