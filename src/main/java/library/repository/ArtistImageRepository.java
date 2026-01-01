package library.repository;

import library.entity.ArtistImage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ArtistImageRepository extends JpaRepository<ArtistImage, Integer> {

    List<ArtistImage> findByArtistIdOrderByDisplayOrderAsc(Integer artistId);

    @Query("SELECT COALESCE(MAX(ai.displayOrder), 0) FROM ArtistImage ai WHERE ai.artistId = :artistId")
    Integer getMaxDisplayOrder(@Param("artistId") Integer artistId);

    void deleteByArtistId(Integer artistId);

    int countByArtistId(Integer artistId);
}
