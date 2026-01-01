package library.repository;

import library.entity.AlbumImage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AlbumImageRepository extends JpaRepository<AlbumImage, Integer> {

    List<AlbumImage> findByAlbumIdOrderByDisplayOrderAsc(Integer albumId);

    @Query("SELECT COALESCE(MAX(ai.displayOrder), 0) FROM AlbumImage ai WHERE ai.albumId = :albumId")
    Integer getMaxDisplayOrder(@Param("albumId") Integer albumId);

    void deleteByAlbumId(Integer albumId);

    int countByAlbumId(Integer albumId);
}

