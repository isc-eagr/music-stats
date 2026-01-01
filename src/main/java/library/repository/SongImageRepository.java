package library.repository;

import library.entity.SongImage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SongImageRepository extends JpaRepository<SongImage, Integer> {

    List<SongImage> findBySongIdOrderByDisplayOrderAsc(Integer songId);

    @Query("SELECT COALESCE(MAX(si.displayOrder), 0) FROM SongImage si WHERE si.songId = :songId")
    Integer getMaxDisplayOrder(@Param("songId") Integer songId);

    void deleteBySongId(Integer songId);

    int countBySongId(Integer songId);
}

