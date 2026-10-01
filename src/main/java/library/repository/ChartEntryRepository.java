package library.repository;

import library.entity.ChartEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ChartEntryRepository extends JpaRepository<ChartEntry, Integer> {
    /**
     * Delete all entries for a chart (used when regenerating).
     */
    void deleteByChartId(Integer chartId);

    /**
     * Count entries for a chart.
     */
    long countByChartId(Integer chartId);

    /**
     * Get chart entries with song and artist names populated.
     * Returns entries with transient fields filled via a native query.
     * Returns separate has_image (song's single_cover) and album_has_image for hover pattern.
     */
    @Query(value = "SELECT ce.id, ce.chart_id, ce.position, ce.song_id, s.album_id, ce.play_count, " +
            "s.name as song_name, a.name as artist_name, " +
            "CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id) THEN 1 ELSE 0 END as has_image, " +
            "a.id as artist_id, " +
            "(SELECT al.name FROM Album al WHERE al.id = s.album_id) as album_name, " +
            "a.gender_id, " +
            "CASE WHEN EXISTS(SELECT 1 FROM Album al WHERE al.id = s.album_id AND (al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id))) THEN 1 ELSE 0 END as album_has_image, " +
            "(SELECT g.name FROM Genre g WHERE g.id = COALESCE(s.override_genre_id, (SELECT al2.override_genre_id FROM Album al2 WHERE al2.id = s.album_id), a.genre_id)) as genre_name " +
            "FROM ChartEntry ce " +
            "INNER JOIN Song s ON ce.song_id = s.id " +
            "INNER JOIN Artist a ON s.artist_id = a.id " +
            "WHERE ce.chart_id = :chartId " +
            "ORDER BY ce.position ASC", nativeQuery = true)
    List<Object[]> findEntriesWithSongDetailsRaw(@Param("chartId") Integer chartId);
    
    /**
     * Get album chart entries with album and artist names populated.
     */
    @Query(value = "SELECT ce.id, ce.chart_id, ce.position, ce.song_id, ce.album_id, ce.play_count, " +
            "al.name as album_name, a.name as artist_name, " +
            "CASE WHEN al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id) THEN 1 ELSE 0 END as has_image, " +
            "a.id as artist_id, " +
            "a.gender_id, " +
            "(SELECT g.name FROM Genre g WHERE g.id = COALESCE(al.override_genre_id, a.genre_id)) as genre_name " +
            "FROM ChartEntry ce " +
            "INNER JOIN Album al ON ce.album_id = al.id " +
            "INNER JOIN Artist a ON al.artist_id = a.id " +
            "WHERE ce.chart_id = :chartId " +
            "ORDER BY ce.position ASC", nativeQuery = true)
    List<Object[]> findEntriesWithAlbumDetailsRaw(@Param("chartId") Integer chartId);
}
