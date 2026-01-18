package library.repository;

import library.entity.ChartEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ChartEntryRepository extends JpaRepository<ChartEntry, Integer> {
    
    /**
     * Find all entries for a chart, ordered by position.
     */
    @Query("SELECT e FROM ChartEntry e WHERE e.chartId = :chartId ORDER BY e.position ASC")
    List<ChartEntry> findByChartIdOrderByPosition(@Param("chartId") Integer chartId);
    
    /**
     * Find an entry by chart and song.
     */
    Optional<ChartEntry> findByChartIdAndSongId(Integer chartId, Integer songId);
    
    /**
     * Find an entry by chart and album.
     */
    Optional<ChartEntry> findByChartIdAndAlbumId(Integer chartId, Integer albumId);
    
    /**
     * Find all chart entries for a song across all charts of a given type.
     * Used for calculating chart run history.
     */
    @Query(value = "SELECT ce.* FROM ChartEntry ce " +
            "INNER JOIN Chart c ON ce.chart_id = c.id " +
            "WHERE ce.song_id = :songId AND c.chart_type = :chartType " +
            "ORDER BY c.period_start_date ASC", nativeQuery = true)
    List<ChartEntry> findAllBySongIdAndChartType(@Param("songId") Integer songId, @Param("chartType") String chartType);
    
    /**
     * Find all chart entries for an album across all charts of a given type.
     */
    @Query(value = "SELECT ce.* FROM ChartEntry ce " +
            "INNER JOIN Chart c ON ce.chart_id = c.id " +
            "WHERE ce.album_id = :albumId AND c.chart_type = :chartType " +
            "ORDER BY c.period_start_date ASC", nativeQuery = true)
    List<ChartEntry> findAllByAlbumIdAndChartType(@Param("albumId") Integer albumId, @Param("chartType") String chartType);
    
    /**
     * Delete all entries for a chart (used when regenerating).
     */
    void deleteByChartId(Integer chartId);
    
    /**
     * Count entries for a chart.
     */
    long countByChartId(Integer chartId);
    
    /**
     * Find the position of a song in a specific chart (if it exists).
     */
    @Query("SELECT e.position FROM ChartEntry e WHERE e.chartId = :chartId AND e.songId = :songId")
    Optional<Integer> findPositionByChartIdAndSongId(@Param("chartId") Integer chartId, @Param("songId") Integer songId);
    
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
            "CASE WHEN EXISTS(SELECT 1 FROM Album al WHERE al.id = s.album_id AND al.image IS NOT NULL) THEN 1 ELSE 0 END as album_has_image " +
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
            "CASE WHEN al.image IS NOT NULL THEN 1 ELSE 0 END as has_image, " +
            "a.id as artist_id, " +
            "a.gender_id " +
            "FROM ChartEntry ce " +
            "INNER JOIN Album al ON ce.album_id = al.id " +
            "INNER JOIN Artist a ON al.artist_id = a.id " +
            "WHERE ce.chart_id = :chartId " +
            "ORDER BY ce.position ASC", nativeQuery = true)
    List<Object[]> findEntriesWithAlbumDetailsRaw(@Param("chartId") Integer chartId);
}
