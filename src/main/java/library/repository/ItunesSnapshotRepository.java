package library.repository;

import library.entity.ItunesSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for iTunes snapshot persistence operations.
 * Used to store and retrieve the last known state of the iTunes library.
 */
@Repository
public interface ItunesSnapshotRepository extends JpaRepository<ItunesSnapshot, Long> {

    /**
     * Find a snapshot entry by its persistent ID.
     */
    Optional<ItunesSnapshot> findByPersistentId(String persistentId);

    /**
     * Check if any snapshot exists (to determine if this is the first run).
     */
    @Query("SELECT COUNT(s) > 0 FROM ItunesSnapshot s")
    boolean hasAnySnapshot();

    /**
     * Get the most recent snapshot date.
     */
    @Query("SELECT MAX(s.snapshotDate) FROM ItunesSnapshot s")
    LocalDateTime getLastSnapshotDate();

    /**
     * Get all persistent IDs from the snapshot.
     */
    @Query("SELECT s.persistentId FROM ItunesSnapshot s")
    List<String> findAllPersistentIds();

    /**
     * Delete all snapshot entries (for full refresh).
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM ItunesSnapshot")
    void deleteAllSnapshots();
}
