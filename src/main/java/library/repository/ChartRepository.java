package library.repository;

import library.entity.Chart;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
public interface ChartRepository extends JpaRepository<Chart, Integer> {
    
    /**
     * Find a chart by type and period key.
     */
    Optional<Chart> findByChartTypeAndPeriodKey(String chartType, String periodKey);
    
    /**
     * Check if a chart exists for a given type and period.
     */
    boolean existsByChartTypeAndPeriodKey(String chartType, String periodKey);
    
    /**
     * Find all period keys that have charts for a given type.
     */
    @Query("SELECT c.periodKey FROM Chart c WHERE c.chartType = :chartType")
    Set<String> findAllPeriodKeysByChartType(@Param("chartType") String chartType);
    
    /**
     * Find the latest chart by type (most recent period_start_date).
     */
    @Query("SELECT c FROM Chart c WHERE c.chartType = :chartType ORDER BY c.periodStartDate DESC")
    List<Chart> findLatestByChartType(@Param("chartType") String chartType);
    
    /**
     * Find previous chart (the one before the given period key).
     */
    @Query(value = "SELECT * FROM Chart WHERE chart_type = :chartType AND period_start_date < " +
            "(SELECT period_start_date FROM Chart WHERE chart_type = :chartType AND period_key = :periodKey) " +
            "ORDER BY period_start_date DESC LIMIT 1", nativeQuery = true)
    Optional<Chart> findPreviousChart(@Param("chartType") String chartType, @Param("periodKey") String periodKey);
    
    /**
     * Find next chart (the one after the given period key).
     */
    @Query(value = "SELECT * FROM Chart WHERE chart_type = :chartType AND period_start_date > " +
            "(SELECT period_start_date FROM Chart WHERE chart_type = :chartType AND period_key = :periodKey) " +
            "ORDER BY period_start_date ASC LIMIT 1", nativeQuery = true)
    Optional<Chart> findNextChart(@Param("chartType") String chartType, @Param("periodKey") String periodKey);
    
    /**
     * Count all charts by type.
     */
    long countByChartType(String chartType);
    
    /**
     * Find all charts ordered by period for a given type.
     */
    @Query("SELECT c FROM Chart c WHERE c.chartType = :chartType ORDER BY c.periodStartDate ASC")
    List<Chart> findAllByChartTypeOrderByPeriodStartDateAsc(@Param("chartType") String chartType);
}
