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
     * Find the latest weekly chart (period_type = 'weekly').
     */
    @Query(value = "SELECT * FROM Chart WHERE chart_type = :chartType AND period_type = 'weekly' ORDER BY period_start_date DESC LIMIT 1", nativeQuery = true)
    Optional<Chart> findLatestWeeklyChart(@Param("chartType") String chartType);
    
    /**
     * Find the latest finalized weekly chart (period_type = 'weekly' AND is_finalized = 1).
     */
    @Query(value = "SELECT * FROM Chart WHERE chart_type = :chartType AND period_type = 'weekly' AND is_finalized = 1 ORDER BY period_start_date DESC LIMIT 1", nativeQuery = true)
    Optional<Chart> findLatestFinalizedWeeklyChart(@Param("chartType") String chartType);
    
    /**
     * Find previous weekly chart (the one before the given period key, weekly charts only).
     */
    @Query(value = "SELECT * FROM Chart WHERE chart_type = :chartType AND period_type = 'weekly' AND period_start_date < " +
            "(SELECT period_start_date FROM Chart WHERE chart_type = :chartType AND period_key = :periodKey AND period_type = 'weekly') " +
            "ORDER BY period_start_date DESC LIMIT 1", nativeQuery = true)
    Optional<Chart> findPreviousChart(@Param("chartType") String chartType, @Param("periodKey") String periodKey);
    
    /**
     * Find next weekly chart (the one after the given period key, weekly charts only).
     */
    @Query(value = "SELECT * FROM Chart WHERE chart_type = :chartType AND period_type = 'weekly' AND period_start_date > " +
            "(SELECT period_start_date FROM Chart WHERE chart_type = :chartType AND period_key = :periodKey AND period_type = 'weekly') " +
            "ORDER BY period_start_date ASC LIMIT 1", nativeQuery = true)
    Optional<Chart> findNextChart(@Param("chartType") String chartType, @Param("periodKey") String periodKey);
    
    /**
     * Count all charts by type.
     */
    long countByChartType(String chartType);
    
    /**
     * Find all weekly charts ordered by period for a given type.
     */
    @Query("SELECT c FROM Chart c WHERE c.chartType = :chartType AND c.periodType = 'weekly' ORDER BY c.periodStartDate ASC")
    List<Chart> findAllByChartTypeOrderByPeriodStartDateAsc(@Param("chartType") String chartType);
    
    // ==================== Seasonal/Yearly Chart Methods ====================
    
    /**
     * Find a chart by type, period type, and period key.
     */
    Optional<Chart> findByChartTypeAndPeriodTypeAndPeriodKey(String chartType, String periodType, String periodKey);
    
    /**
     * Check if a chart exists for a given type, period type, and period.
     */
    boolean existsByChartTypeAndPeriodTypeAndPeriodKey(String chartType, String periodType, String periodKey);
    
    /**
     * Find all period keys that have finalized charts for a given period type.
     */
    @Query("SELECT c.periodKey FROM Chart c WHERE c.periodType = :periodType AND c.isFinalized = true")
    Set<String> findFinalizedPeriodKeysByPeriodType(@Param("periodType") String periodType);
    
    /**
     * Find all period keys that have any charts (finalized or draft) for a given period type.
     */
    @Query("SELECT c.periodKey FROM Chart c WHERE c.periodType = :periodType")
    Set<String> findAllPeriodKeysByPeriodType(@Param("periodType") String periodType);
    
    /**
     * Find chart by period type and period key (for song or album - returns the chart record).
     */
    @Query("SELECT c FROM Chart c WHERE c.periodType = :periodType AND c.periodKey = :periodKey AND c.chartType = :chartType")
    Optional<Chart> findByPeriodTypeAndPeriodKeyAndChartType(
            @Param("periodType") String periodType, 
            @Param("periodKey") String periodKey,
            @Param("chartType") String chartType);
    
    /**
     * Find latest chart by period type (most recent period_start_date).
     */
    @Query("SELECT c FROM Chart c WHERE c.periodType = :periodType AND c.chartType = :chartType AND c.isFinalized = true ORDER BY c.periodStartDate DESC")
    List<Chart> findLatestByPeriodTypeAndChartType(@Param("periodType") String periodType, @Param("chartType") String chartType);
    
    /**
     * Check if a finalized chart exists for a given period type and period key.
     */
    @Query("SELECT CASE WHEN COUNT(c) > 0 THEN true ELSE false END FROM Chart c WHERE c.periodType = :periodType AND c.periodKey = :periodKey AND c.isFinalized = true")
    boolean existsFinalizedChart(@Param("periodType") String periodType, @Param("periodKey") String periodKey);
}
