package library.entity;

import jakarta.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Entity representing a generated chart (weekly top songs or albums).
 * Each chart has a type (song/album), a period key (e.g., 2024-W48),
 * and date boundaries for the period.
 * 
 * Note: Dates are stored as strings for SQLite compatibility.
 * - period dates: "yyyy-MM-dd" format
 * - generated_date: "yyyy-MM-dd HH:mm:ss" format
 */
@Entity
@Table(name = "Chart")
public class Chart {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;
    
    @Column(name = "chart_type", length = 20, nullable = false)
    private String chartType;  // 'song' or 'album'
    
    @Column(name = "period_key", length = 20, nullable = false)
    private String periodKey;  // e.g., '2024-W48'
    
    @Column(name = "period_start_date", nullable = false)
    private String periodStartDate;  // Stored as "yyyy-MM-dd"
    
    @Column(name = "period_end_date", nullable = false)
    private String periodEndDate;    // Stored as "yyyy-MM-dd"
    
    @Column(name = "generated_date")
    private String generatedDate;    // Stored as "yyyy-MM-dd HH:mm:ss"
    
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Default constructor
    public Chart() {
    }
    
    // Constructor with fields (accepts LocalDate, stores as String)
    public Chart(String chartType, String periodKey, LocalDate periodStartDate, LocalDate periodEndDate) {
        this.chartType = chartType;
        this.periodKey = periodKey;
        this.periodStartDate = periodStartDate.toString();  // yyyy-MM-dd format
        this.periodEndDate = periodEndDate.toString();      // yyyy-MM-dd format
        this.generatedDate = LocalDateTime.now().format(DATETIME_FORMAT);
    }
    
    // Getters and setters
    public Integer getId() {
        return id;
    }
    
    public void setId(Integer id) {
        this.id = id;
    }
    
    public String getChartType() {
        return chartType;
    }
    
    public void setChartType(String chartType) {
        this.chartType = chartType;
    }
    
    public String getPeriodKey() {
        return periodKey;
    }
    
    public void setPeriodKey(String periodKey) {
        this.periodKey = periodKey;
    }
    
    public String getPeriodStartDate() {
        return periodStartDate;
    }
    
    public void setPeriodStartDate(String periodStartDate) {
        this.periodStartDate = periodStartDate;
    }
    
    public String getPeriodEndDate() {
        return periodEndDate;
    }
    
    public void setPeriodEndDate(String periodEndDate) {
        this.periodEndDate = periodEndDate;
    }
    
    /**
     * Get period start date as LocalDate.
     */
    public LocalDate getPeriodStartDateAsLocalDate() {
        if (periodStartDate == null) return null;
        return LocalDate.parse(periodStartDate);
    }
    
    /**
     * Get period end date as LocalDate.
     */
    public LocalDate getPeriodEndDateAsLocalDate() {
        if (periodEndDate == null) return null;
        return LocalDate.parse(periodEndDate);
    }
    
    public String getGeneratedDate() {
        return generatedDate;
    }
    
    public void setGeneratedDate(String generatedDate) {
        this.generatedDate = generatedDate;
    }
    
    /**
     * Get a formatted display string for the period dates.
     * e.g., "Nov 16 - Nov 22, 2024"
     */
    public String getFormattedPeriod() {
        if (periodStartDate == null || periodEndDate == null) {
            return periodKey;
        }
        LocalDate start = LocalDate.parse(periodStartDate);
        LocalDate end = LocalDate.parse(periodEndDate);
        DateTimeFormatter monthDay = DateTimeFormatter.ofPattern("MMM d");
        DateTimeFormatter full = DateTimeFormatter.ofPattern("MMM d, yyyy");
        
        if (start.getYear() == end.getYear()) {
            return start.format(monthDay) + " - " + end.format(full);
        } else {
            return start.format(full) + " - " + end.format(full);
        }
    }
}
