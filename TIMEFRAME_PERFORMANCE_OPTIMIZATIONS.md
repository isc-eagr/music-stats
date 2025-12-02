# Timeframe Pages Performance Optimizations - REVISED

## Problem
The timeframe pages (/days, /weeks, /months, /seasons, /years, /decades) were experiencing severe performance degradation because:

1. **All period_stats were computed upfront** - Even when viewing page 1 of 50 results, the query computed stats for ALL periods in the database (e.g., all 2000+ days)
2. **Winning attributes computed for ALL periods** - The 5 winning CTEs (gender, genre, ethnicity, language, country) scanned the entire Scrobble table 5 times for ALL periods
3. **LIMIT/OFFSET applied too late** - Pagination was applied at the very end, after all the heavy computation was done

This meant if you had 2000 days worth of data, the query computed stats for all 2000 days even though you only viewed 50 on page 1.

## Solution

### 1. Early Filtering and Pagination
**File: `TimeframeService.java` - `getTimeframeCards()` method**

Created a `filtered_periods` CTE that:
- Computes base period stats (play counts, artist counts, male percentages, etc.)
- **Applies all filters BEFORE pagination** (count ranges, percentage ranges)
- **Applies ORDER BY and LIMIT** to reduce to just the visible page
- Result: Only 50 periods are passed to subsequent CTEs instead of 2000+

### 2. Winning Attributes for Visible Rows Only
The winning CTEs now:
- **JOIN to `filtered_periods`** to only process the 50 visible periods
- Use `INNER JOIN filtered_periods fp ON period_key = fp.period_key`
- Result: 5 scans of 50 periods worth of scrobbles instead of 5 scans of ALL scrobbles

### 3. Optimized Count Query
**File: `TimeframeService.java` - `countTimeframes()` method**

- Only creates winning CTEs if they're actually being filtered on
- Uses conditional logic to skip unused winning computations
- Result: If you're not filtering by winning genre, that CTE isn't created at all

### 4. Targeted Indexes
**File: `db_timeframe_performance_indexes.sql`**

Added covering indexes to make the base scan as fast as possible:

```sql
-- Covering index for scrobble scans (filter on date, never touch heap)
CREATE INDEX idx_scrobble_covering_timeframe ON Scrobble(
    scrobble_date,   -- WHERE clause
    song_id          -- JOIN key
) WHERE scrobble_date IS NOT NULL;

-- Covering index for Song table (all needed columns in index)
CREATE INDEX idx_song_covering_joins ON Song(
    id, artist_id, album_id, length_seconds,
    override_gender_id, override_genre_id, 
    override_ethnicity_id, override_language_id
);
```

## Performance Impact

### Before Optimization
- **Periods computed:** ALL periods in database (e.g., 2000 days)
- **Winning scans:** 5 full table scans × all periods
- **Query time:** 3-10+ seconds
- **I/O:** Very high

### After Optimization
- **Periods computed:** Filtered + paginated set only (e.g., 50 days)
- **Winning scans:** 5 scans × only visible periods (97.5% reduction)
- **Query time:** <1 second (estimated 5-10x improvement)
- **I/O:** Dramatically reduced

## Example Scenario
**Before:** Viewing page 1 of /days with 2000 total days
- period_stats: Computes 2000 periods
- winning_gender: Scans scrobbles for all 2000 periods
- winning_genre: Scans scrobbles for all 2000 periods  
- (... 3 more full scans ...)
- Final SELECT: Applies LIMIT 50 at the very end
- **Total work: 2000 periods × 6 scans**

**After:** Viewing page 1 of /days with 2000 total days
- period_summary: Computes 2000 periods (still needed for sorting)
- filtered_periods: Filters, sorts, LIMITs to 50 periods
- winning_gender: Scans scrobbles for only those 50 periods
- winning_genre: Scans scrobbles for only those 50 periods
- (... 3 more scans of only 50 periods ...)
- **Total work: 2000 periods × 1 scan + 50 periods × 5 scans**

**Work reduction: ~97% less data processed by winning CTEs**

## How to Apply

### Step 1: Apply Indexes
```powershell
cd C:\Code\music-stats
sqlite3 "C:\Music Stats DB\music-stats.db" < db_timeframe_performance_indexes.sql
```

### Step 2: Code is Already Updated
The optimized queries are in `TimeframeService.java` - no code changes needed.

### Step 3: Test
Navigate to any timeframe page and compare performance:
- `/days`
- `/weeks`
- `/months`
- `/seasons`
- `/years`
- `/decades`

## Monitoring

To verify the optimization is working, check the query plan:
```sql
EXPLAIN QUERY PLAN
-- paste query from TimeframeService here
```

Look for:
- "USING INDEX idx_scrobble_covering_timeframe"
- "USING INDEX idx_song_covering_joins"
- The filtered_periods CTE should have a LIMIT clause
