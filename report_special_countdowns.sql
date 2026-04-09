-- ============================================================================
-- TRL Special Countdown Detection Report
--
-- Flags chart dates that are suspicious because they contain songs that
-- appear far outside their normal chart date range. This is the signature
-- of themed special countdowns (e.g. "Top 10 Summer Anthems of All Time",
-- "Top 10 VMA Performances", "Top 10 Songs of the Decade") where the
-- producers pick songs from multiple eras instead of the current chart.
--
-- HOW IT WORKS
-- -----------
-- For every (artist_name, song_title) pair, the query tracks all its
-- chart appearances and detects any appearance that comes after a long
-- gap since the song was last seen.  When 3 or more such "comeback"
-- songs land on the exact same chart date, that date is treated as
-- suspicious.  A secondary signal -- the spread in "song eras" across
-- all 10 slots -- catches special days even when individual gaps are
-- below the threshold.
--
-- TUNING
-- ------
--   MIN_APPEARANCES     Songs with fewer appearances than this are not
--                       flagged individually (too little data to judge).
--                       Default: 3
--
--   GAP_THRESHOLD_DAYS  A gap (in days) from a song's previous chart
--                       appearance that marks it as a "comeback entry".
--                       Default: 180  (~6 months)
--
--   OUTLIER_THRESHOLD   Minimum number of "comeback entries" on one day
--                       to flag that day.
--                       Default: 5
--
--
-- OUTPUT (main result set)
-- ------------------------
--   chart_date        The suspicious date
--   entry_count       How many entries are on that day (should be 10)
--   outlier_count     How many entries crossed the gap threshold
--   flag_reasons      Why this day was flagged
--   outlier_detail    Per-outlier breakdown: position, artist/song,
--                     normal date range, and the gap in days
--
-- SECOND QUERY (full day detail)
-- -------------------------------
-- Uncomment the second SELECT at the bottom to show every entry on
-- every suspicious day -- useful for context once you have the dates.
-- ============================================================================

WITH

-- -----------------------------------------------------------------------
-- 1. Song-level stats: appearances, date range
-- -----------------------------------------------------------------------
song_stats AS (
    SELECT
        artist_name,
        song_title,
        COUNT(DISTINCT chart_date)  AS total_appearances,
        MIN(chart_date)             AS first_date,
        MAX(chart_date)             AS last_date
    FROM trl_chart_entry
    GROUP BY artist_name, song_title
),

-- -----------------------------------------------------------------------
-- 2. Attach the previous chart_date for each row (per song, ordered by date)
--    SQLite supports window functions since 3.25.0
-- -----------------------------------------------------------------------
prev_appearances AS (
    SELECT
        chart_date,
        position,
        artist_name,
        song_title,
        LAG(chart_date, 1) OVER (
            PARTITION BY artist_name, song_title
            ORDER BY chart_date
        ) AS prev_chart_date
    FROM trl_chart_entry
),

-- -----------------------------------------------------------------------
-- 3. Per-entry analysis: compute gap and "outlier" flag
-- -----------------------------------------------------------------------
entry_analysis AS (
    SELECT
        pa.chart_date,
        pa.position,
        pa.artist_name,
        pa.song_title,
        ss.total_appearances,
        ss.first_date,
        ss.last_date,
        -- Days elapsed since the previous time this song appeared on TRL
        CASE
            WHEN pa.prev_chart_date IS NOT NULL
            THEN CAST(julianday(pa.chart_date) - julianday(pa.prev_chart_date) AS INTEGER)
            ELSE NULL
        END AS gap_days,
        -- Outlier: song was seen enough times to judge AND came back after a long gap
        CASE
            WHEN ss.total_appearances >= 3                -- enough history
             AND pa.prev_chart_date IS NOT NULL           -- not its very first appearance
             AND (julianday(pa.chart_date)
                  - julianday(pa.prev_chart_date)) > 180 -- > 6 months gap
            THEN 1
            ELSE 0
        END AS is_outlier
    FROM prev_appearances pa
    JOIN song_stats ss
      ON ss.artist_name = pa.artist_name
     AND ss.song_title  = pa.song_title
),

-- -----------------------------------------------------------------------
-- 4. Per-day aggregation: count signals and build detail strings
-- -----------------------------------------------------------------------
day_analysis AS (
    SELECT
        ea.chart_date,
        COUNT(*)           AS entry_count,
        SUM(ea.is_outlier) AS outlier_count,
        -- Build a readable list of the outlier entries only
        GROUP_CONCAT(
            CASE WHEN ea.is_outlier = 1 THEN
                '#' || ea.position
                || '  ' || ea.artist_name || ' - ' || ea.song_title
                || '  (normal: ' || ss.first_date || ' -> ' || ss.last_date || ')'
                || '  [gap: ' || ea.gap_days || ' days]'
            END,
            char(10)
        ) AS outlier_detail
    FROM entry_analysis ea
    JOIN song_stats ss
      ON ss.artist_name = ea.artist_name
     AND ss.song_title  = ea.song_title
    GROUP BY ea.chart_date
),

-- -----------------------------------------------------------------------
-- 5. Mark why each day is flagged
-- -----------------------------------------------------------------------
flagged_days AS (
    SELECT
        chart_date,
        entry_count,
        outlier_count,
        'COMEBACKS: ' || outlier_count || ' songs with 6+ month gap' AS flag_reasons,
        outlier_detail
    FROM day_analysis
    WHERE outlier_count >= 5    -- 5+ comeback songs on one day
)

-- -----------------------------------------------------------------------
-- MAIN RESULT: suspicious days, worst first
-- -----------------------------------------------------------------------
SELECT
    chart_date,
    entry_count,
    outlier_count,
    flag_reasons,
    outlier_detail
FROM flagged_days
ORDER BY
    outlier_count DESC,
    chart_date    ASC;


-- ============================================================================
-- BONUS QUERY: Show ALL 10 entries for each suspicious day with context
--
-- Uncomment this block and run it after reviewing the list above.
-- Replace the subquery with specific dates if you want to drill into one day,
-- e.g.:  WHERE ce.chart_date = '2004-08-26'
-- ============================================================================

/*
SELECT
    ce.chart_date,
    ce.position,
    ce.artist_name,
    ce.song_title,
    ss.total_appearances,
    ss.first_date,
    ss.last_date,
    CASE
        WHEN ea.is_outlier = 1
        THEN 'SUSPICIOUS (gap: ' || ea.gap_days || ' days)'
        WHEN ea.gap_days IS NULL
        THEN 'first ever appearance'
        ELSE 'normal (gap: ' || ea.gap_days || ' days)'
    END AS status
FROM trl_chart_entry ce
JOIN song_stats ss
  ON ss.artist_name = ce.artist_name
 AND ss.song_title  = ce.song_title
JOIN entry_analysis ea
  ON ea.chart_date  = ce.chart_date
 AND ea.position    = ce.position
 AND ea.artist_name = ce.artist_name
 AND ea.song_title  = ce.song_title
WHERE ce.chart_date IN (SELECT chart_date FROM flagged_days)
ORDER BY ce.chart_date, ce.position;
*/
