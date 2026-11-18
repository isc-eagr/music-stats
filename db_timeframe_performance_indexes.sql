-- ============================================
-- TIMEFRAME PERFORMANCE INDEXES
-- ============================================
-- These indexes specifically optimize the timeframe queries that aggregate
-- plays by date periods (days, weeks, months, seasons, years, decades).
--
-- Run with: sqlite3 "C:\Music Stats DB\music-stats.db" < db_timeframe_performance_indexes.sql
-- ============================================

-- ===========================================
-- INDEX 1: Play date + song_id covering index
-- ===========================================
-- This is the MOST important index for timeframe queries.
-- The period_summary CTE needs: play_date (for GROUP BY period), song_id (for JOIN)
-- Including play_date first allows SQLite to scan by date ranges efficiently.
CREATE INDEX IF NOT EXISTS idx_play_date_song_id ON Play(play_date, song_id)
WHERE play_date IS NOT NULL;

-- Expression indexes for timeframe period keys.
-- The list pages group and filter by these exact expressions, so indexing them
-- avoids recomputing period buckets across the full Play table on every load.
CREATE INDEX IF NOT EXISTS idx_play_period_day_song
    ON Play(SUBSTR(play_date, 1, 10), play_date, song_id)
    WHERE play_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_play_period_week_song
    ON Play(strftime('%Y-W%W', play_date), play_date, song_id)
    WHERE play_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_play_period_month_song
    ON Play(SUBSTR(play_date, 1, 7), play_date, song_id)
    WHERE play_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_play_period_season_song
    ON Play(
        CASE
            WHEN SUBSTR(play_date, 6, 2) = '12'
                THEN (SUBSTR(play_date, 1, 4) + 1) || '-Winter'
            WHEN SUBSTR(play_date, 6, 2) <= '02'
                THEN SUBSTR(play_date, 1, 4) || '-Winter'
            WHEN SUBSTR(play_date, 6, 2) <= '05'
                THEN SUBSTR(play_date, 1, 4) || '-Spring'
            WHEN SUBSTR(play_date, 6, 2) <= '08'
                THEN SUBSTR(play_date, 1, 4) || '-Summer'
            ELSE SUBSTR(play_date, 1, 4) || '-Fall'
        END,
        play_date,
        song_id
    )
    WHERE play_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_play_period_year_song
    ON Play(SUBSTR(play_date, 1, 4), play_date, song_id)
    WHERE play_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_play_period_decade_song
    ON Play((SUBSTR(play_date, 1, 4) / 10 * 10) || 's', play_date, song_id)
    WHERE play_date IS NOT NULL;

-- ===========================================
-- INDEX 2: Artist attribute lookups
-- ===========================================
-- When computing winning attributes, timeframe queries read one attribute per artist.
-- Earlier versions of this script created idx_artist_attrs, idx_album_overrides and idx_song_timeframe_cover;
-- the live DB uses these narrower indexes instead, and Song joins use idx_song_artist_cover /
-- idx_song_album_cover (db_last_listened_performance_indexes.sql).
CREATE INDEX IF NOT EXISTS idx_artist_id_gender ON Artist(id, gender_id);
CREATE INDEX IF NOT EXISTS idx_artist_id_ethnicity ON Artist(id, ethnicity_id);
CREATE INDEX IF NOT EXISTS idx_artist_id_country ON Artist(id, country);

-- ===========================================
-- INDEX 3: Album override covering index
-- ===========================================
-- Album override columns are checked when Song override is NULL
CREATE INDEX IF NOT EXISTS idx_album_id_overrides ON Album(id, override_genre_id, override_language_id);

-- ===========================================
-- INDEX 4: Play song_id index (if not exists)
-- ===========================================
-- Essential for the Play->Song join performance
CREATE INDEX IF NOT EXISTS idx_play_song_date ON Play(song_id, play_date);

-- ===========================================
-- Update SQLite query planner statistics
-- ===========================================
ANALYZE;

-- ===========================================
-- NOTES
-- ===========================================
-- After creating these indexes, timeframe queries should:
-- 1. Use idx_play_date_song_id for the main GROUP BY period_key scan
-- 2. Use idx_song_artist_cover / idx_song_album_cover for Song joins without table lookups
-- 3. Use idx_artist_id_* for Artist attribute resolution
--
-- To verify indexes are being used, run:
-- EXPLAIN QUERY PLAN <your query here>
