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

-- ===========================================
-- INDEX 2: Song covering index for joins
-- ===========================================
-- Timeframe queries join Song and need: artist_id, album_id, length_seconds, override columns
-- This covering index allows index-only lookups without hitting the table.
CREATE INDEX IF NOT EXISTS idx_song_timeframe_cover ON Song(
    id, 
    artist_id, 
    album_id, 
    length_seconds,
    override_gender_id,
    override_genre_id,
    override_ethnicity_id,
    override_language_id
);

-- ===========================================
-- INDEX 3: Artist covering index for attribute lookups
-- ===========================================
-- When computing winning attributes, we need quick access to artist properties.
CREATE INDEX IF NOT EXISTS idx_artist_attrs ON Artist(
    id,
    gender_id,
    genre_id,
    ethnicity_id,
    language_id,
    country
);

-- ===========================================
-- INDEX 4: Album override covering index
-- ===========================================
-- Album override columns are checked when Song override is NULL
CREATE INDEX IF NOT EXISTS idx_album_overrides ON Album(
    id,
    override_genre_id,
    override_language_id
);

-- ===========================================
-- INDEX 5: Play song_id index (if not exists)
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
-- 2. Use idx_song_timeframe_cover for Song joins without table lookups
-- 3. Use idx_artist_attrs for Artist attribute resolution
--
-- To verify indexes are being used, run:
-- EXPLAIN QUERY PLAN <your query here>
