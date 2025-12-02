-- ============================================
-- Performance Indexes for Timeframe Pages - REVISED
-- ============================================
-- These indexes optimize the /days, /weeks, /months, /seasons, /years, /decades pages
-- Focus: Make the existing CTEs as fast as possible with targeted indexes

-- ============================================
-- PRIMARY BOTTLENECK FIX: Covering Index for Scrobble Scans
-- ============================================

-- Critical covering index for the main scrobble scan in ALL CTEs
-- This index allows SQLite to scan ONLY the index, never touching the heap
CREATE INDEX IF NOT EXISTS idx_scrobble_covering_timeframe ON Scrobble(
    scrobble_date,   -- WHERE clause filter
    song_id          -- JOIN key
) WHERE scrobble_date IS NOT NULL;

-- ============================================
-- Optimize Song JOINs (all CTEs join to Song)
-- ============================================

-- Covering index for Song table to avoid heap lookups
CREATE INDEX IF NOT EXISTS idx_song_covering_joins ON Song(
    id,                    -- JOIN key
    artist_id,             -- JOIN to Artist  
    album_id,              -- JOIN to Album
    length_seconds,        -- SUM aggregation
    override_gender_id,    -- COALESCE resolution
    override_genre_id,     -- COALESCE resolution
    override_ethnicity_id, -- COALESCE resolution
    override_language_id   -- COALESCE resolution
);

-- ============================================
-- Optimize Artist JOINs
-- ============================================

-- Covering index for Artist to minimize lookups
CREATE INDEX IF NOT EXISTS idx_artist_covering ON Artist(
    id,
    gender_id,
    genre_id,
    ethnicity_id,
    language_id,
    country
);

-- ============================================
-- Optimize Album Override Lookups  
-- ============================================

CREATE INDEX IF NOT EXISTS idx_album_override_covering ON Album(
    id,
    override_genre_id,
    override_language_id
);

-- ============================================
-- Optimize Lookup Table Scans
-- ============================================

-- These make the Gender.name LIKE checks faster
CREATE INDEX IF NOT EXISTS idx_gender_for_filtering ON Gender(id, name);
CREATE INDEX IF NOT EXISTS idx_genre_for_filtering ON Genre(id, name);
CREATE INDEX IF NOT EXISTS idx_ethnicity_for_filtering ON Ethnicity(id, name);
CREATE INDEX IF NOT EXISTS idx_language_for_filtering ON Language(id, name);

-- ============================================
-- Update Statistics
-- ============================================

ANALYZE;
