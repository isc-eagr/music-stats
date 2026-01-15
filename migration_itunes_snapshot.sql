-- ============================================
-- Migration: iTunes Snapshot Table
-- ============================================
-- Stores a snapshot of the iTunes Library.xml for change detection.
-- Each row represents one song from the last parsed state.
-- The Persistent ID is the stable unique identifier for a track.
--
-- Created: 2026-01-14
-- ============================================

-- ItunesSnapshot table
-- Stores the last known state of each song in iTunes library
CREATE TABLE IF NOT EXISTS ItunesSnapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persistent_id VARCHAR(20) NOT NULL UNIQUE,    -- iTunes Persistent ID (hex string, stable identifier)
    track_id INTEGER,                              -- iTunes Track ID (numeric, may change)
    artist VARCHAR(500),
    album VARCHAR(500),
    name VARCHAR(500) NOT NULL,
    track_number INTEGER,
    year INTEGER,
    snapshot_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When this snapshot was taken
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_itunes_snapshot_persistent_id ON ItunesSnapshot(persistent_id);
CREATE INDEX IF NOT EXISTS idx_itunes_snapshot_artist_album_name ON ItunesSnapshot(artist, album, name);
