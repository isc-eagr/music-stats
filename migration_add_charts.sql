-- Migration: Add Charts feature tables
-- This migration adds support for weekly song/album charts

-- Chart table: stores metadata about each generated chart
CREATE TABLE IF NOT EXISTS Chart (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_type VARCHAR(20) NOT NULL,           -- 'song' or 'album'
    period_key VARCHAR(20) NOT NULL,           -- e.g., '2024-W48' for week 48 of 2024
    period_start_date DATE NOT NULL,           -- First day of the period
    period_end_date DATE NOT NULL,             -- Last day of the period
    generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chart_type, period_key)
);

-- ChartEntry table: stores individual entries (songs/albums) in each chart
CREATE TABLE IF NOT EXISTS ChartEntry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_id INTEGER NOT NULL,
    position INTEGER NOT NULL,                  -- 1-20 for songs, 1-10 for albums
    song_id INTEGER,                            -- FK to Song table (for song charts)
    album_id INTEGER,                           -- FK to Album table (for album charts)
    play_count INTEGER NOT NULL,                -- Number of plays during the period
    FOREIGN KEY (chart_id) REFERENCES Chart(id) ON DELETE CASCADE,
    FOREIGN KEY (song_id) REFERENCES Song(id),
    FOREIGN KEY (album_id) REFERENCES Album(id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_chart_type_period ON Chart(chart_type, period_key);
CREATE INDEX IF NOT EXISTS idx_chart_type ON Chart(chart_type);
CREATE INDEX IF NOT EXISTS idx_chartentry_chart_id ON ChartEntry(chart_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_song_id ON ChartEntry(song_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_album_id ON ChartEntry(album_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_position ON ChartEntry(chart_id, position);
