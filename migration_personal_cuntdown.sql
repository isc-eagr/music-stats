-- ==========================================================
-- Personal Cuntdown Migration Script
-- Vato's Censored Countdown (Feb 2003 - Sep 2009)
-- Run manually in SQLite against C:/Music Stats DB/music-stats.db
-- ==========================================================

CREATE TABLE IF NOT EXISTS pc_countdown_entry (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date    TEXT NOT NULL,   -- YYYY-MM-DD
    position      INTEGER NOT NULL,
    artist_name   TEXT NOT NULL,
    song_title    TEXT NOT NULL,
    is_close_call INTEGER NOT NULL DEFAULT 0,  -- 1 if in close calls section
    song_id       INTEGER,         -- FK to Song.id (nullable until linked)
    FOREIGN KEY (song_id) REFERENCES Song(id)
);

CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_chart_date ON pc_countdown_entry(chart_date);
CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_song_id    ON pc_countdown_entry(song_id);
CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_artist_song ON pc_countdown_entry(artist_name, song_title);
