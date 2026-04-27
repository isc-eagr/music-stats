-- ==========================================================
-- Billboard Hot 100 Migration Script
-- Weekly chart rows imported from the public GitHub mirror.
-- Run manually in SQLite against C:/Music Stats DB/music-stats.db
-- ==========================================================

CREATE TABLE IF NOT EXISTS billboard_hot100_entry (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date     TEXT NOT NULL,
    position       INTEGER NOT NULL,
    artist_name    TEXT NOT NULL,
    song_title     TEXT NOT NULL,
    peak_position  INTEGER NOT NULL,
    weeks_on_chart INTEGER NOT NULL,
    song_id        INTEGER,
    FOREIGN KEY (song_id) REFERENCES Song(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_pos
    ON billboard_hot100_entry(chart_date, position);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_song_id
    ON billboard_hot100_entry(song_id);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_date
    ON billboard_hot100_entry(chart_date);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_artist_song
    ON billboard_hot100_entry(artist_name, song_title);-- ==========================================================
-- Billboard Hot 100 Migration Script
-- Weekly chart history imported from mhollingshead/billboard-hot-100
-- ==========================================================

CREATE TABLE IF NOT EXISTS billboard_hot100_entry (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date     TEXT NOT NULL,
    position       INTEGER NOT NULL,
    artist_name    TEXT NOT NULL,
    song_title     TEXT NOT NULL,
    peak_position  INTEGER NOT NULL,
    weeks_on_chart INTEGER NOT NULL,
    song_id        INTEGER,
    FOREIGN KEY (song_id) REFERENCES Song(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_pos
    ON billboard_hot100_entry(chart_date, position);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_song_id
    ON billboard_hot100_entry(song_id);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_date
    ON billboard_hot100_entry(chart_date);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_artist_song
    ON billboard_hot100_entry(artist_name, song_title);