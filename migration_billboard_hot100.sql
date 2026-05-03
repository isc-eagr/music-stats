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

CREATE TABLE IF NOT EXISTS billboard_hot100_debut (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_key       TEXT NOT NULL,
    matched            INTEGER NOT NULL,
    song_id            INTEGER,
    resolved_artist_id INTEGER,
    artist_name        TEXT NOT NULL,
    song_title         TEXT NOT NULL,
    gender_name        TEXT,
    debut_week         TEXT,
    last_week          TEXT,
    peak_week          TEXT,
    debut_position     INTEGER,
    peak_position      INTEGER,
    weeks_on_chart     INTEGER NOT NULL,
    weeks_at_peak      INTEGER NOT NULL,
    weeks_at_top1      INTEGER NOT NULL,
    weeks_at_top5      INTEGER NOT NULL,
    weeks_at_top10     INTEGER NOT NULL,
    weeks_at_top20     INTEGER NOT NULL,
    weeks_at_top50     INTEGER NOT NULL,
    weeks_at_top100    INTEGER NOT NULL,
    FOREIGN KEY (song_id) REFERENCES Song(id),
    FOREIGN KEY (resolved_artist_id) REFERENCES Artist(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_billboard_hot100_debut_identity
    ON billboard_hot100_debut(identity_key);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_song_id
    ON billboard_hot100_debut(song_id);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_artist_song
    ON billboard_hot100_debut(artist_name, song_title);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_debut_week
    ON billboard_hot100_debut(debut_week);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_last_week
    ON billboard_hot100_debut(last_week);

CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_peak_position
    ON billboard_hot100_debut(peak_position);