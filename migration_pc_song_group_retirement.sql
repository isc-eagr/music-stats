-- ==========================================================
-- Personal Cuntdown pc_debut Retirement Script
-- Converts legacy pc_debut/debut_id installs to pc_countdown_entry-only
-- Run manually in SQLite against C:/Music Stats DB/music-stats.db
-- ==========================================================

PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

CREATE TABLE pc_countdown_entry_new (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date    TEXT NOT NULL,
    position      INTEGER NOT NULL,
    artist_name   TEXT NOT NULL,
    song_title    TEXT NOT NULL,
    is_close_call INTEGER NOT NULL DEFAULT 0,
    song_id       INTEGER,
    FOREIGN KEY (song_id) REFERENCES Song(id)
);

INSERT INTO pc_countdown_entry_new (id, chart_date, position, artist_name, song_title, is_close_call, song_id)
SELECT e.id,
       e.chart_date,
       e.position,
       e.artist_name,
       e.song_title,
       e.is_close_call,
       d.song_id
FROM pc_countdown_entry e
LEFT JOIN pc_debut d ON d.id = e.debut_id;

DROP TABLE pc_countdown_entry;
ALTER TABLE pc_countdown_entry_new RENAME TO pc_countdown_entry;

CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_chart_date ON pc_countdown_entry(chart_date);
CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_song_id ON pc_countdown_entry(song_id);
CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_artist_song ON pc_countdown_entry(artist_name, song_title);

DROP TABLE pc_debut;

COMMIT;
PRAGMA foreign_keys = ON;