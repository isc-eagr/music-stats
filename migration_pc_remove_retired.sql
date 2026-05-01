-- ==========================================================
-- Vato's Cuntdown Remove Retired Column
-- Converts existing vatos_cuntdown_entry installs to remove retired
-- Run manually in SQLite against C:/Music Stats DB/music-stats.db
-- ==========================================================

PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

CREATE TABLE vatos_cuntdown_entry_new (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date    TEXT NOT NULL,
    position      INTEGER NOT NULL,
    artist_name   TEXT NOT NULL,
    song_title    TEXT NOT NULL,
    is_close_call INTEGER NOT NULL DEFAULT 0,
    song_id       INTEGER,
    FOREIGN KEY (song_id) REFERENCES Song(id)
);

INSERT INTO vatos_cuntdown_entry_new (id, chart_date, position, artist_name, song_title, is_close_call, song_id)
SELECT id, chart_date, position, artist_name, song_title, is_close_call, song_id
FROM vatos_cuntdown_entry;

DROP TABLE vatos_cuntdown_entry;
ALTER TABLE vatos_cuntdown_entry_new RENAME TO vatos_cuntdown_entry;

CREATE INDEX IF NOT EXISTS idx_vatos_cuntdown_entry_chart_date ON vatos_cuntdown_entry(chart_date);
CREATE INDEX IF NOT EXISTS idx_vatos_cuntdown_entry_song_id ON vatos_cuntdown_entry(song_id);
CREATE INDEX IF NOT EXISTS idx_vatos_cuntdown_entry_artist_song ON vatos_cuntdown_entry(artist_name, song_title);

COMMIT;
PRAGMA foreign_keys = ON;