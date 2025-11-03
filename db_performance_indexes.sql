-- Recommended indexes to speed up category page queries
-- Adjust or skip if the index already exists.

-- Table: scrobble
CREATE INDEX idx_scrobble_song_id ON scrobble (song_id);
CREATE INDEX idx_scrobble_scrobble_date ON scrobble (scrobble_date);
CREATE INDEX idx_scrobble_account_date ON scrobble (account, scrobble_date);

-- Table: song
-- Use prefix indexes for wide character columns to avoid key length limits on utf8mb4/TEXT
CREATE INDEX idx_song_artist ON song (artist(191));
CREATE INDEX idx_song_album ON song (album(191));
CREATE INDEX idx_song_genre ON song (genre);
CREATE INDEX idx_song_year ON song (year);
CREATE INDEX idx_song_language ON song (language);
CREATE INDEX idx_song_sex ON song (sex);
CREATE INDEX idx_song_race ON song (race);
CREATE INDEX idx_song_cloud_status ON song (cloud_status);