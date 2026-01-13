-- Migration: Drop obsolete 'status' column from Song table
-- Date: 2026-01-13
-- Description: The status column is no longer used in the application

-- SQLite doesn't support DROP COLUMN directly, so we need to recreate the table
-- This migration preserves all data except the status column

-- Disable foreign key checks temporarily (Scrobble references Song)
PRAGMA foreign_keys = OFF;

-- Step 1: Drop the status index
DROP INDEX IF EXISTS idx_song_status;

-- Step 2: Create a new table without the status column
CREATE TABLE Song_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    album_id INTEGER,
    name VARCHAR(500) NOT NULL,
    length_seconds INTEGER,
    is_single INTEGER DEFAULT 0,
    override_genre_id INTEGER,
    override_subgenre_id INTEGER,
    override_language_id INTEGER,
    override_gender_id INTEGER,
    override_ethnicity_id INTEGER,
    release_date DATE,
    organized INTEGER DEFAULT 0,
    single_cover BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (album_id) REFERENCES Album(id) ON DELETE SET NULL,
    FOREIGN KEY (override_genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_language_id) REFERENCES Language(id) ON DELETE SET NULL,
    FOREIGN KEY (override_gender_id) REFERENCES Gender(id) ON DELETE SET NULL,
    FOREIGN KEY (override_ethnicity_id) REFERENCES Ethnicity(id) ON DELETE SET NULL
);

-- Step 3: Copy data from old table to new table (excluding status column)
INSERT INTO Song_new (
    id, artist_id, album_id, name, length_seconds, is_single,
    override_genre_id, override_subgenre_id, override_language_id,
    override_gender_id, override_ethnicity_id, release_date, organized,
    single_cover, creation_date, update_date
)
SELECT 
    id, artist_id, album_id, name, length_seconds, is_single,
    override_genre_id, override_subgenre_id, override_language_id,
    override_gender_id, override_ethnicity_id, release_date, organized,
    single_cover, creation_date, update_date
FROM Song;

-- Step 4: Drop the old table
DROP TABLE Song;

-- Step 5: Rename the new table to the original name
ALTER TABLE Song_new RENAME TO Song;

-- Step 6: Recreate indexes (except status index which is no longer needed)
CREATE INDEX IF NOT EXISTS idx_song_artist ON Song(artist_id);
CREATE INDEX IF NOT EXISTS idx_song_album ON Song(album_id);
CREATE INDEX IF NOT EXISTS idx_song_name ON Song(name);
CREATE INDEX IF NOT EXISTS idx_song_release_date ON Song(release_date);
CREATE INDEX IF NOT EXISTS idx_song_organized ON Song(organized);

-- Re-enable foreign key checks
PRAGMA foreign_keys = ON;

-- Verify the column was removed
SELECT COUNT(*) as song_count FROM Song;
