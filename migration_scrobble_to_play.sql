-- ============================================
-- Migration: Rename Scrobble to Play
-- ============================================
-- This migration renames the Scrobble table and all related
-- indexes, triggers, and columns to use "Play" terminology.
--
-- Run with: sqlite3 "C:\Music Stats DB\music-stats.db" < migration_scrobble_to_play.sql
-- ============================================

-- Step 1: Rename the table
ALTER TABLE Scrobble RENAME TO Play;

-- Step 2: Rename scrobble_date column to play_date
ALTER TABLE Play RENAME COLUMN scrobble_date TO play_date;

-- Step 3: Drop old indexes
DROP INDEX IF EXISTS idx_scrobble_song_id;
DROP INDEX IF EXISTS idx_scrobble_date;
DROP INDEX IF EXISTS idx_scrobble_account;
DROP INDEX IF EXISTS idx_scrobble_artist;
DROP INDEX IF EXISTS idx_scrobble_lastfm_id;
DROP INDEX IF EXISTS idx_scrobble_account_songid;
DROP INDEX IF EXISTS idx_scrobble_account_date;
DROP INDEX IF EXISTS idx_scrobble_songid_account;
DROP INDEX IF EXISTS idx_scrobble_songid_date;
DROP INDEX IF EXISTS idx_scrobble_cover_plays;
DROP INDEX IF EXISTS idx_scrobble_date_songid;
DROP INDEX IF EXISTS idx_scrobble_songid_account_date;
DROP INDEX IF EXISTS idx_scrobble_covering_timeframe;
DROP INDEX IF EXISTS idx_scrobble_date_song_id;
DROP INDEX IF EXISTS idx_scrobble_song_date;

-- Step 4: Create new indexes with Play naming
CREATE INDEX IF NOT EXISTS idx_play_song_id ON Play(song_id);
CREATE INDEX IF NOT EXISTS idx_play_date ON Play(play_date);
CREATE INDEX IF NOT EXISTS idx_play_account ON Play(account);
CREATE INDEX IF NOT EXISTS idx_play_artist ON Play(artist);
CREATE INDEX IF NOT EXISTS idx_play_lastfm_id ON Play(lastfm_id);
CREATE INDEX IF NOT EXISTS idx_play_account_songid ON Play(account, song_id);
CREATE INDEX IF NOT EXISTS idx_play_account_date ON Play(account, play_date);
CREATE INDEX IF NOT EXISTS idx_play_songid_account ON Play(song_id, account);
CREATE INDEX IF NOT EXISTS idx_play_songid_date ON Play(song_id, play_date);
CREATE INDEX IF NOT EXISTS idx_play_cover_plays ON Play(song_id, account, play_date);
CREATE INDEX IF NOT EXISTS idx_play_date_songid ON Play(play_date, song_id);
CREATE INDEX IF NOT EXISTS idx_play_songid_account_date ON Play(song_id, account, play_date);
CREATE INDEX IF NOT EXISTS idx_play_covering_timeframe ON Play(play_date, song_id) WHERE play_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_play_date_song_id ON Play(play_date, song_id) WHERE play_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_play_song_date ON Play(song_id, play_date);

-- Step 5: Drop old trigger
DROP TRIGGER IF EXISTS update_scrobble_timestamp;

-- Step 6: Create new trigger with Play naming
CREATE TRIGGER IF NOT EXISTS update_play_timestamp 
AFTER UPDATE ON Play
FOR EACH ROW
BEGIN
    UPDATE Play SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Step 7: Update statistics
ANALYZE;

-- ============================================
-- END OF MIGRATION
-- ============================================
