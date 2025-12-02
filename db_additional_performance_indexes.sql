-- Additional Performance Indexes for Music Stats Application
-- SQLite Database
-- These indexes optimize the most common query patterns

-- ============================================
-- Composite Indexes for Account Filtering
-- ============================================

-- Optimize account filtering on Artist list (JOIN Scrobble -> Song -> Artist)
CREATE INDEX IF NOT EXISTS idx_scrobble_account_songid ON Scrobble(account, song_id);

-- Optimize Song to Artist joins for scrobble aggregations
CREATE INDEX IF NOT EXISTS idx_song_artistid_albumid ON Song(artist_id, album_id);

-- Optimize Song to Album joins for scrobble aggregations  
CREATE INDEX IF NOT EXISTS idx_song_albumid_artistid ON Song(album_id, artist_id);

-- ============================================
-- Indexes for Override Resolution (Song -> Album -> Artist)
-- ============================================

-- Optimize genre resolution with overrides
CREATE INDEX IF NOT EXISTS idx_song_override_genre ON Song(override_genre_id);
CREATE INDEX IF NOT EXISTS idx_album_override_genre ON Album(override_genre_id);

-- Optimize subgenre resolution with overrides
CREATE INDEX IF NOT EXISTS idx_song_override_subgenre ON Song(override_subgenre_id);
CREATE INDEX IF NOT EXISTS idx_album_override_subgenre ON Album(override_subgenre_id);

-- Optimize language resolution with overrides
CREATE INDEX IF NOT EXISTS idx_song_override_language ON Song(override_language_id);
CREATE INDEX IF NOT EXISTS idx_album_override_language ON Album(override_language_id);

-- Optimize gender resolution with overrides
CREATE INDEX IF NOT EXISTS idx_song_override_gender ON Song(override_gender_id);

-- Optimize ethnicity resolution with overrides
CREATE INDEX IF NOT EXISTS idx_song_override_ethnicity ON Song(override_ethnicity_id);

-- ============================================
-- Indexes for Chart Aggregations
-- ============================================

-- Optimize gender-based breakdowns (most common chart type)
CREATE INDEX IF NOT EXISTS idx_artist_gender_id ON Artist(gender_id) WHERE gender_id IS NOT NULL;

-- Optimize ethnicity-based breakdowns
CREATE INDEX IF NOT EXISTS idx_artist_ethnicity_country ON Artist(ethnicity_id, country);

-- Optimize genre-based breakdowns with gender
CREATE INDEX IF NOT EXISTS idx_artist_genre_gender ON Artist(genre_id, gender_id);

-- ============================================
-- Indexes for Artist Detail Page
-- ============================================

-- Optimize play count aggregations by artist
CREATE INDEX IF NOT EXISTS idx_scrobble_songid_account ON Scrobble(song_id, account);

-- Optimize date range queries on scrobbles
CREATE INDEX IF NOT EXISTS idx_scrobble_songid_date ON Scrobble(song_id, scrobble_date);

-- Optimize song length calculations
CREATE INDEX IF NOT EXISTS idx_song_artistid_length ON Song(artist_id, length_seconds);

-- ============================================
-- Covering Indexes for Hot Queries
-- ============================================

-- Covering index for scrobble aggregations (reduces table lookups)
CREATE INDEX IF NOT EXISTS idx_scrobble_cover_plays ON Scrobble(song_id, account, scrobble_date);

-- Covering index for song to artist/album resolution
CREATE INDEX IF NOT EXISTS idx_song_cover_joins ON Song(id, artist_id, album_id, length_seconds);

-- ============================================
-- Indexes for Top Charts Tab Performance
-- ============================================

-- Critical index for Top Artists aggregation (scrobble -> song -> artist)
-- This covering index allows the GROUP BY to run entirely from the index
CREATE INDEX IF NOT EXISTS idx_song_artistid_albumid_length ON Song(artist_id, album_id, length_seconds);

-- Critical index for Top Albums aggregation (scrobble -> song -> album)
-- Covers the album_id IS NOT NULL filter and GROUP BY
CREATE INDEX IF NOT EXISTS idx_song_albumid_length_notnull ON Song(album_id, length_seconds) WHERE album_id IS NOT NULL;

-- Optimize date filtering on scrobbles for Top tab
CREATE INDEX IF NOT EXISTS idx_scrobble_date_songid ON Scrobble(scrobble_date, song_id);

-- Combined index for scrobble aggregations with account filtering
CREATE INDEX IF NOT EXISTS idx_scrobble_songid_account_date ON Scrobble(song_id, account, scrobble_date);

-- ============================================
-- ANALYZE Command for Query Optimizer
-- ============================================

-- Update SQLite statistics for better query plans
ANALYZE;
