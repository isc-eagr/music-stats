-- ===========================================
-- Last Listened Performance Indexes
-- ===========================================
-- These indexes accompany the two-level aggregation refactor in
-- ArtistRepositoryImpl and AlbumRepository.  The refactor splits the
-- play_stats subquery into:
--   1. Inner:  GROUP BY p.song_id  (uses idx_play_cover_plays as a pure
--              covering index scan -- no Play heap access)
--   2. Outer:  JOIN Song, GROUP BY artist_id / album_id
--
-- The indexes below make the outer GROUP BY step index-only as well.

-- INDEX 1: Song covering index for artist-level aggregation
-- Lets SQLite scan Song in artist_id order and read length_seconds from the
-- index leaf without touching the main table.
CREATE INDEX IF NOT EXISTS idx_song_artist_cover
    ON Song(artist_id, id, length_seconds);

-- INDEX 2: Song covering index for album-level aggregation
-- Same benefit for the album play_stats outer GROUP BY.
CREATE INDEX IF NOT EXISTS idx_song_album_cover
    ON Song(album_id, id, length_seconds);

-- Update query-planner statistics so SQLite picks the new indexes.
ANALYZE;
