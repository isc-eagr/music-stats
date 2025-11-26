-- Migration: Add Song Featured Artists table
-- This table stores the many-to-many relationship between songs and featured artists
-- Run this manually in SQLite

-- Create the SongFeaturedArtist table
CREATE TABLE IF NOT EXISTS SongFeaturedArtist (
    song_id INTEGER NOT NULL,
    artist_id INTEGER NOT NULL,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (song_id, artist_id),
    FOREIGN KEY (song_id) REFERENCES Song(id) ON DELETE CASCADE,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_song_featured_artist_song ON SongFeaturedArtist(song_id);
CREATE INDEX IF NOT EXISTS idx_song_featured_artist_artist ON SongFeaturedArtist(artist_id);
