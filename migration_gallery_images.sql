-- ============================================
-- Migration Script: Add Gallery Image Tables
-- ============================================
-- Run this script on existing databases to add support for
-- multiple images per Artist, Album, and Song.
--
-- Created: 2025-12-31
-- ============================================

-- ArtistImage table (additional images for artists)
CREATE TABLE IF NOT EXISTS ArtistImage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    image BLOB NOT NULL,
    display_order INTEGER DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE
);

-- AlbumImage table (additional images for albums)
CREATE TABLE IF NOT EXISTS AlbumImage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    album_id INTEGER NOT NULL,
    image BLOB NOT NULL,
    display_order INTEGER DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (album_id) REFERENCES Album(id) ON DELETE CASCADE
);

-- SongImage table (additional images for songs)
CREATE TABLE IF NOT EXISTS SongImage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    song_id INTEGER NOT NULL,
    image BLOB NOT NULL,
    display_order INTEGER DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (song_id) REFERENCES Song(id) ON DELETE CASCADE
);

-- Indexes for gallery image tables
CREATE INDEX IF NOT EXISTS idx_artist_image_artist ON ArtistImage(artist_id);
CREATE INDEX IF NOT EXISTS idx_album_image_album ON AlbumImage(album_id);
CREATE INDEX IF NOT EXISTS idx_song_image_song ON SongImage(song_id);

