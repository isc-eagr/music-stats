-- Migration: Artist Image Themes
-- Creates two new tables to support artist image themes:
--   ArtistTheme    - stores theme definitions (name, is_active)
--   ArtistImageTheme - links an artist+theme to a specific image
--
-- Run this script manually in SQLite before starting the application.

-- Table: ArtistTheme
-- Holds theme definitions. Only one may be active at a time (enforced in app layer).
CREATE TABLE IF NOT EXISTS ArtistTheme (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    is_active     INTEGER NOT NULL DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: ArtistImageTheme
-- Maps an artist+theme pair to a specific image.
-- artist_image_id = NULL means the artist's default (main) image is assigned to this theme.
-- Unique constraint ensures only one image per theme per artist.
CREATE TABLE IF NOT EXISTS ArtistImageTheme (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    theme_id        INTEGER NOT NULL,
    artist_id       INTEGER NOT NULL,
    artist_image_id INTEGER,          -- NULL = default/main image; otherwise FK -> ArtistImage.id
    creation_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (theme_id)        REFERENCES ArtistTheme(id)  ON DELETE CASCADE,
    FOREIGN KEY (artist_id)       REFERENCES Artist(id)       ON DELETE CASCADE,
    FOREIGN KEY (artist_image_id) REFERENCES ArtistImage(id)  ON DELETE SET NULL,
    UNIQUE (theme_id, artist_id)
);

-- Index to speed up the per-artist lookup when resolving which image to show.
CREATE INDEX IF NOT EXISTS idx_artist_image_theme_artist_id ON ArtistImageTheme(artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_image_theme_theme_id  ON ArtistImageTheme(theme_id);
