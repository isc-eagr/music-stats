-- Migration: Artist Image Themes - Initialization
-- Creates the "Default" theme and assigns all existing artist images to it.
-- 
-- Run this script manually in SQLite after running migration_artist_themes.sql

-- Create the "Default" theme (if it doesn't exist)
INSERT OR IGNORE INTO ArtistTheme (id, name, is_active, creation_date)
VALUES (1, 'Default', 1, CURRENT_TIMESTAMP);

-- Deactivate any other active themes (only one can be active)
UPDATE ArtistTheme SET is_active = 0 WHERE id != 1;

-- Assign all existing artist images to the Default theme
-- This inserts assignments for all artists that have a non-null image column
INSERT OR IGNORE INTO ArtistImageTheme (theme_id, artist_id, artist_image_id, creation_date)
SELECT 
    1 as theme_id,
    a.id as artist_id,
    NULL as artist_image_id,
    CURRENT_TIMESTAMP as creation_date
FROM Artist a
WHERE a.image IS NOT NULL;
