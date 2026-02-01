-- Migration: Add album_artist column to ItunesSnapshot table
-- Date: 2026-01-31
-- Description: Adds album_artist field to track Album Artist changes in iTunes

ALTER TABLE ItunesSnapshot ADD COLUMN album_artist VARCHAR(500);
