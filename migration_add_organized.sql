-- Migration: Add 'organized' column to Artist, Album, and Song tables
-- Run this script manually in SQLite before restarting the application
-- Date: 2025-11-27

-- Add organized column to Artist table
ALTER TABLE Artist ADD COLUMN organized INTEGER DEFAULT 0;

-- Add organized column to Album table  
ALTER TABLE Album ADD COLUMN organized INTEGER DEFAULT 0;

-- Add organized column to Song table
ALTER TABLE Song ADD COLUMN organized INTEGER DEFAULT 0;

-- Create indexes for efficient filtering
CREATE INDEX IF NOT EXISTS idx_artist_organized ON Artist(organized);
CREATE INDEX IF NOT EXISTS idx_album_organized ON Album(organized);
CREATE INDEX IF NOT EXISTS idx_song_organized ON Song(organized);
