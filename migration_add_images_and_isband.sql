-- Migration Script: Add image fields to lookup tables and is_band to Artist
-- SQLite Database
-- Created: 2025-11-25
-- 
-- Run this script on existing databases to add the new columns.
-- SQLite doesn't support IF NOT EXISTS for ADD COLUMN, so these may fail if already run.
-- That's okay - just ignore the errors for columns that already exist.

-- ============================================
-- Add image column to lookup tables
-- ============================================

-- Add image to Gender table
ALTER TABLE Gender ADD COLUMN image BLOB;

-- Add image to Ethnicity table
ALTER TABLE Ethnicity ADD COLUMN image BLOB;

-- Add image to Language table
ALTER TABLE Language ADD COLUMN image BLOB;

-- Add image to Genre table
ALTER TABLE Genre ADD COLUMN image BLOB;

-- Add image to SubGenre table
ALTER TABLE SubGenre ADD COLUMN image BLOB;

-- ============================================
-- Add is_band column to Artist table
-- ============================================

-- Add is_band to Artist table (0 = solo artist, 1 = band)
ALTER TABLE Artist ADD COLUMN is_band INTEGER DEFAULT 0;

-- ============================================
-- Verification queries (optional)
-- ============================================

-- Uncomment to verify columns were added:
-- PRAGMA table_info(Gender);
-- PRAGMA table_info(Ethnicity);
-- PRAGMA table_info(Language);
-- PRAGMA table_info(Genre);
-- PRAGMA table_info(SubGenre);
-- PRAGMA table_info(Artist);
