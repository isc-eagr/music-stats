-- ============================================
-- Migration: Add Birth Date and Death Date to Artist
-- ============================================
-- Date: 2025-01-01
-- Description: Adds optional birth_date and death_date columns to the Artist table
--
-- Usage: Run this script on existing databases to add the new columns
--
-- ============================================

-- Add birth_date column to Artist table
ALTER TABLE Artist ADD COLUMN birth_date DATE;

-- Add death_date column to Artist table
ALTER TABLE Artist ADD COLUMN death_date DATE;

-- Migration complete
-- Both columns are optional and default to NULL
-- Date format: dd/MM/yyyy (when populated)
