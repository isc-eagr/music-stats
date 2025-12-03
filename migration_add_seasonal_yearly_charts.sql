-- Migration: Add Seasonal and Yearly Charts support
-- This migration extends the Chart table to support seasonal and yearly charts
-- which are manually curated (unlike weekly charts which are auto-generated)

-- Add period_type column to distinguish weekly/seasonal/yearly charts
-- Default 'weekly' for existing charts
ALTER TABLE Chart ADD COLUMN period_type VARCHAR(20) DEFAULT 'weekly' NOT NULL;

-- Add is_finalized column for seasonal/yearly charts
-- Finalized charts cannot be edited; draft charts can be saved incrementally
ALTER TABLE Chart ADD COLUMN is_finalized INTEGER DEFAULT 0 NOT NULL;

-- Update existing charts to be marked as 'weekly' period type (already the default)
UPDATE Chart SET period_type = 'weekly' WHERE period_type IS NULL OR period_type = '';

-- Drop the old unique constraint and create a new one that includes period_type
-- SQLite doesn't support DROP CONSTRAINT, so we need to recreate the index
-- First, drop the existing unique index if it exists
DROP INDEX IF EXISTS sqlite_autoindex_Chart_1;

-- Create new unique index on (chart_type, period_type, period_key)
CREATE UNIQUE INDEX IF NOT EXISTS idx_chart_type_period_type_key ON Chart(chart_type, period_type, period_key);

-- Make play_count nullable in ChartEntry for manual seasonal/yearly charts
-- SQLite doesn't support ALTER COLUMN, but play_count is already nullable in practice
-- Just document that for seasonal/yearly charts, play_count can be NULL

-- Add index for querying by period_type
CREATE INDEX IF NOT EXISTS idx_chart_period_type ON Chart(period_type);

-- Add index for querying finalized charts
CREATE INDEX IF NOT EXISTS idx_chart_is_finalized ON Chart(is_finalized);

-- Combined index for common queries
CREATE INDEX IF NOT EXISTS idx_chart_period_type_finalized ON Chart(period_type, is_finalized);
