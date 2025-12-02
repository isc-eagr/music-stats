-- Migration: Fix Chart date storage format
-- The dates were stored as timestamps (milliseconds) instead of human-readable format.
-- This migration converts them to human-readable format.

-- Fix period_start_date (convert from milliseconds to yyyy-MM-dd)
UPDATE Chart 
SET period_start_date = date(period_start_date / 1000, 'unixepoch')
WHERE period_start_date GLOB '[0-9]*' 
  AND length(period_start_date) > 12;

-- Fix period_end_date (convert from milliseconds to yyyy-MM-dd)
UPDATE Chart 
SET period_end_date = date(period_end_date / 1000, 'unixepoch')
WHERE period_end_date GLOB '[0-9]*' 
  AND length(period_end_date) > 12;

-- Fix generated_date (convert from milliseconds to yyyy-MM-dd HH:mm:ss)
UPDATE Chart 
SET generated_date = datetime(generated_date / 1000, 'unixepoch', 'localtime')
WHERE generated_date GLOB '[0-9]*' 
  AND length(generated_date) > 12;

-- Verify the changes
-- SELECT id, period_start_date, period_end_date, generated_date FROM Chart LIMIT 5;
