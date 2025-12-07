-- ============================================
-- Featured Artist Population Script
-- ============================================
-- This script populates the SongFeaturedArtist table by parsing song titles
-- for featured artist mentions (e.g., "Song Name (Feat. Artist)")
-- 
-- IMPORTANT: This is an alternative SQL-only approach to the Java class.
-- Only use this if you prefer SQL over running the Java populator.
-- 
-- Usage:
--   1. Review the patterns and test on a subset first
--   2. Run this script against your SQLite database
--   3. Review the results and adjust as needed
-- ============================================


-- Step 1: Create a temporary view to extract featured artists from song titles
-- This finds songs with "feat.", "ft.", or "featuring" patterns
DROP VIEW IF EXISTS temp_featured_artists_raw;
CREATE TEMP VIEW temp_featured_artists_raw AS
SELECT 
    s.id AS song_id,
    s.name AS song_name,
    s.artist_id AS primary_artist_id,
    -- Extract the featured section from parentheses/brackets
    CASE 
        -- Pattern: (feat. Artist), (ft. Artist), (featuring Artist)
        WHEN LOWER(s.name) LIKE '%(feat.%' OR LOWER(s.name) LIKE '%(ft.%' OR LOWER(s.name) LIKE '%(featuring%' THEN
            TRIM(SUBSTR(
                s.name,
                INSTR(LOWER(s.name), 'feat') + 
                    CASE 
                        WHEN INSTR(LOWER(s.name), 'feat.') > 0 THEN 5
                        WHEN INSTR(LOWER(s.name), 'featuring') > 0 THEN 10
                        WHEN INSTR(LOWER(s.name), 'ft.') > 0 THEN 3
                        ELSE 4
                    END,
                INSTR(SUBSTR(s.name, INSTR(LOWER(s.name), 'feat')), ')') - 
                    CASE 
                        WHEN INSTR(LOWER(s.name), 'feat.') > 0 THEN 5
                        WHEN INSTR(LOWER(s.name), 'featuring') > 0 THEN 10
                        WHEN INSTR(LOWER(s.name), 'ft.') > 0 THEN 3
                        ELSE 4
                    END
            ))
        -- Pattern: [feat. Artist], [ft. Artist], [featuring Artist]
        WHEN LOWER(s.name) LIKE '%[feat.%' OR LOWER(s.name) LIKE '%[ft.%' OR LOWER(s.name) LIKE '%[featuring%' THEN
            TRIM(SUBSTR(
                s.name,
                INSTR(LOWER(s.name), 'feat') + 
                    CASE 
                        WHEN INSTR(LOWER(s.name), 'feat.') > 0 THEN 5
                        WHEN INSTR(LOWER(s.name), 'featuring') > 0 THEN 10
                        WHEN INSTR(LOWER(s.name), 'ft.') > 0 THEN 3
                        ELSE 4
                    END,
                INSTR(SUBSTR(s.name, INSTR(LOWER(s.name), 'feat')), ']') - 
                    CASE 
                        WHEN INSTR(LOWER(s.name), 'feat.') > 0 THEN 5
                        WHEN INSTR(LOWER(s.name), 'featuring') > 0 THEN 10
                        WHEN INSTR(LOWER(s.name), 'ft.') > 0 THEN 3
                        ELSE 4
                    END
            ))
        ELSE NULL
    END AS featured_section
FROM Song s
WHERE featured_section IS NOT NULL;


-- Step 2: Preview what will be matched
-- Run this SELECT to review before inserting
SELECT 
    tfar.song_id,
    tfar.song_name,
    pa.name AS primary_artist,
    tfar.featured_section,
    a.id AS featured_artist_id,
    a.name AS featured_artist_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM SongFeaturedArtist sfa 
            WHERE sfa.song_id = tfar.song_id 
            AND sfa.artist_id = a.id
        ) THEN 'EXISTS'
        ELSE 'NEW'
    END AS status
FROM temp_featured_artists_raw tfar
INNER JOIN Artist pa ON tfar.primary_artist_id = pa.id
INNER JOIN Artist a ON LOWER(TRIM(tfar.featured_section)) = LOWER(a.name)
ORDER BY tfar.song_id;


-- Step 3: Insert featured artist relationships
-- CAUTION: Review Step 2 results before running this!
-- Uncomment the following lines to execute the insert:

/*
INSERT OR IGNORE INTO SongFeaturedArtist (song_id, artist_id, creation_date)
SELECT 
    tfar.song_id,
    a.id AS artist_id,
    CURRENT_TIMESTAMP
FROM temp_featured_artists_raw tfar
INNER JOIN Artist a ON LOWER(TRIM(tfar.featured_section)) = LOWER(a.name)
WHERE NOT EXISTS (
    SELECT 1 FROM SongFeaturedArtist sfa 
    WHERE sfa.song_id = tfar.song_id 
    AND sfa.artist_id = a.id
);
*/


-- Step 4: Get statistics about what was found
SELECT 
    'Total songs with feat. pattern' AS metric,
    COUNT(*) AS count
FROM temp_featured_artists_raw
UNION ALL
SELECT 
    'Songs with matched artists' AS metric,
    COUNT(DISTINCT tfar.song_id) AS count
FROM temp_featured_artists_raw tfar
INNER JOIN Artist a ON LOWER(TRIM(tfar.featured_section)) = LOWER(a.name)
UNION ALL
SELECT 
    'Total matches found' AS metric,
    COUNT(*) AS count
FROM temp_featured_artists_raw tfar
INNER JOIN Artist a ON LOWER(TRIM(tfar.featured_section)) = LOWER(a.name)
UNION ALL
SELECT 
    'Already in SongFeaturedArtist' AS metric,
    COUNT(*) AS count
FROM temp_featured_artists_raw tfar
INNER JOIN Artist a ON LOWER(TRIM(tfar.featured_section)) = LOWER(a.name)
WHERE EXISTS (
    SELECT 1 FROM SongFeaturedArtist sfa 
    WHERE sfa.song_id = tfar.song_id 
    AND sfa.artist_id = a.id
);


-- Step 5: Clean up
DROP VIEW IF EXISTS temp_featured_artists_raw;


-- ============================================
-- NOTES AND LIMITATIONS
-- ============================================
-- 
-- This SQL script has limitations compared to the Java version:
-- 1. Only handles single featured artists (no splitting by & or ,)
-- 2. SQLite's string manipulation is limited for complex parsing
-- 3. Case-insensitive matching only
-- 4. Cannot handle variations like "feat Artist1 & Artist2"
-- 
-- For more robust parsing (multiple artists, variations, etc.),
-- use the FeaturedArtistPopulator.java class instead.
-- 
-- Common patterns this will catch:
--   - Song Name (feat. Artist)
--   - Song Name (ft. Artist)
--   - Song Name (featuring Artist)
--   - Song Name [feat. Artist]
--   - Song Name [ft. Artist]
-- 
-- Patterns this will NOT catch well:
--   - Song Name (feat. Artist1 & Artist2)
--   - Song Name feat. Artist (no parentheses)
--   - Complex variations with commas
-- ============================================
