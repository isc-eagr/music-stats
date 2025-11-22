-- Migration Script: Old Schema to New Normalized Schema
-- SQLite Database
-- Created: 2025-11-20
--
-- This script migrates data from the old denormalized schema to the new normalized schema
-- 
-- IMPORTANT: 
-- 1. Backup your database before running this script!
-- 2. This assumes the old tables have been renamed to: song_old_backup, scrobble_old_backup
-- 3. This assumes the new tables have been created using db_schema_new.sql
-- 4. If you want to run this in a transaction, execute BEGIN TRANSACTION manually before running this script

-- ============================================
-- STEP 1: Populate Lookup Tables from Old Data
-- ============================================

-- Populate Gender table (from old 'sex' field)
INSERT OR IGNORE INTO Gender (name)
SELECT DISTINCT 
    CASE 
        WHEN sex IS NULL OR sex = '' THEN 'Unknown'
        ELSE sex 
    END as name
FROM song_old_backup
WHERE sex IS NOT NULL
ORDER BY name;

-- Populate Ethnicity table (from old 'race' field)
INSERT OR IGNORE INTO Ethnicity (name)
SELECT DISTINCT 
    CASE 
        WHEN race IS NULL OR race = '' THEN 'Unknown'
        ELSE race 
    END as name
FROM song_old_backup
WHERE race IS NOT NULL
ORDER BY name;

-- Populate Language table (from old 'language' field)
INSERT OR IGNORE INTO Language (name)
SELECT DISTINCT 
    CASE 
        WHEN language IS NULL OR language = '' THEN 'Other'
        ELSE language 
    END as name
FROM song_old_backup
WHERE language IS NOT NULL
ORDER BY name;

-- Populate Genre table (from old 'genre' field)
INSERT OR IGNORE INTO Genre (name)
SELECT DISTINCT 
    CASE 
        WHEN genre IS NULL OR genre = '' THEN 'Unknown'
        ELSE genre 
    END as name
FROM song_old_backup
WHERE genre IS NOT NULL
ORDER BY name;

-- ============================================
-- STEP 2: Create Temporary Mapping Tables
-- ============================================

-- Create a temporary table to map old song records to new artist IDs
DROP TABLE IF EXISTS temp_artist_mapping;
CREATE TEMPORARY TABLE temp_artist_mapping AS
SELECT DISTINCT 
    artist,
    (SELECT id FROM Gender WHERE name = COALESCE(NULLIF(sex, ''), 'Unknown')) as gender_id,
    (SELECT id FROM Ethnicity WHERE name = COALESCE(NULLIF(race, ''), 'Unknown')) as ethnicity_id,
    (SELECT id FROM Genre WHERE name = COALESCE(NULLIF(genre, ''), 'Unknown')) as genre_id,
    (SELECT id FROM Language WHERE name = COALESCE(NULLIF(language, ''), 'Other')) as language_id
FROM song_old_backup
WHERE artist IS NOT NULL AND artist != '';

-- ============================================
-- STEP 3: Migrate Artists
-- ============================================

INSERT INTO Artist (name, gender_id, ethnicity_id, genre_id, language_id, creation_date)
SELECT DISTINCT
    artist,
    gender_id,
    ethnicity_id,
    genre_id,
    language_id,
    CURRENT_TIMESTAMP
FROM temp_artist_mapping
ORDER BY artist;

-- ============================================
-- STEP 4: Migrate Albums
-- ============================================

-- Create temporary table for unique artist-album combinations
DROP TABLE IF EXISTS temp_album_mapping;
CREATE TEMPORARY TABLE temp_album_mapping AS
SELECT DISTINCT
    artist,
    CASE 
        WHEN album IS NULL OR album = '' THEN '(single)'
        ELSE album 
    END as album,
    MIN(year) as year,
    COUNT(DISTINCT song) as song_count
FROM song_old_backup
WHERE artist IS NOT NULL AND artist != ''
GROUP BY artist, 
    CASE 
        WHEN album IS NULL OR album = '' THEN '(single)'
        ELSE album 
    END;

-- Insert albums
INSERT INTO Album (artist_id, name, release_date, number_of_songs, creation_date)
SELECT 
    (SELECT id FROM Artist WHERE name = tam.artist) as artist_id,
    tam.album,
    CASE 
        WHEN tam.year > 1900 AND tam.year <= 2100 THEN tam.year || '-01-01'
        ELSE NULL 
    END as release_date,
    tam.song_count,
    CURRENT_TIMESTAMP
FROM temp_album_mapping tam
WHERE (SELECT id FROM Artist WHERE name = tam.artist) IS NOT NULL
ORDER BY tam.artist, tam.album;

-- ============================================
-- STEP 5: Migrate Songs
-- ============================================

-- Insert songs with proper foreign key references
INSERT INTO Song (
    artist_id, 
    album_id, 
    name, 
    status, 
    length_seconds,
    release_date,
    creation_date
)
SELECT 
    (SELECT id FROM Artist WHERE name = s.artist) as artist_id,
    (SELECT id FROM Album 
     WHERE artist_id = (SELECT id FROM Artist WHERE name = s.artist)
     AND name = COALESCE(NULLIF(s.album, ''), '(single)')
     LIMIT 1) as album_id,
    s.song as name,
    s.cloud_status as status,
    s.duration as length_seconds,
    CASE 
        WHEN s.year > 1900 AND s.year <= 2100 THEN s.year || '-01-01'
        ELSE NULL 
    END as release_date,
    COALESCE(s.created, CURRENT_TIMESTAMP) as creation_date
FROM song_old_backup s
WHERE s.artist IS NOT NULL 
  AND s.artist != ''
  AND s.song IS NOT NULL
  AND s.song != ''
ORDER BY s.artist, s.album, s.song;

-- ============================================
-- STEP 6: Create Song ID Mapping Table
-- ============================================

-- Create a mapping from old song IDs to new song IDs
DROP TABLE IF EXISTS temp_song_id_mapping;
CREATE TEMPORARY TABLE temp_song_id_mapping AS
SELECT 
    old_song.id as old_id,
    new_song.id as new_id
FROM song_old_backup old_song
INNER JOIN Song new_song ON (
    new_song.name = old_song.song
    AND new_song.artist_id = (SELECT id FROM Artist WHERE name = old_song.artist)
    AND new_song.album_id = (
        SELECT id FROM Album 
        WHERE artist_id = (SELECT id FROM Artist WHERE name = old_song.artist)
        AND name = COALESCE(NULLIF(old_song.album, ''), '(single)')
        LIMIT 1
    )
)
WHERE old_song.artist IS NOT NULL 
  AND old_song.song IS NOT NULL;

-- ============================================
-- STEP 7: Migrate Scrobbles
-- ============================================

-- Insert scrobbles with updated song_id references
INSERT INTO Scrobble (
    artist,
    album,
    song,
    lastfm_id,
    scrobble_date,
    song_id,
    account,
    creation_date
)
SELECT 
    sc.artist,
    sc.album,
    sc.song,
    sc.lastfm_id,
    sc.scrobble_date,
    COALESCE(
        (SELECT new_id FROM temp_song_id_mapping WHERE old_id = sc.song_id),
        NULL
    ) as song_id,
    sc.account,
    CURRENT_TIMESTAMP
FROM scrobble_old_backup sc
ORDER BY sc.scrobble_date;

-- ============================================
-- STEP 8: Data Validation & Statistics
-- ============================================

-- Display migration statistics
SELECT 'Migration Statistics:' as info;

SELECT 'Artists migrated:' as metric, COUNT(*) as count FROM Artist;
SELECT 'Albums migrated:' as metric, COUNT(*) as count FROM Album;
SELECT 'Songs migrated:' as metric, COUNT(*) as count FROM Song;
SELECT 'Scrobbles migrated:' as metric, COUNT(*) as count FROM Scrobble;
SELECT 'Genres created:' as metric, COUNT(*) as count FROM Genre;
SELECT 'Languages created:' as metric, COUNT(*) as count FROM Language;
SELECT 'Genders created:' as metric, COUNT(*) as count FROM Gender;
SELECT 'Ethnicities created:' as metric, COUNT(*) as count FROM Ethnicity;

-- Check for orphaned scrobbles (scrobbles with no matching song_id)
SELECT 'Scrobbles without song match:' as metric, 
       COUNT(*) as count 
FROM Scrobble 
WHERE song_id IS NULL;

-- Check for duplicate songs
SELECT 'Potential duplicate songs:' as metric,
       COUNT(*) as count
FROM (
    SELECT artist_id, album_id, name, COUNT(*) as cnt
    FROM Song
    GROUP BY artist_id, album_id, name
    HAVING COUNT(*) > 1
);

-- ============================================
-- STEP 9: Clean Up Temporary Tables
-- ============================================

DROP TABLE IF EXISTS temp_artist_mapping;
DROP TABLE IF EXISTS temp_album_mapping;
DROP TABLE IF EXISTS temp_song_id_mapping;

-- ============================================
-- MIGRATION COMPLETE
-- ============================================

-- Review the statistics above. 
-- If you ran this in a transaction (BEGIN TRANSACTION), you can now:
--   - COMMIT; to save the changes
--   - ROLLBACK; to undo if something went wrong

-- ============================================
-- POST-MIGRATION TASKS
-- ============================================

-- After successful migration, you may want to:
-- 1. Your old tables have been backed up as:
--    - song_old_backup
--    - scrobble_old_backup
--    You can drop them once you've verified the migration:
--    DROP TABLE song_old_backup;
--    DROP TABLE scrobble_old_backup;
--
-- 2. Run VACUUM to reclaim space:
--    VACUUM;
--
-- 3. Update statistics for query optimizer:
--    ANALYZE;
