-- ============================================
-- Populate Album Release Dates from Song Data
-- ============================================
-- Logic:
-- 1. For each album, find the most frequent release_date among its songs
-- 2. Set that as the album's release_date
-- 3. Clear release_date from songs that match the album's new date
-- 4. Keep release_date on songs with different dates (these are overrides)
-- ============================================

-- ============================================
-- DIAGNOSTICS: Check current state
-- ============================================

SELECT '=== BEFORE ===' as status;

SELECT 'Total albums:' as metric, COUNT(*) as value FROM Album;
SELECT 'Albums with release_date:' as metric, COUNT(*) as value FROM Album WHERE release_date IS NOT NULL;
SELECT 'Songs with release_date:' as metric, COUNT(*) as value FROM Song WHERE release_date IS NOT NULL;
SELECT 'Songs with album_id:' as metric, COUNT(*) as value FROM Song WHERE album_id IS NOT NULL;
SELECT 'Songs with both album_id and release_date:' as metric, COUNT(*) as value FROM Song WHERE album_id IS NOT NULL AND release_date IS NOT NULL;

-- Preview: Show albums that would be updated (top 20)
SELECT '=== ALBUMS TO UPDATE (first 20) ===' as status;
SELECT 
    s.album_id,
    a.name as album_name,
    s.release_date,
    COUNT(*) as song_count
FROM Song s
JOIN Album a ON s.album_id = a.id
WHERE s.album_id IS NOT NULL 
  AND s.release_date IS NOT NULL
  AND (a.release_date IS NULL OR a.release_date != s.release_date)
GROUP BY s.album_id, s.release_date
ORDER BY song_count DESC
LIMIT 20;

-- ============================================
-- STEP 1: Update albums with the most common release date from their songs
-- Using a subquery approach that works in all SQLite versions
-- ============================================

UPDATE Album
SET release_date = (
    SELECT s.release_date
    FROM Song s
    WHERE s.album_id = Album.id
      AND s.release_date IS NOT NULL
    GROUP BY s.release_date
    ORDER BY COUNT(*) DESC, s.release_date ASC
    LIMIT 1
)
WHERE id IN (
    SELECT DISTINCT album_id 
    FROM Song 
    WHERE album_id IS NOT NULL 
      AND release_date IS NOT NULL
)
AND (release_date IS NULL OR release_date != (
    SELECT s.release_date
    FROM Song s
    WHERE s.album_id = Album.id
      AND s.release_date IS NOT NULL
    GROUP BY s.release_date
    ORDER BY COUNT(*) DESC, s.release_date ASC
    LIMIT 1
));

SELECT 'Albums updated:' as metric, changes() as value;

-- ============================================
-- STEP 2: Clear song release dates that match the album date
-- (these are now redundant since the album has the date)
-- ============================================

UPDATE Song
SET release_date = NULL
WHERE album_id IS NOT NULL
  AND release_date IS NOT NULL
  AND release_date = (
      SELECT release_date 
      FROM Album 
      WHERE Album.id = Song.album_id
  );

SELECT 'Song dates cleared (now inherited from album):' as metric, changes() as value;

-- ============================================
-- FINAL DIAGNOSTICS
-- ============================================

SELECT '=== AFTER ===' as status;

SELECT 'Albums with release_date:' as metric, COUNT(*) as value FROM Album WHERE release_date IS NOT NULL;
SELECT 'Songs with release_date (overrides only):' as metric, COUNT(*) as value FROM Song WHERE release_date IS NOT NULL;

-- Show songs that still have release dates (these are overrides - different from album)
SELECT '=== SONGS WITH OVERRIDE DATES (first 30) ===' as status;
SELECT 
    ar.name as artist,
    a.name as album,
    a.release_date as album_date,
    s.name as song,
    s.release_date as song_override_date
FROM Song s
JOIN Album a ON s.album_id = a.id
JOIN Artist ar ON a.artist_id = ar.id
WHERE s.release_date IS NOT NULL
ORDER BY ar.name, a.name, s.name
LIMIT 30;

-- ============================================
-- Done! 
-- ============================================
