-- Migration: Set single_cover from first SongImage for songs without a default image
-- This fixes songs that have gallery images but no single_cover set
-- Run this once to migrate existing data

-- First, let's see how many songs are affected
SELECT COUNT(*) as songs_to_fix
FROM Song s
WHERE s.single_cover IS NULL
  AND EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id);

-- Preview which songs will be updated (optional - comment out if not needed)
SELECT s.id, s.name, 
       (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id) as gallery_count,
       (SELECT MIN(id) FROM SongImage WHERE song_id = s.id) as first_image_id
FROM Song s
WHERE s.single_cover IS NULL
  AND EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id)
ORDER BY s.id;

-- Update: Set single_cover to the first gallery image (by display_order, then by id)
UPDATE Song
SET single_cover = (
    SELECT si.image 
    FROM SongImage si 
    WHERE si.song_id = Song.id 
    ORDER BY si.display_order ASC, si.id ASC 
    LIMIT 1
)
WHERE single_cover IS NULL
  AND EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = Song.id);

-- Verify the fix
SELECT COUNT(*) as songs_still_without_cover
FROM Song s
WHERE s.single_cover IS NULL
  AND EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id);

-- ============================================================
-- DUPLICATE IMAGE CLEANUP
-- ============================================================

-- Find duplicate images in SongImage table (same song, same image data)
-- Uses length comparison first, then actual data comparison for accuracy
SELECT 
    si1.id as duplicate_id,
    si1.song_id,
    s.name as song_name,
    si1.display_order,
    length(si1.image) as image_size
FROM SongImage si1
JOIN Song s ON s.id = si1.song_id
WHERE EXISTS (
    SELECT 1 FROM SongImage si2 
    WHERE si2.song_id = si1.song_id 
      AND si2.id < si1.id
      AND length(si2.image) = length(si1.image)
      AND si2.image = si1.image
)
ORDER BY si1.song_id, si1.id;

-- Count how many duplicates we have
SELECT COUNT(*) as duplicate_images_to_delete
FROM SongImage si1
WHERE EXISTS (
    SELECT 1 FROM SongImage si2 
    WHERE si2.song_id = si1.song_id 
      AND si2.id < si1.id
      AND length(si2.image) = length(si1.image)
      AND si2.image = si1.image
);

-- DELETE duplicate images (keeps the one with lowest id for each unique image per song)
DELETE FROM SongImage
WHERE id IN (
    SELECT si1.id
    FROM SongImage si1
    WHERE EXISTS (
        SELECT 1 FROM SongImage si2 
        WHERE si2.song_id = si1.song_id 
          AND si2.id < si1.id
          AND length(si2.image) = length(si1.image)
          AND si2.image = si1.image
    )
);

-- Also check for duplicates between single_cover and SongImage
-- (same image in both places)
SELECT 
    si.id as gallery_image_id,
    si.song_id,
    s.name as song_name,
    'Duplicate of single_cover' as reason
FROM SongImage si
JOIN Song s ON s.id = si.song_id
WHERE s.single_cover IS NOT NULL
  AND length(s.single_cover) = length(si.image)
  AND s.single_cover = si.image;

-- Count duplicates of single_cover in gallery
SELECT COUNT(*) as gallery_duplicates_of_single_cover
FROM SongImage si
JOIN Song s ON s.id = si.song_id
WHERE s.single_cover IS NOT NULL
  AND length(s.single_cover) = length(si.image)
  AND s.single_cover = si.image;

-- DELETE gallery images that are duplicates of single_cover
DELETE FROM SongImage
WHERE id IN (
    SELECT si.id
    FROM SongImage si
    JOIN Song s ON s.id = si.song_id
    WHERE s.single_cover IS NOT NULL
      AND length(s.single_cover) = length(si.image)
      AND s.single_cover = si.image
);

-- Verify cleanup
SELECT 
    (SELECT COUNT(*) FROM SongImage) as total_gallery_images,
    (SELECT COUNT(DISTINCT song_id) FROM SongImage) as songs_with_gallery_images;
