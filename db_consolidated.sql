-- ============================================
-- Music Stats Database - Consolidated Schema
-- ============================================
-- SQLite Database
-- 
-- This file consolidates all schema definitions, migrations, 
-- indexes, and triggers into a single script for new installations.
--
-- Created: 2025-11-20
-- Last Updated: 2025-12-03
--
-- USAGE:
--   For NEW databases: Run this entire script
--   For EXISTING databases: See individual migration scripts
--
-- ============================================


-- ============================================
-- SECTION 1: LOOKUP/REFERENCE TABLES
-- ============================================

-- Gender table
CREATE TABLE IF NOT EXISTS Gender (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ethnicity table
CREATE TABLE IF NOT EXISTS Ethnicity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Language table
CREATE TABLE IF NOT EXISTS Language (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Genre table
CREATE TABLE IF NOT EXISTS Genre (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SubGenre table
CREATE TABLE IF NOT EXISTS SubGenre (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    parent_genre_id INTEGER NOT NULL,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_genre_id) REFERENCES Genre(id) ON DELETE CASCADE,
    UNIQUE(name, parent_genre_id)
);


-- ============================================
-- SECTION 2: MAIN ENTITY TABLES
-- ============================================

-- Artist table
CREATE TABLE IF NOT EXISTS Artist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(500) NOT NULL,
    gender_id INTEGER,
    country VARCHAR(100),
    ethnicity_id INTEGER,
    genre_id INTEGER,
    subgenre_id INTEGER,
    language_id INTEGER,
    is_band INTEGER DEFAULT 0,
    organized INTEGER DEFAULT 0,
    birth_date DATE,
    death_date DATE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (gender_id) REFERENCES Gender(id) ON DELETE SET NULL,
    FOREIGN KEY (ethnicity_id) REFERENCES Ethnicity(id) ON DELETE SET NULL,
    FOREIGN KEY (genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (language_id) REFERENCES Language(id) ON DELETE SET NULL
);

-- Album table
CREATE TABLE IF NOT EXISTS Album (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    name VARCHAR(500) NOT NULL,
    release_date DATE,
    number_of_songs INTEGER DEFAULT 0,
    override_genre_id INTEGER,
    override_subgenre_id INTEGER,
    override_language_id INTEGER,
    organized INTEGER DEFAULT 0,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (override_genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_language_id) REFERENCES Language(id) ON DELETE SET NULL
);

-- Song table
CREATE TABLE IF NOT EXISTS Song (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    album_id INTEGER,
    name VARCHAR(500) NOT NULL,
    length_seconds INTEGER,
    is_single INTEGER DEFAULT 0,
    override_genre_id INTEGER,
    override_subgenre_id INTEGER,
    override_language_id INTEGER,
    override_gender_id INTEGER,
    override_ethnicity_id INTEGER,
    release_date DATE,
    organized INTEGER DEFAULT 0,
    single_cover BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (album_id) REFERENCES Album(id) ON DELETE SET NULL,
    FOREIGN KEY (override_genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_language_id) REFERENCES Language(id) ON DELETE SET NULL,
    FOREIGN KEY (override_gender_id) REFERENCES Gender(id) ON DELETE SET NULL,
    FOREIGN KEY (override_ethnicity_id) REFERENCES Ethnicity(id) ON DELETE SET NULL
);

-- Scrobble table (play history)
CREATE TABLE IF NOT EXISTS Scrobble (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist VARCHAR(255),
    album VARCHAR(255),
    song VARCHAR(255),
    lastfm_id INTEGER,
    scrobble_date TIMESTAMP,
    song_id INTEGER,
    account VARCHAR(255),
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (song_id) REFERENCES Song(id) ON DELETE SET NULL
);

-- SongFeaturedArtist table (many-to-many for featured artists)
CREATE TABLE IF NOT EXISTS SongFeaturedArtist (
    song_id INTEGER NOT NULL,
    artist_id INTEGER NOT NULL,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (song_id, artist_id),
    FOREIGN KEY (song_id) REFERENCES Song(id) ON DELETE CASCADE,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE
);

-- ArtistMember table (many-to-many for artist group membership, e.g., Beyoncé is part of Destiny's Child)
CREATE TABLE IF NOT EXISTS ArtistMember (
    group_artist_id INTEGER NOT NULL,
    member_artist_id INTEGER NOT NULL,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_artist_id, member_artist_id),
    FOREIGN KEY (group_artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (member_artist_id) REFERENCES Artist(id) ON DELETE CASCADE
);

-- ============================================
-- SECTION 3: GALLERY IMAGE TABLES
-- ============================================

-- ArtistImage table (additional images for artists)
CREATE TABLE IF NOT EXISTS ArtistImage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    image BLOB NOT NULL,
    display_order INTEGER DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE
);

-- AlbumImage table (additional images for albums)
CREATE TABLE IF NOT EXISTS AlbumImage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    album_id INTEGER NOT NULL,
    image BLOB NOT NULL,
    display_order INTEGER DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (album_id) REFERENCES Album(id) ON DELETE CASCADE
);

-- SongImage table (additional images for songs)
CREATE TABLE IF NOT EXISTS SongImage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    song_id INTEGER NOT NULL,
    image BLOB NOT NULL,
    display_order INTEGER DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (song_id) REFERENCES Song(id) ON DELETE CASCADE
);


-- ============================================
-- SECTION 3: CHART TABLES
-- ============================================

-- Chart table: metadata for weekly/seasonal/yearly charts
CREATE TABLE IF NOT EXISTS Chart (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_type VARCHAR(20) NOT NULL,           -- 'song' or 'album'
    period_type VARCHAR(20) DEFAULT 'weekly' NOT NULL,  -- 'weekly', 'seasonal', 'yearly'
    period_key VARCHAR(20) NOT NULL,           -- e.g., '2024-W48', 'spring-2024', '2024'
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    is_finalized INTEGER DEFAULT 0 NOT NULL,   -- For seasonal/yearly: draft vs final
    generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ChartEntry table: individual positions in each chart
CREATE TABLE IF NOT EXISTS ChartEntry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    song_id INTEGER,
    album_id INTEGER,
    play_count INTEGER,                        -- Nullable for manual charts
    FOREIGN KEY (chart_id) REFERENCES Chart(id) ON DELETE CASCADE,
    FOREIGN KEY (song_id) REFERENCES Song(id),
    FOREIGN KEY (album_id) REFERENCES Album(id)
);


-- ============================================
-- SECTION 4: BASIC INDEXES
-- ============================================

-- Artist indexes
CREATE INDEX IF NOT EXISTS idx_artist_name ON Artist(name);
CREATE INDEX IF NOT EXISTS idx_artist_gender ON Artist(gender_id);
CREATE INDEX IF NOT EXISTS idx_artist_ethnicity ON Artist(ethnicity_id);
CREATE INDEX IF NOT EXISTS idx_artist_genre ON Artist(genre_id);
CREATE INDEX IF NOT EXISTS idx_artist_language ON Artist(language_id);
CREATE INDEX IF NOT EXISTS idx_artist_organized ON Artist(organized);

-- Album indexes
CREATE INDEX IF NOT EXISTS idx_album_artist ON Album(artist_id);
CREATE INDEX IF NOT EXISTS idx_album_name ON Album(name);
CREATE INDEX IF NOT EXISTS idx_album_release_date ON Album(release_date);
CREATE INDEX IF NOT EXISTS idx_album_organized ON Album(organized);

-- Song indexes
CREATE INDEX IF NOT EXISTS idx_song_artist ON Song(artist_id);
CREATE INDEX IF NOT EXISTS idx_song_album ON Song(album_id);
CREATE INDEX IF NOT EXISTS idx_song_name ON Song(name);
CREATE INDEX IF NOT EXISTS idx_song_release_date ON Song(release_date);
CREATE INDEX IF NOT EXISTS idx_song_organized ON Song(organized);

-- Scrobble indexes
CREATE INDEX IF NOT EXISTS idx_scrobble_song_id ON Scrobble(song_id);
CREATE INDEX IF NOT EXISTS idx_scrobble_date ON Scrobble(scrobble_date);
CREATE INDEX IF NOT EXISTS idx_scrobble_account ON Scrobble(account);
CREATE INDEX IF NOT EXISTS idx_scrobble_artist ON Scrobble(artist);
CREATE INDEX IF NOT EXISTS idx_scrobble_lastfm_id ON Scrobble(lastfm_id);

-- SongFeaturedArtist indexes
CREATE INDEX IF NOT EXISTS idx_song_featured_artist_song ON SongFeaturedArtist(song_id);
CREATE INDEX IF NOT EXISTS idx_song_featured_artist_artist ON SongFeaturedArtist(artist_id);

-- ArtistMember indexes
CREATE INDEX IF NOT EXISTS idx_artist_member_group ON ArtistMember(group_artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_member_member ON ArtistMember(member_artist_id);

-- Gallery image indexes
CREATE INDEX IF NOT EXISTS idx_artist_image_artist ON ArtistImage(artist_id);
CREATE INDEX IF NOT EXISTS idx_album_image_album ON AlbumImage(album_id);
CREATE INDEX IF NOT EXISTS idx_song_image_song ON SongImage(song_id);

-- SubGenre indexes
CREATE INDEX IF NOT EXISTS idx_subgenre_parent ON SubGenre(parent_genre_id);
CREATE INDEX IF NOT EXISTS idx_subgenre_name ON SubGenre(name);

-- Chart indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_chart_type_period_type_key ON Chart(chart_type, period_type, period_key);
CREATE INDEX IF NOT EXISTS idx_chart_type ON Chart(chart_type);
CREATE INDEX IF NOT EXISTS idx_chart_period_type ON Chart(period_type);
CREATE INDEX IF NOT EXISTS idx_chart_is_finalized ON Chart(is_finalized);
CREATE INDEX IF NOT EXISTS idx_chart_period_type_finalized ON Chart(period_type, is_finalized);
CREATE INDEX IF NOT EXISTS idx_chartentry_chart_id ON ChartEntry(chart_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_song_id ON ChartEntry(song_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_album_id ON ChartEntry(album_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_position ON ChartEntry(chart_id, position);


-- ============================================
-- SECTION 5: PERFORMANCE INDEXES
-- ============================================

-- Composite indexes for account filtering
CREATE INDEX IF NOT EXISTS idx_scrobble_account_songid ON Scrobble(account, song_id);
CREATE INDEX IF NOT EXISTS idx_scrobble_account_date ON Scrobble(account, scrobble_date);

-- Indexes for Song joins
CREATE INDEX IF NOT EXISTS idx_song_artistid_albumid ON Song(artist_id, album_id);
CREATE INDEX IF NOT EXISTS idx_song_albumid_artistid ON Song(album_id, artist_id);

-- Indexes for override resolution
CREATE INDEX IF NOT EXISTS idx_song_override_genre ON Song(override_genre_id);
CREATE INDEX IF NOT EXISTS idx_song_override_subgenre ON Song(override_subgenre_id);
CREATE INDEX IF NOT EXISTS idx_song_override_language ON Song(override_language_id);
CREATE INDEX IF NOT EXISTS idx_song_override_gender ON Song(override_gender_id);
CREATE INDEX IF NOT EXISTS idx_song_override_ethnicity ON Song(override_ethnicity_id);
CREATE INDEX IF NOT EXISTS idx_album_override_genre ON Album(override_genre_id);
CREATE INDEX IF NOT EXISTS idx_album_override_subgenre ON Album(override_subgenre_id);
CREATE INDEX IF NOT EXISTS idx_album_override_language ON Album(override_language_id);

-- Indexes for chart aggregations
CREATE INDEX IF NOT EXISTS idx_artist_gender_id ON Artist(gender_id) WHERE gender_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artist_ethnicity_country ON Artist(ethnicity_id, country);
CREATE INDEX IF NOT EXISTS idx_artist_genre_gender ON Artist(genre_id, gender_id);

-- Indexes for artist detail page
CREATE INDEX IF NOT EXISTS idx_scrobble_songid_account ON Scrobble(song_id, account);
CREATE INDEX IF NOT EXISTS idx_scrobble_songid_date ON Scrobble(song_id, scrobble_date);
CREATE INDEX IF NOT EXISTS idx_song_artistid_length ON Song(artist_id, length_seconds);

-- Covering indexes for hot queries
CREATE INDEX IF NOT EXISTS idx_scrobble_cover_plays ON Scrobble(song_id, account, scrobble_date);
CREATE INDEX IF NOT EXISTS idx_song_cover_joins ON Song(id, artist_id, album_id, length_seconds);

-- Indexes for Top Charts tab
CREATE INDEX IF NOT EXISTS idx_song_artistid_albumid_length ON Song(artist_id, album_id, length_seconds);
CREATE INDEX IF NOT EXISTS idx_song_albumid_length_notnull ON Song(album_id, length_seconds) WHERE album_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_scrobble_date_songid ON Scrobble(scrobble_date, song_id);
CREATE INDEX IF NOT EXISTS idx_scrobble_songid_account_date ON Scrobble(song_id, account, scrobble_date);


-- ============================================
-- SECTION 6: TIMEFRAME PERFORMANCE INDEXES
-- ============================================

-- Covering index for scrobble scans in timeframe CTEs
CREATE INDEX IF NOT EXISTS idx_scrobble_covering_timeframe ON Scrobble(
    scrobble_date,
    song_id
) WHERE scrobble_date IS NOT NULL;

-- Covering index for Song table joins
CREATE INDEX IF NOT EXISTS idx_song_covering_joins ON Song(
    id,
    artist_id,
    album_id,
    length_seconds,
    override_gender_id,
    override_genre_id,
    override_ethnicity_id,
    override_language_id
);

-- Covering index for Artist joins
CREATE INDEX IF NOT EXISTS idx_artist_covering ON Artist(
    id,
    gender_id,
    genre_id,
    ethnicity_id,
    language_id,
    country
);

-- Covering index for Album override lookups
CREATE INDEX IF NOT EXISTS idx_album_override_covering ON Album(
    id,
    override_genre_id,
    override_language_id
);

-- Lookup table indexes for filtering
CREATE INDEX IF NOT EXISTS idx_gender_for_filtering ON Gender(id, name);
CREATE INDEX IF NOT EXISTS idx_genre_for_filtering ON Genre(id, name);
CREATE INDEX IF NOT EXISTS idx_ethnicity_for_filtering ON Ethnicity(id, name);
CREATE INDEX IF NOT EXISTS idx_language_for_filtering ON Language(id, name);


-- ============================================
-- SECTION 7: UPDATE TRIGGERS
-- ============================================

-- Artist update trigger
CREATE TRIGGER IF NOT EXISTS update_artist_timestamp 
AFTER UPDATE ON Artist
FOR EACH ROW
BEGIN
    UPDATE Artist SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Album update trigger
CREATE TRIGGER IF NOT EXISTS update_album_timestamp 
AFTER UPDATE ON Album
FOR EACH ROW
BEGIN
    UPDATE Album SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Song update trigger
CREATE TRIGGER IF NOT EXISTS update_song_timestamp 
AFTER UPDATE ON Song
FOR EACH ROW
BEGIN
    UPDATE Song SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Scrobble update trigger
CREATE TRIGGER IF NOT EXISTS update_scrobble_timestamp 
AFTER UPDATE ON Scrobble
FOR EACH ROW
BEGIN
    UPDATE Scrobble SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Gender update trigger
CREATE TRIGGER IF NOT EXISTS update_gender_timestamp 
AFTER UPDATE ON Gender
FOR EACH ROW
BEGIN
    UPDATE Gender SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Ethnicity update trigger
CREATE TRIGGER IF NOT EXISTS update_ethnicity_timestamp 
AFTER UPDATE ON Ethnicity
FOR EACH ROW
BEGIN
    UPDATE Ethnicity SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Language update trigger
CREATE TRIGGER IF NOT EXISTS update_language_timestamp 
AFTER UPDATE ON Language
FOR EACH ROW
BEGIN
    UPDATE Language SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Genre update trigger
CREATE TRIGGER IF NOT EXISTS update_genre_timestamp 
AFTER UPDATE ON Genre
FOR EACH ROW
BEGIN
    UPDATE Genre SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- SubGenre update trigger
CREATE TRIGGER IF NOT EXISTS update_subgenre_timestamp 
AFTER UPDATE ON SubGenre
FOR EACH ROW
BEGIN
    UPDATE SubGenre SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;


-- ============================================
-- SECTION 8: ANALYZE FOR QUERY OPTIMIZER
-- ============================================

ANALYZE;


-- ============================================
-- END OF CONSOLIDATED SCHEMA
-- ============================================
