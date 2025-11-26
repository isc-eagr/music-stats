-- New Database Schema for Music Stats Application
-- SQLite Database
-- Created: 2025-11-20

-- ============================================
-- STEP 0: Backup existing tables before creating new schema
-- ============================================

-- Rename old tables to preserve existing data
-- If these tables don't exist, the commands will fail silently
ALTER TABLE song RENAME TO song_old_backup;
ALTER TABLE scrobble RENAME TO scrobble_old_backup;

-- Now drop new tables if they exist (to allow re-running this script)
DROP TABLE IF EXISTS Scrobble;
DROP TABLE IF EXISTS Song;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS Artist;
DROP TABLE IF EXISTS SubGenre;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Ethnicity;
DROP TABLE IF EXISTS Gender;
DROP TABLE IF EXISTS Language;

-- ============================================
-- Lookup/Reference Tables
-- ============================================

-- Gender table
CREATE TABLE Gender (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ethnicity table
CREATE TABLE Ethnicity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Language table
CREATE TABLE Language (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Genre table
CREATE TABLE Genre (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SubGenre table
CREATE TABLE SubGenre (
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
-- Main Tables
-- ============================================

-- Artist table
CREATE TABLE Artist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(500) NOT NULL,
    gender_id INTEGER,
    country VARCHAR(100),
    ethnicity_id INTEGER,
    genre_id INTEGER,
    subgenre_id INTEGER,
    language_id INTEGER,
    is_band INTEGER DEFAULT 0,
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
CREATE TABLE Album (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    name VARCHAR(500) NOT NULL,
    release_date DATE,
    number_of_songs INTEGER DEFAULT 0,
    override_genre_id INTEGER,
    override_subgenre_id INTEGER,
    override_language_id INTEGER,
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (override_genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_language_id) REFERENCES Language(id) ON DELETE SET NULL
);

-- Song table
CREATE TABLE Song (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    album_id INTEGER,
    name VARCHAR(500) NOT NULL,
    status VARCHAR(50),
    length_seconds INTEGER,
    is_single INTEGER DEFAULT 0,
    override_genre_id INTEGER,
    override_subgenre_id INTEGER,
    override_language_id INTEGER,
    override_gender_id INTEGER,
    override_ethnicity_id INTEGER,
    release_date DATE,
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

-- Scrobble table
CREATE TABLE Scrobble (
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

-- ============================================
-- Indexes for Performance
-- ============================================

-- Artist indexes
CREATE INDEX idx_artist_name ON Artist(name);
CREATE INDEX idx_artist_gender ON Artist(gender_id);
CREATE INDEX idx_artist_ethnicity ON Artist(ethnicity_id);
CREATE INDEX idx_artist_genre ON Artist(genre_id);
CREATE INDEX idx_artist_language ON Artist(language_id);

-- Album indexes
CREATE INDEX idx_album_artist ON Album(artist_id);
CREATE INDEX idx_album_name ON Album(name);
CREATE INDEX idx_album_release_date ON Album(release_date);

-- Song indexes
CREATE INDEX idx_song_artist ON Song(artist_id);
CREATE INDEX idx_song_album ON Song(album_id);
CREATE INDEX idx_song_name ON Song(name);
CREATE INDEX idx_song_status ON Song(status);
CREATE INDEX idx_song_release_date ON Song(release_date);

-- Scrobble indexes
CREATE INDEX idx_scrobble_song_id ON Scrobble(song_id);
CREATE INDEX idx_scrobble_date ON Scrobble(scrobble_date);
CREATE INDEX idx_scrobble_account ON Scrobble(account);
CREATE INDEX idx_scrobble_artist ON Scrobble(artist);
CREATE INDEX idx_scrobble_lastfm_id ON Scrobble(lastfm_id);

-- SubGenre indexes
CREATE INDEX idx_subgenre_parent ON SubGenre(parent_genre_id);
CREATE INDEX idx_subgenre_name ON SubGenre(name);

-- ============================================
-- Triggers for Update Date
-- ============================================

-- Artist update trigger
CREATE TRIGGER update_artist_timestamp 
AFTER UPDATE ON Artist
FOR EACH ROW
BEGIN
    UPDATE Artist SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Album update trigger
CREATE TRIGGER update_album_timestamp 
AFTER UPDATE ON Album
FOR EACH ROW
BEGIN
    UPDATE Album SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Song update trigger
CREATE TRIGGER update_song_timestamp 
AFTER UPDATE ON Song
FOR EACH ROW
BEGIN
    UPDATE Song SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Scrobble update trigger
CREATE TRIGGER update_scrobble_timestamp 
AFTER UPDATE ON Scrobble
FOR EACH ROW
BEGIN
    UPDATE Scrobble SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Gender update trigger
CREATE TRIGGER update_gender_timestamp 
AFTER UPDATE ON Gender
FOR EACH ROW
BEGIN
    UPDATE Gender SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Ethnicity update trigger
CREATE TRIGGER update_ethnicity_timestamp 
AFTER UPDATE ON Ethnicity
FOR EACH ROW
BEGIN
    UPDATE Ethnicity SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Language update trigger
CREATE TRIGGER update_language_timestamp 
AFTER UPDATE ON Language
FOR EACH ROW
BEGIN
    UPDATE Language SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Genre update trigger
CREATE TRIGGER update_genre_timestamp 
AFTER UPDATE ON Genre
FOR EACH ROW
BEGIN
    UPDATE Genre SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- SubGenre update trigger
CREATE TRIGGER update_subgenre_timestamp 
AFTER UPDATE ON SubGenre
FOR EACH ROW
BEGIN
    UPDATE SubGenre SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;
