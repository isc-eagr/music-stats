-- ============================================
-- Music Stats Database - Consolidated Schema
-- ============================================
-- SQLite Database
--
-- The complete current schema: every table, index and trigger of the live database
-- (C:/Music Stats DB/music-stats.db), except the old backup tables scrobble_old_backup and
-- song_old_backup.
--
-- Created: 2025-11-20
-- Last Updated: 2026-11-17 (synced with the live schema)
--
-- USAGE:
--   For NEW databases: run this script, then the three index scripts
--     db_detail_performance_indexes.sql, db_last_listened_performance_indexes.sql and
--     db_timeframe_performance_indexes.sql (their indexes are not repeated here).
--   For EXISTING databases: see the individual migration_*.sql scripts.
--
-- Keeping it in sync: a schema change goes here, in a migration_*.sql and in
-- TestDatabaseSupport.createSchema(). tools/devdb/build-dev-db.bat builds the dev DB from this
-- file plus the index scripts and prints any "Schema drift" against the live DB, which should be empty.
--
-- ============================================


-- ============================================
-- SECTION 1: LOOKUP/REFERENCE TABLES
-- ============================================

-- Gender table
CREATE TABLE IF NOT EXISTS Gender (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    image BLOB
);

-- Ethnicity table
CREATE TABLE IF NOT EXISTS Ethnicity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    image BLOB
);

-- Language table
CREATE TABLE IF NOT EXISTS Language (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    image BLOB
);

-- Genre table
CREATE TABLE IF NOT EXISTS Genre (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    image BLOB
);

-- SubGenre table (child of Genre)
CREATE TABLE IF NOT EXISTS SubGenre (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    parent_genre_id INTEGER NOT NULL,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    image BLOB,
    FOREIGN KEY (parent_genre_id) REFERENCES Genre(id) ON DELETE CASCADE,
    UNIQUE(name, parent_genre_id)
);


-- ============================================
-- SECTION 2: CORE ENTITY TABLES
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
    image BLOB,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_band INTEGER DEFAULT 0,
    organized INTEGER DEFAULT 0,
    birth_date DATE,
    death_date DATE,
    FOREIGN KEY (gender_id) REFERENCES Gender(id) ON DELETE SET NULL,
    FOREIGN KEY (ethnicity_id) REFERENCES Ethnicity(id) ON DELETE SET NULL,
    FOREIGN KEY (genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (language_id) REFERENCES Language(id) ON DELETE SET NULL
);

-- Album table (overrides fall back to the artist when NULL)
CREATE TABLE IF NOT EXISTS Album (
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
    organized INTEGER DEFAULT 0,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (override_genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_language_id) REFERENCES Language(id) ON DELETE SET NULL
);

-- Song table (overrides fall back to the album, then the artist, when NULL)
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
    track_number INTEGER,
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (album_id) REFERENCES Album(id) ON DELETE SET NULL,
    FOREIGN KEY (override_genre_id) REFERENCES Genre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_subgenre_id) REFERENCES SubGenre(id) ON DELETE SET NULL,
    FOREIGN KEY (override_language_id) REFERENCES Language(id) ON DELETE SET NULL,
    FOREIGN KEY (override_gender_id) REFERENCES Gender(id) ON DELETE SET NULL,
    FOREIGN KEY (override_ethnicity_id) REFERENCES Ethnicity(id) ON DELETE SET NULL
);

-- Play table (play history imported from Last.fm)
CREATE TABLE IF NOT EXISTS Play (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist VARCHAR(255),
    album VARCHAR(255),
    song VARCHAR(255),
    lastfm_id INTEGER,
    play_date TIMESTAMP,
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

-- ArtistMember table (many-to-many for artist group membership, e.g., Beyonce is part of Destiny's Child)
CREATE TABLE IF NOT EXISTS ArtistMember (
    group_artist_id INTEGER NOT NULL,
    member_artist_id INTEGER NOT NULL,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_artist_id, member_artist_id),
    FOREIGN KEY (group_artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (member_artist_id) REFERENCES Artist(id) ON DELETE CASCADE
);


-- ============================================
-- SECTION 3: GALLERY IMAGES AND THEMES
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

-- ArtistTheme table (named sets of artist images; one is active)
CREATE TABLE IF NOT EXISTS ArtistTheme (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    is_active     INTEGER NOT NULL DEFAULT 0,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ArtistImageTheme table (which image an artist shows in each theme)
CREATE TABLE IF NOT EXISTS ArtistImageTheme (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    theme_id        INTEGER NOT NULL,
    artist_id       INTEGER NOT NULL,
    artist_image_id INTEGER,          -- NULL = default/main image; otherwise FK -> ArtistImage.id
    creation_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (theme_id)        REFERENCES ArtistTheme(id)  ON DELETE CASCADE,
    FOREIGN KEY (artist_id)       REFERENCES Artist(id)       ON DELETE CASCADE,
    FOREIGN KEY (artist_image_id) REFERENCES ArtistImage(id)  ON DELETE SET NULL,
    UNIQUE (theme_id, artist_id)
);


-- ============================================
-- SECTION 4: TAGS
-- ============================================

CREATE TABLE IF NOT EXISTS Tag (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ArtistTag (
    artist_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (artist_id, tag_id),
    FOREIGN KEY (artist_id) REFERENCES Artist(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES Tag(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS AlbumTag (
    album_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (album_id, tag_id),
    FOREIGN KEY (album_id) REFERENCES Album(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES Tag(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS SongTag (
    song_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (song_id, tag_id),
    FOREIGN KEY (song_id) REFERENCES Song(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES Tag(id) ON DELETE CASCADE
);


-- ============================================
-- SECTION 5: CHART TABLES
-- ============================================

-- Chart table: metadata for weekly/seasonal/yearly charts
CREATE TABLE IF NOT EXISTS Chart (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_type VARCHAR(20) NOT NULL,           -- 'song' or 'album'
    period_key VARCHAR(20) NOT NULL,           -- e.g., '2024-W48', 'spring-2024', '2024'
    period_start_date DATE NOT NULL,           -- First day of the period
    period_end_date DATE NOT NULL,             -- Last day of the period
    generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    period_type VARCHAR(20) DEFAULT 'weekly' NOT NULL,  -- 'weekly', 'seasonal', 'yearly'
    is_finalized INTEGER DEFAULT 0 NOT NULL,   -- For seasonal/yearly: draft vs final
    UNIQUE(chart_type, period_key)
);

-- ChartEntry table: individual positions in each chart
CREATE TABLE IF NOT EXISTS ChartEntry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_id INTEGER NOT NULL,
    position INTEGER NOT NULL,                 -- 1-20 for songs, 1-10 for albums
    song_id INTEGER,                           -- FK to Song table (for song charts)
    album_id INTEGER,                          -- FK to Album table (for album charts)
    play_count INTEGER NOT NULL,               -- Number of plays during the period
    FOREIGN KEY (chart_id) REFERENCES Chart(id) ON DELETE CASCADE,
    FOREIGN KEY (song_id) REFERENCES Song(id),
    FOREIGN KEY (album_id) REFERENCES Album(id)
);

-- Billboard Hot 100 weekly entries (imported by BillboardHot100ImportRunner)
CREATE TABLE IF NOT EXISTS billboard_hot100_entry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date TEXT NOT NULL,
    position INTEGER NOT NULL,
    artist_name TEXT NOT NULL,
    song_title TEXT NOT NULL,
    peak_position INTEGER NOT NULL,
    weeks_on_chart INTEGER NOT NULL,
    song_id INTEGER,
    FOREIGN KEY (song_id) REFERENCES Song(id)
);

-- Billboard Hot 100 runs, one row per charting song
CREATE TABLE IF NOT EXISTS billboard_hot100_debut (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_key TEXT NOT NULL,
    matched INTEGER NOT NULL,
    song_id INTEGER,
    resolved_artist_id INTEGER,
    artist_name TEXT NOT NULL,
    song_title TEXT NOT NULL,
    gender_name TEXT,
    debut_week TEXT,
    last_week TEXT,
    peak_week TEXT,
    debut_position INTEGER,
    peak_position INTEGER,
    weeks_on_chart INTEGER NOT NULL,
    weeks_at_peak INTEGER NOT NULL,
    weeks_at_top1 INTEGER NOT NULL,
    weeks_at_top5 INTEGER NOT NULL,
    weeks_at_top10 INTEGER NOT NULL,
    weeks_at_top20 INTEGER NOT NULL,
    weeks_at_top50 INTEGER NOT NULL,
    weeks_at_top100 INTEGER NOT NULL,
    FOREIGN KEY (song_id) REFERENCES Song(id),
    FOREIGN KEY (resolved_artist_id) REFERENCES Artist(id)
);

-- MTV TRL countdown: one row per song, plus its daily chart entries
CREATE TABLE IF NOT EXISTS trl_debut (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    song_title        TEXT NOT NULL,
    artist_name       TEXT NOT NULL,
    retired           INTEGER NOT NULL DEFAULT 0,
    days_on_countdown INTEGER NOT NULL,
    song_id           INTEGER,
    FOREIGN KEY (song_id) REFERENCES Song(id)
);

CREATE TABLE IF NOT EXISTS trl_chart_entry (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date  TEXT    NOT NULL,
    position    INTEGER NOT NULL,
    artist_name TEXT    NOT NULL,
    song_title  TEXT    NOT NULL,
    month_slug  TEXT    NOT NULL,
    debut_id    INTEGER
);

-- Vato's personal cuntdown entries (imported by PersonalCuntdownCrawler)
CREATE TABLE IF NOT EXISTS vatos_cuntdown_entry (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_date    TEXT NOT NULL,
    position      INTEGER NOT NULL,
    artist_name   TEXT NOT NULL,
    song_title    TEXT NOT NULL,
    is_close_call INTEGER NOT NULL DEFAULT 0,
    song_id       INTEGER,
    FOREIGN KEY (song_id) REFERENCES Song(id)
);


-- ============================================
-- SECTION 6: ITUNES SNAPSHOT
-- ============================================

-- Last saved copy of the iTunes library, used to detect iTunes changes
CREATE TABLE IF NOT EXISTS ItunesSnapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persistent_id VARCHAR(20) NOT NULL UNIQUE,    -- iTunes Persistent ID (hex string, stable identifier)
    track_id INTEGER,                              -- iTunes Track ID (numeric, may change)
    artist VARCHAR(500),
    album VARCHAR(500),
    name VARCHAR(500) NOT NULL,
    track_number INTEGER,
    year INTEGER,
    snapshot_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When this snapshot was taken
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_time INTEGER,
    album_artist TEXT,
    genre VARCHAR(200)
);


-- ============================================
-- SECTION 7: APP RUNTIME TABLES
-- ============================================
-- The app also creates most of these at startup (CREATE TABLE IF NOT EXISTS in AppConfigService,
-- PlayAutomationStateService and SongLinkService); keep both definitions in step.

-- Runtime settings (Config page)
CREATE TABLE IF NOT EXISTS app_config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Automated Last.fm import state (single row) and run log
CREATE TABLE IF NOT EXISTS play_import_automation_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    unmatched_banner_active INTEGER NOT NULL DEFAULT 0,
    unmatched_count INTEGER NOT NULL DEFAULT 0,
    imported_since_seen INTEGER NOT NULL DEFAULT 0,
    sync_issue_active INTEGER NOT NULL DEFAULT 0,
    sync_issue_message TEXT,
    last_attempt_at TEXT,
    last_success_at TEXT,
    last_issue_at TEXT,
    run_interval_minutes INTEGER NOT NULL DEFAULT 10,
    last_attempt_epoch_millis INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS play_import_automation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_started_at TEXT NOT NULL,
    run_finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
    interval_minutes INTEGER NOT NULL,
    imported_count INTEGER NOT NULL DEFAULT 0,
    unmatched_count INTEGER NOT NULL DEFAULT 0,
    lastfm_playcount INTEGER,
    local_playcount INTEGER
);

-- Linked songs (versions of the same song counted together)
CREATE TABLE IF NOT EXISTS song_link_group (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS song_link_group_member (
    group_id INTEGER NOT NULL,
    song_id INTEGER NOT NULL PRIMARY KEY,
    created_at TEXT NOT NULL
);

-- Songs queued for the iOS playlist shortcut
CREATE TABLE IF NOT EXISTS playlist_shortcuts_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    song_id INTEGER NOT NULL,
    song_name TEXT NOT NULL,
    artist_name TEXT,
    album_name TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- ============================================
-- SECTION 8: INDEXES
-- ============================================
-- Indexes from the three db_*_performance_indexes.sql scripts are not repeated here.

-- Lookup tables
CREATE INDEX IF NOT EXISTS idx_gender_for_filtering ON Gender(id, name);
CREATE INDEX IF NOT EXISTS idx_gender_id_name ON Gender(id, name);
CREATE INDEX IF NOT EXISTS idx_ethnicity_for_filtering ON Ethnicity(id, name);
CREATE INDEX IF NOT EXISTS idx_ethnicity_id_name ON Ethnicity(id, name);
CREATE INDEX IF NOT EXISTS idx_language_for_filtering ON Language(id, name);
CREATE INDEX IF NOT EXISTS idx_language_id_name ON Language(id, name);
CREATE INDEX IF NOT EXISTS idx_genre_for_filtering ON Genre(id, name);
CREATE INDEX IF NOT EXISTS idx_genre_id_name ON Genre(id, name);
CREATE INDEX IF NOT EXISTS idx_subgenre_parent ON SubGenre(parent_genre_id);
CREATE INDEX IF NOT EXISTS idx_subgenre_name ON SubGenre(name);

-- Artist
CREATE INDEX IF NOT EXISTS idx_artist_name ON Artist(name);
CREATE INDEX IF NOT EXISTS idx_artist_name_lower ON Artist(LOWER(TRIM(name)));
CREATE INDEX IF NOT EXISTS idx_artist_gender ON Artist(gender_id);
CREATE INDEX IF NOT EXISTS idx_artist_gender_id ON Artist(gender_id) WHERE gender_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artist_ethnicity ON Artist(ethnicity_id);
CREATE INDEX IF NOT EXISTS idx_artist_ethnicity_country ON Artist(ethnicity_id, country);
CREATE INDEX IF NOT EXISTS idx_artist_genre ON Artist(genre_id);
CREATE INDEX IF NOT EXISTS idx_artist_genre_gender ON Artist(genre_id, gender_id);
CREATE INDEX IF NOT EXISTS idx_artist_language ON Artist(language_id);
CREATE INDEX IF NOT EXISTS idx_artist_organized ON Artist(organized);
CREATE INDEX IF NOT EXISTS idx_artist_covering ON Artist(
    id,
    gender_id,
    genre_id,
    ethnicity_id,
    language_id,
    country
);

-- Album
CREATE INDEX IF NOT EXISTS idx_album_artist ON Album(artist_id);
CREATE INDEX IF NOT EXISTS idx_album_name ON Album(name);
CREATE INDEX IF NOT EXISTS idx_album_name_lower ON Album(LOWER(TRIM(name)));
CREATE INDEX IF NOT EXISTS idx_album_release_date ON Album(release_date);
CREATE INDEX IF NOT EXISTS idx_album_organized ON Album(organized);
CREATE INDEX IF NOT EXISTS idx_album_override_genre ON Album(override_genre_id);
CREATE INDEX IF NOT EXISTS idx_album_override_subgenre ON Album(override_subgenre_id);
CREATE INDEX IF NOT EXISTS idx_album_override_language ON Album(override_language_id);
CREATE INDEX IF NOT EXISTS idx_album_override_covering ON Album(
    id,
    override_genre_id,
    override_language_id
);

-- Song (covering indexes for aggregation are in db_last_listened_performance_indexes.sql)
CREATE INDEX IF NOT EXISTS idx_song_artist ON Song(artist_id);
CREATE INDEX IF NOT EXISTS idx_song_album ON Song(album_id);
CREATE INDEX IF NOT EXISTS idx_song_name ON Song(name);
CREATE INDEX IF NOT EXISTS idx_song_release_date ON Song(release_date);
CREATE INDEX IF NOT EXISTS idx_song_organized ON Song(organized);

-- Play (the idx_scrobble_* names date from before the Scrobble -> Play rename)
CREATE INDEX IF NOT EXISTS idx_play_song_id ON Play(song_id);
CREATE INDEX IF NOT EXISTS idx_play_date ON Play(play_date);
CREATE INDEX IF NOT EXISTS idx_play_account ON Play(account);
CREATE INDEX IF NOT EXISTS idx_play_artist ON Play(artist);
CREATE INDEX IF NOT EXISTS idx_play_lastfm_id ON Play(lastfm_id);
CREATE INDEX IF NOT EXISTS idx_play_account_songid ON Play(account, song_id);
CREATE INDEX IF NOT EXISTS idx_play_account_date ON Play(account, play_date);
CREATE INDEX IF NOT EXISTS idx_play_songid_account ON Play(song_id, account);
CREATE INDEX IF NOT EXISTS idx_play_songid_date ON Play(song_id, play_date);
CREATE INDEX IF NOT EXISTS idx_play_cover_plays ON Play(song_id, account, play_date);
CREATE INDEX IF NOT EXISTS idx_play_songid_account_date ON Play(song_id, account, play_date);
CREATE INDEX IF NOT EXISTS idx_play_date_songid ON Play(play_date, song_id);
CREATE INDEX IF NOT EXISTS idx_play_covering_timeframe ON Play(play_date, song_id) WHERE play_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_scrobble_account_song ON Play(account, song_id);
CREATE INDEX IF NOT EXISTS idx_scrobble_date_songid_id ON Play(play_date, song_id, id);
-- Unmatched plays (no song yet), looked up by artist name
CREATE INDEX IF NOT EXISTS idx_scrobble_song_id_null ON Play(id) WHERE song_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_scrobble_artist_lower ON Play(LOWER(TRIM(artist))) WHERE song_id IS NULL;

-- Featured artists and group members
CREATE INDEX IF NOT EXISTS idx_song_featured_artist_song ON SongFeaturedArtist(song_id);
CREATE INDEX IF NOT EXISTS idx_song_featured_artist_artist ON SongFeaturedArtist(artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_member_group ON ArtistMember(group_artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_member_member ON ArtistMember(member_artist_id);

-- Gallery images and themes
CREATE INDEX IF NOT EXISTS idx_artist_image_artist ON ArtistImage(artist_id);
CREATE INDEX IF NOT EXISTS idx_album_image_album ON AlbumImage(album_id);
CREATE INDEX IF NOT EXISTS idx_song_image_song ON SongImage(song_id);
CREATE INDEX IF NOT EXISTS idx_artist_image_theme_artist_id ON ArtistImageTheme(artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_image_theme_theme_id ON ArtistImageTheme(theme_id);

-- Tags
CREATE UNIQUE INDEX IF NOT EXISTS idx_tag_name_lower ON Tag(LOWER(name));
CREATE INDEX IF NOT EXISTS idx_artist_tag_tag ON ArtistTag(tag_id);
CREATE INDEX IF NOT EXISTS idx_album_tag_tag ON AlbumTag(tag_id);
CREATE INDEX IF NOT EXISTS idx_song_tag_tag ON SongTag(tag_id);

-- Charts
CREATE UNIQUE INDEX IF NOT EXISTS idx_chart_type_period_type_key ON Chart(chart_type, period_type, period_key);
CREATE INDEX IF NOT EXISTS idx_chart_type ON Chart(chart_type);
CREATE INDEX IF NOT EXISTS idx_chart_type_period ON Chart(chart_type, period_key);
CREATE INDEX IF NOT EXISTS idx_chart_period_type ON Chart(period_type);
CREATE INDEX IF NOT EXISTS idx_chart_is_finalized ON Chart(is_finalized);
CREATE INDEX IF NOT EXISTS idx_chart_period_type_finalized ON Chart(period_type, is_finalized);
CREATE INDEX IF NOT EXISTS idx_chartentry_chart_id ON ChartEntry(chart_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_song_id ON ChartEntry(song_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_album_id ON ChartEntry(album_id);
CREATE INDEX IF NOT EXISTS idx_chartentry_position ON ChartEntry(chart_id, position);

-- Billboard Hot 100
CREATE UNIQUE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_pos ON billboard_hot100_entry(chart_date, position);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_chart_date ON billboard_hot100_entry(chart_date);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_song_id ON billboard_hot100_entry(song_id);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_entry_artist_song ON billboard_hot100_entry(artist_name, song_title);
CREATE UNIQUE INDEX IF NOT EXISTS idx_billboard_hot100_debut_identity ON billboard_hot100_debut(identity_key);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_song_id ON billboard_hot100_debut(song_id);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_artist_song ON billboard_hot100_debut(artist_name, song_title);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_debut_week ON billboard_hot100_debut(debut_week);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_last_week ON billboard_hot100_debut(last_week);
CREATE INDEX IF NOT EXISTS idx_billboard_hot100_debut_peak_position ON billboard_hot100_debut(peak_position);

-- TRL
CREATE INDEX IF NOT EXISTS idx_trl_debut_retired ON trl_debut(retired);
CREATE INDEX IF NOT EXISTS idx_trl_debut_song_id ON trl_debut(song_id);
CREATE INDEX IF NOT EXISTS idx_trl_chart_entry_date ON trl_chart_entry(chart_date);
CREATE INDEX IF NOT EXISTS idx_trl_chart_entry_slug ON trl_chart_entry(month_slug);
CREATE INDEX IF NOT EXISTS idx_trl_chart_entry_artist ON trl_chart_entry(artist_name);
CREATE INDEX IF NOT EXISTS idx_trl_chart_entry_debut_id ON trl_chart_entry(debut_id);

-- Vato's cuntdown (index names date from when the table was pc_countdown_entry)
CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_chart_date ON vatos_cuntdown_entry(chart_date);
CREATE INDEX IF NOT EXISTS idx_pc_countdown_entry_song_id ON vatos_cuntdown_entry(song_id);

-- iTunes snapshot
CREATE INDEX IF NOT EXISTS idx_itunes_snapshot_persistent_id ON ItunesSnapshot(persistent_id);
CREATE INDEX IF NOT EXISTS idx_itunes_snapshot_artist_album_name ON ItunesSnapshot(artist, album, name);

-- Linked songs
CREATE INDEX IF NOT EXISTS idx_song_link_group_member_group ON song_link_group_member(group_id);


-- ============================================
-- SECTION 9: UPDATE TRIGGERS
-- ============================================
-- Song has no update trigger: the live DB lost it when migration_drop_song_status.sql rebuilt the table.

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

-- Play update trigger
CREATE TRIGGER IF NOT EXISTS update_play_timestamp
AFTER UPDATE ON Play
FOR EACH ROW
BEGIN
    UPDATE Play SET update_date = CURRENT_TIMESTAMP WHERE id = NEW.id;
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
