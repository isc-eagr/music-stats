-- Track-number filters and sorting must not scan song rows containing image BLOBs.
CREATE INDEX IF NOT EXISTS idx_song_track_number ON Song(track_number);

-- Shared by listen-year catalogs and the yearly rankings on detail pages.
-- Keep strftime semantics (including malformed dates and time zone suffixes).
CREATE INDEX IF NOT EXISTS idx_play_year_song_account_date
    ON Play(strftime('%Y', play_date), song_id, account, play_date);
