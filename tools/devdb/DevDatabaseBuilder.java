import org.sqlite.SQLiteConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Rebuilds the dev database from scratch with a small, consistent slice of the live database.
 *
 * The live database is opened on its own connection in read-only mode (mode=ro, SQLite read-only
 * open flag and PRAGMA query_only), so nothing here can write to it. All writes go to the dev file.
 *
 * Schema: db_consolidated.sql plus the three db_*_performance_indexes.sql files (migration_*.sql
 * scripts are history for older databases and are not replayed). Anything that still differs from
 * the live schema is fixed from the live schema and listed as "schema drift", so the scripts can be
 * brought back in sync. With the scripts up to date the list is empty.
 *
 * Data: every lookup table, app_config (with the automated Last.fm import switched off), ~80
 * main artists picked to cover both genders, Mixed, groups and members, featured artists, overrides,
 * legacy robertlover plays, galleries, themes, tags and linked songs, plus their albums (complete
 * tracklists), songs, plays and the chart / Billboard / TRL / cuntdown / iTunes rows that point at them.
 *
 * Run tools/devdb/build-dev-db.bat (stop the dev app first: the dev file is deleted and recreated).
 * Args: [live db] [dev db], defaulting to C:/Music Stats DB/music-stats.db and music-stats-dev.db.
 */
public class DevDatabaseBuilder {

    static final Path DEFAULT_LIVE = Path.of("C:/Music Stats DB/music-stats.db");
    static final Path DEFAULT_DEV = Path.of("C:/Music Stats DB/music-stats-dev.db");
    static final int FEMALE = 1, MALE = 2, MIXED = 3;
    static final int MAX_ALBUM_TRACKS = 20;
    /** Live tables that are old backups and never read by the app. */
    static final Set<String> SKIPPED_TABLES = Set.of("scrobble_old_backup", "song_old_backup");
    /** Live tables whose rows stay behind: the dev app rebuilds or never needs them. */
    static final Set<String> EMPTY_TABLES = Set.of("play_import_automation_log", "play_import_automation_state");

    final Connection live;
    final Connection dev;
    final List<String> drift = new ArrayList<>();

    // In-memory view of the live catalog used to pick the slice.
    final Map<Long, Integer> artistGender = new HashMap<>();
    final Map<Long, Long> artistPlays = new HashMap<>();
    final Map<Long, Long> artistLegacyPlays = new HashMap<>();
    final Map<Long, long[]> songInfo = new HashMap<>(); // id -> {artist_id, album_id or 0}
    final Map<Long, Long> songPlays = new HashMap<>();
    final Map<Long, Long> albumArtist = new HashMap<>();
    final Map<Long, List<Long>> albumSongs = new HashMap<>();
    final Map<Long, Long> albumPlays = new HashMap<>();
    final Map<Long, List<Long>> artistSongs = new HashMap<>();
    final Map<Long, List<Long>> artistAlbums = new HashMap<>();
    final Map<Long, List<Long>> songFeatures = new HashMap<>();

    final Set<Long> artists = new LinkedHashSet<>();
    final Set<Long> albums = new LinkedHashSet<>();
    final Set<Long> songs = new LinkedHashSet<>();

    DevDatabaseBuilder(Connection live, Connection dev) {
        this.live = live;
        this.dev = dev;
    }

    public static void main(String[] args) throws Exception {
        Path livePath = (args.length > 0 ? Path.of(args[0]) : DEFAULT_LIVE).toAbsolutePath().normalize();
        Path devPath = (args.length > 1 ? Path.of(args[1]) : DEFAULT_DEV).toAbsolutePath().normalize();
        Path repo = Path.of("").toAbsolutePath();
        if (!Files.exists(repo.resolve("db_consolidated.sql"))) {
            throw new IllegalStateException("Run from the repo root (db_consolidated.sql not found in " + repo + ")");
        }
        if (devPath.equals(livePath) || !devPath.getFileName().toString().contains("-dev")) {
            throw new IllegalStateException("Refusing to write " + devPath + ": the dev file name must contain '-dev' and differ from the live DB");
        }
        if (!Files.exists(livePath)) throw new IllegalStateException("Live DB not found: " + livePath);
        for (String suffix : List.of("", "-wal", "-shm", "-journal")) {
            Path file = Path.of(devPath + suffix);
            try {
                Files.deleteIfExists(file);
            } catch (java.nio.file.FileSystemException e) {
                throw new IllegalStateException("Cannot delete " + file + ". Stop the dev app (port 8081) first.", e);
            }
        }

        SQLiteConfig readOnly = new SQLiteConfig();
        readOnly.setReadOnly(true);
        readOnly.setBusyTimeout(10000);
        String liveUrl = "jdbc:sqlite:" + livePath.toUri() + "?mode=ro";
        try (Connection live = DriverManager.getConnection(liveUrl, readOnly.toProperties());
             Connection dev = DriverManager.getConnection("jdbc:sqlite:" + devPath)) {
            try (Statement s = live.createStatement()) {
                s.execute("PRAGMA query_only = ON");
            }
            if (!live.isReadOnly()) throw new IllegalStateException("Live connection is not read-only");
            System.out.println("Live (read-only): " + livePath);
            System.out.println("Dev:              " + devPath);
            new DevDatabaseBuilder(live, dev).build(repo);
        }
        System.out.printf("Dev DB ready: %s (%.1f MB)%n", devPath, Files.size(devPath) / 1e6);
    }

    void build(Path repo) throws Exception {
        long start = System.currentTimeMillis();
        try (Statement s = dev.createStatement()) {
            s.execute("PRAGMA journal_mode = OFF");
            s.execute("PRAGMA synchronous = OFF");
            s.execute("PRAGMA foreign_keys = OFF");
        }
        buildSchema(repo);
        loadCatalog();
        pickSlice();
        dev.setAutoCommit(false);
        copyData();
        dev.commit();
        dev.setAutoCommit(true);
        try (Statement s = dev.createStatement()) {
            s.execute("ANALYZE");
            s.execute("PRAGMA journal_mode = WAL");
        }
        report();
        System.out.printf("Built in %.1f s%n", (System.currentTimeMillis() - start) / 1000.0);
    }

    // ------------------------------------------------------------------ schema

    static final List<String> SCHEMA_SCRIPTS = List.of("db_consolidated.sql", "db_detail_performance_indexes.sql",
            "db_last_listened_performance_indexes.sql", "db_timeframe_performance_indexes.sql");

    void buildSchema(Path repo) throws Exception {
        for (String script : SCHEMA_SCRIPTS) {
            for (String statement : splitSql(Files.readString(repo.resolve(script)))) {
                try {
                    exec(dev, statement);
                } catch (SQLException e) {
                    drift.add("failed in " + script + ": " + firstLine(statement) + " (" + e.getMessage() + ")");
                }
            }
        }
        reconcileWithLive();
    }

    /** Make the dev schema match the live one and record every difference the scripts left. */
    void reconcileWithLive() throws SQLException {
        Map<String, String> liveTables = schemaObjects(live, "table");
        Map<String, String> devTables = schemaObjects(dev, "table");
        for (var entry : liveTables.entrySet()) {
            String table = entry.getKey();
            if (SKIPPED_TABLES.contains(table) || table.startsWith("sqlite_")) continue;
            if (!devTables.containsKey(table)) {
                exec(dev, entry.getValue());
                drift.add("table " + table + ": missing from the scripts, created from the live schema");
                continue;
            }
            List<String[]> liveColumns = columnInfo(live, table);
            Set<String> devColumns = columns(dev, table);
            Set<String> liveNames = liveColumns.stream().map(c -> c[0].toLowerCase()).collect(Collectors.toSet());
            List<String> extra = devColumns.stream().filter(c -> !liveNames.contains(c)).toList();
            if (!extra.isEmpty()) {
                exec(dev, "DROP TABLE \"" + table + "\"");
                exec(dev, entry.getValue());
                drift.add("table " + table + ": scripts have columns the live DB dropped " + extra + ", recreated from the live schema");
                continue;
            }
            for (String[] column : liveColumns) {
                if (!devColumns.contains(column[0].toLowerCase())) {
                    exec(dev, "ALTER TABLE \"" + table + "\" ADD COLUMN \"" + column[0] + "\" " + column[1]);
                    drift.add("column " + table + "." + column[0] + ": missing from the scripts, added");
                }
            }
        }
        for (String table : devTables.keySet()) {
            if (!liveTables.containsKey(table) && !table.startsWith("sqlite_")) {
                exec(dev, "DROP TABLE \"" + table + "\"");
                drift.add("table " + table + ": in the scripts but not in the live DB, dropped");
            }
        }
        for (String type : List.of("index", "trigger")) {
            Map<String, String> liveObjects = schemaObjects(live, type);
            Map<String, String> devObjects = schemaObjects(dev, type);
            for (var entry : liveObjects.entrySet()) {
                if (entry.getValue() == null || devObjects.containsKey(entry.getKey())) continue;
                if (SKIPPED_TABLES.contains(tableOf(live, entry.getKey()))) continue;
                exec(dev, entry.getValue());
                drift.add(type + " " + entry.getKey() + ": missing from the scripts, created from the live schema");
            }
            for (String name : devObjects.keySet()) {
                if (!liveObjects.containsKey(name) && devObjects.get(name) != null) {
                    exec(dev, "DROP " + type.toUpperCase() + " IF EXISTS \"" + name + "\"");
                    drift.add(type + " " + name + ": in the scripts but not in the live DB, dropped");
                }
            }
        }
    }

    // ------------------------------------------------------------------ selection

    void loadCatalog() throws SQLException {
        query(live, "SELECT id, gender_id FROM Artist", rs -> artistGender.put(rs.getLong(1), rs.getObject(2) == null ? 0 : rs.getInt(2)));
        query(live, "SELECT id, artist_id FROM Album", rs -> albumArtist.put(rs.getLong(1), rs.getLong(2)));
        query(live, "SELECT id, artist_id, album_id FROM Song", rs -> {
            long id = rs.getLong(1), artist = rs.getLong(2), album = rs.getLong(3);
            songInfo.put(id, new long[]{artist, album});
            artistSongs.computeIfAbsent(artist, k -> new ArrayList<>()).add(id);
            if (album != 0) albumSongs.computeIfAbsent(album, k -> new ArrayList<>()).add(id);
        });
        albumArtist.forEach((album, artist) -> artistAlbums.computeIfAbsent(artist, k -> new ArrayList<>()).add(album));
        query(live, "SELECT song_id, account, COUNT(*) FROM Play WHERE song_id IS NOT NULL GROUP BY song_id, account", rs -> {
            long song = rs.getLong(1), count = rs.getLong(3);
            long[] info = songInfo.get(song);
            if (info == null) return;
            songPlays.merge(song, count, Long::sum);
            artistPlays.merge(info[0], count, Long::sum);
            if (info[1] != 0) albumPlays.merge(info[1], count, Long::sum);
            if ("robertlover".equals(rs.getString(2))) artistLegacyPlays.merge(info[0], count, Long::sum);
        });
        query(live, "SELECT song_id, artist_id FROM SongFeaturedArtist", rs -> songFeatures.computeIfAbsent(rs.getLong(1), k -> new ArrayList<>()).add(rs.getLong(2)));
    }

    void pickSlice() throws SQLException {
        Set<Long> primary = new LinkedHashSet<>();
        for (int gender : List.of(FEMALE, MALE)) {
            List<Long> ranked = rankArtists(a -> artistGender.get(a) == gender);
            primary.addAll(first(ranked, 30));
            // A few mid and low-ranked artists so small counts show up too.
            for (int rank = 200; rank < ranked.size() && rank < 260; rank += 20) primary.add(ranked.get(rank));
            for (int rank = 600; rank < ranked.size() && rank < 900; rank += 100) primary.add(ranked.get(rank));
        }
        primary.addAll(first(rankArtists(a -> artistGender.get(a) == MIXED), 8));
        primary.addAll(first(rankArtists(a -> artistGender.get(a) == 0), 3));
        primary.addAll(first(artistLegacyPlays.entrySet().stream().sorted(Map.Entry.<Long, Long>comparingByValue().reversed()).map(Map.Entry::getKey).toList(), 10));
        primary.addAll(first(rankIds(longs(live, "SELECT DISTINCT group_artist_id FROM ArtistMember")), 6));
        primary.addAll(first(rankIds(longs(live, "SELECT DISTINCT artist_id FROM ArtistImage")), 6));
        primary.addAll(first(rankIds(longs(live, "SELECT DISTINCT artist_id FROM ArtistImageTheme")), 4));
        primary.addAll(first(rankIds(longs(live, "SELECT id FROM Artist WHERE death_date IS NOT NULL")), 2));

        // The three biggest female and male artists get two albums, everyone else one.
        Set<Long> stars = new HashSet<>(first(rankArtists(a -> artistGender.get(a) == FEMALE), 3));
        stars.addAll(first(rankArtists(a -> artistGender.get(a) == MALE), 3));
        for (long artist : primary) {
            artists.add(artist);
            List<Long> ownAlbums = artistAlbums.getOrDefault(artist, List.of()).stream()
                    .filter(a -> albumSongs.getOrDefault(a, List.of()).size() <= MAX_ALBUM_TRACKS)
                    .sorted(Comparator.comparing((Long a) -> albumPlays.getOrDefault(a, 0L)).reversed()).toList();
            for (long album : first(ownAlbums, stars.contains(artist) ? 2 : 1)) addAlbum(album);
            // Top songs that are not on an album (singles), so albumless songs are covered.
            artistSongs.getOrDefault(artist, List.of()).stream()
                    .filter(s -> songInfo.get(s)[1] == 0)
                    .sorted(Comparator.comparing((Long s) -> songPlays.getOrDefault(s, 0L)).reversed())
                    .limit(3).forEach(songs::add);
        }
        // Group members, with their best single if they have one.
        query(live, "SELECT group_artist_id, member_artist_id FROM ArtistMember", rs -> {
            if (artists.contains(rs.getLong(1))) {
                long member = rs.getLong(2);
                artists.add(member);
                artistSongs.getOrDefault(member, List.of()).stream().filter(s -> songInfo.get(s)[1] == 0)
                        .max(Comparator.comparing((Long s) -> songPlays.getOrDefault(s, 0L))).ifPresent(songs::add);
            }
        });
        // Overrides (album and song level) with their complete albums.
        for (long album : longs(live, "SELECT id FROM Album WHERE override_genre_id IS NOT NULL OR override_subgenre_id IS NOT NULL OR override_language_id IS NOT NULL")) addAlbum(album);
        for (long song : longs(live, "SELECT id FROM Song WHERE override_genre_id IS NOT NULL OR override_subgenre_id IS NOT NULL OR override_language_id IS NOT NULL OR override_gender_id IS NOT NULL OR override_ethnicity_id IS NOT NULL")) addSong(song);
        // Linked songs, tagged songs, song galleries and Billboard / cuntdown matches.
        for (long song : longs(live, "SELECT song_id FROM song_link_group_member")) addSong(song);
        for (long song : first(rankSongs(longs(live, "SELECT DISTINCT song_id FROM SongTag")), 8)) addSong(song);
        for (long song : first(rankSongs(longs(live, "SELECT DISTINCT song_id FROM SongImage")), 6)) addSong(song);
        // Songs with featured artists, in case the picks above have few.
        long featured = songs.stream().filter(songFeatures::containsKey).count();
        if (featured < 25) {
            for (long song : first(rankSongs(songFeatures.keySet().stream().filter(s -> songInfo.containsKey(s) && songInfo.get(s)[1] == 0).toList()), (int) (25 - featured))) addSong(song);
        }
        // Closure: every artist a copied album or song points at, and featured artists.
        songs.removeIf(s -> !songInfo.containsKey(s));
        for (long song : songs) {
            artists.add(songInfo.get(song)[0]);
            artists.addAll(songFeatures.getOrDefault(song, List.of()));
        }
        for (long album : albums) artists.add(albumArtist.get(album));
        artists.removeIf(a -> !artistGender.containsKey(a));
    }

    void addAlbum(long album) {
        if (!albumArtist.containsKey(album)) return;
        albums.add(album);
        songs.addAll(albumSongs.getOrDefault(album, List.of()));
    }

    /** Adds a song with its complete album, so album tracklists stay whole. */
    void addSong(long song) {
        long[] info = songInfo.get(song);
        if (info == null) return;
        if (info[1] != 0) addAlbum(info[1]);
        else songs.add(song);
    }

    List<Long> rankArtists(Predicate<Long> filter) {
        return artistGender.keySet().stream().filter(filter)
                .sorted(Comparator.comparing((Long a) -> artistPlays.getOrDefault(a, 0L)).reversed().thenComparing(a -> a)).toList();
    }

    List<Long> rankIds(Collection<Long> ids) {
        return ids.stream().sorted(Comparator.comparing((Long a) -> artistPlays.getOrDefault(a, 0L)).reversed()).toList();
    }

    List<Long> rankSongs(Collection<Long> ids) {
        return ids.stream().sorted(Comparator.comparing((Long s) -> songPlays.getOrDefault(s, 0L)).reversed()).toList();
    }

    static <T> List<T> first(List<T> list, int n) {
        return list.subList(0, Math.min(n, list.size()));
    }

    // ------------------------------------------------------------------ copy

    void copyData() throws SQLException {
        for (String lookup : List.of("Gender", "Ethnicity", "Language", "Genre", "SubGenre", "Tag", "ArtistTheme", "Chart", "app_config")) {
            copy(lookup, "1 = 1");
        }
        exec(dev, "UPDATE app_config SET config_value = 'false' WHERE config_key = 'automation.enabled'");
        exec(dev, "UPDATE app_config SET config_value = '' WHERE config_key = 'automation.apiKey'");

        copyIn("Artist", "id", artists);
        copyIn("Album", "id", albums);
        copyIn("Song", "id", songs);
        copyIn("Play", "song_id", songs);
        // A sample of unmatched plays (no song) by the copied artists, for the unmatched plays screens.
        Set<String> names = new HashSet<>();
        for (List<Object> chunk : chunks(artists)) {
            query(live, "SELECT lower(name) FROM Artist WHERE id IN (" + marks(chunk) + ")", chunk, rs -> names.add(rs.getString(1)));
        }
        List<Long> unmatched = new ArrayList<>();
        query(live, "SELECT id, lower(artist) FROM Play WHERE song_id IS NULL ORDER BY play_date DESC", rs -> {
            if (unmatched.size() < 400 && names.contains(rs.getString(2))) unmatched.add(rs.getLong(1));
        });
        copyIn("Play", "id", unmatched);

        copyIn("SongFeaturedArtist", "song_id", songs, "artist_id", artists);
        copyIn("ArtistMember", "group_artist_id", artists, "member_artist_id", artists);
        copyIn("ArtistImage", "artist_id", artists);
        copyIn("AlbumImage", "album_id", albums);
        copyIn("SongImage", "song_id", songs);
        copyIn("ArtistImageTheme", "artist_id", artists);
        copyIn("ArtistTag", "artist_id", artists);
        copyIn("AlbumTag", "album_id", albums);
        copyIn("SongTag", "song_id", songs);
        copyIn("ChartEntry", "song_id", songs);
        copyIn("ChartEntry", "album_id", albums);
        copyIn("billboard_hot100_entry", "song_id", songs);
        Set<Long> debuts = new LinkedHashSet<>();
        for (List<Object> chunk : chunks(songs)) {
            query(live, "SELECT id FROM billboard_hot100_debut WHERE song_id IN (" + marks(chunk) + ")", chunk, rs -> debuts.add(rs.getLong(1)));
        }
        for (List<Object> chunk : chunks(artists)) {
            query(live, "SELECT id FROM billboard_hot100_debut WHERE resolved_artist_id IN (" + marks(chunk) + ")", chunk, rs -> debuts.add(rs.getLong(1)));
        }
        copyIn("billboard_hot100_debut", "id", debuts);
        copyIn("trl_debut", "song_id", songs);
        copyIn("trl_chart_entry", "debut_id", longs(dev, "SELECT id FROM trl_debut"));
        copyIn("vatos_cuntdown_entry", "song_id", songs);
        copyIn("playlist_shortcuts_queue", "song_id", songs);
        copyIn("song_link_group_member", "song_id", songs);
        copyIn("song_link_group", "id", longs(dev, "SELECT DISTINCT group_id FROM song_link_group_member"));
        Set<String> artistNames = new HashSet<>(names);
        List<Long> snapshots = new ArrayList<>();
        query(live, "SELECT id, lower(artist) FROM ItunesSnapshot", rs -> {
            if (artistNames.contains(rs.getString(2))) snapshots.add(rs.getLong(1));
        });
        copyIn("ItunesSnapshot", "id", snapshots);

        Set<String> handled = new HashSet<>(List.of("Gender", "Ethnicity", "Language", "Genre", "SubGenre", "Tag", "ArtistTheme", "Chart", "app_config",
                "Artist", "Album", "Song", "Play", "SongFeaturedArtist", "ArtistMember", "ArtistImage", "AlbumImage", "SongImage", "ArtistImageTheme",
                "ArtistTag", "AlbumTag", "SongTag", "ChartEntry", "billboard_hot100_entry", "billboard_hot100_debut", "trl_debut", "trl_chart_entry",
                "vatos_cuntdown_entry", "playlist_shortcuts_queue", "song_link_group_member", "song_link_group", "ItunesSnapshot"));
        handled.addAll(EMPTY_TABLES);
        for (String table : schemaObjects(dev, "table").keySet()) {
            if (!handled.contains(table) && !table.startsWith("sqlite_")) {
                System.out.println("WARNING: table " + table + " is not handled by the builder and stays empty. Add it to DevDatabaseBuilder.copyData().");
            }
        }
    }

    void copyIn(String table, String column, Collection<Long> ids) throws SQLException {
        for (List<Object> chunk : chunks(ids)) copy(table, column + " IN (" + marks(chunk) + ")", chunk);
    }

    /** Copies rows whose first column is in the first set and second column in the second set. */
    void copyIn(String table, String column, Collection<Long> ids, String otherColumn, Set<Long> otherIds) throws SQLException {
        List<Object> others = new ArrayList<>(otherIds);
        for (List<Object> chunk : chunks(ids)) {
            List<Object> params = new ArrayList<>(chunk);
            params.addAll(others);
            copy(table, column + " IN (" + marks(chunk) + ") AND " + otherColumn + " IN (" + marks(others) + ")", params);
        }
    }

    void copy(String table, String where) throws SQLException {
        copy(table, where, List.of());
    }

    void copy(String table, String where, List<Object> params) throws SQLException {
        Set<String> liveColumns = columns(live, table);
        List<String> cols = columnInfo(dev, table).stream().map(c -> c[0]).filter(c -> liveColumns.contains(c.toLowerCase())).toList();
        String list = cols.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(", "));
        String insert = "INSERT OR IGNORE INTO \"" + table + "\" (" + list + ") VALUES (" + marks(cols) + ")";
        try (PreparedStatement select = live.prepareStatement("SELECT " + list + " FROM \"" + table + "\" WHERE " + where);
             PreparedStatement out = dev.prepareStatement(insert)) {
            for (int i = 0; i < params.size(); i++) select.setObject(i + 1, params.get(i));
            try (ResultSet rs = select.executeQuery()) {
                int batch = 0;
                while (rs.next()) {
                    for (int i = 1; i <= cols.size(); i++) out.setObject(i, rs.getObject(i));
                    out.addBatch();
                    if (++batch % 1000 == 0) out.executeBatch();
                }
                out.executeBatch();
            }
        }
    }

    // ------------------------------------------------------------------ report

    void report() throws SQLException {
        System.out.println();
        System.out.println("Rows copied:");
        for (String table : schemaObjects(dev, "table").keySet()) {
            if (table.startsWith("sqlite_")) continue;
            System.out.printf("  %-30s %8d%n", table, count("SELECT COUNT(*) FROM \"" + table + "\""));
        }
        System.out.println();
        System.out.println("Coverage:");
        System.out.println("  female / male / mixed / no gender artists: " + count("SELECT COUNT(*) FROM Artist WHERE gender_id = 1") + " / "
                + count("SELECT COUNT(*) FROM Artist WHERE gender_id = 2") + " / " + count("SELECT COUNT(*) FROM Artist WHERE gender_id = 3") + " / "
                + count("SELECT COUNT(*) FROM Artist WHERE gender_id IS NULL"));
        System.out.println("  groups with members: " + count("SELECT COUNT(DISTINCT group_artist_id) FROM ArtistMember"));
        System.out.println("  songs with featured artists: " + count("SELECT COUNT(DISTINCT song_id) FROM SongFeaturedArtist"));
        System.out.println("  album / song overrides: " + count("SELECT COUNT(*) FROM Album WHERE override_genre_id IS NOT NULL OR override_subgenre_id IS NOT NULL OR override_language_id IS NOT NULL")
                + " / " + count("SELECT COUNT(*) FROM Song WHERE override_genre_id IS NOT NULL OR override_subgenre_id IS NOT NULL OR override_language_id IS NOT NULL OR override_gender_id IS NOT NULL OR override_ethnicity_id IS NOT NULL"));
        System.out.println("  robertlover / vatito plays: " + count("SELECT COUNT(*) FROM Play WHERE account = 'robertlover'") + " / "
                + count("SELECT COUNT(*) FROM Play WHERE account = 'vatito'") + ", unmatched: " + count("SELECT COUNT(*) FROM Play WHERE song_id IS NULL"));
        System.out.println("  albumless songs: " + count("SELECT COUNT(*) FROM Song WHERE album_id IS NULL"));
        System.out.println("  orphans (songs without artist, albums without artist, plays without song): "
                + count("SELECT COUNT(*) FROM Song s LEFT JOIN Artist a ON a.id = s.artist_id WHERE a.id IS NULL") + " / "
                + count("SELECT COUNT(*) FROM Album al LEFT JOIN Artist a ON a.id = al.artist_id WHERE a.id IS NULL") + " / "
                + count("SELECT COUNT(*) FROM Play p LEFT JOIN Song s ON s.id = p.song_id WHERE p.song_id IS NOT NULL AND s.id IS NULL"));
        System.out.println("  automation.enabled: " + string("SELECT config_value FROM app_config WHERE config_key = 'automation.enabled'"));
        System.out.println();
        if (drift.isEmpty()) {
            System.out.println("Schema drift: none, the scripts match the live schema.");
        } else {
            System.out.println("Schema drift (the SQL scripts differ from the live DB; fixed in the dev copy):");
            drift.forEach(d -> System.out.println("  - " + d));
        }
    }

    long count(String sql) throws SQLException {
        try (Statement s = dev.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    String string(String sql) throws SQLException {
        try (Statement s = dev.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ------------------------------------------------------------------ helpers

    interface RowHandler { void handle(ResultSet rs) throws SQLException; }

    static void query(Connection c, String sql, RowHandler handler) throws SQLException {
        query(c, sql, List.of(), handler);
    }

    static void query(Connection c, String sql, List<Object> params, RowHandler handler) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) handler.handle(rs);
            }
        }
    }

    static List<Long> longs(Connection c, String sql) throws SQLException {
        List<Long> out = new ArrayList<>();
        query(c, sql, rs -> { if (rs.getObject(1) != null) out.add(rs.getLong(1)); });
        return out;
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static Map<String, String> schemaObjects(Connection c, String type) throws SQLException {
        Map<String, String> out = new TreeMap<>();
        query(c, "SELECT name, sql FROM sqlite_master WHERE type = ?", List.of(type), rs -> out.put(rs.getString(1), rs.getString(2)));
        return out;
    }

    static String tableOf(Connection c, String object) throws SQLException {
        List<String> out = new ArrayList<>();
        query(c, "SELECT tbl_name FROM sqlite_master WHERE name = ?", List.of(object), rs -> out.add(rs.getString(1)));
        return out.isEmpty() ? "" : out.getFirst();
    }

    /** {name, declared type} per column. */
    static List<String[]> columnInfo(Connection c, String table) throws SQLException {
        List<String[]> out = new ArrayList<>();
        query(c, "SELECT name, type FROM pragma_table_info(?)", List.of(table), rs -> out.add(new String[]{rs.getString(1), rs.getString(2)}));
        return out;
    }

    static Set<String> columns(Connection c, String table) throws SQLException {
        return columnInfo(c, table).stream().map(col -> col[0].toLowerCase()).collect(Collectors.toSet());
    }

    static List<List<Object>> chunks(Collection<Long> ids) {
        List<List<Object>> out = new ArrayList<>();
        List<Object> current = new ArrayList<>();
        for (Long id : ids) {
            current.add(id);
            if (current.size() == 400) { out.add(current); current = new ArrayList<>(); }
        }
        if (!current.isEmpty()) out.add(current);
        return out;
    }

    static String marks(Collection<?> items) {
        return String.join(", ", Collections.nCopies(items.size(), "?"));
    }

    static String firstLine(String sql) {
        String line = sql.strip().lines().findFirst().orElse("");
        return line.length() > 90 ? line.substring(0, 90) + "..." : line;
    }

    static final Pattern TRIGGER_START = Pattern.compile("(?is)^CREATE\\s+(TEMP\\s+|TEMPORARY\\s+)?TRIGGER\\b.*");
    static final Pattern TRIGGER_END = Pattern.compile("(?is).*\\bEND$");

    /** Splits a SQL script into statements, dropping comments and keeping trigger bodies whole. */
    static List<String> splitSql(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int i = 0, n = script.length();
        while (i < n) {
            char ch = script.charAt(i);
            if (ch == '-' && i + 1 < n && script.charAt(i + 1) == '-') {
                while (i < n && script.charAt(i) != '\n') i++;
                continue;
            }
            if (ch == '/' && i + 1 < n && script.charAt(i + 1) == '*') {
                int end = script.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
                continue;
            }
            if (ch == '\'' || ch == '"') {
                int end = i + 1;
                while (end < n && !(script.charAt(end) == ch && (end + 1 >= n || script.charAt(end + 1) != ch))) {
                    end += script.charAt(end) == ch ? 2 : 1;
                }
                current.append(script, i, Math.min(end + 1, n));
                i = end + 1;
                continue;
            }
            if (ch == ';') {
                String statement = current.toString().strip();
                if (TRIGGER_START.matcher(statement).matches() && !TRIGGER_END.matcher(statement).matches()) {
                    current.append(ch);
                } else {
                    if (!statement.isEmpty()) statements.add(statement);
                    current.setLength(0);
                }
                i++;
                continue;
            }
            current.append(ch);
            i++;
        }
        if (!current.toString().isBlank()) statements.add(current.toString().strip());
        return statements;
    }
}
