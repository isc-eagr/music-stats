package library;

import library.dto.AlbumStatsQuery;
import library.dto.ArtistStatsQuery;
import library.dto.SongStatsQuery;
import library.repository.AlbumRepository;
import library.repository.ArtistRepositoryImpl;
import library.repository.SongRepository;
import library.service.AppConfigService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class TestDatabaseSupport implements AutoCloseable {

    private final SingleConnectionDataSource dataSource;
    final JdbcTemplate jdbcTemplate;
    final SongRepository songRepository;
    final ArtistRepositoryImpl artistRepository;
    final AlbumRepository albumRepository;

    private TestDatabaseSupport() {
        this.dataSource = new SingleConnectionDataSource();
        this.dataSource.setDriverClassName("org.sqlite.JDBC");
        this.dataSource.setUrl("jdbc:sqlite:file:music-stats-test-" + UUID.randomUUID() + "?mode=memory&cache=shared");
        this.dataSource.setSuppressClose(true);
        this.jdbcTemplate = new JdbcTemplate(dataSource);

        AppConfigService appConfigService = mock(AppConfigService.class);
        when(appConfigService.getAlbumFullListenConfig())
                .thenReturn(new AppConfigService.AlbumFullListenConfig(0, 2, 3, 4));

        createSchema();
        seedCatalog();

        this.songRepository = new SongRepository(jdbcTemplate, appConfigService);
        this.artistRepository = new ArtistRepositoryImpl(jdbcTemplate);
        this.albumRepository = new AlbumRepository(jdbcTemplate, appConfigService);
    }

    static TestDatabaseSupport create() {
        return new TestDatabaseSupport();
    }

    @Override
    public void close() {
        dataSource.destroy();
    }

    private void createSchema() {
        executeAll(List.of(
                "CREATE TABLE Gender (id INTEGER PRIMARY KEY, name TEXT NOT NULL, image BLOB)",
                "CREATE TABLE Ethnicity (id INTEGER PRIMARY KEY, name TEXT NOT NULL, image BLOB)",
                "CREATE TABLE Genre (id INTEGER PRIMARY KEY, name TEXT NOT NULL, image BLOB)",
                "CREATE TABLE SubGenre (id INTEGER PRIMARY KEY, name TEXT NOT NULL, parent_genre_id INTEGER, image BLOB)",
                "CREATE TABLE Language (id INTEGER PRIMARY KEY, name TEXT NOT NULL, image BLOB)",
                """
                CREATE TABLE Artist (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    gender_id INTEGER,
                    country TEXT,
                    ethnicity_id INTEGER,
                    genre_id INTEGER,
                    subgenre_id INTEGER,
                    language_id INTEGER,
                    is_band INTEGER DEFAULT 0,
                    organized INTEGER DEFAULT 0,
                    birth_date TEXT,
                    death_date TEXT,
                    image BLOB
                )
                """,
                """
                CREATE TABLE Album (
                    id INTEGER PRIMARY KEY,
                    artist_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    release_date TEXT,
                    number_of_songs INTEGER,
                    override_genre_id INTEGER,
                    override_subgenre_id INTEGER,
                    override_language_id INTEGER,
                    organized INTEGER DEFAULT 0,
                    image BLOB
                )
                """,
                """
                CREATE TABLE Song (
                    id INTEGER PRIMARY KEY,
                    artist_id INTEGER NOT NULL,
                    album_id INTEGER,
                    name TEXT NOT NULL,
                    length_seconds INTEGER,
                    is_single INTEGER DEFAULT 0,
                    override_genre_id INTEGER,
                    override_subgenre_id INTEGER,
                    override_language_id INTEGER,
                    override_gender_id INTEGER,
                    override_ethnicity_id INTEGER,
                    release_date TEXT,
                    organized INTEGER DEFAULT 0,
                    single_cover BLOB,
                    track_number INTEGER
                )
                """,
                """
                CREATE TABLE Play (
                    id INTEGER PRIMARY KEY,
                    artist TEXT,
                    album TEXT,
                    song TEXT,
                    lastfm_id INTEGER,
                    play_date TEXT,
                    song_id INTEGER,
                    account TEXT
                )
                """,
                "CREATE TABLE SongFeaturedArtist (song_id INTEGER NOT NULL, artist_id INTEGER NOT NULL)",
                "CREATE TABLE ArtistImage (id INTEGER PRIMARY KEY, artist_id INTEGER NOT NULL, image BLOB, display_order INTEGER)",
                "CREATE TABLE AlbumImage (id INTEGER PRIMARY KEY, album_id INTEGER NOT NULL, image BLOB, display_order INTEGER)",
                "CREATE TABLE SongImage (id INTEGER PRIMARY KEY, song_id INTEGER NOT NULL, image BLOB, display_order INTEGER)",
                "CREATE TABLE ArtistTheme (id INTEGER PRIMARY KEY, name TEXT, is_active INTEGER DEFAULT 1)",
                "CREATE TABLE ArtistImageTheme (artist_id INTEGER NOT NULL, theme_id INTEGER NOT NULL)",
                "CREATE TABLE ArtistTag (artist_id INTEGER NOT NULL, tag_id INTEGER NOT NULL)",
                "CREATE TABLE AlbumTag (album_id INTEGER NOT NULL, tag_id INTEGER NOT NULL)",
                "CREATE TABLE SongTag (song_id INTEGER NOT NULL, tag_id INTEGER NOT NULL)",
                """
                CREATE TABLE Chart (
                    id INTEGER PRIMARY KEY,
                    chart_type TEXT NOT NULL,
                    period_type TEXT NOT NULL,
                    period_key TEXT NOT NULL,
                    period_start_date TEXT NOT NULL,
                    period_end_date TEXT,
                    is_finalized INTEGER DEFAULT 0
                )
                """,
                """
                CREATE TABLE ChartEntry (
                    id INTEGER PRIMARY KEY,
                    chart_id INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    song_id INTEGER,
                    album_id INTEGER,
                    play_count INTEGER
                )
                """,
                "CREATE TABLE trl_debut (id INTEGER PRIMARY KEY, song_id INTEGER NOT NULL)",
                "CREATE TABLE trl_chart_entry (id INTEGER PRIMARY KEY, debut_id INTEGER NOT NULL, chart_date TEXT NOT NULL, position INTEGER NOT NULL)",
                "CREATE TABLE vatos_cuntdown_entry (id INTEGER PRIMARY KEY, song_id INTEGER NOT NULL, chart_date TEXT NOT NULL, position INTEGER NOT NULL, is_close_call INTEGER DEFAULT 0)",
                """
                CREATE TABLE billboard_hot100_entry (
                    id INTEGER PRIMARY KEY,
                    chart_date TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    artist_name TEXT NOT NULL,
                    song_title TEXT NOT NULL,
                    peak_position INTEGER NOT NULL,
                    weeks_on_chart INTEGER NOT NULL,
                    song_id INTEGER
                )
                """,
                "CREATE TABLE billboard_hot100_debut (id INTEGER PRIMARY KEY, song_id INTEGER NOT NULL, chart_date TEXT, peak_position INTEGER, weeks_on_chart INTEGER, weeks_at_peak INTEGER)",
                "CREATE INDEX idx_play_cover_plays ON Play(song_id, account, play_date)",
                "CREATE INDEX idx_song_artist_album ON Song(artist_id, album_id)",
                "CREATE INDEX idx_song_album ON Song(album_id)",
                "CREATE INDEX idx_chartentry_song ON ChartEntry(song_id)",
                "CREATE INDEX idx_chartentry_album ON ChartEntry(album_id)",
                "CREATE INDEX idx_billboard_hot100_entry_song ON billboard_hot100_entry(song_id)"
        ));
    }

    private void executeAll(List<String> statements) {
        for (String statement : statements) {
            jdbcTemplate.execute(statement);
        }
    }

    private void seedCatalog() {
        jdbcTemplate.update("INSERT INTO Gender (id, name) VALUES (1, 'Male'), (2, 'Female'), (3, 'Other')");
        jdbcTemplate.update("INSERT INTO Ethnicity (id, name) VALUES (1, 'Latina'), (2, 'White'), (3, 'Black')");
        jdbcTemplate.update("INSERT INTO Genre (id, name) VALUES (1, 'Pop'), (2, 'Rock'), (3, 'Dance')");
        jdbcTemplate.update("INSERT INTO SubGenre (id, name, parent_genre_id) VALUES (1, 'Synth Pop', 1), (2, 'Alt Rock', 2), (3, 'Dance Pop', 3)");
        jdbcTemplate.update("INSERT INTO Language (id, name) VALUES (1, 'Spanish'), (2, 'English')");

        jdbcTemplate.update("""
                INSERT INTO Artist
                    (id, name, gender_id, country, ethnicity_id, genre_id, subgenre_id, language_id, is_band, organized, birth_date, image)
                VALUES
                    (1, 'Selena', 2, 'Mexico', 1, 1, 1, 1, 0, 1, '1971-04-16', X'01'),
                    (2, 'Bad Bunny', 1, 'Puerto Rico', 1, 1, 3, 1, 0, 1, '1994-03-10', NULL),
                    (3, 'The Static Hearts', 1, 'United States', 2, 2, 2, 2, 1, 0, '1980-01-01', NULL),
                    (4, 'Guest Singer', 2, 'Colombia', 1, 3, 3, 1, 0, 1, '1990-06-01', NULL),
                    (5, 'Mystery Artist', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, NULL, NULL),
                    (6, 'Legacy Legend', 1, 'Mexico', 2, 2, 2, 2, 0, 1, '1950-01-01', NULL)
                """);
        jdbcTemplate.update("UPDATE Artist SET death_date = '2020-12-31' WHERE id = 6");

        jdbcTemplate.update("""
                INSERT INTO Album
                    (id, artist_id, name, release_date, number_of_songs, override_genre_id, override_subgenre_id, override_language_id, organized, image)
                VALUES
                    (1, 1, 'Amor Prohibido', '1994-03-13', 2, 2, 2, NULL, 1, X'02'),
                    (2, 2, 'Un Verano Sin Ti', '2022-05-06', 2, NULL, NULL, NULL, 1, NULL),
                    (3, 3, 'Silent Record', '2020-01-01', 1, NULL, NULL, 2, 0, NULL),
                    (4, 5, 'Unknown Album', NULL, 1, NULL, NULL, NULL, 0, NULL),
                    (5, 6, 'Legacy Collection', '1975-01-01', 1, NULL, NULL, NULL, 1, NULL)
                """);

        jdbcTemplate.update("""
                INSERT INTO Song
                    (id, artist_id, album_id, name, length_seconds, is_single, override_genre_id, override_subgenre_id,
                     override_language_id, override_gender_id, override_ethnicity_id, release_date, organized, single_cover, track_number)
                VALUES
                    (1, 1, 1, 'Bidi Bidi Bom Bom', 210, 0, NULL, NULL, NULL, NULL, NULL, NULL, 1, X'03', 1),
                    (2, 1, 1, 'No Me Queda Mas', 200, 0, 3, 3, 2, NULL, NULL, NULL, 1, NULL, 2),
                    (3, 2, 2, 'Titi Me Pregunto', 243, 0, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, 1),
                    (4, 2, 2, 'Ojitos Lindos', 258, 0, NULL, NULL, NULL, 2, NULL, NULL, 1, NULL, 2),
                    (5, 1, NULL, 'Standalone Jam', 180, 1, NULL, NULL, NULL, NULL, NULL, '1995-01-01', 0, NULL, NULL),
                    (6, 3, 3, 'Quiet Track', 120, 0, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, 1),
                    (7, 5, 4, 'Unknown Silence', NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL),
                    (8, 6, 5, 'Old Hit', 240, 1, NULL, NULL, NULL, NULL, NULL, '1975-01-01', 1, NULL, 1)
                """);

        jdbcTemplate.update("INSERT INTO SongFeaturedArtist (song_id, artist_id) VALUES (2, 4)");
        jdbcTemplate.update("INSERT INTO ArtistImage (id, artist_id, image, display_order) VALUES (1, 1, X'04', 1)");
        jdbcTemplate.update("INSERT INTO AlbumImage (id, album_id, image, display_order) VALUES (1, 1, X'05', 1)");
        jdbcTemplate.update("INSERT INTO SongImage (id, song_id, image, display_order) VALUES (1, 2, X'06', 1)");
        jdbcTemplate.update("INSERT INTO ArtistTheme (id, name, is_active) VALUES (1, 'Stage', 1), (2, 'Dormant', 0)");
        jdbcTemplate.update("INSERT INTO ArtistImageTheme (artist_id, theme_id) VALUES (1, 1), (3, 2)");
        jdbcTemplate.update("INSERT INTO ArtistTag (artist_id, tag_id) VALUES (1, 10), (2, 20)");
        jdbcTemplate.update("INSERT INTO AlbumTag (album_id, tag_id) VALUES (1, 10), (2, 20)");
        jdbcTemplate.update("INSERT INTO SongTag (song_id, tag_id) VALUES (1, 10), (3, 20)");

        jdbcTemplate.update("""
                INSERT INTO Play (id, artist, album, song, play_date, song_id, account)
                VALUES
                    (1, 'Selena', 'Amor Prohibido', 'Bidi Bidi Bom Bom', '2024-01-01 10:00:00', 1, 'vatito'),
                    (2, 'Selena', 'Amor Prohibido', 'Bidi Bidi Bom Bom', '2024-01-02 10:00:00', 1, 'robertlover'),
                    (3, 'Selena', 'Amor Prohibido', 'Bidi Bidi Bom Bom', '2024-02-01 10:00:00', 1, 'vatito'),
                    (4, 'Selena', 'Amor Prohibido', 'No Me Queda Mas', '2024-01-05 10:00:00', 2, 'vatito'),
                    (5, 'Bad Bunny', 'Un Verano Sin Ti', 'Titi Me Pregunto', '2024-01-03 10:00:00', 3, 'vatito'),
                    (6, 'Bad Bunny', 'Un Verano Sin Ti', 'Titi Me Pregunto', '2024-01-04 10:00:00', 3, 'vatito'),
                    (7, 'Bad Bunny', 'Un Verano Sin Ti', 'Titi Me Pregunto', '2024-03-01 10:00:00', 3, 'robertlover'),
                    (8, 'Bad Bunny', 'Un Verano Sin Ti', 'Titi Me Pregunto', '2024-03-02 10:00:00', 3, 'vatito'),
                    (9, 'Bad Bunny', 'Un Verano Sin Ti', 'Titi Me Pregunto', '2024-03-03 10:00:00', 3, 'robertlover'),
                    (10, 'Selena', NULL, 'Standalone Jam', '2024-04-01 10:00:00', 5, 'vatito'),
                    (11, 'Selena', NULL, 'Standalone Jam', '2024-04-02 10:00:00', 5, 'robertlover'),
                    (12, 'Legacy Legend', NULL, 'Old Hit', '2024-05-01 10:00:00', 8, 'vatito')
                """);

        jdbcTemplate.update("""
                INSERT INTO Chart (id, chart_type, period_type, period_key, period_start_date, period_end_date, is_finalized)
                VALUES
                    (1, 'song', 'weekly', '2024-W01', '2024-01-01', '2024-01-07', 1),
                    (2, 'song', 'weekly', '2024-W02', '2024-01-08', '2024-01-14', 1),
                    (3, 'album', 'weekly', '2024-W01', '2024-01-01', '2024-01-07', 1),
                    (4, 'song', 'seasonal', '2024-Spring', '2024-03-01', '2024-05-31', 1),
                    (5, 'song', 'yearly', '2024', '2024-01-01', '2024-12-31', 1),
                    (6, 'album', 'seasonal', '2024-Spring', '2024-03-01', '2024-05-31', 1),
                    (7, 'album', 'yearly', '2024', '2024-01-01', '2024-12-31', 1)
                """);
        jdbcTemplate.update("""
                INSERT INTO ChartEntry (id, chart_id, position, song_id, album_id, play_count)
                VALUES
                    (1, 1, 1, 3, NULL, 2),
                    (2, 1, 2, 1, NULL, 2),
                    (3, 2, 1, 1, NULL, 1),
                    (4, 3, 1, NULL, 1, 4),
                    (5, 4, 1, 3, NULL, 3),
                    (6, 5, 2, 1, NULL, 4),
                    (7, 6, 1, NULL, 2, 5),
                    (8, 7, 2, NULL, 1, 4)
                """);
        jdbcTemplate.update("INSERT INTO trl_debut (id, song_id) VALUES (1, 1), (2, 3)");
        jdbcTemplate.update("""
                INSERT INTO trl_chart_entry (id, debut_id, chart_date, position)
                VALUES
                    (1, 1, '2024-01-01', 1),
                    (2, 1, '2024-01-02', 2),
                    (3, 2, '2024-03-01', 3)
                """);
        jdbcTemplate.update("""
                INSERT INTO vatos_cuntdown_entry (id, song_id, chart_date, position, is_close_call)
                VALUES
                    (1, 1, '2024-01-01', 1, 0),
                    (2, 1, '2024-01-02', 1, 0),
                    (3, 3, '2024-03-01', 2, 0),
                    (4, 3, '2024-03-02', 1, 1)
                """);
        jdbcTemplate.update("""
                INSERT INTO billboard_hot100_debut (id, song_id, chart_date, peak_position, weeks_on_chart, weeks_at_peak)
                VALUES
                    (1, 1, '2024-01-01', 5, 10, 2),
                    (2, 3, '2024-03-01', 1, 20, 3)
                """);
        jdbcTemplate.update("""
                INSERT INTO billboard_hot100_entry (id, chart_date, position, artist_name, song_title, peak_position, weeks_on_chart, song_id)
                VALUES
                    (1, '2024-01-01', 5, 'Selena', 'Bidi Bidi Bom Bom', 5, 1, 1),
                    (2, '2024-01-08', 5, 'Selena', 'Bidi Bidi Bom Bom', 5, 2, 1),
                    (3, '2024-01-15', 8, 'Selena', 'Bidi Bidi Bom Bom', 5, 3, 1),
                    (4, '2024-01-22', 9, 'Selena', 'Bidi Bidi Bom Bom', 5, 4, 1),
                    (5, '2024-01-29', 10, 'Selena', 'Bidi Bidi Bom Bom', 5, 5, 1),
                    (6, '2024-02-05', 11, 'Selena', 'Bidi Bidi Bom Bom', 5, 6, 1),
                    (7, '2024-02-12', 12, 'Selena', 'Bidi Bidi Bom Bom', 5, 7, 1),
                    (8, '2024-02-19', 13, 'Selena', 'Bidi Bidi Bom Bom', 5, 8, 1),
                    (9, '2024-02-26', 14, 'Selena', 'Bidi Bidi Bom Bom', 5, 9, 1),
                    (10, '2024-03-04', 15, 'Selena', 'Bidi Bidi Bom Bom', 5, 10, 1),
                    (11, '2024-03-11', 1, 'Bad Bunny', 'Titi Me Pregunto', 1, 1, 3),
                    (12, '2024-03-18', 1, 'Bad Bunny', 'Titi Me Pregunto', 1, 2, 3),
                    (13, '2024-03-25', 1, 'Bad Bunny', 'Titi Me Pregunto', 1, 3, 3),
                    (14, '2024-04-01', 2, 'Bad Bunny', 'Titi Me Pregunto', 1, 4, 3),
                    (15, '2024-04-08', 3, 'Bad Bunny', 'Titi Me Pregunto', 1, 5, 3),
                    (16, '2024-04-15', 4, 'Bad Bunny', 'Titi Me Pregunto', 1, 6, 3),
                    (17, '2024-04-22', 5, 'Bad Bunny', 'Titi Me Pregunto', 1, 7, 3),
                    (18, '2024-04-29', 6, 'Bad Bunny', 'Titi Me Pregunto', 1, 8, 3),
                    (19, '2024-05-06', 7, 'Bad Bunny', 'Titi Me Pregunto', 1, 9, 3),
                    (20, '2024-05-13', 8, 'Bad Bunny', 'Titi Me Pregunto', 1, 10, 3),
                    (21, '2024-05-20', 9, 'Bad Bunny', 'Titi Me Pregunto', 1, 11, 3),
                    (22, '2024-05-27', 10, 'Bad Bunny', 'Titi Me Pregunto', 1, 12, 3),
                    (23, '2024-06-03', 11, 'Bad Bunny', 'Titi Me Pregunto', 1, 13, 3),
                    (24, '2024-06-10', 12, 'Bad Bunny', 'Titi Me Pregunto', 1, 14, 3),
                    (25, '2024-06-17', 13, 'Bad Bunny', 'Titi Me Pregunto', 1, 15, 3),
                    (26, '2024-06-24', 14, 'Bad Bunny', 'Titi Me Pregunto', 1, 16, 3),
                    (27, '2024-07-01', 15, 'Bad Bunny', 'Titi Me Pregunto', 1, 17, 3),
                    (28, '2024-07-08', 16, 'Bad Bunny', 'Titi Me Pregunto', 1, 18, 3),
                    (29, '2024-07-15', 17, 'Bad Bunny', 'Titi Me Pregunto', 1, 19, 3),
                    (30, '2024-07-22', 18, 'Bad Bunny', 'Titi Me Pregunto', 1, 20, 3)
                """);
    }

    static SongStatsQuery songQuery(String sortBy, String sortDirection) {
        return recordQuery(SongStatsQuery.class, mapOf(
                "sortBy", sortBy,
                "sortDirection", sortDirection
        ));
    }

    static SongStatsQuery songQuery(
            List<Integer> genderIds,
            String genderMode,
            List<String> accounts,
            String accountMode,
            String listenedDateFrom,
            String listenedDateTo,
            String sortBy,
            String sortDirection) {
        return recordQuery(SongStatsQuery.class, mapOf(
                "genderIds", genderIds,
                "genderMode", genderMode,
                "accounts", accounts,
                "accountMode", accountMode,
                "listenedDateFrom", listenedDateFrom,
                "listenedDateTo", listenedDateTo,
                "sortBy", sortBy,
                "sortDirection", sortDirection
        ));
    }

    static SongStatsQuery songQueryWithExpensiveStats(String sortBy, String sortDirection) {
        return recordQuery(SongStatsQuery.class, mapOf(
                "sortBy", sortBy,
                "sortDirection", sortDirection,
                "includeExpensiveStats", true
        ));
    }

    static SongStatsQuery songQueryWith(Map<String, Object> overrides) {
        Map<String, Object> values = new HashMap<>(overrides);
        values.putIfAbsent("sortBy", "name");
        values.putIfAbsent("sortDirection", "asc");
        values.putIfAbsent("limit", 100);
        values.putIfAbsent("offset", 0);
        return recordQuery(SongStatsQuery.class, values);
    }

    static ArtistStatsQuery artistQuery(String sortBy, String sortDir) {
        return recordQuery(ArtistStatsQuery.class, mapOf(
                "sortBy", sortBy,
                "sortDir", sortDir
        ));
    }

    static ArtistStatsQuery artistQuery(
            List<Integer> genderIds,
            String genderMode,
            List<String> accounts,
            String accountMode,
            String listenedDateFrom,
            String listenedDateTo,
            String sortBy,
            String sortDir) {
        return recordQuery(ArtistStatsQuery.class, mapOf(
                "genderIds", genderIds,
                "genderMode", genderMode,
                "accounts", accounts,
                "accountMode", accountMode,
                "listenedDateFrom", listenedDateFrom,
                "listenedDateTo", listenedDateTo,
                "sortBy", sortBy,
                "sortDir", sortDir
        ));
    }

    static ArtistStatsQuery artistQueryWith(Map<String, Object> overrides) {
        Map<String, Object> values = new HashMap<>(overrides);
        values.putIfAbsent("sortBy", "name");
        values.putIfAbsent("sortDir", "asc");
        values.putIfAbsent("limit", 100);
        values.putIfAbsent("offset", 0);
        return recordQuery(ArtistStatsQuery.class, values);
    }

    static AlbumStatsQuery albumQuery(String sortBy, String sortDir) {
        return recordQuery(AlbumStatsQuery.class, mapOf(
                "sortBy", sortBy,
                "sortDir", sortDir
        ));
    }

    static AlbumStatsQuery albumQuery(
            List<Integer> genderIds,
            String genderMode,
            List<String> accounts,
            String accountMode,
            String listenedDateFrom,
            String listenedDateTo,
            String sortBy,
            String sortDir) {
        return recordQuery(AlbumStatsQuery.class, mapOf(
                "genderIds", genderIds,
                "genderMode", genderMode,
                "accounts", accounts,
                "accountMode", accountMode,
                "listenedDateFrom", listenedDateFrom,
                "listenedDateTo", listenedDateTo,
                "sortBy", sortBy,
                "sortDir", sortDir
        ));
    }

    static AlbumStatsQuery albumQueryWith(Map<String, Object> overrides) {
        Map<String, Object> values = new HashMap<>(overrides);
        values.putIfAbsent("sortBy", "name");
        values.putIfAbsent("sortDir", "asc");
        values.putIfAbsent("limit", 100);
        values.putIfAbsent("offset", 0);
        return recordQuery(AlbumStatsQuery.class, values);
    }

    static Map<String, Object> mapOf(Object... keysAndValues) {
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            Object value = keysAndValues[i + 1];
            if (value != null) {
                values.put((String) keysAndValues[i], value);
            }
        }
        return values;
    }

    private static <T> T recordQuery(Class<T> recordType, Map<String, Object> overrides) {
        try {
            RecordComponent[] components = recordType.getRecordComponents();
            Class<?>[] parameterTypes = new Class<?>[components.length];
            Object[] args = new Object[components.length];

            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                parameterTypes[i] = component.getType();
                args[i] = defaultValue(component);
                if (overrides.containsKey(component.getName())) {
                    args[i] = overrides.get(component.getName());
                }
            }

            Constructor<T> constructor = recordType.getDeclaredConstructor(parameterTypes);
            return constructor.newInstance(args);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to build test query record " + recordType.getSimpleName(), ex);
        }
    }

    private static Object defaultValue(RecordComponent component) {
        if (component.getType() == int.class) {
            return switch (component.getName()) {
                case "limit" -> 100;
                case "offset" -> 0;
                default -> 0;
            };
        }
        if (component.getType() == boolean.class) {
            return false;
        }
        return null;
    }
}
