package library;

import library.dto.AlbumStatsRow;
import library.dto.ArtistStatsRow;
import library.dto.SongStatsRow;
import library.repository.ArtistImageRepository;
import library.repository.ArtistRepository;
import library.repository.LookupRepository;
import library.service.ArtistService;
import library.service.ItunesService;
import library.service.SongLinkService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static library.TestDatabaseSupport.albumQuery;
import static library.TestDatabaseSupport.albumQueryWith;
import static library.TestDatabaseSupport.artistQuery;
import static library.TestDatabaseSupport.artistQueryWith;
import static library.TestDatabaseSupport.mapOf;
import static library.TestDatabaseSupport.songQuery;
import static library.TestDatabaseSupport.songQueryWith;
import static library.TestDatabaseSupport.songQueryWithExpensiveStats;
import static library.util.ArtistFilterMode.encode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MusicCatalogRepositoryRegressionTest {

    @Test
    void artistScopeCombinesMainGroupAndFeaturedMetricsWithSourceBreakdowns() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ArtistStatsRow scoped = db.artistRepository.findArtistsWithStats(artistQueryWith(mapOf(
                    "name", "Guest Singer",
                    "includeMain", false,
                    "includeGroups", true,
                    "includeFeatured", true,
                    "sortBy", "plays",
                    "sortDir", "desc"
            ))).getFirst();

            assertThat(scoped.songCount()).isEqualTo(2);
            assertThat(scoped.albumCount()).isEqualTo(1);
            assertThat(scoped.playCount()).isEqualTo(1);
            assertThat(scoped.mainPlayCount()).isZero();
            assertThat(scoped.groupPlayCount()).isZero();
            assertThat(scoped.featuredPlayCount()).isEqualTo(1);
            assertThat(scoped.vatitoPlayCount()).isEqualTo(1);

            assertThat(db.artistRepository.countArtistsWithFilters(artistQueryWith(mapOf(
                    "name", "Guest Singer",
                    "includeMain", false,
                    "includeFeatured", true,
                    "playCountMin", 1
            )))).isEqualTo(1);
        }
    }

    @Test
    void artistDetailScopedMetricsUseTheSameGroupAndFeaturedCatalog() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ItunesService itunesService = mock(ItunesService.class);
            when(itunesService.getAllItunesSongIdsJson()).thenReturn("[]");
            ArtistService artistService = new ArtistService(
                    mock(ArtistRepository.class), mock(ArtistImageRepository.class), mock(LookupRepository.class),
                    db.jdbcTemplate, itunesService, mock(SongLinkService.class));

            ArtistService.ArtistScopedMetrics scoped = artistService.getScopedMetricsForArtist(4, false, true, true);

            assertThat(scoped.songCount()).isEqualTo(2);
            assertThat(scoped.albumCount()).isEqualTo(1);
            assertThat(scoped.playCount()).isEqualTo(1);
            assertThat(scoped.featuredPlayCount()).isEqualTo(1);
            assertThat(scoped.groupPlayCount()).isZero();
            assertThat(scoped.totalSongLength()).isEqualTo(320);
            assertThat(scoped.uniqueDays()).isEqualTo(1);
            assertThat(scoped.averagePlaysPerSong()).isEqualTo(0.5);
        }
    }

    @Test
    void songListUsesPlayCountsOverrideFallbacksGenderOverridesAndStableSorts() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<SongStatsRow> rows = db.songRepository.findSongsWithStats(songQuery("plays", "desc"));

            assertThat(rows)
                    .extracting(SongStatsRow::name)
                    .containsExactly(
                            "Titi Me Pregunto",
                            "Bidi Bidi Bom Bom",
                            "Standalone Jam",
                            "No Me Queda Mas",
                            "Old Hit",
                            "Ojitos Lindos",
                            "Quiet Track",
                            "Unknown Silence"
                    );

            Map<String, SongStatsRow> byName = indexBy(rows, SongStatsRow::name);
            assertThat(byName.get("Bidi Bidi Bom Bom").playCount()).isEqualTo(3);
            assertThat(byName.get("Bidi Bidi Bom Bom").genreName()).isEqualTo("Rock");
            assertThat(byName.get("Bidi Bidi Bom Bom").subgenreName()).isEqualTo("Alt Rock");
            assertThat(byName.get("Bidi Bidi Bom Bom").languageName()).isEqualTo("Spanish");
            assertThat(byName.get("Bidi Bidi Bom Bom").hasImage()).isTrue();
            assertThat(byName.get("Bidi Bidi Bom Bom").albumHasImage()).isTrue();

            assertThat(byName.get("No Me Queda Mas").genreName()).isEqualTo("Dance");
            assertThat(byName.get("No Me Queda Mas").subgenreName()).isEqualTo("Dance Pop");
            assertThat(byName.get("No Me Queda Mas").languageName()).isEqualTo("English");
            assertThat(byName.get("No Me Queda Mas").featuredArtistCount()).isEqualTo(1);

            List<SongStatsRow> femaleRows = db.songRepository.findSongsWithStats(
                    songQuery(List.of(2), "includes", null, null, null, null, "name", "asc"));
            assertThat(femaleRows)
                    .extracting(SongStatsRow::name)
                    .containsExactly("Bidi Bidi Bom Bom", "No Me Queda Mas", "Ojitos Lindos", "Standalone Jam");
            assertThat(indexBy(femaleRows, SongStatsRow::name).get("Ojitos Lindos").genderName()).isEqualTo("Female");
        }
    }

    @Test
    void songAndAlbumListsFilterByFeaturedArtist() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<SongStatsRow> songs = db.songRepository.findSongsWithStats(songQueryWith(mapOf(
                    "featuredArtistIds", List.of(4),
                    "sortBy", "name",
                    "sortDirection", "asc"
            )));
            List<AlbumStatsRow> albums = db.albumRepository.findAlbumsWithStats(albumQueryWith(mapOf(
                    "featuredArtistIds", List.of(4),
                    "sortBy", "name",
                    "sortDir", "asc"
            )));

            assertThat(songs).extracting(SongStatsRow::name).containsExactly("No Me Queda Mas");
            assertThat(albums).extracting(AlbumStatsRow::name).containsExactly("Amor Prohibido");
        }
    }

    @Test
    void songAndAlbumListsCanExcludePrimaryArtists() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<Integer> excludedArtists = encode(List.of(2), "excludes");

            List<SongStatsRow> songs = db.songRepository.findSongsWithStats(songQueryWith(mapOf(
                    "artistName", excludedArtists,
                    "sortBy", "name",
                    "sortDirection", "asc"
            )));
            List<AlbumStatsRow> albums = db.albumRepository.findAlbumsWithStats(albumQueryWith(mapOf(
                    "artistName", excludedArtists,
                    "sortBy", "name",
                    "sortDir", "asc"
            )));

            assertThat(songs).extracting(SongStatsRow::artistId).doesNotContain(2);
            assertThat(albums).extracting(AlbumStatsRow::artistId).doesNotContain(2);
            assertThat(songs).extracting(SongStatsRow::name).contains("Bidi Bidi Bom Bom");
            assertThat(albums).extracting(AlbumStatsRow::name).contains("Amor Prohibido");
        }
    }

    @Test
    void songListScopesPlayStatsByAccountAndListenedDate() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<SongStatsRow> legacyOnly = db.songRepository.findSongsWithStats(
                    songQuery(null, null, List.of("robertlover"), "includes", null, null, "plays", "desc"));

            assertThat(legacyOnly)
                    .extracting(SongStatsRow::name)
                    .containsExactly("Titi Me Pregunto", "Bidi Bidi Bom Bom", "Standalone Jam");
            assertThat(indexBy(legacyOnly, SongStatsRow::name).get("Titi Me Pregunto").playCount()).isEqualTo(2);
            assertThat(indexBy(legacyOnly, SongStatsRow::name).get("Bidi Bidi Bom Bom").playCount()).isEqualTo(1);

            List<SongStatsRow> januaryOnly = db.songRepository.findSongsWithStats(
                    songQuery(null, null, null, null, "2024-01-01", "2024-01-31", "plays", "desc"));

            assertThat(januaryOnly)
                    .extracting(SongStatsRow::name)
                    .containsExactly("Bidi Bidi Bom Bom", "Titi Me Pregunto", "No Me Queda Mas");
            assertThat(indexBy(januaryOnly, SongStatsRow::name).get("Bidi Bidi Bom Bom").playCount()).isEqualTo(2);
            assertThat(indexBy(januaryOnly, SongStatsRow::name).get("No Me Queda Mas").lastListened())
                    .isEqualTo("2024-01-05 10:00:00");
        }
    }

    @Test
    void songListCanIncludeExpensiveChartStatsWithoutChangingBaseRows() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<SongStatsRow> rows = db.songRepository.findSongsWithStats(
                    songQueryWithExpensiveStats("weekly_chart_weeks", "desc"));

            Map<String, SongStatsRow> byName = indexBy(rows, SongStatsRow::name);
            assertThat(byName.get("Bidi Bidi Bom Bom").weeklyChartPeak()).isEqualTo(1);
            assertThat(byName.get("Bidi Bidi Bom Bom").weeklyChartWeeks()).isEqualTo(2);
            assertThat(byName.get("Titi Me Pregunto").weeklyChartPeak()).isEqualTo(1);
            assertThat(byName.get("Quiet Track").weeklyChartPeak()).isNull();
        }
    }

    @Test
    void catalogSummaryQueriesDeferExtendedStatisticsUntilRequested() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            ArtistStatsRow artistSummary = db.artistRepository.findArtistsWithStats(artistQueryWith(mapOf(
                    "name", "Selena", "includeExtendedStats", false
            ))).getFirst();
            ArtistStatsRow artistDetails = db.artistRepository.findArtistsWithStats(artistQueryWith(mapOf(
                    "name", "Selena", "includeExtendedStats", true
            ))).getFirst();
            assertThat(artistSummary.daysListened()).isZero();
            assertThat(artistSummary.songCount()).isZero();
            assertThat(artistDetails.daysListened()).isPositive();
            assertThat(artistDetails.songCount()).isEqualTo(3);

            AlbumStatsRow albumSummary = db.albumRepository.findAlbumsWithStats(albumQueryWith(mapOf(
                    "name", "Amor Prohibido", "includeExtendedStats", false
            ))).getFirst();
            AlbumStatsRow albumDetails = db.albumRepository.findAlbumsWithStats(albumQueryWith(mapOf(
                    "name", "Amor Prohibido", "includeExtendedStats", true
            ))).getFirst();
            assertThat(albumSummary.daysListened()).isZero();
            assertThat(albumSummary.featuredArtistCount()).isZero();
            assertThat(albumDetails.daysListened()).isPositive();
            assertThat(albumDetails.featuredArtistCount()).isEqualTo(1);

            SongStatsRow songSummary = db.songRepository.findSongsWithStats(songQueryWith(mapOf(
                    "name", "Bidi Bidi Bom Bom", "includeExpensiveStats", false
            ))).getFirst();
            SongStatsRow songDetails = db.songRepository.findSongsWithStats(songQueryWith(mapOf(
                    "name", "Bidi Bidi Bom Bom", "includeExpensiveStats", true
            ))).getFirst();
            assertThat(songSummary.daysListened()).isZero();
            assertThat(songSummary.weeklyChartPeak()).isNull();
            assertThat(songDetails.daysListened()).isPositive();
            assertThat(songDetails.weeklyChartPeak()).isEqualTo(1);
        }
    }

    @Test
    void artistListAggregatesStatsAndGenderCountsFromFilteredCatalog() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<ArtistStatsRow> rows = db.artistRepository.findArtistsWithStats(artistQuery("plays", "desc"));

            assertThat(rows)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Selena", "Bad Bunny", "Legacy Legend", "Guest Singer", "Mystery Artist", "The Static Hearts");

            Map<String, ArtistStatsRow> byName = indexBy(rows, ArtistStatsRow::name);
            assertThat(byName.get("Selena").playCount()).isEqualTo(6);
            assertThat(byName.get("Selena").songCount()).isEqualTo(3);
            assertThat(byName.get("Selena").albumCount()).isEqualTo(1);
            assertThat(byName.get("Selena").featuredArtistCount()).isEqualTo(1);
            assertThat(byName.get("Selena").imageCount()).isEqualTo(2);

            List<ArtistStatsRow> femaleRows = db.artistRepository.findArtistsWithStats(
                    artistQuery(List.of(2), "includes", null, null, null, null, "name", "asc"));
            assertThat(femaleRows)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Guest Singer", "Selena");

            Map<Integer, Long> genderCounts = db.artistRepository.countArtistsByGenderWithFilters(artistQuery("name", "asc"));
            assertThat(genderCounts).containsEntry(1, 3L).containsEntry(2, 2L);
        }
    }

    @Test
    void artistListScopesStatsByAccountAndListenedDate() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<ArtistStatsRow> rows = db.artistRepository.findArtistsWithStats(
                    artistQuery(null, null, List.of("robertlover"), "includes", null, null, "plays", "desc"));

            assertThat(rows)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Bad Bunny", "Selena");
            assertThat(indexBy(rows, ArtistStatsRow::name).get("Bad Bunny").playCount()).isEqualTo(2);
            assertThat(indexBy(rows, ArtistStatsRow::name).get("Selena").playCount()).isEqualTo(2);

            List<ArtistStatsRow> januaryOnly = db.artistRepository.findArtistsWithStats(
                    artistQuery(null, null, null, null, "2024-01-01", "2024-01-31", "plays", "desc"));

            assertThat(januaryOnly)
                    .extracting(ArtistStatsRow::name)
                    .containsExactly("Selena", "Bad Bunny");
            assertThat(indexBy(januaryOnly, ArtistStatsRow::name).get("Selena").playCount()).isEqualTo(3);
            assertThat(indexBy(januaryOnly, ArtistStatsRow::name).get("Bad Bunny").playCount()).isEqualTo(2);
        }
    }

    @Test
    void albumListAggregatesStatsOverridesAndSkipsFullListenStatsForNonAlbums() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<AlbumStatsRow> rows = db.albumRepository.findAlbumsWithStats(albumQuery("plays", "desc"));

            assertThat(rows)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Un Verano Sin Ti", "Amor Prohibido", "Legacy Collection", "Silent Record", "Unknown Album");

            Map<String, AlbumStatsRow> byName = indexBy(rows, AlbumStatsRow::name);
            assertThat(byName.get("Amor Prohibido").playCount()).isEqualTo(4);
            assertThat(byName.get("Amor Prohibido").songCount()).isEqualTo(2);
            assertThat(byName.get("Amor Prohibido").albumLength()).isEqualTo(410);
            assertThat(byName.get("Amor Prohibido").genreName()).isEqualTo("Rock");
            assertThat(byName.get("Amor Prohibido").languageName()).isEqualTo("Spanish");
            assertThat(byName.get("Amor Prohibido").featuredArtistCount()).isEqualTo(1);
            assertThat(byName.get("Amor Prohibido").imageCount()).isEqualTo(2);

            List<AlbumStatsRow> fullListenRows = db.albumRepository.findAlbumsWithStats(
                    albumQuery("last_full_listen", "desc"));
            assertThat(indexBy(fullListenRows, AlbumStatsRow::name).get("Amor Prohibido").firstFullListenDate())
                    .isNull();
            assertThat(indexBy(fullListenRows, AlbumStatsRow::name).get("Amor Prohibido").lastFullListenDate())
                    .isNull();
            assertThat(indexBy(fullListenRows, AlbumStatsRow::name).get("Amor Prohibido").fullAlbumPlays())
                    .isZero();
            assertThat(indexBy(fullListenRows, AlbumStatsRow::name).get("Un Verano Sin Ti").lastFullListenDate())
                    .isNull();
            assertThat(indexBy(fullListenRows, AlbumStatsRow::name).get("Un Verano Sin Ti").fullAlbumPlays())
                    .isZero();

            List<AlbumStatsRow> extendedStatsRows = db.albumRepository.findAlbumsWithStats(
                    albumQueryWith(mapOf("includeFullListenStats", true, "sortBy", "plays", "sortDir", "desc")));
            assertThat(indexBy(extendedStatsRows, AlbumStatsRow::name).get("Amor Prohibido").fullAlbumPlays())
                    .isZero();

            Map<Integer, library.dto.AlbumFullListenStats> selectedStats =
                    db.albumRepository.findFullListenStatsForAlbums(List.of(1, 2));
            assertThat(selectedStats).doesNotContainKeys(1, 2);
        }
    }

    @Test
    void fullAlbumPlaysCountsBackToBackAlbumListensSeparately() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            db.jdbcTemplate.update("DELETE FROM Play");
            db.jdbcTemplate.update("INSERT INTO Album (id, artist_id, name, number_of_songs) VALUES (6, 1, 'Back To Back Album', 5)");
            for (int track = 1; track <= 5; track++) {
                db.jdbcTemplate.update("INSERT INTO Song (id, artist_id, album_id, name, track_number) VALUES (?, 1, 6, ?, ?)",
                        20 + track, "Track " + track, track);
            }
            db.jdbcTemplate.update("""
                    INSERT INTO Play (id, artist, album, song, play_date, song_id, account)
                    VALUES
                        (1, 'Selena', 'Back To Back Album', 'Track 1', '2024-06-01 10:00:00', 21, 'vatito'),
                        (2, 'Selena', 'Back To Back Album', 'Track 2', '2024-06-01 10:01:00', 22, 'vatito'),
                        (3, 'Selena', 'Back To Back Album', 'Track 3', '2024-06-01 10:02:00', 23, 'vatito'),
                        (4, 'Selena', 'Back To Back Album', 'Track 4', '2024-06-01 10:03:00', 24, 'vatito'),
                        (5, 'Selena', 'Back To Back Album', 'Track 5', '2024-06-01 10:04:00', 25, 'vatito'),
                        (6, 'Selena', 'Back To Back Album', 'Track 1', '2024-06-02 10:00:00', 21, 'vatito'),
                        (7, 'Selena', 'Back To Back Album', 'Track 2', '2024-06-02 10:01:00', 22, 'vatito'),
                        (8, 'Selena', 'Back To Back Album', 'Track 3', '2024-06-02 10:02:00', 23, 'vatito'),
                        (9, 'Selena', 'Back To Back Album', 'Track 4', '2024-06-02 10:03:00', 24, 'vatito'),
                        (10, 'Selena', 'Back To Back Album', 'Track 5', '2024-06-02 10:04:00', 25, 'vatito')
                    """);

            AlbumStatsRow album = indexBy(db.albumRepository.findAlbumsWithStats(
                    albumQuery("full_album_plays", "desc")), AlbumStatsRow::name).get("Back To Back Album");

            assertThat(album.fullAlbumPlays()).isEqualTo(2);
            assertThat(album.firstFullListenDate()).isEqualTo("2024-06-01");
            assertThat(album.lastFullListenDate()).isEqualTo("2024-06-02");

            var detailStats = db.albumRepository.findFullListenStatsForAlbum(6);
            assertThat(detailStats.fullAlbumPlays()).isEqualTo(2);
            assertThat(detailStats.firstFullListenDate()).isEqualTo("2024-06-01");
            assertThat(detailStats.lastFullListenDate()).isEqualTo("2024-06-02");
        }
    }

    @Test
    void fullAlbumPlayAllowsFiveInterruptionsDistributedAcrossAnUntimedRun() {
        var lenientConfig = new library.service.AppConfigService.AlbumFullListenConfig(0, 0, 0, 0, 0, 5);
        try (TestDatabaseSupport db = TestDatabaseSupport.create(lenientConfig)) {
            seedInterruptedTenTrackAlbum(db);

            var stats = db.albumRepository.findFullListenStatsForAlbum(6);
            assertThat(stats.fullAlbumPlays()).isEqualTo(1);
            assertThat(stats.firstFullListenDate()).isEqualTo("2024-07-03");
            assertThat(stats.lastFullListenDate()).isEqualTo("2024-07-03");
        }

        var stricterConfig = new library.service.AppConfigService.AlbumFullListenConfig(0, 0, 0, 0, 0, 4);
        try (TestDatabaseSupport db = TestDatabaseSupport.create(stricterConfig)) {
            seedInterruptedTenTrackAlbum(db);

            assertThat(db.albumRepository.findFullListenStatsForAlbum(6).fullAlbumPlays()).isZero();
        }
    }

    @Test
    void fullAlbumListenIgnoresRemixesAndUsesTheUpToFifteenTrackTier() {
        var noLeniency = new library.service.AppConfigService.AlbumFullListenConfig(0, 0, 0, 0, 0, 0);
        try (TestDatabaseSupport db = TestDatabaseSupport.create(noLeniency)) {
            db.jdbcTemplate.update("DELETE FROM Play");
            db.jdbcTemplate.update("INSERT INTO Album (id, artist_id, name, number_of_songs) VALUES (6, 1, 'Remix Album', 6)");
            for (int track = 1; track <= 5; track++) {
                db.jdbcTemplate.update("INSERT INTO Song (id, artist_id, album_id, name, track_number) VALUES (?, 1, 6, ?, ?)",
                        20 + track, "Track " + track, track);
            }
            db.jdbcTemplate.update("INSERT INTO Song (id, artist_id, album_id, name, track_number) VALUES (26, 1, 6, 'Track 2 Remix', 6)");
            db.jdbcTemplate.update("""
                    INSERT INTO Play (id, play_date, song_id, account)
                    VALUES
                        (1, '2024-08-01 10:00:00', 21, 'vatito'),
                        (2, '2024-08-01 10:01:00', 22, 'vatito'),
                        (3, '2024-08-01 10:02:00', 26, 'vatito'),
                        (4, '2024-08-01 10:03:00', 23, 'vatito'),
                        (5, '2024-08-01 10:04:00', 24, 'vatito'),
                        (6, '2024-08-01 10:05:00', 25, 'vatito')
                    """);

            var stats = db.albumRepository.findFullListenStatsForAlbum(6);
            assertThat(stats.fullAlbumPlays()).isEqualTo(1);
            assertThat(stats.lastFullListenDate()).isEqualTo("2024-08-01");
        }

        var fifteenTrackLeniency = new library.service.AppConfigService.AlbumFullListenConfig(0, 0, 1, 0, 0, 0);
        try (TestDatabaseSupport db = TestDatabaseSupport.create(fifteenTrackLeniency)) {
            db.jdbcTemplate.update("DELETE FROM Play");
            db.jdbcTemplate.update("INSERT INTO Album (id, artist_id, name, number_of_songs) VALUES (6, 1, 'Twelve Track Album', 12)");
            for (int track = 1; track <= 12; track++) {
                db.jdbcTemplate.update("INSERT INTO Song (id, artist_id, album_id, name, track_number) VALUES (?, 1, 6, ?, ?)",
                        20 + track, "Track " + track, track);
            }
            for (int track = 1; track <= 11; track++) {
                db.jdbcTemplate.update("INSERT INTO Play (id, play_date, song_id, account) VALUES (?, ?, ?, 'vatito')",
                        track, "2024-09-01 10:%02d:00".formatted(track), 20 + track);
            }

            assertThat(db.albumRepository.findFullListenStatsForAlbum(6).fullAlbumPlays()).isEqualTo(1);
        }
    }

    private static void seedInterruptedTenTrackAlbum(TestDatabaseSupport db) {
        db.jdbcTemplate.update("DELETE FROM Play");
        db.jdbcTemplate.update("INSERT INTO Album (id, artist_id, name, number_of_songs) VALUES (6, 1, 'Interrupted Album', 10)");
        db.jdbcTemplate.update("""
                INSERT INTO Song (id, artist_id, album_id, name, track_number)
                VALUES
                    (20, 1, 6, 'Track 1', 1), (21, 1, 6, 'Track 2', 2),
                    (22, 1, 6, 'Track 3', 3), (23, 1, 6, 'Track 4', 4),
                    (24, 1, 6, 'Track 5', 5), (25, 1, 6, 'Track 6', 6),
                    (26, 1, 6, 'Track 7', 7), (27, 1, 6, 'Track 8', 8),
                    (28, 1, 6, 'Track 9', 9), (29, 1, 6, 'Track 10', 10)
                """);
        db.jdbcTemplate.update("""
                INSERT INTO Play (id, play_date, song_id, account)
                VALUES
                    (1,  '2024-07-01 10:00:00', 20, 'vatito'),
                    (2,  '2024-07-01 10:01:00', 21, 'vatito'),
                    (3,  '2024-07-01 10:02:00', 3,  'vatito'),
                    (4,  '2024-07-01 10:03:00', 22, 'vatito'),
                    (5,  '2024-07-01 10:04:00', 23, 'vatito'),
                    (6,  '2024-07-01 10:05:00', 24, 'vatito'),
                    (7,  '2024-07-01 10:06:00', 25, 'vatito'),
                    (8,  '2024-07-01 10:07:00', 3,  'vatito'),
                    (9,  '2024-07-01 10:08:00', 3,  'vatito'),
                    (10, '2024-07-01 10:09:00', 3,  'vatito'),
                    (11, '2024-07-01 10:10:00', 26, 'vatito'),
                    (12, '2024-07-01 10:11:00', 27, 'vatito'),
                    (13, '2024-07-01 10:12:00', 28, 'vatito'),
                    (14, '2024-07-01 10:13:00', 3,  'vatito'),
                    (15, '2024-07-03 02:13:00', 29, 'vatito')
                """);
    }

    @Test
    void albumListScopesStatsByGenderAccountAndListenedDate() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<AlbumStatsRow> femaleRows = db.albumRepository.findAlbumsWithStats(
                    albumQuery(List.of(2), "includes", null, null, null, null, "name", "asc"));
            assertThat(femaleRows)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Amor Prohibido");

            List<AlbumStatsRow> legacyOnly = db.albumRepository.findAlbumsWithStats(
                    albumQuery(null, null, List.of("robertlover"), "includes", null, null, "plays", "desc"));
            assertThat(legacyOnly)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Un Verano Sin Ti", "Amor Prohibido");
            assertThat(indexBy(legacyOnly, AlbumStatsRow::name).get("Un Verano Sin Ti").playCount()).isEqualTo(2);
            assertThat(indexBy(legacyOnly, AlbumStatsRow::name).get("Amor Prohibido").playCount()).isEqualTo(1);

            List<AlbumStatsRow> januaryOnly = db.albumRepository.findAlbumsWithStats(
                    albumQuery(null, null, null, null, "2024-01-01", "2024-01-31", "plays", "desc"));
            assertThat(januaryOnly)
                    .extracting(AlbumStatsRow::name)
                    .containsExactly("Amor Prohibido", "Un Verano Sin Ti");
        }
    }

    @Test
    void representativeCatalogQueriesStayWithinPerformanceSmokeBudget() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            assertTimeout(Duration.ofSeconds(2), () -> {
                db.songRepository.findSongsWithStats(songQuery("plays", "desc"));
                db.songRepository.findSongsWithStats(songQueryWithExpensiveStats("weekly_chart_weeks", "desc"));
                db.artistRepository.findArtistsWithStats(artistQuery("plays", "desc"));
                db.artistRepository.countArtistsByGenderWithFilters(artistQuery("name", "asc"));
                db.albumRepository.findAlbumsWithStats(albumQuery("plays", "desc"));
                db.albumRepository.findAlbumsWithStats(albumQuery("first_full_listen", "asc"));
                db.albumRepository.findAlbumsWithStats(albumQuery("last_full_listen", "desc"));
                db.albumRepository.findAlbumsWithStats(albumQuery("full_album_plays", "desc"));
                db.albumRepository.findFullListenStatsForAlbum(1);
            });
        }
    }

    private static <T> Map<String, T> indexBy(List<T> rows, Function<T, String> keyExtractor) {
        return rows.stream().collect(Collectors.toMap(keyExtractor, Function.identity()));
    }
}
