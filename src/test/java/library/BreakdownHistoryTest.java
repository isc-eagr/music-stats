package library;

import library.controller.BreakdownHistoryController;
import library.service.BreakdownHistoryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.*;

class BreakdownHistoryTest {
    private TestDatabaseSupport db;
    private BreakdownHistoryService service;

    @BeforeEach
    void setup() {
        db = TestDatabaseSupport.create();
        service = new BreakdownHistoryService(db.jdbcTemplate);
        db.jdbcTemplate.update("DELETE FROM Play");
        db.jdbcTemplate.update("DELETE FROM Song");
        db.jdbcTemplate.update("DELETE FROM Album");
        db.jdbcTemplate.update("DELETE FROM Artist");
        for (String table : new String[]{"Gender", "Genre", "Ethnicity", "Language"}) {
            db.jdbcTemplate.update("DELETE FROM " + table);
        }
        db.jdbcTemplate.update("INSERT INTO Gender (id, name) VALUES (1, 'Female'), (2, 'Male')");
        db.jdbcTemplate.update("INSERT INTO Genre (id, name) VALUES (1, 'Rock'), (2, 'Pop'), (3, 'Jazz')");
        db.jdbcTemplate.update("INSERT INTO Language (id, name) VALUES (1, 'English'), (2, 'Spanish'), (3, 'French')");
        db.jdbcTemplate.update("INSERT INTO Ethnicity (id, name) VALUES (1, 'A'), (2, 'B')");
        db.jdbcTemplate.update("""
                INSERT INTO Artist (id, name, gender_id, genre_id, ethnicity_id, language_id)
                VALUES (1, 'Male artist', 2, 1, 1, 1), (2, 'Female artist', 1, 2, 2, 2),
                       (3, 'Unplayed artist', 1, 3, 2, 3)
                """);
        db.jdbcTemplate.update("""
                INSERT INTO Album (id, artist_id, name, override_genre_id, override_language_id)
                VALUES (1, 1, 'Album', 2, 2), (2, 2, 'Second album', NULL, NULL),
                       (3, 3, 'Unplayed album', NULL, NULL)
                """);
        db.jdbcTemplate.update("""
                INSERT INTO Song (id, artist_id, album_id, name, length_seconds,
                                  override_gender_id, override_ethnicity_id, override_genre_id, override_language_id)
                VALUES (1, 1, 1, 'Inherited', 60, NULL, NULL, NULL, NULL),
                       (2, 1, 1, 'Override', 120, 1, 2, 3, 3),
                       (3, 2, 2, 'Second artist', NULL, NULL, NULL, NULL, NULL),
                       (4, 1, NULL, 'No album', -30, NULL, NULL, NULL, NULL),
                       (5, 3, 3, 'Unplayed', 200, NULL, NULL, NULL, NULL)
                """);
    }

    @AfterEach
    void cleanup() { db.close(); }

    @Test
    void cumulativeCountsFirstPlaysOnceAndCarriesHistoryIntoSelectedYear() {
        play("2023-12-31 23:59", 1);
        play("2024-01-01 00:00", 1);
        play("2024-01-02 00:00", 2);
        play("2024-03-01 00:00", 3);
        play("2025-01-01 00:00", 5);
        var songs = history("gender", "songs", "cumulative", "month", 2024);
        assertThat(songs.periods()).hasSize(12);
        assertThat(songs.totals()).startsWith(2L, 2L, 3L).endsWith(3L);
        assertThat(values(songs, "Male")).startsWith(1, 1, 1);
        assertThat(values(songs, "Female")).startsWith(1, 1, 2);
        var artists = history("gender", "artists", "cumulative", "month", 2024);
        assertThat(artists.totals()).startsWith(1L, 1L, 2L).endsWith(2L);
        assertThat(values(artists, "Male")).startsWith(1, 1, 1);
        var albums = history("gender", "albums", "cumulative", "month", 2024);
        assertThat(albums.totals()).startsWith(1L, 1L, 2L).endsWith(2L);
        assertThat(history("gender", "plays", "cumulative", "month", 2024).totals())
                .startsWith(3L, 3L, 4L).endsWith(4L);
    }

    @Test
    void periodsCountDistinctItemsAcrossWholeBucketAndKeepEmptyPeriods() {
        play("2024-01-01", 1);
        play("2024-01-02", 1);
        play("2024-03-01", 1);
        play("2024-03-02", 2);
        var monthly = history("gender", "songs", "period", "month", 2024);
        assertThat(monthly.totals()).startsWith(1L, 0L, 2L);
        var quarterly = history("gender", "songs", "period", "quarter", 2024);
        assertThat(quarterly.totals()).containsExactly(2L, 0L, 0L, 0L);
        assertThat(quarterly.periods().getFirst().end()).isEqualTo("2024-03-31");
        var weekly = history("gender", "songs", "period", "week", 2024);
        assertThat(weekly.periods().getFirst()).isEqualTo(new BreakdownHistoryService.Period("2024-01-01", "2024-01-07"));
        assertThat(weekly.totals().getFirst()).isEqualTo(1L);
        assertThat(weekly.totals().get(8)).isEqualTo(2L);
        assertThat(history("gender", "artists", "period", "year", 2024).totals()).containsExactly(1L);
        assertThat(history("gender", "albums", "period", "year", 2024).totals()).containsExactly(1L);
        assertThat(history("gender", "plays", "period", "year", 2024).totals()).containsExactly(4L);
    }

    @Test
    void respectsEveryOverrideLevelAndUsesEntityClassificationForCatalogCounts() {
        play("2024-02-01", 1);
        play("2024-02-02", 2);
        play("2024-02-03", 4);
        for (String measure : new String[]{"songs", "plays"}) {
            var genre = history("genre", measure, "period", "year", 2024);
            assertThat(values(genre, "Jazz")).containsExactly(1);
            assertThat(values(genre, "Pop")).containsExactly(1);
            assertThat(values(genre, "Rock")).containsExactly(1);
            assertThat(values(history("language", measure, "period", "year", 2024), "French")).containsExactly(1);
            assertThat(values(history("language", measure, "period", "year", 2024), "Spanish")).containsExactly(1);
            assertThat(values(history("language", measure, "period", "year", 2024), "English")).containsExactly(1);
            assertThat(values(history("ethnicity", measure, "period", "year", 2024), "B")).containsExactly(1);
        }
        assertThat(values(history("genre", "artists", "cumulative", "year", 2024), "Rock")).containsExactly(1);
        assertThat(values(history("genre", "albums", "cumulative", "year", 2024), "Pop")).containsExactly(1);
        assertThat(values(history("language", "albums", "period", "year", 2024), "Spanish")).containsExactly(1);
        assertThat(values(history("ethnicity", "albums", "period", "year", 2024), "A")).containsExactly(1);
        assertThat(values(history("gender", "albums", "period", "year", 2024), "Male")).containsExactly(1);
        var time = history("gender", "time", "period", "year", 2024);
        assertThat(time.totals()).containsExactly(180L);
        assertThat(values(time, "Female")).containsExactly(120);
    }

    @Test
    void includesUnmatchedPlaysAsUnknownAndRejectsInvalidDatesAndFutureData() {
        play("2024-02-29 23:59", null);
        play("2024-03-01 00:00", 999);
        play("not a date", 1);
        play("9999-01-01", 1);
        var plays = history("gender", "plays", "period", "month", 2024);
        assertThat(values(plays, "Other / unknown")).startsWith(0, 1, 1);
        assertThat(plays.periods().get(1).end()).isEqualTo("2024-02-29");
        assertThat(values(history("genre", "plays", "period", "year", 2024), "Unknown")).containsExactly(2);
        assertThat(history("gender", "songs", "period", "year", 2024).totals()).containsExactly(0L);
        assertThat(history("gender", "time", "period", "year", 2024).totals()).containsExactly(0L);
        assertThat(history("gender", "plays", "cumulative", "year", null).totals()).allMatch(total -> total <= 2);
    }

    @Test
    void initialImportRemainsIncludedInEveryPeriodView() {
        play("2005-02-28 23:59", 1);
        play("2005-02-28 23:59", 1);
        play("2005-03-01 00:00", 2);
        play("2005-04-01 00:00", 1);
        var monthly = history("gender", "plays", "period", "month", 2005);
        assertThat(monthly.periods().getFirst().start()).isEqualTo("2005-01-01");
        assertThat(monthly.totals()).startsWith(0L, 2L, 1L, 1L).endsWith(0L);
        var allHistory = history("genre", "plays", "period", "month", null);
        assertThat(allHistory.periods().getFirst().start()).isEqualTo("2005-02-01");
        assertThat(allHistory.totals()).startsWith(2L, 1L, 1L);
        var quarter = history("gender", "plays", "period", "quarter", 2005);
        assertThat(quarter.periods().getFirst().end()).isEqualTo("2005-03-31");
        assertThat(quarter.totals()).containsExactly(3L, 1L, 0L, 0L);
        assertThat(history("gender", "plays", "period", "year", 2005).totals()).containsExactly(4L);
        assertThat(history("gender", "plays", "cumulative", "year", 2005).totals()).containsExactly(4L);
        assertThat(history("gender", "songs", "period", "year", 2005).totals()).containsExactly(2L);
    }

    @Test
    void emptyDataAndRequestValidation() {
        var controller = new BreakdownHistoryController(service);
        assertThat(controller.history("gender", "plays", "cumulative", "month", null).periods()).isEmpty();
        assertThat(controller.history("gender", "plays", "cumulative", "month", 9998).periods()).isEmpty();
        assertThatThrownBy(() -> controller.history("invalid", "plays", "cumulative", "month", null))
                .isInstanceOfSatisfying(ResponseStatusException.class, error -> assertThat(error.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST));
        assertThatThrownBy(() -> history("gender", "bad", "cumulative", "month", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> history("gender", "plays", "bad", "month", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> history("gender", "plays", "period", "bad", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> history("gender", "plays", "period", "month", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    private BreakdownHistoryService.History history(String breakdown, String measure, String mode, String interval, Integer year) {
        return service.getHistory(breakdown, measure, mode, interval, year);
    }

    private long[] values(BreakdownHistoryService.History history, String name) {
        return history.series().stream().filter(series -> series.name().equals(name)).findFirst().orElseThrow().values();
    }

    private void play(String date, Integer song) {
        db.jdbcTemplate.update("INSERT INTO Play (play_date, song_id) VALUES (?, ?)", date, song);
    }
}
