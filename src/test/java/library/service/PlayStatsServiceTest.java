package library.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class PlayStatsServiceTest {

    private SingleConnectionDataSource dataSource;
    private JdbcTemplate jdbc;
    private PlayStatsService service;

    @BeforeEach
    void setUp() {
        dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE Gender (id INTEGER PRIMARY KEY, name TEXT)");
        jdbc.execute("CREATE TABLE Artist (id INTEGER PRIMARY KEY, gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, artist_id INTEGER, override_gender_id INTEGER)");
        jdbc.execute("CREATE TABLE Play (song_id INTEGER, play_date TEXT, account TEXT)");
        // Deliberately use IDs unrelated to gender names.
        jdbc.execute("INSERT INTO Gender VALUES (10, 'Male'), (20, 'Female'), (30, 'Other')");
        jdbc.execute("INSERT INTO Artist VALUES (1, 10), (2, 20), (3, 30), (4, NULL)");
        jdbc.execute("INSERT INTO Song VALUES (1, 1, NULL), (2, 2, NULL), (3, 2, 10), (4, 1, 20), (5, 3, NULL), (6, 4, NULL)");
        // UTC is already October 6, but Mexico City is still October 5.
        service = new PlayStatsService(jdbc,
                Clock.fixed(Instant.parse("2026-10-06T03:00:00Z"), ZoneOffset.UTC));
    }

    @AfterEach
    void tearDown() {
        dataSource.destroy();
    }

    @Test
    void usesMexicoCityDayWithInclusiveStartAndExclusiveEnd() {
        play(1, "2026-10-04 23:59", "first");
        play(1, "2026-10-05 00:00", "first");
        play(2, "2026-10-05 23:59:59", "second");
        play(2, "2026-10-06 00:00", "second");

        assertStats("50.00", "1 male out of 2");
    }

    @Test
    void honorsOverridesAndIncludesOtherUnknownAndUnmatchedPlaysInTotal() {
        play(1, "2026-10-05 10:00", "first");
        play(3, "2026-10-05 10:01", "second");
        play(4, "2026-10-05 10:02", "first");
        play(5, "2026-10-05 10:03", "second");
        play(6, "2026-10-05 10:04", "first");
        play(null, "2026-10-05 10:05", "second");

        assertStats("50.00", "2 male out of 6");
    }

    @Test
    void unmatchedPlaysContributeToTotalButNotMaleCount() {
        for (int i = 0; i < 32; i++) {
            play(1, "2026-10-05 12:00", "first");
        }
        play(null, "2026-10-05 12:01", "first");
        play(null, "2026-10-05 12:02", "first");

        assertStats("100.00", "32 male out of 34");
    }

    @Test
    void returnsZeroWhenTodayHasOnlyUnknownAndUnmatchedPlays() {
        play(6, "2026-10-05 12:00", "first");
        play(null, "2026-10-05 12:01", "first");

        assertStats("0.00", "0 male out of 2");
    }

    @Test
    void returnsExactCountsForMixedGenders() {
        play(1, "2026-10-05 10:00", "first");
        play(1, "2026-10-05 10:01", "first");
        play(2, "2026-10-05 10:02", "first");

        assertStats("66.67", "2 male out of 3");
    }

    @Test
    void returnsZeroCountsWhenThereAreNoPlaysToday() {
        play(1, "2026-10-04 12:00", "first");

        assertStats("0.00", "0 male out of 0");
    }

    @Test
    void returnsMatchingCountsWhenAllPlaysAreMale() {
        play(1, "2026-10-05 12:00", "first");

        assertStats("100.00", "1 male out of 1");
    }

    private void assertStats(String percentage, String text) {
        var stats = service.getTodayMalePlayStats();
        assertThat(stats.percentage()).isEqualTo(percentage);
        assertThat(stats.text()).isEqualTo(text);
    }

    private void play(Integer songId, String playDate, String account) {
        jdbc.update("INSERT INTO Play (song_id, play_date, account) VALUES (?, ?, ?)", songId, playDate, account);
    }
}
