package library;

import com.zaxxer.hikari.HikariDataSource;
import library.dto.TimeframeCardDTO;
import library.service.TimeframeService;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class TimeframeConcurrentDetailsTest {

    @Test
    void pooledConnectionsLoadThePageDetailsConcurrentlyWithIdenticalCards() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create();
             HikariDataSource pool = new HikariDataSource()) {
            pool.setJdbcUrl(((SingleConnectionDataSource) db.jdbcTemplate.getDataSource()).getUrl());
            pool.setMaximumPoolSize(4);
            ThreadRecordingJdbcTemplate pooledJdbc = new ThreadRecordingJdbcTemplate(pool);

            TimeframeService sequential = new TimeframeService(db.jdbcTemplate);
            TimeframeService concurrent = new TimeframeService(pooledJdbc);

            for (String periodType : List.of("days", "weeks", "months", "seasons", "years", "decades")) {
                for (String sortBy : List.of("plays", "maledays")) {
                    assertThat(cards(concurrent, periodType, sortBy))
                            .as("%s sorted by %s", periodType, sortBy)
                            .isEqualTo(cards(sequential, periodType, sortBy));
                }
            }
            assertThat(pooledJdbc.threadNames).contains("timeframe-page-details");
        }
    }

    private static List<String> cards(TimeframeService service, String periodType, String sortBy) {
        return service.getTimeframeCardsWithCount(periodType,
                        null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, sortBy, "desc", 0, 50)
                .getTimeframes().stream()
                .map(TimeframeConcurrentDetailsTest::describe)
                .toList();
    }

    private static String describe(TimeframeCardDTO card) {
        return String.join("|", String.valueOf(card.getPeriodKey()), String.valueOf(card.getPlayCount()),
                String.valueOf(card.getTopArtistName()), String.valueOf(card.getTopAlbumName()),
                String.valueOf(card.getTopSongName()), String.valueOf(card.getWinningGenderName()),
                String.valueOf(card.getWinningGenreName()), String.valueOf(card.getWinningLanguageName()),
                String.valueOf(card.getWinningEthnicityName()), String.valueOf(card.getWinningCountry()),
                String.valueOf(card.getMaleDays()), String.valueOf(card.getTotalDays()));
    }

    private static final class ThreadRecordingJdbcTemplate extends JdbcTemplate {
        final Set<String> threadNames = ConcurrentHashMap.newKeySet();

        ThreadRecordingJdbcTemplate(DataSource dataSource) {
            super(dataSource);
        }

        @Override
        public <T> T query(String sql, PreparedStatementSetter setter, ResultSetExtractor<T> extractor) {
            threadNames.add(Thread.currentThread().getName());
            return super.query(sql, setter, extractor);
        }
    }
}
