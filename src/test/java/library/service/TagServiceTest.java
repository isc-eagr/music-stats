package library.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TagServiceTest {

    @Test
    void addSongTagsAppliesEverySelectedTagWithoutReplacingExistingTags() {
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite::memory:");
        dataSource.setSuppressClose(true);

        try {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            jdbcTemplate.execute("CREATE TABLE Tag (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
            jdbcTemplate.execute("""
                    CREATE TABLE SongTag (
                        song_id INTEGER NOT NULL,
                        tag_id INTEGER NOT NULL,
                        PRIMARY KEY (song_id, tag_id)
                    )
                    """);
            jdbcTemplate.update("INSERT INTO Tag (id, name) VALUES (10, 'Favorite'), (20, 'Workout')");
            jdbcTemplate.update("INSERT INTO SongTag (song_id, tag_id) VALUES (1, 10)");

            TagService service = new TagService(jdbcTemplate);

            int appliedCount = service.addSongTags(List.of(1, 2, 2), List.of(10, 20, 20));

            List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                    SELECT song_id, tag_id
                    FROM SongTag
                    ORDER BY song_id, tag_id
                    """);
            assertThat(appliedCount).isEqualTo(3);
            assertThat(rows).containsExactly(
                    Map.of("song_id", 1, "tag_id", 10),
                    Map.of("song_id", 1, "tag_id", 20),
                    Map.of("song_id", 2, "tag_id", 10),
                    Map.of("song_id", 2, "tag_id", 20)
            );
        } finally {
            dataSource.destroy();
        }
    }
}
