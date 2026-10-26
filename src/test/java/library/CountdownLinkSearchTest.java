package library;

import library.service.SongService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class CountdownLinkSearchTest {

    @ParameterizedTest
    @ValueSource(strings = {"pc", "trl", "billboard-hot100"})
    void libraryLinkSearchRequestsUnlimitedResults(String page) throws IOException {
        String html = Files.readString(Path.of("src/main/resources/templates/misc", page + ".html"));

        assertThat(html)
                .contains("fetch('/songs/api/search?song=' + enc + '&artist=' + enc + '&limit=0')")
                .doesNotContain("&limit=50");
    }

    @Test
    void unlimitedSearchReturnsEveryTitleAndArtistMatchBeyondFifty() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            for (int i = 1; i <= 60; i++) {
                db.jdbcTemplate.update("INSERT INTO Song (artist_id, name) VALUES (1, ?)",
                        "Beautiful " + String.format("%02d", i));
            }
            db.jdbcTemplate.update("UPDATE Artist SET name = 'Beautiful Artist' WHERE id = 3");
            SongService service = new SongService(db.songRepository, null, null, db.jdbcTemplate,
                    null, null, null);

            var results = service.searchSongs("Beautiful", "Beautiful", 0);

            assertThat(results).hasSize(61);
            assertThat(results).extracting(song -> song.get("name"))
                    .contains("Beautiful 01", "Beautiful 60", "Quiet Track")
                    .doesNotContain("Unknown Silence");
        }
    }
}
