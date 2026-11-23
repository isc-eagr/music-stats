package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The items ranked right around an artist, album or song inside one of its sidebar attributes
 * (gender, genre, country, release year, ...) or overall: the rows behind the hover tooltip on the detail page ranks.
 * Ordering matches the rank badges ({@code getAll*Rankings}, the per-attribute rank methods and the overall
 * position), so the
 * current item lands on the same number the badge shows.
 */
@Service
public class RankNeighborsService {

    /** Items shown on each side of the current one. */
    public static final int RADIUS = 5;

    /** Attribute for the overall standing among every artist, album or song (no partition, plays then id). */
    public static final String OVERALL = "overall";

    public record Neighbor(int rank, int id, String name, String artistName, long plays, String gender, boolean current) {}

    public record RankNeighbors(int total, List<Neighbor> items) {}

    /** Partition key per attribute. Attributes ranked with the first-play tiebreak are listed in FIRST_PLAY_TIEBREAK. */
    private static final Map<String, String> ARTIST_KEYS = Map.of(
            "gender", "a.gender_id",
            "genre", "a.genre_id",
            "subgenre", "a.subgenre_id",
            "ethnicity", "a.ethnicity_id",
            "language", "a.language_id",
            "country", "a.country",
            "spanishRap", spanishRapKey("a.genre_id", "a.language_id"));

    private static final Map<String, String> ALBUM_KEYS = Map.of(
            "gender", "ar.gender_id",
            "genre", "COALESCE(alb.override_genre_id, ar.genre_id)",
            "subgenre", "COALESCE(alb.override_subgenre_id, ar.subgenre_id)",
            "ethnicity", "ar.ethnicity_id",
            "language", "COALESCE(alb.override_language_id, ar.language_id)",
            "country", "ar.country",
            "spanishRap", spanishRapKey("COALESCE(alb.override_genre_id, ar.genre_id)",
                    "COALESCE(alb.override_language_id, ar.language_id)"),
            "releaseYear", "strftime('%Y', alb.release_date)",
            "artist", "alb.artist_id");

    private static final Map<String, String> SONG_KEYS = Map.of(
            "gender", "COALESCE(s.override_gender_id, ar.gender_id)",
            "genre", "COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id)",
            "subgenre", "COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id)",
            "ethnicity", "ar.ethnicity_id",
            "language", "COALESCE(s.override_language_id, alb.override_language_id, ar.language_id)",
            "country", "ar.country",
            "spanishRap", spanishRapKey("COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id)",
                    "COALESCE(s.override_language_id, alb.override_language_id, ar.language_id)"),
            "releaseYear", "strftime('%Y', COALESCE(s.release_date, alb.release_date))",
            "artist", "s.artist_id",
            "album", "s.album_id");

    private static final Set<String> FIRST_PLAY_TIEBREAK =
            Set.of("gender", "genre", "subgenre", "ethnicity", "language", "country");

    private final JdbcTemplate jdbcTemplate;

    public RankNeighborsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public RankNeighbors forArtist(int artistId, String attribute) {
        String key = keyFor(ARTIST_KEYS, attribute);
        if (key == null) return null;
        String base = """
                SELECT a.id AS id, a.name AS name, NULL AS artist_name, a.gender_id AS gender_id, %s AS rank_key,
                       COALESCE(SUM(pt.play_count), 0) AS play_count, MIN(pt.first_play) AS first_play
                FROM Artist a
                LEFT JOIN Song s ON s.artist_id = a.id
                LEFT JOIN play_totals pt ON pt.song_id = s.id
                GROUP BY a.id
                """.formatted(key);
        return query(base, attribute, artistId);
    }

    public RankNeighbors forAlbum(int albumId, String attribute) {
        String key = keyFor(ALBUM_KEYS, attribute);
        if (key == null) return null;
        String base = """
                SELECT alb.id AS id, alb.name AS name, ar.name AS artist_name, ar.gender_id AS gender_id, %s AS rank_key,
                       COALESCE(SUM(pt.play_count), 0) AS play_count, MIN(pt.first_play) AS first_play
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Song s ON s.album_id = alb.id
                LEFT JOIN play_totals pt ON pt.song_id = s.id
                GROUP BY alb.id
                """.formatted(key);
        return query(base, attribute, albumId);
    }

    public RankNeighbors forSong(int songId, String attribute) {
        String key = keyFor(SONG_KEYS, attribute);
        if (key == null) return null;
        String base = """
                SELECT s.id AS id, s.name AS name, ar.name AS artist_name,
                       COALESCE(s.override_gender_id, ar.gender_id) AS gender_id, %s AS rank_key,
                       COALESCE(pt.play_count, 0) AS play_count, pt.first_play AS first_play
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN play_totals pt ON pt.song_id = s.id
                """.formatted(key);
        return query(base, attribute, songId);
    }

    private RankNeighbors query(String base, String attribute, int targetId) {
        String order = FIRST_PLAY_TIEBREAK.contains(attribute)
                ? "play_count DESC, first_play ASC, id ASC"
                : "play_count DESC, id ASC";
        String sql = """
                WITH play_totals AS MATERIALIZED (
                    SELECT song_id, COUNT(*) AS play_count, MIN(play_date) AS first_play
                    FROM Play GROUP BY song_id
                ), base AS MATERIALIZED (
                %s
                ), ranked AS MATERIALIZED (
                    SELECT id, name, artist_name, gender_id, play_count,
                           ROW_NUMBER() OVER (ORDER BY %s) AS rnk,
                           COUNT(*) OVER () AS total
                    FROM base
                    WHERE rank_key IS NOT NULL
                      AND rank_key = (SELECT rank_key FROM base WHERE id = ?)
                ), target AS (
                    SELECT rnk FROM ranked WHERE id = ?
                )
                SELECT r.id, r.name, r.artist_name, r.play_count, r.rnk, r.total, g.name AS gender_name
                FROM ranked r
                CROSS JOIN target t
                LEFT JOIN Gender g ON g.id = r.gender_id
                WHERE r.rnk BETWEEN t.rnk - ? AND t.rnk + ?
                ORDER BY r.rnk
                """.formatted(base, order);
        List<Neighbor> items = new ArrayList<>();
        int[] total = {0};
        jdbcTemplate.query(sql, rs -> {
            total[0] = rs.getInt("total");
            int id = rs.getInt("id");
            items.add(new Neighbor(rs.getInt("rnk"), id, rs.getString("name"), rs.getString("artist_name"),
                    rs.getLong("play_count"), genderClass(rs.getString("gender_name")), id == targetId));
        }, targetId, targetId, RADIUS, RADIUS);
        return new RankNeighbors(total[0], items);
    }

    /** Same rule as the detail templates: female, male or other (no gender). */
    static String genderClass(String genderName) {
        if (genderName == null) return "other";
        String lower = genderName.toLowerCase(Locale.ROOT);
        if (lower.contains("female")) return "female";
        if (lower.contains("male")) return "male";
        return "other";
    }

    /** "overall" (the header standing badge) ranks every item together, so its key is the same for all of them. */
    private static String keyFor(Map<String, String> keys, String attribute) {
        return OVERALL.equals(attribute) ? "1" : keys.get(attribute);
    }

    private static String spanishRapKey(String genreExpr, String languageExpr) {
        return "CASE WHEN (SELECT name FROM Genre WHERE id = " + genreExpr + ") = 'Rap'"
                + " AND (SELECT name FROM Language WHERE id = " + languageExpr + ") = 'Spanish' THEN 1 END";
    }
}
