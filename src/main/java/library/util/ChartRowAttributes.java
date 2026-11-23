package library.util;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Genre, ethnicity, language and country for chart rows, used by the breakdown tiles on the weekly chart
 * and the Hot 100 / TRL / Vato's Cuntdown recap pages. Overrides cascade in SQL:
 * songs use the song override, then the album's (genre, language), then the artist's; albums use the album
 * override, then the artist's. Ethnicity can be overridden per song only; country is always the artist's.
 */
public final class ChartRowAttributes {

    /** {genre, ethnicity, language, country}; any value may be null. */
    public record Attributes(String genre, String ethnicity, String language, String country) {
    }

    private ChartRowAttributes() {
    }

    public static Map<Integer, Attributes> forSongs(JdbcTemplate jdbcTemplate, Collection<Integer> songIds) {
        return lookup(jdbcTemplate, songIds, """
                SELECT s.id as item_id,
                       (SELECT g.name FROM Genre g WHERE g.id = COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id)) as genre_name,
                       (SELECT e.name FROM Ethnicity e WHERE e.id = COALESCE(s.override_ethnicity_id, ar.ethnicity_id)) as ethnicity_name,
                       (SELECT l.name FROM Language l WHERE l.id = COALESCE(s.override_language_id, al.override_language_id, ar.language_id)) as language_name,
                       ar.country as country_name
                FROM Song s
                INNER JOIN Artist ar ON ar.id = s.artist_id
                LEFT JOIN Album al ON al.id = s.album_id
                WHERE s.id IN (%s)
                """);
    }

    public static Map<Integer, Attributes> forAlbums(JdbcTemplate jdbcTemplate, Collection<Integer> albumIds) {
        return lookup(jdbcTemplate, albumIds, """
                SELECT al.id as item_id,
                       (SELECT g.name FROM Genre g WHERE g.id = COALESCE(al.override_genre_id, ar.genre_id)) as genre_name,
                       (SELECT e.name FROM Ethnicity e WHERE e.id = ar.ethnicity_id) as ethnicity_name,
                       (SELECT l.name FROM Language l WHERE l.id = COALESCE(al.override_language_id, ar.language_id)) as language_name,
                       ar.country as country_name
                FROM Album al
                INNER JOIN Artist ar ON ar.id = al.artist_id
                WHERE al.id IN (%s)
                """);
    }

    public static Map<Integer, Attributes> forArtists(JdbcTemplate jdbcTemplate, Collection<Integer> artistIds) {
        return lookup(jdbcTemplate, artistIds, """
                SELECT ar.id as item_id,
                       (SELECT g.name FROM Genre g WHERE g.id = ar.genre_id) as genre_name,
                       (SELECT e.name FROM Ethnicity e WHERE e.id = ar.ethnicity_id) as ethnicity_name,
                       (SELECT l.name FROM Language l WHERE l.id = ar.language_id) as language_name,
                       ar.country as country_name
                FROM Artist ar
                WHERE ar.id IN (%s)
                """);
    }

    /**
     * Adds genreName, ethnicityName, languageName and countryName to recap rows (maps with songId / artistId).
     * A row linked to a library song uses the song's values; a row only matched to an artist uses the artist's.
     */
    public static void applyToRecapRows(JdbcTemplate jdbcTemplate, List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        Map<Integer, Attributes> songs = forSongs(jdbcTemplate, rows.stream().map(row -> asInteger(row.get("songId"))).toList());
        Map<Integer, Attributes> artists = forArtists(jdbcTemplate, rows.stream()
                .filter(row -> row.get("songId") == null)
                .map(row -> asInteger(row.get("artistId")))
                .toList());
        for (Map<String, Object> row : rows) {
            Integer songId = asInteger(row.get("songId"));
            Integer artistId = asInteger(row.get("artistId"));
            // Rows matched to neither a song nor an artist get nulls (never look up a null key)
            Attributes attributes = songId != null ? songs.get(songId) : artistId != null ? artists.get(artistId) : null;
            row.put("genreName", attributes != null ? attributes.genre() : null);
            row.put("ethnicityName", attributes != null ? attributes.ethnicity() : null);
            row.put("languageName", attributes != null ? attributes.language() : null);
            row.put("countryName", attributes != null ? attributes.country() : null);
        }
    }

    private static Integer asInteger(Object value) {
        return value instanceof Number number ? number.intValue() : null;
    }

    private static Map<Integer, Attributes> lookup(JdbcTemplate jdbcTemplate, Collection<Integer> ids, String sqlTemplate) {
        List<Integer> distinct = ids.stream().filter(Objects::nonNull).distinct().toList();
        if (distinct.isEmpty()) {
            return new HashMap<>();
        }
        String sql = sqlTemplate.formatted(String.join(",", Collections.nCopies(distinct.size(), "?")));
        Map<Integer, Attributes> result = new HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            result.put(rs.getInt("item_id"), new Attributes(rs.getString("genre_name"), rs.getString("ethnicity_name"),
                    rs.getString("language_name"), rs.getString("country_name")));
        }, distinct.toArray());
        return result;
    }
}
