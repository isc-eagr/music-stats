package library.util;

import library.dto.FeaturedArtistRef;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class ChartAggregationUtils {

    private ChartAggregationUtils() {
    }

    /**
     * Map a lower-cased gender name to the CSS gender class used for styling.
     */
    public static String genderClass(String genderName) {
        if (genderName == null) {
            return null;
        }
        if (genderName.contains("female")) {
            return "gender-female";
        }
        if (genderName.contains("male")) {
            return "gender-male";
        }
        return null;
    }

    /**
     * Load featured artists for the given songs, keyed by song id (artists ordered by name).
     */
    public static Map<Integer, List<FeaturedArtistRef>> loadFeaturedArtistRefs(JdbcTemplate jdbcTemplate, List<Integer> songIds) {
        if (songIds == null || songIds.isEmpty()) {
            return Map.of();
        }
        String placeholders = songIds.stream().map(ignored -> "?").collect(Collectors.joining(","));
        String sql = """
            SELECT sfa.song_id,
                   a.id AS artist_id,
                   a.name AS artist_name,
                   LOWER(g.name) AS gender_name,
                   CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END AS artist_has_image
            FROM SongFeaturedArtist sfa
            INNER JOIN Artist a ON a.id = sfa.artist_id
            LEFT JOIN Gender g ON g.id = a.gender_id
            WHERE sfa.song_id IN (%s)
            ORDER BY sfa.song_id ASC, a.name COLLATE NOCASE ASC
            """.formatted(placeholders);

        Map<Integer, List<FeaturedArtistRef>> refsBySongId = new LinkedHashMap<>();
        jdbcTemplate.query(sql, (rs) -> {
            FeaturedArtistRef ref = new FeaturedArtistRef(
                rs.getInt("artist_id"),
                rs.getString("artist_name"),
                genderClass(rs.getString("gender_name")),
                rs.getInt("artist_has_image") == 1
            );
            refsBySongId.computeIfAbsent(rs.getInt("song_id"), ignored -> new ArrayList<>()).add(ref);
        }, songIds.toArray());
        return refsBySongId;
    }

    public static String normalizeKeyPart(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    public static String pickPreferredDisplayValue(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank()) {
            return candidateValue;
        }

        int ignoreCase = candidateValue.compareToIgnoreCase(currentValue);
        if (ignoreCase < 0) {
            return candidateValue;
        }
        if (ignoreCase == 0 && candidateValue.compareTo(currentValue) < 0) {
            return candidateValue;
        }
        return currentValue;
    }

    public static String minDate(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank() || candidateValue.compareTo(currentValue) < 0) {
            return candidateValue;
        }
        return currentValue;
    }

    public static String maxDate(String currentValue, String candidateValue) {
        if (candidateValue == null || candidateValue.isBlank()) {
            return currentValue;
        }
        if (currentValue == null || currentValue.isBlank() || candidateValue.compareTo(currentValue) > 0) {
            return candidateValue;
        }
        return currentValue;
    }

    public static String safeLower(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }
}