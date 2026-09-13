package library.service;

import library.dto.BillboardHot100OverviewRowDTO;
import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartSongOverviewRowDTO;
import library.dto.PcOverviewRowDTO;
import library.entity.TrlDebut;
import library.repository.LookupRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Applies catalog lookup filters to overview rows using the same override inheritance as catalog lists. */
@Service
public class OverviewFacetService {

    private static final List<String> FACET_NAMES = List.of(
            "gender", "genre", "subgenre", "country", "ethnicity", "language", "tag");

    private final JdbcTemplate jdbcTemplate;
    private final LookupRepository lookupRepository;

    public OverviewFacetService(JdbcTemplate jdbcTemplate, LookupRepository lookupRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.lookupRepository = lookupRepository;
    }

    public record Option(String value, String label, Integer genderId) {
        public Option(String value, String label) {
            this(value, label, null);
        }
    }

    public Map<String, List<Option>> getOptions() {
        Map<String, List<Option>> options = new LinkedHashMap<>();
        options.put("gender", lookupRepository.getAllGenders().entrySet().stream()
                .map(entry -> new Option(String.valueOf(entry.getKey()), entry.getValue(), entry.getKey()))
                .toList());
        options.put("genre", toOptions(lookupRepository.getAllGenres()));
        options.put("subgenre", toOptions(lookupRepository.getAllSubGenres()));
        options.put("country", jdbcTemplate.query(
                "SELECT DISTINCT TRIM(country) AS value FROM Artist WHERE country IS NOT NULL AND TRIM(country) <> '' ORDER BY value COLLATE NOCASE",
                (rs, rowNum) -> new Option(rs.getString("value"), rs.getString("value"))));
        options.put("ethnicity", toOptions(lookupRepository.getAllEthnicities()));
        options.put("language", toOptions(lookupRepository.getAllLanguages()));
        options.put("tag", toOptions(lookupRepository.getAllTags()));
        return options;
    }

    public <T> List<T> filter(String tab, List<T> rows, MultiValueMap<String, String> params) {
        boolean includeFeatured = "song".equals(tab) && Boolean.parseBoolean(first(params, "includeFeaturedSongs"));
        boolean hasFacetFilter = FACET_NAMES.stream().anyMatch(name -> hasFilter(params, name));
        boolean hasFeaturedArtistFilter = includeFeatured && hasSelectedValues(params, "artist");
        if ((!hasFacetFilter && !hasFeaturedArtistFilter) || rows.isEmpty()) {
            return rows;
        }

        Snapshot snapshot = loadSnapshot(tab, rows);
        List<T> result = new ArrayList<>();
        for (T row : rows) {
            Facets facets = facetsFor(tab, row, snapshot);
            if (matchesFacets(facets, params) && matchesArtistWithFeatured(facets, params, hasFeaturedArtistFilter)) {
                result.add(row);
            }
        }
        return result;
    }

    private boolean matchesFacets(Facets facets, MultiValueMap<String, String> params) {
        for (String name : FACET_NAMES) {
            List<String> selected = clean(params == null ? null : params.get(name));
            String mode = normalizeMode(first(params, name + "Mode"));
            if (selected.isEmpty() && !"isnull".equals(mode) && !"isnotnull".equals(mode)) {
                continue;
            }
            Object actual = switch (name) {
                case "gender" -> facets == null ? null : facets.genderId;
                case "genre" -> facets == null ? null : facets.genreId;
                case "subgenre" -> facets == null ? null : facets.subgenreId;
                case "country" -> facets == null ? null : facets.country;
                case "ethnicity" -> facets == null ? null : facets.ethnicityId;
                case "language" -> facets == null ? null : facets.languageId;
                case "tag" -> facets == null ? Set.of() : facets.tagIds;
                default -> null;
            };
            if (!matches(actual, selected, mode)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesArtistWithFeatured(Facets facets, MultiValueMap<String, String> params, boolean enabled) {
        if (!enabled) {
            return true;
        }
        Set<String> selected = new HashSet<>(clean(params.get("artist")));
        boolean matched = facets != null && (selected.contains(String.valueOf(facets.artistId))
                || facets.featuredArtistIds.stream().map(String::valueOf).anyMatch(selected::contains));
        return "excludes".equals(normalizeMode(first(params, "artistMode"))) ? !matched : matched;
    }

    private boolean matches(Object actual, List<String> selected, String mode) {
        boolean empty = actual == null
                || actual instanceof String value && value.isBlank()
                || actual instanceof Set<?> values && values.isEmpty();
        if ("isnull".equals(mode)) return empty;
        if ("isnotnull".equals(mode)) return !empty;
        if (empty) return "excludes".equals(mode);

        boolean found;
        if (actual instanceof Set<?> values) {
            found = values.stream().map(String::valueOf).anyMatch(selected::contains);
        } else {
            String actualValue = String.valueOf(actual);
            found = selected.stream().anyMatch(value -> value.equalsIgnoreCase(actualValue));
        }
        return "excludes".equals(mode) ? !found : found;
    }

    private Facets facetsFor(String tab, Object row, Snapshot snapshot) {
        return switch (tab) {
            case "album" -> snapshot.albums.get(albumId(row));
            case "artist" -> snapshot.artists.get(artistId(row));
            default -> snapshot.songs.get(songId(row));
        };
    }

    private Integer songId(Object row) {
        if (row instanceof ChartSongOverviewRowDTO value) return value.getSongId();
        if (row instanceof PcOverviewRowDTO value) return value.getSongId();
        if (row instanceof BillboardHot100OverviewRowDTO value) return value.getSongId();
        if (row instanceof TrlDebut value) return value.getSongId();
        return null;
    }

    private Integer albumId(Object row) {
        return row instanceof ChartAlbumOverviewRowDTO value ? value.getAlbumId() : null;
    }

    private Integer artistId(Object row) {
        return row instanceof ChartArtistOverviewRowDTO value ? value.getResolvedArtistId() : null;
    }

    private Snapshot loadSnapshot(String tab, List<?> rows) {
        // IDs come from typed database DTOs, never raw request text. Restrict this
        // request's metadata to the charted entities in the active tab.
        String ids = rows.stream().map(row -> switch (tab) {
            case "album" -> albumId(row);
            case "artist" -> artistId(row);
            default -> songId(row);
        }).filter(Objects::nonNull).distinct().map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(","));
        Map<Integer, Facets> artists = new HashMap<>();
        Map<Integer, Facets> albums = new HashMap<>();
        Map<Integer, Facets> songs = new HashMap<>();
        if (ids.isEmpty()) return new Snapshot(songs, albums, artists);
        if ("artist".equals(tab)) {
        jdbcTemplate.query("SELECT id, gender_id, genre_id, subgenre_id, country, ethnicity_id, language_id FROM Artist WHERE id IN (" + ids + ")", rs -> {
            int id = rs.getInt("id");
            artists.put(id, new Facets(id, nullableInt(rs, "gender_id"), nullableInt(rs, "genre_id"),
                    nullableInt(rs, "subgenre_id"), rs.getString("country"), nullableInt(rs, "ethnicity_id"),
                    nullableInt(rs, "language_id")));
        });
        loadTags("ArtistTag", "artist_id", artists);
        } else if ("album".equals(tab)) {
        jdbcTemplate.query("""
                SELECT al.id, al.artist_id,
                       a.gender_id,
                       COALESCE(al.override_genre_id, a.genre_id) AS genre_id,
                       COALESCE(al.override_subgenre_id, a.subgenre_id) AS subgenre_id,
                       a.country,
                       a.ethnicity_id,
                       COALESCE(al.override_language_id, a.language_id) AS language_id
                FROM Album al
                INNER JOIN Artist a ON a.id = al.artist_id
                """ + " WHERE al.id IN (" + ids + ")", rs -> {
            int id = rs.getInt("id");
            albums.put(id, new Facets(nullableInt(rs, "artist_id"), nullableInt(rs, "gender_id"),
                    nullableInt(rs, "genre_id"), nullableInt(rs, "subgenre_id"), rs.getString("country"),
                    nullableInt(rs, "ethnicity_id"), nullableInt(rs, "language_id")));
        });
        loadTags("AlbumTag", "album_id", albums);
        } else {
        jdbcTemplate.query("""
                SELECT s.id, s.artist_id,
                       COALESCE(s.override_gender_id, a.gender_id) AS gender_id,
                       COALESCE(s.override_genre_id, al.override_genre_id, a.genre_id) AS genre_id,
                       COALESCE(s.override_subgenre_id, al.override_subgenre_id, a.subgenre_id) AS subgenre_id,
                       a.country,
                       COALESCE(s.override_ethnicity_id, a.ethnicity_id) AS ethnicity_id,
                       COALESCE(s.override_language_id, al.override_language_id, a.language_id) AS language_id
                FROM Song s
                INNER JOIN Artist a ON a.id = s.artist_id
                LEFT JOIN Album al ON al.id = s.album_id
                """ + " WHERE s.id IN (" + ids + ")", rs -> {
            int id = rs.getInt("id");
            songs.put(id, new Facets(nullableInt(rs, "artist_id"), nullableInt(rs, "gender_id"),
                    nullableInt(rs, "genre_id"), nullableInt(rs, "subgenre_id"), rs.getString("country"),
                    nullableInt(rs, "ethnicity_id"), nullableInt(rs, "language_id")));
        });

        loadTags("SongTag", "song_id", songs);
        jdbcTemplate.query("SELECT song_id, artist_id FROM SongFeaturedArtist WHERE song_id IN (" + ids + ")", rs -> {
            Facets facets = songs.get(rs.getInt("song_id"));
            if (facets != null) facets.featuredArtistIds.add(rs.getInt("artist_id"));
        });
        }
        return new Snapshot(songs, albums, artists);
    }

    private void loadTags(String table, String idColumn, Map<Integer, Facets> facetsById) {
        if (facetsById.isEmpty()) return;
        String ids = facetsById.keySet().stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(","));
        jdbcTemplate.query("SELECT " + idColumn + ", tag_id FROM " + table + " WHERE " + idColumn + " IN (" + ids + ")", rs -> {
            Facets facets = facetsById.get(rs.getInt(idColumn));
            if (facets != null) facets.tagIds.add(rs.getInt("tag_id"));
        });
    }

    private List<Option> toOptions(Map<Integer, String> values) {
        return values.entrySet().stream().map(entry -> new Option(String.valueOf(entry.getKey()), entry.getValue())).toList();
    }

    private static Integer nullableInt(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static boolean hasFilter(MultiValueMap<String, String> params, String name) {
        return hasSelectedValues(params, name) || List.of("isnull", "isnotnull").contains(normalizeMode(first(params, name + "Mode")));
    }

    private static boolean hasSelectedValues(MultiValueMap<String, String> params, String name) {
        return !clean(params == null ? null : params.get(name)).isEmpty();
    }

    private static String first(MultiValueMap<String, String> params, String key) {
        return params == null ? null : params.getFirst(key);
    }

    private static String normalizeMode(String mode) {
        return mode == null ? "includes" : mode.toLowerCase(Locale.ROOT);
    }

    private static List<String> clean(List<String> values) {
        return values == null ? List.of() : values.stream().filter(Objects::nonNull)
                .map(String::trim).filter(value -> !value.isBlank()).toList();
    }

    private static final class Facets {
        private final Integer artistId;
        private final Integer genderId;
        private final Integer genreId;
        private final Integer subgenreId;
        private final String country;
        private final Integer ethnicityId;
        private final Integer languageId;
        private final Set<Integer> tagIds = new HashSet<>();
        private final Set<Integer> featuredArtistIds = new HashSet<>();

        private Facets(Integer artistId, Integer genderId, Integer genreId, Integer subgenreId,
                       String country, Integer ethnicityId, Integer languageId) {
            this.artistId = artistId;
            this.genderId = genderId;
            this.genreId = genreId;
            this.subgenreId = subgenreId;
            this.country = country;
            this.ethnicityId = ethnicityId;
            this.languageId = languageId;
        }
    }

    private record Snapshot(Map<Integer, Facets> songs, Map<Integer, Facets> albums, Map<Integer, Facets> artists) {
    }
}
