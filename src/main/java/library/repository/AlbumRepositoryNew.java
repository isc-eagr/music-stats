package library.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class AlbumRepositoryNew {
    
    private final JdbcTemplate jdbcTemplate;
    
    public AlbumRepositoryNew(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<Object[]> findAlbumsWithStats(String name, String artistName,
                                               List<Integer> genreIds, String genreMode,
                                               List<Integer> subgenreIds, String subgenreMode,
                                               List<Integer> languageIds, String languageMode,
                                               List<Integer> genderIds, String genderMode,
                                               List<Integer> ethnicityIds, String ethnicityMode,
                                               List<String> countries, String countryMode,
                                               String sortBy, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                a.id,
                a.name,
                ar.name as artist_name,
                ar.id as artist_id,
                COALESCE(g_override.name, g_artist.name) as genre_name,
                COALESCE(sg_override.name, sg_artist.name) as subgenre_name,
                COALESCE(l_override.name, l_artist.name) as language_name,
                CAST(strftime('%Y', a.release_date) AS TEXT) as release_year,
                (SELECT COUNT(*) FROM Song s WHERE s.album_id = a.id) as song_count,
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                gender.name as gender_name,
                (SELECT COUNT(*) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id)) as play_count,
                (SELECT COALESCE(SUM(s.length_seconds * scr_count.play_count), 0) 
                 FROM Song s 
                 LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble GROUP BY song_id) scr_count ON s.id = scr_count.song_id 
                 WHERE s.album_id = a.id) as time_listened
            FROM Album a
            LEFT JOIN Artist ar ON a.artist_id = ar.id
            LEFT JOIN Gender gender ON ar.gender_id = gender.id
            LEFT JOIN Genre g_override ON a.override_genre_id = g_override.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON a.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON a.override_language_id = l_override.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND a.name LIKE ?");
            params.add("%" + name + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ar.name LIKE ?");
            params.add("%" + artistName + "%");
        }
        
        if (genreMode != null) {
            String placeholders = genreIds != null ? String.join(",", genreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genreMode)) {
                sql.append(" AND ((a.override_genre_id IS NULL) AND (ar.genre_id IS NULL))");
            } else if ("isnotnull".equals(genreMode)) {
                sql.append(" AND (a.override_genre_id IS NOT NULL OR ar.genre_id IS NOT NULL)");
            } else if (genreIds != null && !genreIds.isEmpty()) {
                if ("includes".equals(genreMode)) {
                    sql.append(" AND ((a.override_genre_id IN (").append(placeholders).append(") ) OR (a.override_genre_id IS NULL AND ar.genre_id IN (").append(placeholders).append(") ))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                } else if ("excludes".equals(genreMode)) {
                    sql.append(" AND ((a.override_genre_id NOT IN (").append(placeholders).append(") OR a.override_genre_id IS NULL) AND (ar.genre_id NOT IN (").append(placeholders).append(") OR ar.genre_id IS NULL))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                }
            }
        }
        
        if (subgenreMode != null) {
            String placeholders = subgenreIds != null ? String.join(",", subgenreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(subgenreMode)) {
                sql.append(" AND ((a.override_subgenre_id IS NULL) AND (ar.subgenre_id IS NULL))");
            } else if ("isnotnull".equals(subgenreMode)) {
                sql.append(" AND (a.override_subgenre_id IS NOT NULL OR ar.subgenre_id IS NOT NULL)");
            } else if (subgenreIds != null && !subgenreIds.isEmpty()) {
                if ("includes".equals(subgenreMode)) {
                    sql.append(" AND ((a.override_subgenre_id IN (").append(placeholders).append(") ) OR (a.override_subgenre_id IS NULL AND ar.subgenre_id IN (").append(placeholders).append(") ))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                } else if ("excludes".equals(subgenreMode)) {
                    sql.append(" AND ((a.override_subgenre_id NOT IN (").append(placeholders).append(") OR a.override_subgenre_id IS NULL) AND (ar.subgenre_id NOT IN (").append(placeholders).append(") OR ar.subgenre_id IS NULL))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                }
            }
        }
        
        // Language filter with override inheritance: album -> artist
        if (languageMode != null) {
            String placeholders = languageIds != null ? String.join(",", languageIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(languageMode)) {
                sql.append(" AND ((a.override_language_id IS NULL) AND (ar.language_id IS NULL))");
            } else if ("isnotnull".equals(languageMode)) {
                sql.append(" AND (a.override_language_id IS NOT NULL OR ar.language_id IS NOT NULL)");
            } else if (languageIds != null && !languageIds.isEmpty()) {
                if ("includes".equals(languageMode)) {
                    sql.append(" AND ((a.override_language_id IN (").append(placeholders).append(") ) OR (a.override_language_id IS NULL AND ar.language_id IN (").append(placeholders).append(") ))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                } else if ("excludes".equals(languageMode)) {
                    sql.append(" AND ((a.override_language_id NOT IN (").append(placeholders).append(") OR a.override_language_id IS NULL) AND (ar.language_id NOT IN (").append(placeholders).append(") OR ar.language_id IS NULL))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                }
            }
        }
        
        // Gender filter - from artist only (no album override for gender)
        if (genderMode != null) {
            String placeholders = genderIds != null ? String.join(",", genderIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genderMode)) {
                sql.append(" AND ar.gender_id IS NULL");
            } else if ("isnotnull".equals(genderMode)) {
                sql.append(" AND ar.gender_id IS NOT NULL");
            } else if (genderIds != null && !genderIds.isEmpty()) {
                if ("includes".equals(genderMode)) {
                    sql.append(" AND ar.gender_id IN (").append(placeholders).append(")");
                    params.addAll(genderIds);
                } else if ("excludes".equals(genderMode)) {
                    sql.append(" AND (ar.gender_id NOT IN (").append(placeholders).append(") OR ar.gender_id IS NULL)");
                    params.addAll(genderIds);
                }
            }
        }
        
        // Ethnicity filter - from artist only (no album override for ethnicity)
        if (ethnicityMode != null) {
            String placeholders = ethnicityIds != null ? String.join(",", ethnicityIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(ethnicityMode)) {
                sql.append(" AND ar.ethnicity_id IS NULL");
            } else if ("isnotnull".equals(ethnicityMode)) {
                sql.append(" AND ar.ethnicity_id IS NOT NULL");
            } else if (ethnicityIds != null && !ethnicityIds.isEmpty()) {
                if ("includes".equals(ethnicityMode)) {
                    sql.append(" AND ar.ethnicity_id IN (").append(placeholders).append(")");
                    params.addAll(ethnicityIds);
                } else if ("excludes".equals(ethnicityMode)) {
                    sql.append(" AND (ar.ethnicity_id NOT IN (").append(placeholders).append(") OR ar.ethnicity_id IS NULL)");
                    params.addAll(ethnicityIds);
                }
            }
        }
        
        // Country filter - always from artist (no overrides for country)
        if (countryMode != null) {
            String placeholders = countries != null ? String.join(",", countries.stream().map(c -> "?").toList()) : null;
            if ("isnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NULL");
            } else if ("isnotnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NOT NULL");
            } else if (countries != null && !countries.isEmpty()) {
                if ("includes".equals(countryMode)) {
                    sql.append(" AND ar.country IN (").append(placeholders).append(")");
                    params.addAll(countries);
                } else if ("excludes".equals(countryMode)) {
                    sql.append(" AND (ar.country NOT IN (").append(placeholders).append(") OR ar.country IS NULL)");
                    params.addAll(countries);
                }
            }
        }
        
        // Sorting
        switch (sortBy != null ? sortBy : "name") {
            case "artist" -> sql.append(" ORDER BY ar.name, a.name");
            case "release_date" -> sql.append(" ORDER BY a.release_date DESC NULLS LAST, a.name");
            case "song_count" -> sql.append(" ORDER BY song_count DESC, a.name");
            case "plays" -> sql.append(" ORDER BY play_count DESC, a.name");
            case "time" -> sql.append(" ORDER BY (SELECT COALESCE(SUM(s.length_seconds * scr_count.play_count), 0) FROM Song s LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble GROUP BY song_id) scr_count ON s.id = scr_count.song_id WHERE s.album_id = a.id) DESC, a.name");
            default -> sql.append(" ORDER BY a.name");
        }
        
        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Object[] row = new Object[13];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getString("artist_name");
            row[3] = rs.getInt("artist_id");
            row[4] = rs.getString("genre_name");
            row[5] = rs.getString("subgenre_name");
            row[6] = rs.getString("language_name");
            row[7] = rs.getString("release_year");
            row[8] = rs.getInt("song_count");
            row[9] = rs.getInt("has_image");
            row[10] = rs.getString("gender_name");
            row[11] = rs.getInt("play_count");
            row[12] = rs.getLong("time_listened");
            return row;
        });
    }
    
    public long countAlbumsWithFilters(String name, String artistName,
                                       List<Integer> genreIds, String genreMode,
                                       List<Integer> subgenreIds, String subgenreMode,
                                       List<Integer> languageIds, String languageMode,
                                       List<Integer> genderIds, String genderMode,
                                       List<Integer> ethnicityIds, String ethnicityMode,
                                       List<String> countries, String countryMode) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*)
            FROM Album a
            LEFT JOIN Artist ar ON a.artist_id = ar.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND a.name LIKE ?");
            params.add("%" + name + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ar.name LIKE ?");
            params.add("%" + artistName + "%");
        }
        
        if (genreMode != null) {
            String placeholders = genreIds != null ? String.join(",", genreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genreMode)) {
                sql.append(" AND ((a.override_genre_id IS NULL) AND (ar.genre_id IS NULL))");
            } else if ("isnotnull".equals(genreMode)) {
                sql.append(" AND (a.override_genre_id IS NOT NULL OR ar.genre_id IS NOT NULL)");
            } else if (genreIds != null && !genreIds.isEmpty()) {
                if ("includes".equals(genreMode)) {
                    sql.append(" AND ((a.override_genre_id IN (").append(placeholders).append(") ) OR (a.override_genre_id IS NULL AND ar.genre_id IN (").append(placeholders).append(") ))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                } else if ("excludes".equals(genreMode)) {
                    sql.append(" AND ((a.override_genre_id NOT IN (").append(placeholders).append(") OR a.override_genre_id IS NULL) AND (ar.genre_id NOT IN (").append(placeholders).append(") OR ar.genre_id IS NULL))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                }
            }
        }
        
        if (subgenreMode != null) {
            String placeholders = subgenreIds != null ? String.join(",", subgenreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(subgenreMode)) {
                sql.append(" AND ((a.override_subgenre_id IS NULL) AND (ar.subgenre_id IS NULL))");
            } else if ("isnotnull".equals(subgenreMode)) {
                sql.append(" AND (a.override_subgenre_id IS NOT NULL OR ar.subgenre_id IS NOT NULL)");
            } else if (subgenreIds != null && !subgenreIds.isEmpty()) {
                if ("includes".equals(subgenreMode)) {
                    sql.append(" AND ((a.override_subgenre_id IN (").append(placeholders).append(") ) OR (a.override_subgenre_id IS NULL AND ar.subgenre_id IN (").append(placeholders).append(") ))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                } else if ("excludes".equals(subgenreMode)) {
                    sql.append(" AND ((a.override_subgenre_id NOT IN (").append(placeholders).append(") OR a.override_subgenre_id IS NULL) AND (ar.subgenre_id NOT IN (").append(placeholders).append(") OR ar.subgenre_id IS NULL))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                }
            }
        }
        
        if (languageMode != null) {
            String placeholders = languageIds != null ? String.join(",", languageIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(languageMode)) {
                sql.append(" AND ((a.override_language_id IS NULL) AND (ar.language_id IS NULL))");
            } else if ("isnotnull".equals(languageMode)) {
                sql.append(" AND (a.override_language_id IS NOT NULL OR ar.language_id IS NOT NULL)");
            } else if (languageIds != null && !languageIds.isEmpty()) {
                if ("includes".equals(languageMode)) {
                    sql.append(" AND ((a.override_language_id IN (").append(placeholders).append(") ) OR (a.override_language_id IS NULL AND ar.language_id IN (").append(placeholders).append(") ))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                } else if ("excludes".equals(languageMode)) {
                    sql.append(" AND ((a.override_language_id NOT IN (").append(placeholders).append(") OR a.override_language_id IS NULL) AND (ar.language_id NOT IN (").append(placeholders).append(") OR ar.language_id IS NULL))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                }
            }
        }
        
        if (genderMode != null) {
            String placeholders = genderIds != null ? String.join(",", genderIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genderMode)) {
                sql.append(" AND ar.gender_id IS NULL");
            } else if ("isnotnull".equals(genderMode)) {
                sql.append(" AND ar.gender_id IS NOT NULL");
            } else if (genderIds != null && !genderIds.isEmpty()) {
                if ("includes".equals(genderMode)) {
                    sql.append(" AND ar.gender_id IN (").append(placeholders).append(")");
                    params.addAll(genderIds);
                } else if ("excludes".equals(genderMode)) {
                    sql.append(" AND (ar.gender_id NOT IN (").append(placeholders).append(") OR ar.gender_id IS NULL)");
                    params.addAll(genderIds);
                }
            }
        }
        
        if (ethnicityMode != null) {
            String placeholders = ethnicityIds != null ? String.join(",", ethnicityIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(ethnicityMode)) {
                sql.append(" AND ar.ethnicity_id IS NULL");
            } else if ("isnotnull".equals(ethnicityMode)) {
                sql.append(" AND ar.ethnicity_id IS NOT NULL");
            } else if (ethnicityIds != null && !ethnicityIds.isEmpty()) {
                if ("includes".equals(ethnicityMode)) {
                    sql.append(" AND ar.ethnicity_id IN (").append(placeholders).append(")");
                    params.addAll(ethnicityIds);
                } else if ("excludes".equals(ethnicityMode)) {
                    sql.append(" AND (ar.ethnicity_id NOT IN (").append(placeholders).append(") OR ar.ethnicity_id IS NULL)");
                    params.addAll(ethnicityIds);
                }
            }
        }
        
        if (countryMode != null) {
            String placeholders = countries != null ? String.join(",", countries.stream().map(c -> "?").toList()) : null;
            if ("isnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NULL");
            } else if ("isnotnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NOT NULL");
            } else if (countries != null && !countries.isEmpty()) {
                if ("includes".equals(countryMode)) {
                    sql.append(" AND ar.country IN (").append(placeholders).append(")");
                    params.addAll(countries);
                } else if ("excludes".equals(countryMode)) {
                    sql.append(" AND (ar.country NOT IN (").append(placeholders).append(") OR ar.country IS NULL)");
                    params.addAll(countries);
                }
            }
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), params.toArray(), Long.class);
        return count != null ? count : 0;
    }
}
