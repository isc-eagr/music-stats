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
                                               String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                               String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                               String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                               String sortBy, String sortDir, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                a.id,
                a.name,
                ar.name as artist_name,
                ar.id as artist_id,
                COALESCE(a.override_genre_id, ar.genre_id) as genre_id,
                COALESCE(g_override.name, g_artist.name) as genre_name,
                COALESCE(a.override_subgenre_id, ar.subgenre_id) as subgenre_id,
                COALESCE(sg_override.name, sg_artist.name) as subgenre_name,
                COALESCE(a.override_language_id, ar.language_id) as language_id,
                COALESCE(l_override.name, l_artist.name) as language_name,
                ar.ethnicity_id as ethnicity_id,
                e.name as ethnicity_name,
                CAST(strftime('%Y', a.release_date) AS TEXT) as release_year,
                (SELECT COUNT(*) FROM Song s WHERE s.album_id = a.id) as song_count,
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                gender.name as gender_name,
                (SELECT COUNT(*) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id)) as play_count,
                (SELECT COALESCE(SUM(s.length_seconds * scr_count.play_count), 0) 
                 FROM Song s 
                 LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble GROUP BY song_id) scr_count ON s.id = scr_count.song_id 
                 WHERE s.album_id = a.id) as time_listened,
                (SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id)) as first_listened,
                (SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id)) as last_listened,
                ar.country as country
            FROM Album a
            LEFT JOIN Artist ar ON a.artist_id = ar.id
            LEFT JOIN Gender gender ON ar.gender_id = gender.id
            LEFT JOIN Genre g_override ON a.override_genre_id = g_override.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON a.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON a.override_language_id = l_override.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity e ON ar.ethnicity_id = e.id
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
        
        // Release Date filter
        if (releaseDateMode != null && !releaseDateMode.isEmpty()) {
            switch (releaseDateMode) {
                case "exact":
                    if (releaseDate != null && !releaseDate.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) = ?");
                        params.add(releaseDate);
                    }
                    break;
                case "gte":
                    if (releaseDate != null && !releaseDate.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) >= ?");
                        params.add(releaseDate);
                    }
                    break;
                case "lte":
                    if (releaseDate != null && !releaseDate.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) <= ?");
                        params.add(releaseDate);
                    }
                    break;
                case "between":
                    if (releaseDateFrom != null && !releaseDateFrom.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) >= ?");
                        params.add(releaseDateFrom);
                    }
                    if (releaseDateTo != null && !releaseDateTo.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) <= ?");
                        params.add(releaseDateTo);
                    }
                    break;
            }
        }
        
        // First Listened Date filter
        if (firstListenedDateMode != null && !firstListenedDateMode.isEmpty()) {
            String subquery = "(SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
            switch (firstListenedDateMode) {
                case "exact":
                    if (firstListenedDate != null && !firstListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") = ?");
                        params.add(firstListenedDate);
                    }
                    break;
                case "gte":
                    if (firstListenedDate != null && !firstListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(firstListenedDate);
                    }
                    break;
                case "lte":
                    if (firstListenedDate != null && !firstListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(firstListenedDate);
                    }
                    break;
                case "between":
                    if (firstListenedDateFrom != null && !firstListenedDateFrom.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(firstListenedDateFrom);
                    }
                    if (firstListenedDateTo != null && !firstListenedDateTo.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(firstListenedDateTo);
                    }
                    break;
            }
        }
        
        // Last Listened Date filter
        if (lastListenedDateMode != null && !lastListenedDateMode.isEmpty()) {
            String subquery = "(SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
            switch (lastListenedDateMode) {
                case "exact":
                    if (lastListenedDate != null && !lastListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") = ?");
                        params.add(lastListenedDate);
                    }
                    break;
                case "gte":
                    if (lastListenedDate != null && !lastListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(lastListenedDate);
                    }
                    break;
                case "lte":
                    if (lastListenedDate != null && !lastListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(lastListenedDate);
                    }
                    break;
                case "between":
                    if (lastListenedDateFrom != null && !lastListenedDateFrom.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(lastListenedDateFrom);
                    }
                    if (lastListenedDateTo != null && !lastListenedDateTo.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(lastListenedDateTo);
                    }
                    break;
            }
        }
        
        // Sorting
        String direction = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        switch (sortBy != null ? sortBy : "name") {
            case "artist" -> sql.append(" ORDER BY ar.name " + direction + ", a.name");
            case "release_date" -> sql.append(" ORDER BY a.release_date " + direction + " NULLS LAST, a.name");
            case "song_count" -> sql.append(" ORDER BY song_count " + direction + ", a.name");
            case "plays" -> sql.append(" ORDER BY play_count " + direction + ", a.name");
            case "time" -> sql.append(" ORDER BY (SELECT COALESCE(SUM(s.length_seconds * scr_count.play_count), 0) FROM Song s LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble GROUP BY song_id) scr_count ON s.id = scr_count.song_id WHERE s.album_id = a.id) " + direction + ", a.name");
            case "first_listened" -> sql.append(" ORDER BY first_listened " + direction + " NULLS LAST, a.name");
            case "last_listened" -> sql.append(" ORDER BY last_listened " + direction + " NULLS LAST, a.name");
            default -> sql.append(" ORDER BY a.name " + direction);
        }
        
        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Object[] row = new Object[21];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getString("artist_name");
            row[3] = rs.getInt("artist_id");
            row[4] = rs.getObject("genre_id");
            row[5] = rs.getString("genre_name");
            row[6] = rs.getObject("subgenre_id");
            row[7] = rs.getString("subgenre_name");
            row[8] = rs.getObject("language_id");
            row[9] = rs.getString("language_name");
            row[10] = rs.getObject("ethnicity_id");
            row[11] = rs.getString("ethnicity_name");
            row[12] = rs.getString("release_year");
            row[13] = rs.getInt("song_count");
            row[14] = rs.getInt("has_image");
            row[15] = rs.getString("gender_name");
            row[16] = rs.getInt("play_count");
            row[17] = rs.getLong("time_listened");
            row[18] = rs.getString("first_listened");
            row[19] = rs.getString("last_listened");
            row[20] = rs.getString("country");
            return row;
        });
    }
    
    public long countAlbumsWithFilters(String name, String artistName,
                                       List<Integer> genreIds, String genreMode,
                                       List<Integer> subgenreIds, String subgenreMode,
                                       List<Integer> languageIds, String languageMode,
                                       List<Integer> genderIds, String genderMode,
                                       List<Integer> ethnicityIds, String ethnicityMode,
                                       List<String> countries, String countryMode,
                                       String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                       String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                       String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode) {
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
        
        // Release Date filter
        if (releaseDateMode != null && !releaseDateMode.isEmpty()) {
            switch (releaseDateMode) {
                case "exact":
                    if (releaseDate != null && !releaseDate.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) = ?");
                        params.add(releaseDate);
                    }
                    break;
                case "gte":
                    if (releaseDate != null && !releaseDate.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) >= ?");
                        params.add(releaseDate);
                    }
                    break;
                case "lte":
                    if (releaseDate != null && !releaseDate.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) <= ?");
                        params.add(releaseDate);
                    }
                    break;
                case "between":
                    if (releaseDateFrom != null && !releaseDateFrom.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) >= ?");
                        params.add(releaseDateFrom);
                    }
                    if (releaseDateTo != null && !releaseDateTo.isEmpty()) {
                        sql.append(" AND DATE(a.release_date) <= ?");
                        params.add(releaseDateTo);
                    }
                    break;
            }
        }
        
        // First Listened Date filter
        if (firstListenedDateMode != null && !firstListenedDateMode.isEmpty()) {
            String subquery = "(SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
            switch (firstListenedDateMode) {
                case "exact":
                    if (firstListenedDate != null && !firstListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") = ?");
                        params.add(firstListenedDate);
                    }
                    break;
                case "gte":
                    if (firstListenedDate != null && !firstListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(firstListenedDate);
                    }
                    break;
                case "lte":
                    if (firstListenedDate != null && !firstListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(firstListenedDate);
                    }
                    break;
                case "between":
                    if (firstListenedDateFrom != null && !firstListenedDateFrom.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(firstListenedDateFrom);
                    }
                    if (firstListenedDateTo != null && !firstListenedDateTo.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(firstListenedDateTo);
                    }
                    break;
            }
        }
        
        // Last Listened Date filter
        if (lastListenedDateMode != null && !lastListenedDateMode.isEmpty()) {
            String subquery = "(SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
            switch (lastListenedDateMode) {
                case "exact":
                    if (lastListenedDate != null && !lastListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") = ?");
                        params.add(lastListenedDate);
                    }
                    break;
                case "gte":
                    if (lastListenedDate != null && !lastListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(lastListenedDate);
                    }
                    break;
                case "lte":
                    if (lastListenedDate != null && !lastListenedDate.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(lastListenedDate);
                    }
                    break;
                case "between":
                    if (lastListenedDateFrom != null && !lastListenedDateFrom.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") >= ?");
                        params.add(lastListenedDateFrom);
                    }
                    if (lastListenedDateTo != null && !lastListenedDateTo.isEmpty()) {
                        sql.append(" AND DATE(").append(subquery).append(") <= ?");
                        params.add(lastListenedDateTo);
                    }
                    break;
            }
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), params.toArray(), Long.class);
        return count != null ? count : 0;
    }
}
