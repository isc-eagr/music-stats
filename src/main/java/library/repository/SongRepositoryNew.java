package library.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class SongRepositoryNew {
    
    private final JdbcTemplate jdbcTemplate;
    
    public SongRepositoryNew(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<Object[]> findSongsWithStats(String name, String artistName, String albumName,
                                              List<Integer> genreIds, String genreMode,
                                              List<Integer> subgenreIds, String subgenreMode,
                                              List<Integer> languageIds, String languageMode,
                                              List<Integer> genderIds, String genderMode,
                                              List<Integer> ethnicityIds, String ethnicityMode,
                                              List<String> countries, String countryMode,
                                              String sortBy, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                s.id,
                s.name,
                ar.name as artist_name,
                ar.id as artist_id,
                alb.name as album_name,
                s.album_id,
                COALESCE(g_override.name, g_album.name, g_artist.name) as genre_name,
                COALESCE(sg_override.name, sg_album.name, sg_artist.name) as subgenre_name,
                COALESCE(l_override.name, l_album.name, l_artist.name) as language_name,
                CAST(strftime('%Y', s.release_date) AS TEXT) as release_year,
                s.length_seconds,
                CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END as has_image,
                gender.name as gender_name,
                (SELECT COUNT(*) FROM Scrobble scr WHERE scr.song_id = s.id) as play_count,
                (s.length_seconds * (SELECT COUNT(*) FROM Scrobble scr WHERE scr.song_id = s.id)) as time_listened
            FROM Song s
            LEFT JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Gender gender ON ar.gender_id = gender.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Genre g_override ON s.override_genre_id = g_override.id
            LEFT JOIN Genre g_album ON alb.override_genre_id = g_album.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON s.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_album ON alb.override_subgenre_id = sg_album.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON s.override_language_id = l_override.id
            LEFT JOIN Language l_album ON alb.override_language_id = l_album.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND s.name LIKE ?");
            params.add("%" + name + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ar.name LIKE ?");
            params.add("%" + artistName + "%");
        }
        
        // Album name filter
        if (albumName != null && !albumName.trim().isEmpty()) {
            sql.append(" AND alb.name LIKE ?");
            params.add("%" + albumName + "%");
        }
        
        // Genre filter (song -> album -> artist inheritance)
        if (genreMode != null) {
            String placeholders = genreIds != null ? String.join(",", genreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genreMode)) {
                sql.append(" AND (s.override_genre_id IS NULL AND (alb.override_genre_id IS NULL AND ar.genre_id IS NULL))");
            } else if ("isnotnull".equals(genreMode)) {
                sql.append(" AND (s.override_genre_id IS NOT NULL OR alb.override_genre_id IS NOT NULL OR ar.genre_id IS NOT NULL)");
            } else if (genreIds != null && !genreIds.isEmpty()) {
                if ("includes".equals(genreMode)) {
                    sql.append(" AND ((s.override_genre_id IN (").append(placeholders).append(") ) OR (s.override_genre_id IS NULL AND ((alb.override_genre_id IN (").append(placeholders).append(") ) OR (alb.override_genre_id IS NULL AND ar.genre_id IN (").append(placeholders).append(") ))))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                } else if ("excludes".equals(genreMode)) {
                    sql.append(" AND ((s.override_genre_id NOT IN (").append(placeholders).append(") OR s.override_genre_id IS NULL) AND (alb.override_genre_id NOT IN (").append(placeholders).append(") OR alb.override_genre_id IS NULL) AND (ar.genre_id NOT IN (").append(placeholders).append(") OR ar.genre_id IS NULL))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                }
            }
        }
        
        // Subgenre filter (song -> album -> artist)
        if (subgenreMode != null) {
            String placeholders = subgenreIds != null ? String.join(",", subgenreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(subgenreMode)) {
                sql.append(" AND (s.override_subgenre_id IS NULL AND (alb.override_subgenre_id IS NULL AND ar.subgenre_id IS NULL))");
            } else if ("isnotnull".equals(subgenreMode)) {
                sql.append(" AND (s.override_subgenre_id IS NOT NULL OR alb.override_subgenre_id IS NOT NULL OR ar.subgenre_id IS NOT NULL)");
            } else if (subgenreIds != null && !subgenreIds.isEmpty()) {
                if ("includes".equals(subgenreMode)) {
                    sql.append(" AND ((s.override_subgenre_id IN (").append(placeholders).append(") ) OR (s.override_subgenre_id IS NULL AND ((alb.override_subgenre_id IN (").append(placeholders).append(") ) OR (alb.override_subgenre_id IS NULL AND ar.subgenre_id IN (").append(placeholders).append(") ))))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                } else if ("excludes".equals(subgenreMode)) {
                    sql.append(" AND ((s.override_subgenre_id NOT IN (").append(placeholders).append(") OR s.override_subgenre_id IS NULL) AND (alb.override_subgenre_id NOT IN (").append(placeholders).append(") OR alb.override_subgenre_id IS NULL) AND (ar.subgenre_id NOT IN (").append(placeholders).append(") OR ar.subgenre_id IS NULL))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                }
            }
        }
        
        // Language filter (song -> album -> artist)
        if (languageMode != null) {
            String placeholders = languageIds != null ? String.join(",", languageIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(languageMode)) {
                sql.append(" AND (s.override_language_id IS NULL AND (alb.override_language_id IS NULL AND ar.language_id IS NULL))");
            } else if ("isnotnull".equals(languageMode)) {
                sql.append(" AND (s.override_language_id IS NOT NULL OR alb.override_language_id IS NOT NULL OR ar.language_id IS NOT NULL)");
            } else if (languageIds != null && !languageIds.isEmpty()) {
                if ("includes".equals(languageMode)) {
                    sql.append(" AND ((s.override_language_id IN (").append(placeholders).append(") ) OR (s.override_language_id IS NULL AND ((alb.override_language_id IN (").append(placeholders).append(") ) OR (alb.override_language_id IS NULL AND ar.language_id IN (").append(placeholders).append(") ))))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                } else if ("excludes".equals(languageMode)) {
                    sql.append(" AND ((s.override_language_id NOT IN (").append(placeholders).append(") OR s.override_language_id IS NULL) AND (alb.override_language_id NOT IN (").append(placeholders).append(") OR alb.override_language_id IS NULL) AND (ar.language_id NOT IN (").append(placeholders).append(") OR ar.language_id IS NULL))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                }
            }
        }
        
        // Gender filter (song -> artist, no album override)
        if (genderMode != null) {
            String placeholders = genderIds != null ? String.join(",", genderIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genderMode)) {
                sql.append(" AND (s.override_gender_id IS NULL AND ar.gender_id IS NULL)");
            } else if ("isnotnull".equals(genderMode)) {
                sql.append(" AND (s.override_gender_id IS NOT NULL OR ar.gender_id IS NOT NULL)");
            } else if (genderIds != null && !genderIds.isEmpty()) {
                if ("includes".equals(genderMode)) {
                    sql.append(" AND ((s.override_gender_id IN (").append(placeholders).append(") ) OR (s.override_gender_id IS NULL AND ar.gender_id IN (").append(placeholders).append(") ))");
                    params.addAll(genderIds);
                    params.addAll(genderIds);
                } else if ("excludes".equals(genderMode)) {
                    sql.append(" AND ((s.override_gender_id NOT IN (").append(placeholders).append(") OR s.override_gender_id IS NULL) AND (ar.gender_id NOT IN (").append(placeholders).append(") OR ar.gender_id IS NULL))");
                    params.addAll(genderIds);
                    params.addAll(genderIds);
                }
            }
        }
        
        // Ethnicity filter (song -> artist)
        if (ethnicityMode != null) {
            String placeholders = ethnicityIds != null ? String.join(",", ethnicityIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(ethnicityMode)) {
                sql.append(" AND (s.override_ethnicity_id IS NULL AND ar.ethnicity_id IS NULL)");
            } else if ("isnotnull".equals(ethnicityMode)) {
                sql.append(" AND (s.override_ethnicity_id IS NOT NULL OR ar.ethnicity_id IS NOT NULL)");
            } else if (ethnicityIds != null && !ethnicityIds.isEmpty()) {
                if ("includes".equals(ethnicityMode)) {
                    sql.append(" AND ((s.override_ethnicity_id IN (").append(placeholders).append(") ) OR (s.override_ethnicity_id IS NULL AND ar.ethnicity_id IN (").append(placeholders).append(") ))");
                    params.addAll(ethnicityIds);
                    params.addAll(ethnicityIds);
                } else if ("excludes".equals(ethnicityMode)) {
                    sql.append(" AND ((s.override_ethnicity_id NOT IN (").append(placeholders).append(") OR s.override_ethnicity_id IS NULL) AND (ar.ethnicity_id NOT IN (").append(placeholders).append(") OR ar.ethnicity_id IS NULL))");
                    params.addAll(ethnicityIds);
                    params.addAll(ethnicityIds);
                }
            }
        }
        
        // Country filter (artist only)
        if (countryMode != null) {
            if ("isnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NULL");
            } else if ("isnotnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NOT NULL");
            } else if (countries != null && !countries.isEmpty()) {
                String placeholders = String.join(",", countries.stream().map(c -> "?").toList());
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
            case "artist" -> sql.append(" ORDER BY ar.name, s.name");
            case "album" -> sql.append(" ORDER BY alb.name, s.name");
            case "release_date" -> sql.append(" ORDER BY s.release_date DESC NULLS LAST, s.name");
            case "length" -> sql.append(" ORDER BY s.length_seconds DESC NULLS LAST, s.name");
            case "plays" -> sql.append(" ORDER BY play_count DESC, s.name");
            case "time" -> sql.append(" ORDER BY (s.length_seconds * play_count) DESC, s.name");
            default -> sql.append(" ORDER BY s.name");
        }
        
        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Object[] row = new Object[15];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getString("artist_name");
            row[3] = rs.getInt("artist_id");
            row[4] = rs.getString("album_name");
            row[5] = rs.getObject("album_id");
            row[6] = rs.getString("genre_name");
            row[7] = rs.getString("subgenre_name");
            row[8] = rs.getString("language_name");
            row[9] = rs.getString("release_year");
            row[10] = rs.getObject("length_seconds");
            row[11] = rs.getInt("has_image");
            row[12] = rs.getString("gender_name");
            row[13] = rs.getInt("play_count");
            row[14] = rs.getLong("time_listened");
            return row;
        });
    }
    
    public long countSongsWithFilters(String name, String artistName, String albumName,
                                      List<Integer> genreIds, String genreMode,
                                      List<Integer> subgenreIds, String subgenreMode,
                                      List<Integer> languageIds, String languageMode,
                                      List<Integer> genderIds, String genderMode,
                                      List<Integer> ethnicityIds, String ethnicityMode,
                                      List<String> countries, String countryMode) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*)
            FROM Song s
            LEFT JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND s.name LIKE ?");
            params.add("%" + name + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ar.name LIKE ?");
            params.add("%" + artistName + "%");
        }
        
        // Album name filter
        if (albumName != null && !albumName.trim().isEmpty()) {
            sql.append(" AND alb.name LIKE ?");
            params.add("%" + albumName + "%");
        }
        
        if (genreMode != null) {
            String placeholders = genreIds != null ? String.join(",", genreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genreMode)) {
                sql.append(" AND (s.override_genre_id IS NULL AND (alb.override_genre_id IS NULL AND ar.genre_id IS NULL))");
            } else if ("isnotnull".equals(genreMode)) {
                sql.append(" AND (s.override_genre_id IS NOT NULL OR alb.override_genre_id IS NOT NULL OR ar.genre_id IS NOT NULL)");
            } else if (genreIds != null && !genreIds.isEmpty()) {
                if ("includes".equals(genreMode)) {
                    sql.append(" AND ((s.override_genre_id IN (").append(placeholders).append(") ) OR (s.override_genre_id IS NULL AND ((alb.override_genre_id IN (").append(placeholders).append(") ) OR (alb.override_genre_id IS NULL AND ar.genre_id IN (").append(placeholders).append(") ))))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                } else if ("excludes".equals(genreMode)) {
                    sql.append(" AND ((s.override_genre_id NOT IN (").append(placeholders).append(") OR s.override_genre_id IS NULL) AND (alb.override_genre_id NOT IN (").append(placeholders).append(") OR alb.override_genre_id IS NULL) AND (ar.genre_id NOT IN (").append(placeholders).append(") OR ar.genre_id IS NULL))");
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                    params.addAll(genreIds);
                }
            }
        }
        
        if (subgenreMode != null) {
            String placeholders = subgenreIds != null ? String.join(",", subgenreIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(subgenreMode)) {
                sql.append(" AND (s.override_subgenre_id IS NULL AND (alb.override_subgenre_id IS NULL AND ar.subgenre_id IS NULL))");
            } else if ("isnotnull".equals(subgenreMode)) {
                sql.append(" AND (s.override_subgenre_id IS NOT NULL OR alb.override_subgenre_id IS NOT NULL OR ar.subgenre_id IS NOT NULL)");
            } else if (subgenreIds != null && !subgenreIds.isEmpty()) {
                if ("includes".equals(subgenreMode)) {
                    sql.append(" AND ((s.override_subgenre_id IN (").append(placeholders).append(") ) OR (s.override_subgenre_id IS NULL AND ((alb.override_subgenre_id IN (").append(placeholders).append(") ) OR (alb.override_subgenre_id IS NULL AND ar.subgenre_id IN (").append(placeholders).append(") ))))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                } else if ("excludes".equals(subgenreMode)) {
                    sql.append(" AND ((s.override_subgenre_id NOT IN (").append(placeholders).append(") OR s.override_subgenre_id IS NULL) AND (alb.override_subgenre_id NOT IN (").append(placeholders).append(") OR alb.override_subgenre_id IS NULL) AND (ar.subgenre_id NOT IN (").append(placeholders).append(") OR ar.subgenre_id IS NULL))");
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                    params.addAll(subgenreIds);
                }
            }
        }
        
        if (languageMode != null) {
            String placeholders = languageIds != null ? String.join(",", languageIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(languageMode)) {
                sql.append(" AND (s.override_language_id IS NULL AND (alb.override_language_id IS NULL AND ar.language_id IS NULL))");
            } else if ("isnotnull".equals(languageMode)) {
                sql.append(" AND (s.override_language_id IS NOT NULL OR alb.override_language_id IS NOT NULL OR ar.language_id IS NOT NULL)");
            } else if (languageIds != null && !languageIds.isEmpty()) {
                if ("includes".equals(languageMode)) {
                    sql.append(" AND ((s.override_language_id IN (").append(placeholders).append(") ) OR (s.override_language_id IS NULL AND ((alb.override_language_id IN (").append(placeholders).append(") ) OR (alb.override_language_id IS NULL AND ar.language_id IN (").append(placeholders).append(") ))))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                } else if ("excludes".equals(languageMode)) {
                    sql.append(" AND ((s.override_language_id NOT IN (").append(placeholders).append(") OR s.override_language_id IS NULL) AND (alb.override_language_id NOT IN (").append(placeholders).append(") OR alb.override_language_id IS NULL) AND (ar.language_id NOT IN (").append(placeholders).append(") OR ar.language_id IS NULL))");
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                    params.addAll(languageIds);
                }
            }
        }
        
        if (genderMode != null) {
            String placeholders = genderIds != null ? String.join(",", genderIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genderMode)) {
                sql.append(" AND (s.override_gender_id IS NULL AND ar.gender_id IS NULL)");
            } else if ("isnotnull".equals(genderMode)) {
                sql.append(" AND (s.override_gender_id IS NOT NULL OR ar.gender_id IS NOT NULL)");
            } else if (genderIds != null && !genderIds.isEmpty()) {
                if ("includes".equals(genderMode)) {
                    sql.append(" AND ((s.override_gender_id IN (").append(placeholders).append(") ) OR (s.override_gender_id IS NULL AND ar.gender_id IN (").append(placeholders).append(") ))");
                    params.addAll(genderIds);
                    params.addAll(genderIds);
                } else if ("excludes".equals(genderMode)) {
                    sql.append(" AND ((s.override_gender_id NOT IN (").append(placeholders).append(") OR s.override_gender_id IS NULL) AND (ar.gender_id NOT IN (").append(placeholders).append(") OR ar.gender_id IS NULL))");
                    params.addAll(genderIds);
                    params.addAll(genderIds);
                }
            }
        }
        
        if (ethnicityMode != null) {
            String placeholders = ethnicityIds != null ? String.join(",", ethnicityIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(ethnicityMode)) {
                sql.append(" AND (s.override_ethnicity_id IS NULL AND ar.ethnicity_id IS NULL)");
            } else if ("isnotnull".equals(ethnicityMode)) {
                sql.append(" AND (s.override_ethnicity_id IS NOT NULL OR ar.ethnicity_id IS NOT NULL)");
            } else if (ethnicityIds != null && !ethnicityIds.isEmpty()) {
                if ("includes".equals(ethnicityMode)) {
                    sql.append(" AND ((s.override_ethnicity_id IN (").append(placeholders).append(") ) OR (s.override_ethnicity_id IS NULL AND ar.ethnicity_id IN (").append(placeholders).append(") ))");
                    params.addAll(ethnicityIds);
                    params.addAll(ethnicityIds);
                } else if ("excludes".equals(ethnicityMode)) {
                    sql.append(" AND ((s.override_ethnicity_id NOT IN (").append(placeholders).append(") OR s.override_ethnicity_id IS NULL) AND (ar.ethnicity_id NOT IN (").append(placeholders).append(") OR ar.ethnicity_id IS NULL))");
                    params.addAll(ethnicityIds);
                    params.addAll(ethnicityIds);
                }
            }
        }
        
        if (countryMode != null) {
            if ("isnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NULL");
            } else if ("isnotnull".equals(countryMode)) {
                sql.append(" AND ar.country IS NOT NULL");
            } else if (countries != null && !countries.isEmpty()) {
                String placeholders = String.join(",", countries.stream().map(c -> "?").toList());
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
