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
                                              String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                              String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                              String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                              String sortBy, String sortDirection, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                s.id,
                s.name,
                ar.name as artist_name,
                ar.id as artist_id,
                alb.name as album_name,
                s.album_id,
                COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) as genre_id,
                COALESCE(g_override.name, g_album.name, g_artist.name) as genre_name,
                COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) as subgenre_id,
                COALESCE(sg_override.name, sg_album.name, sg_artist.name) as subgenre_name,
                COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) as language_id,
                COALESCE(l_override.name, l_album.name, l_artist.name) as language_name,
                COALESCE(s.override_ethnicity_id, ar.ethnicity_id) as ethnicity_id,
                e.name as ethnicity_name,
                CAST(strftime('%Y', COALESCE(s.release_date, alb.release_date)) AS TEXT) as release_year,
                s.length_seconds,
                CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END as has_image,
                gender.name as gender_name,
                (SELECT COUNT(*) FROM Scrobble scr WHERE scr.song_id = s.id) as play_count,
                (s.length_seconds * (SELECT COUNT(*) FROM Scrobble scr WHERE scr.song_id = s.id)) as time_listened,
                (SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id) as first_listened,
                (SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id) as last_listened,
                ar.country as country
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
            LEFT JOIN Ethnicity e ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = e.id
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
        
        // Release date filter
        if (releaseDate != null && !releaseDate.trim().isEmpty() && releaseDateMode != null) {
            switch (releaseDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) = DATE(?)");
                    params.add(releaseDate);
                }
                case "gt" -> {
                    sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) > DATE(?)");
                    params.add(releaseDate);
                }
                case "lt" -> {
                    sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) < DATE(?)");
                    params.add(releaseDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) >= DATE(?)");
                    params.add(releaseDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) <= DATE(?)");
                    params.add(releaseDate);
                }
            }
        }
        
        // Between date filter
        if ("between".equals(releaseDateMode) && releaseDateFrom != null && !releaseDateFrom.trim().isEmpty()
                && releaseDateTo != null && !releaseDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) >= DATE(?) AND DATE(COALESCE(s.release_date, alb.release_date)) <= DATE(?)");
            params.add(releaseDateFrom);
            params.add(releaseDateTo);
        }
        
        // First listened date filter
        if (firstListenedDate != null && !firstListenedDate.trim().isEmpty() && firstListenedDateMode != null) {
            switch (firstListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) = DATE(?)");
                    params.add(firstListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?)");
                    params.add(firstListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
                    params.add(firstListenedDate);
                }
            }
        }
        
        // First listened between filter
        if ("between".equals(firstListenedDateMode) && firstListenedDateFrom != null && !firstListenedDateFrom.trim().isEmpty()
                && firstListenedDateTo != null && !firstListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
            params.add(firstListenedDateFrom);
            params.add(firstListenedDateTo);
        }
        
        // Last listened date filter
        if (lastListenedDate != null && !lastListenedDate.trim().isEmpty() && lastListenedDateMode != null) {
            switch (lastListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) = DATE(?)");
                    params.add(lastListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?)");
                    params.add(lastListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
                    params.add(lastListenedDate);
                }
            }
        }
        
        // Last listened between filter
        if ("between".equals(lastListenedDateMode) && lastListenedDateFrom != null && !lastListenedDateFrom.trim().isEmpty()
                && lastListenedDateTo != null && !lastListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
            params.add(lastListenedDateFrom);
            params.add(lastListenedDateTo);
        }
        
        // Determine sort direction
        String dir = "desc".equalsIgnoreCase(sortDirection) ? "DESC" : "ASC";
        String nullsOrder = "desc".equalsIgnoreCase(sortDirection) ? "NULLS LAST" : "NULLS FIRST";
        
        // Sorting with direction
        switch (sortBy != null ? sortBy : "name") {
            case "artist" -> sql.append(" ORDER BY ar.name ").append(dir).append(", s.name");
            case "album" -> sql.append(" ORDER BY alb.name ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "release_date" -> sql.append(" ORDER BY s.release_date ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "length" -> sql.append(" ORDER BY s.length_seconds ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "plays" -> sql.append(" ORDER BY play_count ").append(dir).append(", s.name");
            case "time" -> sql.append(" ORDER BY (s.length_seconds * play_count) ").append(dir).append(", s.name");
            case "first_listened" -> sql.append(" ORDER BY first_listened ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "last_listened" -> sql.append(" ORDER BY last_listened ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            default -> sql.append(" ORDER BY s.name ").append(dir);
        }
        
        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Object[] row = new Object[23];
            row[0] = rs.getInt("id");
            row[1] = rs.getString("name");
            row[2] = rs.getString("artist_name");
            row[3] = rs.getInt("artist_id");
            row[4] = rs.getString("album_name");
            row[5] = rs.getObject("album_id");
            row[6] = rs.getObject("genre_id");
            row[7] = rs.getString("genre_name");
            row[8] = rs.getObject("subgenre_id");
            row[9] = rs.getString("subgenre_name");
            row[10] = rs.getObject("language_id");
            row[11] = rs.getString("language_name");
            row[12] = rs.getObject("ethnicity_id");
            row[13] = rs.getString("ethnicity_name");
            row[14] = rs.getString("release_year");
            row[15] = rs.getObject("length_seconds");
            row[16] = rs.getInt("has_image");
            row[17] = rs.getString("gender_name");
            row[18] = rs.getInt("play_count");
            row[19] = rs.getLong("time_listened");
            row[20] = rs.getString("first_listened");
            row[21] = rs.getString("last_listened");
            row[22] = rs.getString("country");
            return row;
        });
    }
    
    public long countSongsWithFilters(String name, String artistName, String albumName,
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
        
        // Release date filter
        if (releaseDateMode != null) {
            switch (releaseDateMode) {
                case "exact" -> {
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) = DATE(?)");
                        params.add(releaseDate);
                    }
                }
                case "gt" -> {
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) > DATE(?)");
                        params.add(releaseDate);
                    }
                }
                case "lt" -> {
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) < DATE(?)");
                        params.add(releaseDate);
                    }
                }
                case "gte" -> {
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) >= DATE(?)");
                        params.add(releaseDate);
                    }
                }
                case "lte" -> {
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) <= DATE(?)");
                        params.add(releaseDate);
                    }
                }
                case "between" -> {
                    if (releaseDateFrom != null && !releaseDateFrom.trim().isEmpty() && 
                        releaseDateTo != null && !releaseDateTo.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) >= DATE(?) AND DATE(COALESCE(s.release_date, alb.release_date)) <= DATE(?)");
                        params.add(releaseDateFrom);
                        params.add(releaseDateTo);
                    }
                }
            }
        }
        
        // First listened date filter
        if (firstListenedDate != null && !firstListenedDate.trim().isEmpty() && firstListenedDateMode != null) {
            switch (firstListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) = DATE(?)");
                    params.add(firstListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?)");
                    params.add(firstListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
                    params.add(firstListenedDate);
                }
            }
        }
        
        // First listened between filter
        if ("between".equals(firstListenedDateMode) && firstListenedDateFrom != null && !firstListenedDateFrom.trim().isEmpty()
                && firstListenedDateTo != null && !firstListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MIN(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
            params.add(firstListenedDateFrom);
            params.add(firstListenedDateTo);
        }
        
        // Last listened date filter
        if (lastListenedDate != null && !lastListenedDate.trim().isEmpty() && lastListenedDateMode != null) {
            switch (lastListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) = DATE(?)");
                    params.add(lastListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?)");
                    params.add(lastListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
                    params.add(lastListenedDate);
                }
            }
        }
        
        // Last listened between filter
        if ("between".equals(lastListenedDateMode) && lastListenedDateFrom != null && !lastListenedDateFrom.trim().isEmpty()
                && lastListenedDateTo != null && !lastListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MAX(scrobble_date) FROM Scrobble WHERE song_id = s.id)) <= DATE(?)");
            params.add(lastListenedDateFrom);
            params.add(lastListenedDateTo);
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), params.toArray(), Long.class);
        return count != null ? count : 0;
    }
    
    // Get filtered chart data for gender breakdown
    public java.util.Map<String, Object> getFilteredChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        // Build the filter clause that will be reused
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        // Get plays by gender
        data.put("playsByGender", getPlaysByGenderFiltered(filterClause.toString(), params));
        
        // Get songs by gender
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params));
        
        // Get artists by gender (from filtered songs)
        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params));
        
        // Get albums by gender (from filtered songs)
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params));
        
        // Get plays by genre with gender breakdown (from filtered songs)
        data.put("playsByGenreAndGender", getPlaysByGenreAndGenderFiltered(filterClause.toString(), params));
        
        // Get plays by ethnicity with gender breakdown
        data.put("playsByEthnicityAndGender", getPlaysByEthnicityAndGenderFiltered(filterClause.toString(), params));
        
        // Get plays by language with gender breakdown
        data.put("playsByLanguageAndGender", getPlaysByLanguageAndGenderFiltered(filterClause.toString(), params));
        
        // Get plays by year with gender breakdown
        data.put("playsByYearAndGender", getPlaysByYearAndGenderFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private void buildFilterClause(StringBuilder sql, java.util.List<Object> params,
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND s.name LIKE ?");
            params.add("%" + name + "%");
        }
        
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ar.name LIKE ?");
            params.add("%" + artistName + "%");
        }
        
        if (albumName != null && !albumName.trim().isEmpty()) {
            sql.append(" AND alb.name LIKE ?");
            params.add("%" + albumName + "%");
        }
        
        // Genre filter
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
        
        // Subgenre filter
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
        
        // Language filter
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
        
        // Gender filter
        if (genderMode != null) {
            String placeholders = genderIds != null ? String.join(",", genderIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(genderMode)) {
                sql.append(" AND (s.override_gender_id IS NULL AND ar.gender_id IS NULL)");
            } else if ("isnotnull".equals(genderMode)) {
                sql.append(" AND (s.override_gender_id IS NOT NULL OR ar.gender_id IS NOT NULL)");
            } else if (genderIds != null && !genderIds.isEmpty()) {
                if ("includes".equals(genderMode)) {
                    sql.append(" AND ((s.override_gender_id IN (").append(placeholders).append(")) OR (s.override_gender_id IS NULL AND ar.gender_id IN (").append(placeholders).append(")))");
                    params.addAll(genderIds);
                    params.addAll(genderIds);
                } else if ("excludes".equals(genderMode)) {
                    sql.append(" AND ((s.override_gender_id NOT IN (").append(placeholders).append(") OR s.override_gender_id IS NULL) AND (ar.gender_id NOT IN (").append(placeholders).append(") OR ar.gender_id IS NULL))");
                    params.addAll(genderIds);
                    params.addAll(genderIds);
                }
            }
        }
        
        // Ethnicity filter
        if (ethnicityMode != null) {
            String placeholders = ethnicityIds != null ? String.join(",", ethnicityIds.stream().map(id -> "?").toList()) : null;
            if ("isnull".equals(ethnicityMode)) {
                sql.append(" AND (s.override_ethnicity_id IS NULL AND ar.ethnicity_id IS NULL)");
            } else if ("isnotnull".equals(ethnicityMode)) {
                sql.append(" AND (s.override_ethnicity_id IS NOT NULL OR ar.ethnicity_id IS NOT NULL)");
            } else if (ethnicityIds != null && !ethnicityIds.isEmpty()) {
                if ("includes".equals(ethnicityMode)) {
                    sql.append(" AND ((s.override_ethnicity_id IN (").append(placeholders).append(")) OR (s.override_ethnicity_id IS NULL AND ar.ethnicity_id IN (").append(placeholders).append(")))");
                    params.addAll(ethnicityIds);
                    params.addAll(ethnicityIds);
                } else if ("excludes".equals(ethnicityMode)) {
                    sql.append(" AND ((s.override_ethnicity_id NOT IN (").append(placeholders).append(") OR s.override_ethnicity_id IS NULL) AND (ar.ethnicity_id NOT IN (").append(placeholders).append(") OR ar.ethnicity_id IS NULL))");
                    params.addAll(ethnicityIds);
                    params.addAll(ethnicityIds);
                }
            }
        }
        
        // Country filter
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
        
        // Release date filter
        if (releaseDateMode != null) {
            switch (releaseDateMode) {
                case "exact" -> { 
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) = DATE(?)"); 
                        params.add(releaseDate); 
                    }
                }
                case "gt" -> { 
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) > DATE(?)"); 
                        params.add(releaseDate); 
                    }
                }
                case "lt" -> { 
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) < DATE(?)"); 
                        params.add(releaseDate); 
                    }
                }
                case "gte" -> { 
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) >= DATE(?)"); 
                        params.add(releaseDate); 
                    }
                }
                case "lte" -> { 
                    if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) <= DATE(?)"); 
                        params.add(releaseDate); 
                    }
                }
                case "between" -> {
                    if (releaseDateFrom != null && !releaseDateFrom.trim().isEmpty() && 
                        releaseDateTo != null && !releaseDateTo.trim().isEmpty()) {
                        sql.append(" AND DATE(COALESCE(s.release_date, alb.release_date)) >= DATE(?) AND DATE(COALESCE(s.release_date, alb.release_date)) <= DATE(?)");
                        params.add(releaseDateFrom);
                        params.add(releaseDateTo);
                    }
                }
            }
        }
    }
    
    private java.util.Map<String, Long> getPlaysByGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(*) as play_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY gender
            """;
        
        java.util.Map<String, Long> result = new java.util.HashMap<>();
        result.put("male", 0L);
        result.put("female", 0L);
        result.put("other", 0L);
        
        jdbcTemplate.query(sql, filterParams.toArray(), rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("play_count");
            result.put(gender, count);
        });
        
        return result;
    }
    
    private java.util.Map<String, Long> getSongsByGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT s.id) as song_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY gender
            """;
        
        java.util.Map<String, Long> result = new java.util.HashMap<>();
        result.put("male", 0L);
        result.put("female", 0L);
        result.put("other", 0L);
        
        jdbcTemplate.query(sql, filterParams.toArray(), rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("song_count");
            result.put(gender, count);
        });
        
        return result;
    }
    
    private java.util.Map<String, Long> getArtistsByGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT ar.id) as artist_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY gender
            """;
        
        java.util.Map<String, Long> result = new java.util.HashMap<>();
        result.put("male", 0L);
        result.put("female", 0L);
        result.put("other", 0L);
        
        jdbcTemplate.query(sql, filterParams.toArray(), rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("artist_count");
            result.put(gender, count);
        });
        
        return result;
    }
    
    private java.util.Map<String, Long> getAlbumsByGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT alb.id) as album_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            GROUP BY gender
            """;
        
        java.util.Map<String, Long> result = new java.util.HashMap<>();
        result.put("male", 0L);
        result.put("female", 0L);
        result.put("other", 0L);
        
        jdbcTemplate.query(sql, filterParams.toArray(), rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("album_count");
            result.put(gender, count);
        });
        
        return result;
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByGenreAndGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(gr.name, 'Unknown') as genre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) = gr.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(gr.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            LIMIT 10
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByEthnicityAndGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(e.name, 'Unknown') as ethnicity_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Ethnicity e ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = e.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(e.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            LIMIT 10
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByLanguageAndGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(l.name, 'Unknown') as language_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Language l ON COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) = l.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(l.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            LIMIT 10
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByYearAndGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                strftime('%Y', scr.scrobble_date) as year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE scr.scrobble_date IS NOT NULL """ + " " + filterClause + """
            GROUP BY strftime('%Y', scr.scrobble_date)
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY year DESC
            LIMIT 10
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // ==================== NEW TAB-SPECIFIC CHART DATA METHODS ====================
    
    // Get General tab chart data (5 pie charts: Artists, Albums, Songs, Plays, Listening Time by gender)
    public java.util.Map<String, Object> getGeneralChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params));
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params));
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params));
        data.put("playsByGender", getPlaysByGenderFiltered(filterClause.toString(), params));
        data.put("listeningTimeByGender", getListeningTimeByGenderFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.Map<String, Long> getListeningTimeByGenderFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                SUM(COALESCE(s.length_seconds, 0)) as total_seconds
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY gender
            """;
        
        java.util.Map<String, Long> result = new java.util.HashMap<>();
        result.put("male", 0L);
        result.put("female", 0L);
        result.put("other", 0L);
        
        jdbcTemplate.query(sql, filterParams.toArray(), rs -> {
            String gender = rs.getString("gender");
            long seconds = rs.getLong("total_seconds");
            result.put(gender, seconds);
        });
        
        return result;
    }
    
    // Get Genre tab chart data (5 bar charts grouped by genre)
    public java.util.Map<String, Object> getGenreChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByGenre", getArtistsByGenreFiltered(filterClause.toString(), params));
        data.put("albumsByGenre", getAlbumsByGenreFiltered(filterClause.toString(), params));
        data.put("songsByGenre", getSongsByGenreFiltered(filterClause.toString(), params));
        data.put("playsByGenre", getPlaysByGenreFiltered(filterClause.toString(), params));
        data.put("listeningTimeByGenre", getListeningTimeByGenreFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByGenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(gr.name, 'Unknown') as genre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT ar.id as artist_id, ar.genre_id, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + " " + filterClause + """
            ) sub
            INNER JOIN Artist ar ON sub.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            LEFT JOIN Genre gr ON ar.genre_id = gr.id
            GROUP BY COALESCE(gr.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByGenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(gr.name, 'Unknown') as genre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT alb.id as album_id,
                    COALESCE(alb.override_genre_id, ar.genre_id) as effective_genre_id,
                    ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            ) sub
            LEFT JOIN Gender g ON sub.gender_id = g.id
            LEFT JOIN Genre gr ON sub.effective_genre_id = gr.id
            GROUP BY COALESCE(gr.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByGenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(gr.name, 'Unknown') as genre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) = gr.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(gr.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByGenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(gr.name, 'Unknown') as genre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) = gr.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(gr.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByGenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(gr.name, 'Unknown') as genre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) = gr.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(gr.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // Get Subgenre tab chart data (5 bar charts grouped by subgenre)
    public java.util.Map<String, Object> getSubgenreChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsBySubgenre", getArtistsBySubgenreFiltered(filterClause.toString(), params));
        data.put("albumsBySubgenre", getAlbumsBySubgenreFiltered(filterClause.toString(), params));
        data.put("songsBySubgenre", getSongsBySubgenreFiltered(filterClause.toString(), params));
        data.put("playsBySubgenre", getPlaysBySubgenreFiltered(filterClause.toString(), params));
        data.put("listeningTimeBySubgenre", getListeningTimeBySubgenreFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sg.name, 'Unknown') as subgenre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT ar.id as artist_id, ar.subgenre_id, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + " " + filterClause + """
            ) sub
            INNER JOIN Artist ar ON sub.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
            GROUP BY COALESCE(sg.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sg.name, 'Unknown') as subgenre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT alb.id as album_id,
                    COALESCE(alb.override_subgenre_id, ar.subgenre_id) as effective_subgenre_id,
                    ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            ) sub
            LEFT JOIN Gender g ON sub.gender_id = g.id
            LEFT JOIN SubGenre sg ON sub.effective_subgenre_id = sg.id
            GROUP BY COALESCE(sg.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sg.name, 'Unknown') as subgenre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN SubGenre sg ON COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) = sg.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(sg.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sg.name, 'Unknown') as subgenre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN SubGenre sg ON COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) = sg.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(sg.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sg.name, 'Unknown') as subgenre_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN SubGenre sg ON COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) = sg.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(sg.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // Get Ethnicity tab chart data (5 bar charts grouped by ethnicity)
    public java.util.Map<String, Object> getEthnicityChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByEthnicity", getArtistsByEthnicityFiltered(filterClause.toString(), params));
        data.put("albumsByEthnicity", getAlbumsByEthnicityFiltered(filterClause.toString(), params));
        data.put("songsByEthnicity", getSongsByEthnicityFiltered(filterClause.toString(), params));
        data.put("playsByEthnicity", getPlaysByEthnicityFiltered(filterClause.toString(), params));
        data.put("listeningTimeByEthnicity", getListeningTimeByEthnicityFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(e.name, 'Unknown') as ethnicity_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT ar.id as artist_id, ar.ethnicity_id, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + " " + filterClause + """
            ) sub
            INNER JOIN Artist ar ON sub.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            LEFT JOIN Ethnicity e ON ar.ethnicity_id = e.id
            GROUP BY COALESCE(e.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(e.name, 'Unknown') as ethnicity_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT alb.id as album_id, ar.ethnicity_id, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            ) sub
            LEFT JOIN Gender g ON sub.gender_id = g.id
            LEFT JOIN Ethnicity e ON sub.ethnicity_id = e.id
            GROUP BY COALESCE(e.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(e.name, 'Unknown') as ethnicity_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Ethnicity e ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = e.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(e.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(e.name, 'Unknown') as ethnicity_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Ethnicity e ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = e.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(e.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(e.name, 'Unknown') as ethnicity_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Ethnicity e ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = e.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(e.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // Get Language tab chart data (5 bar charts grouped by language)
    public java.util.Map<String, Object> getLanguageChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByLanguage", getArtistsByLanguageFiltered(filterClause.toString(), params));
        data.put("albumsByLanguage", getAlbumsByLanguageFiltered(filterClause.toString(), params));
        data.put("songsByLanguage", getSongsByLanguageFiltered(filterClause.toString(), params));
        data.put("playsByLanguage", getPlaysByLanguageFiltered(filterClause.toString(), params));
        data.put("listeningTimeByLanguage", getListeningTimeByLanguageFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(l.name, 'Unknown') as language_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT ar.id as artist_id, ar.language_id, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + " " + filterClause + """
            ) sub
            INNER JOIN Artist ar ON sub.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            LEFT JOIN Language l ON ar.language_id = l.id
            GROUP BY COALESCE(l.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(l.name, 'Unknown') as language_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT alb.id as album_id, 
                       COALESCE(alb.override_language_id, ar.language_id) as lang_id, 
                       ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            ) sub
            LEFT JOIN Gender g ON sub.gender_id = g.id
            LEFT JOIN Language l ON sub.lang_id = l.id
            GROUP BY COALESCE(l.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(l.name, 'Unknown') as language_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Language l ON COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) = l.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(l.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByLanguageFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(l.name, 'Unknown') as language_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Language l ON COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) = l.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(l.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByLanguageFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(l.name, 'Unknown') as language_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            LEFT JOIN Language l ON COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) = l.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(l.name, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // Get Country tab chart data (5 bar charts grouped by country)
    public java.util.Map<String, Object> getCountryChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByCountry", getArtistsByCountryFiltered(filterClause.toString(), params));
        data.put("albumsByCountry", getAlbumsByCountryFiltered(filterClause.toString(), params));
        data.put("songsByCountry", getSongsByCountryFiltered(filterClause.toString(), params));
        data.put("playsByCountry", getPlaysByCountryFiltered(filterClause.toString(), params));
        data.put("listeningTimeByCountry", getListeningTimeByCountryFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByCountryFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(ar.country, 'Unknown') as country_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT ar.id as artist_id, ar.country, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + " " + filterClause + """
            ) sub
            INNER JOIN Artist ar ON sub.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            GROUP BY COALESCE(ar.country, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByCountryFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sub.country, 'Unknown') as country_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT alb.id as album_id, ar.country, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            ) sub
            LEFT JOIN Gender g ON sub.gender_id = g.id
            GROUP BY COALESCE(sub.country, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByCountryFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(ar.country, 'Unknown') as country_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(ar.country, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByCountryFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(ar.country, 'Unknown') as country_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(ar.country, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByCountryFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(ar.country, 'Unknown') as country_name,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(ar.country, 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY (male_count + female_count + other_count) DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // Get Release Year tab chart data (4 bar charts grouped by release year - no Artists)
    public java.util.Map<String, Object> getReleaseYearChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        // No artists by release year - artists don't have release dates
        data.put("albumsByReleaseYear", getAlbumsByReleaseYearFiltered(filterClause.toString(), params));
        data.put("songsByReleaseYear", getSongsByReleaseYearFiltered(filterClause.toString(), params));
        data.put("playsByReleaseYear", getPlaysByReleaseYearFiltered(filterClause.toString(), params));
        data.put("listeningTimeByReleaseYear", getListeningTimeByReleaseYearFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown') as release_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT alb.id as album_id, alb.release_date, ar.gender_id
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            ) sub
            INNER JOIN Album alb ON sub.album_id = alb.id
            INNER JOIN Artist ar ON alb.artist_id = ar.id
            LEFT JOIN Gender g ON sub.gender_id = g.id
            GROUP BY COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY release_year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown') as release_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY release_year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown') as release_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY release_year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown') as release_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY release_year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // Get Listen Year tab chart data (5 bar charts grouped by scrobble/listen year)
    public java.util.Map<String, Object> getListenYearChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode) {
        
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByListenYear", getArtistsByListenYearFiltered(filterClause.toString(), params));
        data.put("albumsByListenYear", getAlbumsByListenYearFiltered(filterClause.toString(), params));
        data.put("songsByListenYear", getSongsByListenYearFiltered(filterClause.toString(), params));
        data.put("playsByListenYear", getPlaysByListenYearFiltered(filterClause.toString(), params));
        data.put("listeningTimeByListenYear", getListeningTimeByListenYearFiltered(filterClause.toString(), params));
        
        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByListenYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sub.year, 'Unknown') as listen_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT ar.id as artist_id, ar.gender_id, STRFTIME('%Y', scr.scrobble_date) as year
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + " " + filterClause + """
            ) sub
            INNER JOIN Artist ar ON sub.artist_id = ar.id
            LEFT JOIN Gender g ON ar.gender_id = g.id
            GROUP BY sub.year
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY sub.year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByListenYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sub.year, 'Unknown') as listen_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT alb.id as album_id, ar.gender_id, STRFTIME('%Y', scr.scrobble_date) as year
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            ) sub
            LEFT JOIN Gender g ON sub.gender_id = g.id
            GROUP BY sub.year
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY sub.year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByListenYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(sub.year, 'Unknown') as listen_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM (
                SELECT DISTINCT s.id as song_id, COALESCE(s.override_gender_id, ar.gender_id) as gender_id, STRFTIME('%Y', scr.scrobble_date) as year
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + " " + filterClause + """
            ) sub
            LEFT JOIN Gender g ON sub.gender_id = g.id
            GROUP BY sub.year
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY sub.year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getPlaysByListenYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(STRFTIME('%Y', scr.scrobble_date), 'Unknown') as listen_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(STRFTIME('%Y', scr.scrobble_date), 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY listen_year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByListenYearFiltered(String filterClause, java.util.List<Object> filterParams) {
        String sql = """
            SELECT 
                COALESCE(STRFTIME('%Y', scr.scrobble_date), 'Unknown') as listen_year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY COALESCE(STRFTIME('%Y', scr.scrobble_date), 'Unknown')
            HAVING (male_count + female_count + other_count) > 0
            ORDER BY listen_year DESC
            """;
        
        return jdbcTemplate.query(sql, filterParams.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        });
    }
    
    // Get Top chart data (top artists, albums, songs by play count)
    public java.util.Map<String, Object> getTopChartData(
            String name, String artistName, String albumName,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
            int limit) {
        
        // Build filter clause (using the same method as other chart methods)
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> filterParams = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, filterParams, name, artistName, albumName,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        java.util.Map<String, Object> result = new java.util.HashMap<>();
        result.put("topArtists", getTopArtistsFiltered(filterClause.toString(), filterParams, limit));
        result.put("topAlbums", getTopAlbumsFiltered(filterClause.toString(), filterParams, limit));
        result.put("topSongs", getTopSongsFiltered(filterClause.toString(), filterParams, limit));
        
        return result;
    }
    
    private java.util.List<java.util.Map<String, Object>> getTopArtistsFiltered(String filterClause, java.util.List<Object> filterParams, int limit) {
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        params.add(limit);
        
        String sql = """
            SELECT 
                ar.id,
                ar.name,
                ar.genre_id,
                gen.name as genre,
                ar.subgenre_id,
                sg.name as subgenre,
                ar.ethnicity_id,
                eth.name as ethnicity,
                ar.language_id,
                lang.name as language,
                ar.country,
                COUNT(scr.id) as plays,
                SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                COALESCE(SUM(s.length_seconds), 0) as time_listened,
                MIN(scr.scrobble_date) as first_listened,
                MAX(scr.scrobble_date) as last_listened
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Genre gen ON ar.genre_id = gen.id
            LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            LEFT JOIN Language lang ON ar.language_id = lang.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY ar.id, ar.name, ar.genre_id, gen.name, ar.subgenre_id, sg.name, ar.ethnicity_id, eth.name, ar.language_id, lang.name, ar.country
            ORDER BY plays DESC
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, params.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("genreId", rs.getObject("genre_id"));
            row.put("genre", rs.getString("genre"));
            row.put("subgenreId", rs.getObject("subgenre_id"));
            row.put("subgenre", rs.getString("subgenre"));
            row.put("ethnicityId", rs.getObject("ethnicity_id"));
            row.put("ethnicity", rs.getString("ethnicity"));
            row.put("languageId", rs.getObject("language_id"));
            row.put("language", rs.getString("language"));
            row.put("country", rs.getString("country"));
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getTopAlbumsFiltered(String filterClause, java.util.List<Object> filterParams, int limit) {
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        params.add(limit);
        
        String sql = """
            SELECT 
                alb.id,
                alb.name,
                ar.id as artist_id,
                ar.name as artist_name,
                alb.release_date,
                COALESCE(alb.override_genre_id, ar.genre_id) as genre_id,
                COALESCE(g_override.name, g_artist.name) as genre,
                COALESCE(alb.override_subgenre_id, ar.subgenre_id) as subgenre_id,
                COALESCE(sg_override.name, sg_artist.name) as subgenre,
                ar.ethnicity_id,
                eth.name as ethnicity,
                COALESCE(alb.override_language_id, ar.language_id) as language_id,
                COALESCE(l_override.name, l_artist.name) as language,
                ar.country,
                COUNT(scr.id) as plays,
                SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                COALESCE(SUM(s.length_seconds), 0) as time_listened,
                MIN(scr.scrobble_date) as first_listened,
                MAX(scr.scrobble_date) as last_listened
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Genre g_override ON alb.override_genre_id = g_override.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON alb.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON alb.override_language_id = l_override.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            WHERE alb.id IS NOT NULL """ + " " + filterClause + """
            GROUP BY alb.id, alb.name, ar.id, ar.name, alb.release_date, genre_id, genre, subgenre_id, subgenre, ar.ethnicity_id, eth.name, language_id, language, ar.country
            ORDER BY plays DESC
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, params.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("artistId", rs.getInt("artist_id"));
            row.put("artistName", rs.getString("artist_name"));
            row.put("releaseDate", formatDate(rs.getString("release_date")));
            row.put("genreId", rs.getObject("genre_id"));
            row.put("genre", rs.getString("genre"));
            row.put("subgenreId", rs.getObject("subgenre_id"));
            row.put("subgenre", rs.getString("subgenre"));
            row.put("ethnicityId", rs.getObject("ethnicity_id"));
            row.put("ethnicity", rs.getString("ethnicity"));
            row.put("languageId", rs.getObject("language_id"));
            row.put("language", rs.getString("language"));
            row.put("country", rs.getString("country"));
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        });
    }
    
    private java.util.List<java.util.Map<String, Object>> getTopSongsFiltered(String filterClause, java.util.List<Object> filterParams, int limit) {
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        params.add(limit);
        
        String sql = """
            SELECT 
                s.id,
                s.name,
                s.artist_id,
                ar.name as artist_name,
                s.album_id,
                alb.name as album_name,
                COALESCE(s.release_date, alb.release_date) as release_date,
                COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) as genre_id,
                COALESCE(g_song.name, g_album.name, g_artist.name) as genre,
                COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) as subgenre_id,
                COALESCE(sg_song.name, sg_album.name, sg_artist.name) as subgenre,
                COALESCE(s.override_ethnicity_id, ar.ethnicity_id) as ethnicity_id,
                COALESCE(eth_song.name, eth_artist.name) as ethnicity,
                COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) as language_id,
                COALESCE(l_song.name, l_album.name, l_artist.name) as language,
                ar.country,
                COUNT(scr.id) as plays,
                SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                COALESCE(s.length_seconds, 0) * COUNT(scr.id) as time_listened,
                MIN(scr.scrobble_date) as first_listened,
                MAX(scr.scrobble_date) as last_listened
            FROM Scrobble scr
            INNER JOIN Song s ON scr.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Genre g_song ON s.override_genre_id = g_song.id
            LEFT JOIN Genre g_album ON alb.override_genre_id = g_album.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_song ON s.override_subgenre_id = sg_song.id
            LEFT JOIN SubGenre sg_album ON alb.override_subgenre_id = sg_album.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_song ON s.override_language_id = l_song.id
            LEFT JOIN Language l_album ON alb.override_language_id = l_album.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth_song ON s.override_ethnicity_id = eth_song.id
            LEFT JOIN Ethnicity eth_artist ON ar.ethnicity_id = eth_artist.id
            WHERE 1=1 """ + " " + filterClause + """
            GROUP BY s.id, s.name, s.artist_id, ar.name, s.album_id, alb.name, COALESCE(s.release_date, alb.release_date), genre_id, genre, subgenre_id, subgenre, ethnicity_id, ethnicity, language_id, language, ar.country, s.length_seconds
            ORDER BY plays DESC
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, params.toArray(), (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("artistId", rs.getInt("artist_id"));
            row.put("artistName", rs.getString("artist_name"));
            row.put("albumId", rs.getObject("album_id"));
            row.put("albumName", rs.getString("album_name"));
            row.put("releaseDate", formatDate(rs.getString("release_date")));
            row.put("genreId", rs.getObject("genre_id"));
            row.put("genre", rs.getString("genre"));
            row.put("subgenreId", rs.getObject("subgenre_id"));
            row.put("subgenre", rs.getString("subgenre"));
            row.put("ethnicityId", rs.getObject("ethnicity_id"));
            row.put("ethnicity", rs.getString("ethnicity"));
            row.put("languageId", rs.getObject("language_id"));
            row.put("language", rs.getString("language"));
            row.put("country", rs.getString("country"));
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        });
    }
    
    // Helper method to format time in seconds to human-readable format
    private String formatTime(long totalSeconds) {
        if (totalSeconds == 0) {
            return "0m";
        }
        
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        
        if (days > 0) {
            return String.format("%dd %dh", days, hours);
        } else if (hours > 0) {
            return String.format("%dh %dm", hours, minutes);
        } else {
            return String.format("%dm", minutes);
        }
    }
    
    // Helper method to format date strings
    private String formatDate(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
            return null;
        }
        
        try {
            String datePart = dateTimeString.trim();
            if (datePart.contains(" ")) {
                datePart = datePart.split(" ")[0];
            }
            
            String[] parts = datePart.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                
                String[] monthNames = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
                
                return day + " " + monthNames[month - 1] + " " + year;
            }
            
            return datePart;
        } catch (Exception e) {
            return dateTimeString;
        }
    }
}
