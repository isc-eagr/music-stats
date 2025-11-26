package library.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class ArtistRepositoryCustomImpl implements ArtistRepositoryCustom {
    
    private final JdbcTemplate jdbcTemplate;
    
    public ArtistRepositoryCustomImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    @Override
    public List<Object[]> findArtistsWithStats(
            String name,
            List<Integer> genderIds,
            String genderMode,
            List<Integer> ethnicityIds,
            String ethnicityMode,
            List<Integer> genreIds,
            String genreMode,
            List<Integer> subgenreIds,
            String subgenreMode,
            List<Integer> languageIds,
            String languageMode,
            List<String> countries,
            String countryMode,
            String firstListenedDate,
            String firstListenedDateFrom,
            String firstListenedDateTo,
            String firstListenedDateMode,
            String lastListenedDate,
            String lastListenedDateFrom,
            String lastListenedDateTo,
            String lastListenedDateMode,
            String sortBy,
            String sortDir,
            int limit,
            int offset
    ) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                a.id,
                a.name,
                a.gender_id,
                g.name as gender_name,
                a.ethnicity_id,
                e.name as ethnicity_name,
                a.genre_id,
                gen.name as genre_name,
                a.subgenre_id,
                sg.name as subgenre_name,
                a.language_id,
                l.name as language_name,
                a.country,
                COUNT(DISTINCT s.id) as song_count,
                COUNT(DISTINCT alb.id) as album_count,
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                (SELECT COUNT(*) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id)) as play_count,
                (SELECT COALESCE(SUM(s.length_seconds * scr_count.play_count), 0) 
                 FROM Song s 
                 LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble GROUP BY song_id) scr_count ON s.id = scr_count.song_id 
                 WHERE s.artist_id = a.id) as time_listened,
                (SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id)) as first_listened,
                (SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id)) as last_listened
            FROM Artist a
            LEFT JOIN Gender g ON a.gender_id = g.id
            LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
            LEFT JOIN Genre gen ON a.genre_id = gen.id
            LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id
            LEFT JOIN Language l ON a.language_id = l.id
            LEFT JOIN Song s ON s.artist_id = a.id
            LEFT JOIN Album alb ON alb.artist_id = a.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        // Name filter
        if (name != null && !name.isEmpty()) {
            sql.append(" AND LOWER(a.name) LIKE LOWER(?) ");
            params.add(name + "%");
        }
        
        // Gender filter
        appendFilterCondition(sql, params, "a.gender_id", genderIds, genderMode);
        
        // Ethnicity filter
        appendFilterCondition(sql, params, "a.ethnicity_id", ethnicityIds, ethnicityMode);
        
        // Genre filter
        appendFilterCondition(sql, params, "a.genre_id", genreIds, genreMode);
        
        // Subgenre filter
        appendFilterCondition(sql, params, "a.subgenre_id", subgenreIds, subgenreMode);
        
        // Language filter
        appendFilterCondition(sql, params, "a.language_id", languageIds, languageMode);
        
        // Country filter
        appendStringFilterCondition(sql, params, "a.country", countries, countryMode);
        
        // First Listened Date filter
        if (firstListenedDateMode != null && !firstListenedDateMode.isEmpty()) {
            String subquery = "(SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
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
            String subquery = "(SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
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
        
        sql.append(" GROUP BY a.id, a.name, a.gender_id, g.name, a.ethnicity_id, e.name, a.genre_id, gen.name, a.subgenre_id, sg.name, a.language_id, l.name, a.country, a.image ");
        
        // Sorting
        String direction = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        String oppositeDir = "desc".equalsIgnoreCase(sortDir) ? "ASC" : "DESC";
        sql.append(" ORDER BY ");
        if ("songs".equals(sortBy)) {
            sql.append(" COUNT(DISTINCT s.id) " + direction + ", a.name ASC");
        } else if ("albums".equals(sortBy)) {
            sql.append(" COUNT(DISTINCT alb.id) " + direction + ", a.name ASC");
        } else if ("plays".equals(sortBy)) {
            sql.append(" play_count " + direction + ", a.name ASC");
        } else if ("time".equals(sortBy)) {
            sql.append(" (SELECT COALESCE(SUM(s.length_seconds * scr_count.play_count), 0) " +
                       "FROM Song s " +
                       "LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble GROUP BY song_id) scr_count ON s.id = scr_count.song_id " +
                       "WHERE s.artist_id = a.id) " + direction + ", a.name ASC");
        } else if ("first_listened".equals(sortBy)) {
            sql.append(" first_listened " + direction + " NULLS LAST, a.name ASC");
        } else if ("last_listened".equals(sortBy)) {
            sql.append(" last_listened " + direction + " NULLS LAST, a.name ASC");
        } else if ("name".equals(sortBy)) {
            sql.append(" a.name " + direction);
        } else {
            sql.append(" a.name " + direction);
        }
        
        sql.append(" LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            return new Object[] {
                rs.getInt("id"),
                rs.getString("name"),
                rs.getObject("gender_id"),
                rs.getString("gender_name"),
                rs.getObject("ethnicity_id"),
                rs.getString("ethnicity_name"),
                rs.getObject("genre_id"),
                rs.getString("genre_name"),
                rs.getObject("subgenre_id"),
                rs.getString("subgenre_name"),
                rs.getObject("language_id"),
                rs.getString("language_name"),
                rs.getString("country"),
                rs.getInt("song_count"),
                rs.getInt("album_count"),
                rs.getInt("has_image"),
                rs.getInt("play_count"),
                rs.getLong("time_listened"),
                rs.getString("first_listened"),
                rs.getString("last_listened")
            };
        });
    }
    
    @Override
    public Long countArtistsWithFilters(
            String name,
            List<Integer> genderIds,
            String genderMode,
            List<Integer> ethnicityIds,
            String ethnicityMode,
            List<Integer> genreIds,
            String genreMode,
            List<Integer> subgenreIds,
            String subgenreMode,
            List<Integer> languageIds,
            String languageMode,
            List<String> countries,
            String countryMode,
            String firstListenedDate,
            String firstListenedDateFrom,
            String firstListenedDateTo,
            String firstListenedDateMode,
            String lastListenedDate,
            String lastListenedDateFrom,
            String lastListenedDateTo,
            String lastListenedDateMode
    ) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(DISTINCT a.id)
            FROM Artist a
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        // Name filter
        if (name != null && !name.isEmpty()) {
            sql.append(" AND LOWER(a.name) LIKE LOWER(?) ");
            params.add(name + "%");
        }
        
        // Gender filter
        appendFilterCondition(sql, params, "a.gender_id", genderIds, genderMode);
        
        // Ethnicity filter
        appendFilterCondition(sql, params, "a.ethnicity_id", ethnicityIds, ethnicityMode);
        
        // Genre filter
        appendFilterCondition(sql, params, "a.genre_id", genreIds, genreMode);
        
        // Subgenre filter
        appendFilterCondition(sql, params, "a.subgenre_id", subgenreIds, subgenreMode);
        
        // Language filter
        appendFilterCondition(sql, params, "a.language_id", languageIds, languageMode);
        
        // Country filter
        appendStringFilterCondition(sql, params, "a.country", countries, countryMode);
        
        // First Listened Date filter
        if (firstListenedDateMode != null && !firstListenedDateMode.isEmpty()) {
            String subquery = "(SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
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
            String subquery = "(SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
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
        return count != null ? count : 0L;
    }
    
    private void appendFilterCondition(StringBuilder sql, List<Object> params, 
                                      String columnName, List<Integer> ids, String mode) {
        // Support includes, excludes, isnull, isnotnull
        if (mode == null) return;
        switch(mode) {
            case "includes":
                if (ids != null && !ids.isEmpty()) {
                    sql.append(" AND ").append(columnName).append(" IN (");
                    for (int i = 0; i < ids.size(); i++) {
                        if (i > 0) sql.append(",");
                        sql.append("?");
                        params.add(ids.get(i));
                    }
                    sql.append(") ");
                }
                break;
            case "excludes":
                if (ids != null && !ids.isEmpty()) {
                    sql.append(" AND (").append(columnName).append(" NOT IN (");
                    for (int i = 0; i < ids.size(); i++) {
                        if (i > 0) sql.append(",");
                        sql.append("?");
                        params.add(ids.get(i));
                    }
                    sql.append(") OR ").append(columnName).append(" IS NULL) ");
                }
                break;
            case "isnull":
                sql.append(" AND ").append(columnName).append(" IS NULL ");
                break;
            case "isnotnull":
                sql.append(" AND ").append(columnName).append(" IS NOT NULL ");
                break;
            default:
                // unknown mode - ignore
                break;
        }
    }
    
    private void appendStringFilterCondition(StringBuilder sql, List<Object> params, 
                                            String columnName, List<String> values, String mode) {
        // Support includes, excludes, isnull, isnotnull for string columns
        if (mode == null) return;
        switch(mode) {
            case "includes":
                if (values != null && !values.isEmpty()) {
                    sql.append(" AND ").append(columnName).append(" IN (");
                    for (int i = 0; i < values.size(); i++) {
                        if (i > 0) sql.append(",");
                        sql.append("?");
                        params.add(values.get(i));
                    }
                    sql.append(") ");
                }
                break;
            case "excludes":
                if (values != null && !values.isEmpty()) {
                    sql.append(" AND (").append(columnName).append(" NOT IN (");
                    for (int i = 0; i < values.size(); i++) {
                        if (i > 0) sql.append(",");
                        sql.append("?");
                        params.add(values.get(i));
                    }
                    sql.append(") OR ").append(columnName).append(" IS NULL) ");
                }
                break;
            case "isnull":
                sql.append(" AND ").append(columnName).append(" IS NULL ");
                break;
            case "isnotnull":
                sql.append(" AND ").append(columnName).append(" IS NOT NULL ");
                break;
            default:
                break;
        }
    }
}