package library.repository;

import library.util.SqlFilterHelper;
import library.util.StringNormalizer;
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
            String organized,
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
                COALESCE(play_stats.play_count, 0) as play_count,
                COALESCE(play_stats.vatito_play_count, 0) as vatito_play_count,
                COALESCE(play_stats.robertlover_play_count, 0) as robertlover_play_count,
                (SELECT COALESCE(SUM(s.length_seconds * scr_count.play_count), 0) 
                 FROM Song s 
                 LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble GROUP BY song_id) scr_count ON s.id = scr_count.song_id 
                 WHERE s.artist_id = a.id) as time_listened,
                (SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id)) as first_listened,
                (SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id)) as last_listened,
                a.organized
            FROM Artist a
            LEFT JOIN Gender g ON a.gender_id = g.id
            LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
            LEFT JOIN Genre gen ON a.genre_id = gen.id
            LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id
            LEFT JOIN Language l ON a.language_id = l.id
            LEFT JOIN Song s ON s.artist_id = a.id
            LEFT JOIN Album alb ON alb.artist_id = a.id
            LEFT JOIN (
                SELECT 
                    song.artist_id,
                    COUNT(*) as play_count,
                    SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as vatito_play_count,
                    SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_play_count
                FROM Scrobble scr
                JOIN Song song ON scr.song_id = song.id
                GROUP BY song.artist_id
            ) play_stats ON play_stats.artist_id = a.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        // Name filter with accent-insensitive search
        if (name != null && !name.isEmpty()) {
            sql.append(" AND ").append(StringNormalizer.sqlNormalizeColumn("a.name")).append(" LIKE ? ");
            params.add(StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Gender filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.gender_id", genderIds, genderMode);
        
        // Ethnicity filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.ethnicity_id", ethnicityIds, ethnicityMode);
        
        // Genre filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.genre_id", genreIds, genreMode);
        
        // Subgenre filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.subgenre_id", subgenreIds, subgenreMode);
        
        // Language filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.language_id", languageIds, languageMode);
        
        // Country filter
        SqlFilterHelper.appendStringFilter(sql, params, "a.country", countries, countryMode);
        
        // First Listened Date filter
        String firstListenedSubquery = "(SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last Listened Date filter
        String lastListenedSubquery = "(SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, lastListenedSubquery, lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode);
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND a.organized = 1 ");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (a.organized = 0 OR a.organized IS NULL) ");
            }
        }
        
        sql.append(" GROUP BY a.id, a.name, a.gender_id, g.name, a.ethnicity_id, e.name, a.genre_id, gen.name, a.subgenre_id, sg.name, a.language_id, l.name, a.country, a.image, a.organized, play_stats.play_count, play_stats.vatito_play_count, play_stats.robertlover_play_count ");
        
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
                rs.getInt("vatito_play_count"),
                rs.getInt("robertlover_play_count"),
                rs.getLong("time_listened"),
                rs.getString("first_listened"),
                rs.getString("last_listened"),
                rs.getObject("organized")
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
            String lastListenedDateMode,
            String organized
    ) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(DISTINCT a.id)
            FROM Artist a
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        
        // Name filter with accent-insensitive search
        if (name != null && !name.isEmpty()) {
            sql.append(" AND ").append(StringNormalizer.sqlNormalizeColumn("a.name")).append(" LIKE ? ");
            params.add(StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Gender filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.gender_id", genderIds, genderMode);
        
        // Ethnicity filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.ethnicity_id", ethnicityIds, ethnicityMode);
        
        // Genre filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.genre_id", genreIds, genreMode);
        
        // Subgenre filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.subgenre_id", subgenreIds, subgenreMode);
        
        // Language filter
        SqlFilterHelper.appendIdFilter(sql, params, "a.language_id", languageIds, languageMode);
        
        // Country filter
        SqlFilterHelper.appendStringFilter(sql, params, "a.country", countries, countryMode);
        
        // First Listened Date filter
        String firstListenedSubquery = "(SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last Listened Date filter
        String lastListenedSubquery = "(SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, lastListenedSubquery, lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode);
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND a.organized = 1 ");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (a.organized = 0 OR a.organized IS NULL) ");
            }
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), params.toArray(), Long.class);
        return count != null ? count : 0L;
    }
}