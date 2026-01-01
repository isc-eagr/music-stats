package library.repository;

import library.util.SqlFilterHelper;
import library.util.StringNormalizer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class ArtistRepositoryImpl implements ArtistRepositoryCustom {
    
    private final JdbcTemplate jdbcTemplate;
    
    public ArtistRepositoryImpl(JdbcTemplate jdbcTemplate) {
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
            List<String> accounts,
            String accountMode,
            String firstListenedDate,
            String firstListenedDateFrom,
            String firstListenedDateTo,
            String firstListenedDateMode,
            String lastListenedDate,
            String lastListenedDateFrom,
            String lastListenedDateTo,
            String lastListenedDateMode,
            String listenedDateFrom,
            String listenedDateTo,
            String organized,
            String hasImage,
            String isBand,
            Integer playCountMin,
            Integer playCountMax,
            Integer albumCountMin,
            Integer albumCountMax,
            Integer songCountMin,
            Integer songCountMax,
            boolean includeGroups,
            boolean includeFeatured,
            String sortBy,
            String sortDir,
            int limit,
            int offset
    ) {
        // Build account filter subquery for the play_stats join
        StringBuilder accountFilterClause = new StringBuilder();
        List<Object> accountParams = new ArrayList<>();
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND scr.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) accountFilterClause.append(",");
                accountFilterClause.append("?");
                accountParams.add(accounts.get(i));
            }
            accountFilterClause.append(")");
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND scr.account NOT IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) accountFilterClause.append(",");
                accountFilterClause.append("?");
                accountParams.add(accounts.get(i));
            }
            accountFilterClause.append(")");
        }
        
        // Build listened date filter for the play_stats subquery
        StringBuilder listenedDateFilterClause = new StringBuilder();
        List<Object> listenedDateParams = new ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(scr.scrobble_date) >= DATE(?)");
            listenedDateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(scr.scrobble_date) <= DATE(?)");
            listenedDateParams.add(listenedDateTo);
        }
        
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append("    a.id, ");
        sql.append("    a.name, ");
        sql.append("    a.gender_id, ");
        sql.append("    g.name as gender_name, ");
        sql.append("    a.ethnicity_id, ");
        sql.append("    e.name as ethnicity_name, ");
        sql.append("    a.genre_id, ");
        sql.append("    gen.name as genre_name, ");
        sql.append("    a.subgenre_id, ");
        sql.append("    sg.name as subgenre_name, ");
        sql.append("    a.language_id, ");
        sql.append("    l.name as language_name, ");
        sql.append("    a.country, ");
        sql.append("    COALESCE(song_stats.song_count, 0) as song_count, ");
        sql.append("    COALESCE(album_stats.album_count, 0) as album_count, ");
        sql.append("    CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image, ");
        sql.append("    COALESCE(play_stats.play_count, 0) as play_count, ");
        sql.append("    COALESCE(play_stats.vatito_play_count, 0) as vatito_play_count, ");
        sql.append("    COALESCE(play_stats.robertlover_play_count, 0) as robertlover_play_count, ");
        sql.append("    COALESCE(play_stats.time_listened, 0) as time_listened, ");
        sql.append("    play_stats.first_listened, ");
        sql.append("    play_stats.last_listened, ");
        sql.append("    a.organized, ");
        sql.append("    COALESCE(featured_stats.featured_song_count, 0) as featured_song_count ");
        sql.append("FROM Artist a ");
        sql.append("LEFT JOIN Gender g ON a.gender_id = g.id ");
        sql.append("LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id ");
        sql.append("LEFT JOIN Genre gen ON a.genre_id = gen.id ");
        sql.append("LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id ");
        sql.append("LEFT JOIN Language l ON a.language_id = l.id ");
        sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as song_count FROM Song GROUP BY artist_id) song_stats ON song_stats.artist_id = a.id ");
        sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as album_count FROM Album GROUP BY artist_id) album_stats ON album_stats.artist_id = a.id ");
        sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as featured_song_count FROM SongFeaturedArtist GROUP BY artist_id) featured_stats ON featured_stats.artist_id = a.id ");
        
        // Use INNER JOIN when account filter is includes mode or when listened date filter is applied
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        String playStatsJoinType = ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) ? "INNER JOIN" : "LEFT JOIN";
        
        // Build play_stats subquery based on includeGroups and includeFeatured flags
        if (!includeGroups && !includeFeatured) {
            // Standard query - direct plays only
            sql.append(playStatsJoinType).append(" ( ");
            sql.append("    SELECT ");
            sql.append("        song.artist_id, ");
            sql.append("        COUNT(*) as play_count, ");
            sql.append("        SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as vatito_play_count, ");
            sql.append("        SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_play_count, ");
            sql.append("        SUM(song.length_seconds) as time_listened, ");
            sql.append("        MIN(scr.scrobble_date) as first_listened, ");
            sql.append("        MAX(scr.scrobble_date) as last_listened ");
            sql.append("    FROM Scrobble scr ");
            sql.append("    JOIN Song song ON scr.song_id = song.id ");
            sql.append("    WHERE 1=1 ").append(accountFilterClause).append(listenedDateFilterClause).append(" ");
            sql.append("    GROUP BY song.artist_id ");
            sql.append(") play_stats ON play_stats.artist_id = a.id ");
        } else {
            // Build union query to aggregate plays from groups and/or featured songs
            sql.append(playStatsJoinType).append(" ( ");
            sql.append("    SELECT ");
            sql.append("        attributed_artist_id as artist_id, ");
            sql.append("        COUNT(*) as play_count, ");
            sql.append("        SUM(CASE WHEN account = 'vatito' THEN 1 ELSE 0 END) as vatito_play_count, ");
            sql.append("        SUM(CASE WHEN account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_play_count, ");
            sql.append("        SUM(length_seconds) as time_listened, ");
            sql.append("        MIN(scrobble_date) as first_listened, ");
            sql.append("        MAX(scrobble_date) as last_listened ");
            sql.append("    FROM ( ");
            
            // Part 1: Direct artist plays
            sql.append("        SELECT song.artist_id as attributed_artist_id, scr.account, song.length_seconds, scr.scrobble_date ");
            sql.append("        FROM Scrobble scr ");
            sql.append("        JOIN Song song ON scr.song_id = song.id ");
            sql.append("        WHERE 1=1 ").append(accountFilterClause).append(listenedDateFilterClause);
            
            if (includeGroups) {
                // Part 2: Group plays - attribute to group members
                sql.append("        UNION ALL ");
                sql.append("        SELECT am.member_artist_id as attributed_artist_id, scr.account, song.length_seconds, scr.scrobble_date ");
                sql.append("        FROM Scrobble scr ");
                sql.append("        JOIN Song song ON scr.song_id = song.id ");
                sql.append("        JOIN ArtistMember am ON song.artist_id = am.group_artist_id ");
                sql.append("        WHERE 1=1 ").append(accountFilterClause).append(listenedDateFilterClause);
            }
            
            if (includeFeatured) {
                // Part 3: Featured plays - attribute to featured artists
                sql.append("        UNION ALL ");
                sql.append("        SELECT sfa.artist_id as attributed_artist_id, scr.account, song.length_seconds, scr.scrobble_date ");
                sql.append("        FROM Scrobble scr ");
                sql.append("        JOIN Song song ON scr.song_id = song.id ");
                sql.append("        JOIN SongFeaturedArtist sfa ON song.id = sfa.song_id ");
                sql.append("        WHERE 1=1 ").append(accountFilterClause).append(listenedDateFilterClause);
            }
            
            sql.append("    ) ");
            sql.append("    GROUP BY attributed_artist_id ");
            sql.append(") play_stats ON play_stats.artist_id = a.id ");
        }
        
        sql.append("WHERE 1=1 ");
        
        List<Object> params = new ArrayList<>();
        // Add account params and listened date params for each part of the query
        params.addAll(accountParams); // first part
        params.addAll(listenedDateParams); // first part listened date
        if (includeGroups) {
            params.addAll(accountParams); // group part
            params.addAll(listenedDateParams); // group part listened date
        }
        if (includeFeatured) {
            params.addAll(accountParams); // featured part
            params.addAll(listenedDateParams); // featured part listened date
        }
        
        // Name filter with accent-insensitive search
        if (name != null && !name.isEmpty()) {
            sql.append(" AND ").append(StringNormalizer.sqlNormalizeColumn("a.name")).append(" LIKE ? ");
            params.add("%" + StringNormalizer.normalizeForSearch(name) + "%");
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
        
        // Has Image filter
        if (hasImage != null && !hasImage.isEmpty()) {
            if ("true".equalsIgnoreCase(hasImage)) {
                sql.append(" AND a.image IS NOT NULL ");
            } else if ("false".equalsIgnoreCase(hasImage)) {
                sql.append(" AND a.image IS NULL ");
            }
        }
        
        // Is Band filter
        if (isBand != null && !isBand.isEmpty()) {
            if ("true".equalsIgnoreCase(isBand)) {
                sql.append(" AND a.is_band = 1 ");
            } else if ("false".equalsIgnoreCase(isBand)) {
                sql.append(" AND a.is_band = 0 ");
            }
        }
        
        // Play count filter
        if (playCountMin != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) >= ? ");
            params.add(playCountMin);
        }
        if (playCountMax != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) <= ? ");
            params.add(playCountMax);
        }
        
        // Album count filter
        if (albumCountMin != null) {
            sql.append(" AND COALESCE(album_stats.album_count, 0) >= ? ");
            params.add(albumCountMin);
        }
        if (albumCountMax != null) {
            sql.append(" AND COALESCE(album_stats.album_count, 0) <= ? ");
            params.add(albumCountMax);
        }
        
        // Song count filter
        if (songCountMin != null) {
            sql.append(" AND COALESCE(song_stats.song_count, 0) >= ? ");
            params.add(songCountMin);
        }
        if (songCountMax != null) {
            sql.append(" AND COALESCE(song_stats.song_count, 0) <= ? ");
            params.add(songCountMax);
        }
        
        // Sorting
        String direction = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        sql.append(" ORDER BY ");
        if ("songs".equals(sortBy)) {
            sql.append(" song_count " + direction + ", a.name ASC");
        } else if ("featured".equals(sortBy)) {
            sql.append(" featured_song_count " + direction + ", a.name ASC");
        } else if ("albums".equals(sortBy)) {
            sql.append(" album_count " + direction + ", a.name ASC");
        } else if ("plays".equals(sortBy)) {
            sql.append(" play_count " + direction + ", a.name ASC");
        } else if ("time".equals(sortBy)) {
            sql.append(" time_listened " + direction + ", a.name ASC");
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
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
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
                rs.getObject("organized"),
                rs.getInt("featured_song_count")
            };
        }, params.toArray());
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
            List<String> accounts,
            String accountMode,
            String firstListenedDate,
            String firstListenedDateFrom,
            String firstListenedDateTo,
            String firstListenedDateMode,
            String lastListenedDate,
            String lastListenedDateFrom,
            String lastListenedDateTo,
            String lastListenedDateMode,
            String listenedDateFrom,
            String listenedDateTo,
            String organized,
            String hasImage,
            String isBand,
            Integer playCountMin,
            Integer playCountMax,
            Integer albumCountMin,
            Integer albumCountMax,
            Integer songCountMin,
            Integer songCountMax
    ) {
        // Build listened date filter clause
        StringBuilder listenedDateFilterClause = new StringBuilder();
        List<Object> listenedDateParams = new ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(scr.scrobble_date) >= DATE(?)");
            listenedDateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(scr.scrobble_date) <= DATE(?)");
            listenedDateParams.add(listenedDateTo);
        }
        
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        
        StringBuilder sql = new StringBuilder();
        
        // Use a more efficient approach with JOIN for account filtering or listened date filtering
        if ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) {
            sql.append(
                "SELECT COUNT(DISTINCT a.id) " +
                "FROM Artist a " +
                "INNER JOIN Song s ON s.artist_id = a.id " +
                "INNER JOIN Scrobble scr ON scr.song_id = s.id " +
                "WHERE 1=1 ");
            if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
                sql.append("AND scr.account IN (");
                for (int i = 0; i < accounts.size(); i++) {
                    if (i > 0) sql.append(",");
                    sql.append("?");
                }
                sql.append(") ");
            }
            sql.append(listenedDateFilterClause);
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            sql.append(
                "SELECT COUNT(DISTINCT a.id) " +
                "FROM Artist a " +
                "WHERE NOT EXISTS ( " +
                "    SELECT 1 FROM Scrobble scr " +
                "    JOIN Song song ON scr.song_id = song.id " +
                "    WHERE song.artist_id = a.id AND scr.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
            }
            sql.append(") ) AND 1=1 ");
        } else {
            sql.append(
                "SELECT COUNT(DISTINCT a.id) " +
                "FROM Artist a " +
                "WHERE 1=1 ");
        }
        
        List<Object> params = new ArrayList<>();
        
        // Account params first if using includes mode
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            params.addAll(accounts);
        }
        // Listened date params
        if (hasListenedDateFilter) {
            params.addAll(listenedDateParams);
        }
        // Account params for excludes mode
        if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            params.addAll(accounts);
        }
        
        // Name filter with accent-insensitive search
        if (name != null && !name.isEmpty()) {
            sql.append(" AND ").append(StringNormalizer.sqlNormalizeColumn("a.name")).append(" LIKE ? ");
            params.add("%" + StringNormalizer.normalizeForSearch(name) + "%");
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
        
        // Has Image filter
        if (hasImage != null && !hasImage.isEmpty()) {
            if ("true".equalsIgnoreCase(hasImage)) {
                sql.append(" AND a.image IS NOT NULL ");
            } else if ("false".equalsIgnoreCase(hasImage)) {
                sql.append(" AND a.image IS NULL ");
            }
        }
        
        // Is Band filter
        if (isBand != null && !isBand.isEmpty()) {
            if ("true".equalsIgnoreCase(isBand)) {
                sql.append(" AND a.is_band = 1 ");
            } else if ("false".equalsIgnoreCase(isBand)) {
                sql.append(" AND a.is_band = 0 ");
            }
        }
        
        // Play count filter (uses subquery since count query doesn't have play_stats join)
        if (playCountMin != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Scrobble scr JOIN Song song ON scr.song_id = song.id WHERE song.artist_id = a.id), 0) >= ? ");
            params.add(playCountMin);
        }
        if (playCountMax != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Scrobble scr JOIN Song song ON scr.song_id = song.id WHERE song.artist_id = a.id), 0) <= ? ");
            params.add(playCountMax);
        }
        
        // Album count filter
        if (albumCountMin != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Album WHERE artist_id = a.id), 0) >= ? ");
            params.add(albumCountMin);
        }
        if (albumCountMax != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Album WHERE artist_id = a.id), 0) <= ? ");
            params.add(albumCountMax);
        }
        
        // Song count filter
        if (songCountMin != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Song WHERE artist_id = a.id), 0) >= ? ");
            params.add(songCountMin);
        }
        if (songCountMax != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Song WHERE artist_id = a.id), 0) <= ? ");
            params.add(songCountMax);
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0L;
    }
}