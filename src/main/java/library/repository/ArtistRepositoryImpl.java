package library.repository;

import library.dto.ArtistStatsQuery;
import library.dto.ArtistStatsRow;
import library.util.RandomSortUtils;
import library.util.SqlFilterHelper;
import library.util.StringNormalizer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class ArtistRepositoryImpl implements ArtistRepositoryCustom {
    
    private final JdbcTemplate jdbcTemplate;
    
    public ArtistRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    @Override
    public List<ArtistStatsRow> findArtistsWithStats(ArtistStatsQuery query) {
        String name = query.name();
        List<Integer> genderIds = query.genderIds();
        String genderMode = query.genderMode();
        List<Integer> ethnicityIds = query.ethnicityIds();
        String ethnicityMode = query.ethnicityMode();
        List<Integer> genreIds = query.genreIds();
        String genreMode = query.genreMode();
        List<Integer> subgenreIds = query.subgenreIds();
        String subgenreMode = query.subgenreMode();
        List<Integer> languageIds = query.languageIds();
        String languageMode = query.languageMode();
        List<String> countries = query.countries();
        String countryMode = query.countryMode();
        List<Integer> tagIds = query.tagIds();
        String tagMode = query.tagMode();
        String deathDate = query.deathDate();
        String deathDateFrom = query.deathDateFrom();
        String deathDateTo = query.deathDateTo();
        String deathDateMode = query.deathDateMode();
        List<String> accounts = query.accounts();
        String accountMode = query.accountMode();
        Integer ageMin = query.ageMin();
        Integer ageMax = query.ageMax();
        String firstListenedDate = query.firstListenedDate();
        String firstListenedDateFrom = query.firstListenedDateFrom();
        String firstListenedDateTo = query.firstListenedDateTo();
        String firstListenedDateMode = query.firstListenedDateMode();
        String lastListenedDate = query.lastListenedDate();
        String lastListenedDateFrom = query.lastListenedDateFrom();
        String lastListenedDateTo = query.lastListenedDateTo();
        String lastListenedDateMode = query.lastListenedDateMode();
        String listenedDateFrom = query.listenedDateFrom();
        String listenedDateTo = query.listenedDateTo();
        String organized = query.organized();
        Integer imageCountMin = query.imageCountMin();
        Integer imageCountMax = query.imageCountMax();
        Integer imageTheme = query.imageTheme();
        String imageThemeMode = query.imageThemeMode();
        String isBand = query.isBand();
        String itunesIdsJson = query.itunesIdsJson();
        String inItunes = query.inItunes();
        Integer playCountMin = query.playCountMin();
        Integer playCountMax = query.playCountMax();
        Integer albumCountMin = query.albumCountMin();
        Integer albumCountMax = query.albumCountMax();
        String birthDate = query.birthDate();
        String birthDateFrom = query.birthDateFrom();
        String birthDateTo = query.birthDateTo();
        String birthDateMode = query.birthDateMode();
        Integer songCountMin = query.songCountMin();
        Integer songCountMax = query.songCountMax();
        Integer itunesPresenceMin = query.itunesPresenceMin();
        Integer itunesPresenceMax = query.itunesPresenceMax();
        String itunesSongIdsJson = query.itunesSongIdsJson();
        String sortBy = query.sortBy();
        String sortDir = query.sortDir();
        String sortBy2 = query.sortBy2();
        String sortDir2 = query.sortDir2();
        String sortBy3 = query.sortBy3();
        String sortDir3 = query.sortDir3();
        int limit = query.limit();
        int offset = query.offset();
        // Build account filter subquery for the play_stats join
        StringBuilder accountFilterClause = new StringBuilder();
        List<Object> accountParams = new ArrayList<>();
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND p.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) accountFilterClause.append(",");
                accountFilterClause.append("?");
                accountParams.add(accounts.get(i));
            }
            accountFilterClause.append(")");
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND p.account NOT IN (");
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
            listenedDateFilterClause.append(" AND DATE(p.play_date) >= DATE(?)");
            listenedDateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(p.play_date) <= DATE(?)");
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
        sql.append("    COALESCE(consistency_stats.days_listened, 0) as days_listened, ");
        sql.append("    COALESCE(consistency_stats.weeks_listened, 0) as weeks_listened, ");
        sql.append("    COALESCE(consistency_stats.months_listened, 0) as months_listened, ");
        sql.append("    COALESCE(consistency_stats.years_listened, 0) as years_listened, ");
        sql.append("    a.organized, ");
        sql.append("    COALESCE(featured_stats.featured_song_count, 0) as featured_song_count, ");
        sql.append("    a.birth_date, ");
        sql.append("    a.death_date, ");
        sql.append("    (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = a.id) + CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as image_count, ");
        sql.append("    COALESCE(song_stats.total_length, 0) as total_song_length, ");
        sql.append("    COALESCE(fac_stats.featured_artist_count_stat, 0) as featured_artist_count_stat, ");
        sql.append("    COALESCE(solo_stats.solo_song_count, 0) as solo_song_count, ");
    sql.append("    COALESCE(swf_stats.songs_with_feat_count, 0) as songs_with_feat_count, ");
    sql.append("    COALESCE(standalone_stats.standalone_song_count, 0) as standalone_song_count, ");
    sql.append("    CASE WHEN EXISTS (SELECT 1 FROM ArtistImageTheme ait JOIN ArtistTheme t ON t.id = ait.theme_id WHERE t.is_active = 1 AND ait.artist_id = a.id) THEN 1 ELSE 0 END as has_theme_image, ");
        boolean needsItunesJoin = (itunesPresenceMin != null || itunesPresenceMax != null || "itunes_presence".equals(sortBy));
        if (needsItunesJoin) {
            sql.append("    CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(song_stats.song_count, 0), 0) as itunes_presence_ratio ");
        } else {
            sql.append("    NULL as itunes_presence_ratio ");
        }
    sql.append("FROM Artist a ");
    sql.append("LEFT JOIN Gender g ON a.gender_id = g.id ");
    sql.append("LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id ");
    sql.append("LEFT JOIN Genre gen ON a.genre_id = gen.id ");
    sql.append("LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id ");
    sql.append("LEFT JOIN Language l ON a.language_id = l.id ");
    sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as song_count, SUM(length_seconds) as total_length FROM Song GROUP BY artist_id) song_stats ON song_stats.artist_id = a.id ");
    sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as album_count FROM Album GROUP BY artist_id) album_stats ON album_stats.artist_id = a.id ");
    sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as featured_song_count FROM SongFeaturedArtist GROUP BY artist_id) featured_stats ON featured_stats.artist_id = a.id ");
    sql.append("LEFT JOIN (SELECT s.artist_id, COUNT(DISTINCT sfa.artist_id) as featured_artist_count_stat FROM Song s INNER JOIN SongFeaturedArtist sfa ON s.id = sfa.song_id GROUP BY s.artist_id) fac_stats ON fac_stats.artist_id = a.id ");
    sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as solo_song_count FROM Song s WHERE NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id) GROUP BY artist_id) solo_stats ON solo_stats.artist_id = a.id ");
    sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as songs_with_feat_count FROM Song s WHERE EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id) GROUP BY artist_id) swf_stats ON swf_stats.artist_id = a.id ");
    sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as standalone_song_count FROM Song WHERE album_id IS NULL GROUP BY artist_id) standalone_stats ON standalone_stats.artist_id = a.id ");
        if (needsItunesJoin) {
            sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as itunes_song_count FROM Song WHERE id IN (SELECT value FROM json_each(?)) GROUP BY artist_id) itunes_stats ON itunes_stats.artist_id = a.id ");
        }
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        String playStatsJoinType = ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) ? "INNER JOIN" : "LEFT JOIN";
        
        // Build play_stats subquery - two-level aggregation: inner by song_id (covering index scan),
        // outer by artist_id. This avoids a Play->Song join per row, letting SQLite use the
        // idx_play_cover_plays(song_id, account, play_date) index as a pure covering scan.
        sql.append(playStatsJoinType).append(" ( ");
        sql.append("    SELECT ");
        sql.append("        s.artist_id, ");
        sql.append("        SUM(ps.play_count) as play_count, ");
        sql.append("        SUM(ps.vatito_play_count) as vatito_play_count, ");
        sql.append("        SUM(ps.robertlover_play_count) as robertlover_play_count, ");
        sql.append("        SUM(CAST(s.length_seconds AS INTEGER) * ps.play_count) as time_listened, ");
        sql.append("        MIN(ps.first_listened) as first_listened, ");
        sql.append("        MAX(ps.last_listened) as last_listened ");
        sql.append("    FROM Song s ");
        sql.append("    JOIN ( ");
        sql.append("        SELECT p.song_id, ");
        sql.append("               COUNT(*) as play_count, ");
        sql.append("               SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as vatito_play_count, ");
        sql.append("               SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_play_count, ");
        sql.append("               MIN(p.play_date) as first_listened, ");
        sql.append("               MAX(p.play_date) as last_listened ");
        sql.append("        FROM Play p ");
        sql.append("        WHERE 1=1 ").append(accountFilterClause).append(listenedDateFilterClause).append(" ");
        sql.append("        GROUP BY p.song_id ");
        sql.append("    ) ps ON ps.song_id = s.id ");
        sql.append("    GROUP BY s.artist_id ");
        sql.append(") play_stats ON play_stats.artist_id = a.id ");
        sql.append("LEFT JOIN ( ");
        sql.append("    SELECT ");
        sql.append("        s.artist_id, ");
        sql.append("        COUNT(DISTINCT DATE(p.play_date)) as days_listened, ");
        sql.append("        COUNT(DISTINCT strftime('%Y-%W', p.play_date)) as weeks_listened, ");
        sql.append("        COUNT(DISTINCT strftime('%Y-%m', p.play_date)) as months_listened, ");
        sql.append("        COUNT(DISTINCT strftime('%Y', p.play_date)) as years_listened ");
        sql.append("    FROM Song s ");
        sql.append("    JOIN Play p ON p.song_id = s.id ");
        sql.append("    WHERE 1=1 ").append(accountFilterClause).append(listenedDateFilterClause).append(" ");
        sql.append("    GROUP BY s.artist_id ");
        sql.append(") consistency_stats ON consistency_stats.artist_id = a.id ");
        
        sql.append("WHERE 1=1 ");
        
        List<Object> params = new ArrayList<>();
        // itunes_stats JOIN appears before play_stats in SQL, so its ? must come first (when join is active)
        if (needsItunesJoin) {
            params.add(itunesSongIdsJson != null ? itunesSongIdsJson : "[]");
        }
        // Add account params and listened date params
        params.addAll(accountParams);
        params.addAll(listenedDateParams);
        params.addAll(accountParams);
        params.addAll(listenedDateParams);
        
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

        // Tag filter
        SqlFilterHelper.appendTagFilter(sql, params, "a.id", "ArtistTag", "artist_id", tagIds, tagMode);
        
        // First Listened Date filter
        String firstListenedSubquery = "(SELECT MIN(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last Listened Date filter
        String lastListenedSubquery = "(SELECT MAX(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, lastListenedSubquery, lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode);
        
        // Birth Date filter
        SqlFilterHelper.appendDateFilter(sql, params, "a.birth_date", birthDate, birthDateFrom, birthDateTo, birthDateMode);
        
        // Death Date filter
        SqlFilterHelper.appendDateFilter(sql, params, "a.death_date", deathDate, deathDateFrom, deathDateTo, deathDateMode);
        
        // Age filter
        if (ageMin != null || ageMax != null) {
            // Age is calculated as years between birth_date and (death_date or current date)
            String ageExpr = "CAST((julianday(COALESCE(a.death_date, DATE('now'))) - julianday(a.birth_date)) / 365.25 AS INTEGER)";
            if (ageMin != null) {
                sql.append(" AND a.birth_date IS NOT NULL AND ").append(ageExpr).append(" >= ? ");
                params.add(ageMin);
            }
            if (ageMax != null) {
                sql.append(" AND a.birth_date IS NOT NULL AND ").append(ageExpr).append(" <= ? ");
                params.add(ageMax);
            }
        }
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND a.organized = 1 ");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (a.organized = 0 OR a.organized IS NULL) ");
            }
        }
        
        // Image Count filter
        if (imageCountMin != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = a.id)) >= ? ");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = a.id)) <= ? ");
            params.add(imageCountMax);
        }
        
        // Image Theme filter
        if (imageTheme != null) {
            if ("has".equalsIgnoreCase(imageThemeMode)) {
                sql.append(" AND EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = a.id AND theme_id = ?) ");
                params.add(imageTheme);
            } else if ("doesntHave".equalsIgnoreCase(imageThemeMode)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = a.id AND theme_id = ?) ");
                params.add(imageTheme);
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
        
        // iTunes filter (pre-computed ID set via json_each)
        SqlFilterHelper.appendItunesIdFilter(sql, params, "a.id", itunesIdsJson, inItunes);
        
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
        
        // iTunes presence ratio filter
        if (itunesPresenceMin != null) {
            sql.append(" AND CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(song_stats.song_count, 0), 0) >= ? ");
            params.add(itunesPresenceMin);
        }
        if (itunesPresenceMax != null) {
            sql.append(" AND CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(song_stats.song_count, 0), 0) <= ? ");
            params.add(itunesPresenceMax);
        }
        
        appendArtistSortOrder(sql, sortBy, sortDir, sortBy2, sortDir2, sortBy3, sortDir3, query.randomSeed());
        
        sql.append(" LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> ArtistStatsRow.from(rs), params.toArray());
    }

    private void appendArtistSortOrder(StringBuilder sql, String sortBy, String sortDir,
                                       String sortBy2, String sortDir2,
                                       String sortBy3, String sortDir3,
                                       Integer randomSeed) {
        List<String> clauses = new ArrayList<>();
        List<String> appliedSorts = new ArrayList<>();

        appendArtistSortClause(clauses, appliedSorts, sortBy != null ? sortBy : "name", sortDir, randomSeed);
        appendArtistSortClause(clauses, appliedSorts, sortBy2, sortDir2, randomSeed);
        appendArtistSortClause(clauses, appliedSorts, sortBy3, sortDir3, randomSeed);

        clauses.add("play_count DESC");
        clauses.add("a.name ASC");
        sql.append(" ORDER BY ").append(String.join(", ", clauses));
    }

    private void appendArtistSortClause(List<String> clauses, List<String> appliedSorts, String sortBy, String sortDir, Integer randomSeed) {
        if (sortBy == null || sortBy.isBlank() || appliedSorts.contains(sortBy)) {
            return;
        }

        String direction = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        String clause = switch (sortBy) {
            case "age" -> "CAST((julianday(COALESCE(a.death_date, DATE('now'))) - julianday(a.birth_date)) / 365.25 AS INTEGER) " + direction + " NULLS LAST";
            case "avg_length" -> "CAST(COALESCE(song_stats.total_length, 0) AS REAL) / NULLIF(COALESCE(song_stats.song_count, 0), 0) " + direction + " NULLS LAST";
            case "avg_plays" -> "CAST(COALESCE(play_stats.play_count, 0) AS REAL) / NULLIF(COALESCE(song_stats.song_count, 0), 0) " + direction + " NULLS LAST";
            case "avg_plays_album" -> "CAST(COALESCE(play_stats.play_count, 0) AS REAL) / NULLIF(COALESCE(album_stats.album_count, 0), 0) " + direction + " NULLS LAST";
            case "birth_date" -> "a.birth_date " + direction + " NULLS LAST";
            case "death_date" -> "a.death_date " + direction + " NULLS LAST";
            case "songs" -> "song_count " + direction;
            case "featured" -> "featured_song_count " + direction;
            case "albums" -> "album_count " + direction;
            case "plays" -> "play_count " + direction;
            case "time" -> "time_listened " + direction;
            case "first_listened" -> "first_listened " + direction + " NULLS LAST";
            case "last_listened" -> "last_listened " + direction + " NULLS LAST";
            case "days_listened" -> "days_listened " + direction;
            case "weeks_listened" -> "weeks_listened " + direction;
            case "months_listened" -> "months_listened " + direction;
            case "years_listened" -> "years_listened " + direction;
            case "image_count" -> "image_count " + direction;
            case "random" -> RandomSortUtils.sqliteNumericExpression("a.id", randomSeed);
            case "country" -> "a.country " + direction + " NULLS LAST";
            case "ethnicity" -> "e.name " + direction + " NULLS LAST";
            case "featured_artist_count" -> "featured_artist_count_stat " + direction;
            case "genre" -> "gen.name " + direction + " NULLS LAST";
            case "language" -> "l.name " + direction + " NULLS LAST";
            case "legacy_plays" -> "robertlover_play_count " + direction;
            case "primary_plays" -> "vatito_play_count " + direction;
            case "solo_songs" -> "solo_song_count " + direction;
            case "songs_with_features" -> "songs_with_feat_count " + direction;
            case "subgenre" -> "sg.name " + direction + " NULLS LAST";
            case "itunes_presence" -> "CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(song_stats.song_count, 0), 0) " + direction + " NULLS LAST";
            default -> "a.name " + direction;
        };

        clauses.add(clause);
        appliedSorts.add(sortBy);
    }
    
    @Override
    public Long countArtistsWithFilters(ArtistStatsQuery query) {
        String name = query.name();
        List<Integer> genderIds = query.genderIds();
        String genderMode = query.genderMode();
        List<Integer> ethnicityIds = query.ethnicityIds();
        String ethnicityMode = query.ethnicityMode();
        List<Integer> genreIds = query.genreIds();
        String genreMode = query.genreMode();
        List<Integer> subgenreIds = query.subgenreIds();
        String subgenreMode = query.subgenreMode();
        List<Integer> languageIds = query.languageIds();
        String languageMode = query.languageMode();
        List<String> countries = query.countries();
        String countryMode = query.countryMode();
        List<Integer> tagIds = query.tagIds();
        String tagMode = query.tagMode();
        String deathDate = query.deathDate();
        String deathDateFrom = query.deathDateFrom();
        String deathDateTo = query.deathDateTo();
        String deathDateMode = query.deathDateMode();
        List<String> accounts = query.accounts();
        String accountMode = query.accountMode();
        Integer ageMin = query.ageMin();
        Integer ageMax = query.ageMax();
        String firstListenedDate = query.firstListenedDate();
        String firstListenedDateFrom = query.firstListenedDateFrom();
        String firstListenedDateTo = query.firstListenedDateTo();
        String firstListenedDateMode = query.firstListenedDateMode();
        String lastListenedDate = query.lastListenedDate();
        String lastListenedDateFrom = query.lastListenedDateFrom();
        String lastListenedDateTo = query.lastListenedDateTo();
        String lastListenedDateMode = query.lastListenedDateMode();
        String listenedDateFrom = query.listenedDateFrom();
        String listenedDateTo = query.listenedDateTo();
        String organized = query.organized();
        Integer imageCountMin = query.imageCountMin();
        Integer imageCountMax = query.imageCountMax();
        Integer imageTheme = query.imageTheme();
        String imageThemeMode = query.imageThemeMode();
        String isBand = query.isBand();
        String itunesIdsJson = query.itunesIdsJson();
        String inItunes = query.inItunes();
        Integer playCountMin = query.playCountMin();
        Integer playCountMax = query.playCountMax();
        Integer albumCountMin = query.albumCountMin();
        Integer albumCountMax = query.albumCountMax();
        String birthDate = query.birthDate();
        String birthDateFrom = query.birthDateFrom();
        String birthDateTo = query.birthDateTo();
        String birthDateMode = query.birthDateMode();
        Integer songCountMin = query.songCountMin();
        Integer songCountMax = query.songCountMax();
        Integer itunesPresenceMin = query.itunesPresenceMin();
        Integer itunesPresenceMax = query.itunesPresenceMax();
        String itunesSongIdsJson = query.itunesSongIdsJson();
        // Build listened date filter clause
        StringBuilder listenedDateFilterClause = new StringBuilder();
        List<Object> listenedDateParams = new ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(p.play_date) >= DATE(?)");
            listenedDateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(p.play_date) <= DATE(?)");
            listenedDateParams.add(listenedDateTo);
        }

        StringBuilder accountFilterClause = new StringBuilder();
        List<Object> accountParams = new ArrayList<>();
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND p.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) accountFilterClause.append(",");
                accountFilterClause.append("?");
                accountParams.add(accounts.get(i));
            }
            accountFilterClause.append(")");
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND p.account NOT IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) accountFilterClause.append(",");
                accountFilterClause.append("?");
                accountParams.add(accounts.get(i));
            }
            accountFilterClause.append(")");
        }
        
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        
        boolean needsItunesJoin = (itunesPresenceMin != null || itunesPresenceMax != null);
        StringBuilder sql = new StringBuilder();
        
        // Build FROM clause with optional JOINs for account/date filtering and iTunes presence
        sql.append("SELECT COUNT(DISTINCT a.id) FROM Artist a ");
        if ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) {
            sql.append("INNER JOIN Song s ON s.artist_id = a.id INNER JOIN Play p ON p.song_id = s.id ");
        }
        // Pre-aggregate iTunes stats as JOINs to avoid correlated subqueries per row
        if (needsItunesJoin) {
            sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as itunes_song_count FROM Song WHERE id IN (SELECT value FROM json_each(?)) GROUP BY artist_id) itunes_stats ON itunes_stats.artist_id = a.id ");
            sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as song_count FROM Song GROUP BY artist_id) count_song_stats ON count_song_stats.artist_id = a.id ");
        }
        sql.append("WHERE 1=1 ");
        if ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) {
            if (accounts != null && !accounts.isEmpty()) {
                sql.append(accountFilterClause);
            }
            sql.append(listenedDateFilterClause);
        }
        
        List<Object> params = new ArrayList<>();
        
        // iTunes JOIN param comes first (appears in FROM clause before WHERE conditions)
        if (needsItunesJoin) {
            params.add(itunesSongIdsJson != null ? itunesSongIdsJson : "[]");
        }
        if ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) {
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
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

        // Tag filter
        SqlFilterHelper.appendTagFilter(sql, params, "a.id", "ArtistTag", "artist_id", tagIds, tagMode);
        
        // First Listened Date filter
        String firstListenedSubquery = "(SELECT MIN(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last Listened Date filter
        String lastListenedSubquery = "(SELECT MAX(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, lastListenedSubquery, lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode);
        
        // Birth Date filter
        SqlFilterHelper.appendDateFilter(sql, params, "a.birth_date", birthDate, birthDateFrom, birthDateTo, birthDateMode);
        
        // Death Date filter
        SqlFilterHelper.appendDateFilter(sql, params, "a.death_date", deathDate, deathDateFrom, deathDateTo, deathDateMode);
        
        // Age filter
        if (ageMin != null || ageMax != null) {
            // Age is calculated as years between birth_date and (death_date or current date)
            String ageExpr = "CAST((julianday(COALESCE(a.death_date, DATE('now'))) - julianday(a.birth_date)) / 365.25 AS INTEGER)";
            if (ageMin != null) {
                sql.append(" AND a.birth_date IS NOT NULL AND ").append(ageExpr).append(" >= ? ");
                params.add(ageMin);
            }
            if (ageMax != null) {
                sql.append(" AND a.birth_date IS NOT NULL AND ").append(ageExpr).append(" <= ? ");
                params.add(ageMax);
            }
        }
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND a.organized = 1 ");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (a.organized = 0 OR a.organized IS NULL) ");
            }
        }
        
        // Image Count filter (counts primary image + gallery images)
        if (imageCountMin != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = a.id)) >= ? ");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = a.id)) <= ? ");
            params.add(imageCountMax);
        }
        
        // Image Theme filter
        if (imageTheme != null) {
            if ("has".equalsIgnoreCase(imageThemeMode)) {
                sql.append(" AND EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = a.id AND theme_id = ?) ");
                params.add(imageTheme);
            } else if ("doesntHave".equalsIgnoreCase(imageThemeMode)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = a.id AND theme_id = ?) ");
                params.add(imageTheme);
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
        
        // iTunes filter (pre-computed ID set via json_each)
        SqlFilterHelper.appendItunesIdFilter(sql, params, "a.id", itunesIdsJson, inItunes);
        
        // Play count filter (uses subquery since count query doesn't have play_stats join)
        if (playCountMin != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Play p JOIN Song song ON p.song_id = song.id WHERE song.artist_id = a.id");
            sql.append(accountFilterClause).append(listenedDateFilterClause);
            sql.append("), 0) >= ? ");
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
            params.add(playCountMin);
        }
        if (playCountMax != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Play p JOIN Song song ON p.song_id = song.id WHERE song.artist_id = a.id");
            sql.append(accountFilterClause).append(listenedDateFilterClause);
            sql.append("), 0) <= ? ");
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
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
        
        // iTunes presence ratio filter (using pre-joined itunes_stats)
        if (itunesPresenceMin != null) {
            sql.append(" AND CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(count_song_stats.song_count, 0), 0) >= ? ");
            params.add(itunesPresenceMin);
        }
        if (itunesPresenceMax != null) {
            sql.append(" AND CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(count_song_stats.song_count, 0), 0) <= ? ");
            params.add(itunesPresenceMax);
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0L;
    }
    
    @Override
    public Map<Integer, Long> countArtistsByGenderWithFilters(ArtistStatsQuery query) {
        String name = query.name();
        List<Integer> genderIds = query.genderIds();
        String genderMode = query.genderMode();
        List<Integer> ethnicityIds = query.ethnicityIds();
        String ethnicityMode = query.ethnicityMode();
        List<Integer> genreIds = query.genreIds();
        String genreMode = query.genreMode();
        List<Integer> subgenreIds = query.subgenreIds();
        String subgenreMode = query.subgenreMode();
        List<Integer> languageIds = query.languageIds();
        String languageMode = query.languageMode();
        List<String> countries = query.countries();
        String countryMode = query.countryMode();
        List<Integer> tagIds = query.tagIds();
        String tagMode = query.tagMode();
        String deathDate = query.deathDate();
        String deathDateFrom = query.deathDateFrom();
        String deathDateTo = query.deathDateTo();
        String deathDateMode = query.deathDateMode();
        List<String> accounts = query.accounts();
        String accountMode = query.accountMode();
        Integer ageMin = query.ageMin();
        Integer ageMax = query.ageMax();
        String firstListenedDate = query.firstListenedDate();
        String firstListenedDateFrom = query.firstListenedDateFrom();
        String firstListenedDateTo = query.firstListenedDateTo();
        String firstListenedDateMode = query.firstListenedDateMode();
        String lastListenedDate = query.lastListenedDate();
        String lastListenedDateFrom = query.lastListenedDateFrom();
        String lastListenedDateTo = query.lastListenedDateTo();
        String lastListenedDateMode = query.lastListenedDateMode();
        String listenedDateFrom = query.listenedDateFrom();
        String listenedDateTo = query.listenedDateTo();
        String organized = query.organized();
        Integer imageCountMin = query.imageCountMin();
        Integer imageCountMax = query.imageCountMax();
        Integer imageTheme = query.imageTheme();
        String imageThemeMode = query.imageThemeMode();
        String isBand = query.isBand();
        String itunesIdsJson = query.itunesIdsJson();
        String inItunes = query.inItunes();
        Integer playCountMin = query.playCountMin();
        Integer playCountMax = query.playCountMax();
        Integer albumCountMin = query.albumCountMin();
        Integer albumCountMax = query.albumCountMax();
        String birthDate = query.birthDate();
        String birthDateFrom = query.birthDateFrom();
        String birthDateTo = query.birthDateTo();
        String birthDateMode = query.birthDateMode();
        Integer songCountMin = query.songCountMin();
        Integer songCountMax = query.songCountMax();
        Integer itunesPresenceMin = query.itunesPresenceMin();
        Integer itunesPresenceMax = query.itunesPresenceMax();
        String itunesSongIdsJson = query.itunesSongIdsJson();
        // Build listened date filter clause
        StringBuilder listenedDateFilterClause = new StringBuilder();
        List<Object> listenedDateParams = new ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(p.play_date) >= DATE(?)");
            listenedDateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            listenedDateFilterClause.append(" AND DATE(p.play_date) <= DATE(?)");
            listenedDateParams.add(listenedDateTo);
        }

        StringBuilder accountFilterClause = new StringBuilder();
        List<Object> accountParams = new ArrayList<>();
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND p.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) accountFilterClause.append(",");
                accountFilterClause.append("?");
                accountParams.add(accounts.get(i));
            }
            accountFilterClause.append(")");
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            accountFilterClause.append(" AND p.account NOT IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) accountFilterClause.append(",");
                accountFilterClause.append("?");
                accountParams.add(accounts.get(i));
            }
            accountFilterClause.append(")");
        }
        
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        
        boolean needsItunesJoinGender = (itunesPresenceMin != null || itunesPresenceMax != null);
        StringBuilder sql = new StringBuilder();
        
        // Build FROM clause with optional JOINs for account/date filtering and iTunes presence
        sql.append("SELECT a.gender_id, COUNT(DISTINCT a.id) as cnt FROM Artist a ");
        if ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) {
            sql.append("INNER JOIN Song s ON s.artist_id = a.id INNER JOIN Play p ON p.song_id = s.id ");
        }
        // Pre-aggregate iTunes stats as JOINs to avoid correlated subqueries per row
        if (needsItunesJoinGender) {
            sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as itunes_song_count FROM Song WHERE id IN (SELECT value FROM json_each(?)) GROUP BY artist_id) itunes_stats ON itunes_stats.artist_id = a.id ");
            sql.append("LEFT JOIN (SELECT artist_id, COUNT(*) as song_count FROM Song GROUP BY artist_id) count_song_stats ON count_song_stats.artist_id = a.id ");
        }
        sql.append("WHERE 1=1 ");
        if ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) {
            if (accounts != null && !accounts.isEmpty()) {
                sql.append(accountFilterClause);
            }
            sql.append(listenedDateFilterClause);
        }
        
        List<Object> params = new ArrayList<>();
        
        // iTunes JOIN param comes first (appears in FROM clause before WHERE conditions)
        if (needsItunesJoinGender) {
            params.add(itunesSongIdsJson != null ? itunesSongIdsJson : "[]");
        }
        if ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) {
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
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

        // Tag filter
        SqlFilterHelper.appendTagFilter(sql, params, "a.id", "ArtistTag", "artist_id", tagIds, tagMode);
        
        // First Listened Date filter
        String firstListenedSubquery = "(SELECT MIN(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last Listened Date filter
        String lastListenedSubquery = "(SELECT MAX(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE artist_id = a.id))";
        SqlFilterHelper.appendDateFilter(sql, params, lastListenedSubquery, lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode);
        
        // Birth Date filter
        SqlFilterHelper.appendDateFilter(sql, params, "a.birth_date", birthDate, birthDateFrom, birthDateTo, birthDateMode);
        
        // Death Date filter
        SqlFilterHelper.appendDateFilter(sql, params, "a.death_date", deathDate, deathDateFrom, deathDateTo, deathDateMode);
        
        // Age filter
        if (ageMin != null || ageMax != null) {
            String ageExpr = "CAST((julianday(COALESCE(a.death_date, DATE('now'))) - julianday(a.birth_date)) / 365.25 AS INTEGER)";
            if (ageMin != null) {
                sql.append(" AND a.birth_date IS NOT NULL AND ").append(ageExpr).append(" >= ? ");
                params.add(ageMin);
            }
            if (ageMax != null) {
                sql.append(" AND a.birth_date IS NOT NULL AND ").append(ageExpr).append(" <= ? ");
                params.add(ageMax);
            }
        }
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND a.organized = 1 ");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (a.organized = 0 OR a.organized IS NULL) ");
            }
        }
        
        // Image Count filter
        if (imageCountMin != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = a.id)) >= ? ");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = a.id)) <= ? ");
            params.add(imageCountMax);
        }
        
        // Image Theme filter
        if (imageTheme != null) {
            if ("has".equalsIgnoreCase(imageThemeMode)) {
                sql.append(" AND EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = a.id AND theme_id = ?) ");
                params.add(imageTheme);
            } else if ("doesntHave".equalsIgnoreCase(imageThemeMode)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = a.id AND theme_id = ?) ");
                params.add(imageTheme);
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
        
        // iTunes filter (pre-computed ID set via json_each)
        SqlFilterHelper.appendItunesIdFilter(sql, params, "a.id", itunesIdsJson, inItunes);
        
        // Play count filter
        if (playCountMin != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Play p JOIN Song song ON p.song_id = song.id WHERE song.artist_id = a.id");
            sql.append(accountFilterClause).append(listenedDateFilterClause);
            sql.append("), 0) >= ? ");
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
            params.add(playCountMin);
        }
        if (playCountMax != null) {
            sql.append(" AND COALESCE((SELECT COUNT(*) FROM Play p JOIN Song song ON p.song_id = song.id WHERE song.artist_id = a.id");
            sql.append(accountFilterClause).append(listenedDateFilterClause);
            sql.append("), 0) <= ? ");
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
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
        
        // iTunes presence ratio filter (using pre-joined itunes_stats)
        if (itunesPresenceMin != null) {
            sql.append(" AND CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(count_song_stats.song_count, 0), 0) >= ? ");
            params.add(itunesPresenceMin);
        }
        if (itunesPresenceMax != null) {
            sql.append(" AND CAST(COALESCE(itunes_stats.itunes_song_count, 0) AS REAL) * 100.0 / NULLIF(COALESCE(count_song_stats.song_count, 0), 0) <= ? ");
            params.add(itunesPresenceMax);
        }
        
        // Add GROUP BY
        sql.append(" GROUP BY a.gender_id");
        
        Map<Integer, Long> result = new HashMap<>();
        jdbcTemplate.query(sql.toString(), rs -> {
            Integer genderId = rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null;
            Long cnt = rs.getLong("cnt");
            result.put(genderId, cnt);
        }, params.toArray());
        
        return result;
    }
}
