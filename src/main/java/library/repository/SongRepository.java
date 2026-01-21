package library.repository;

import library.dto.ChartFilterDTO;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class SongRepository {
    
    private final JdbcTemplate jdbcTemplate;
    
    public SongRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<Object[]> findSongsWithStats(String name, String artistName, String albumName,
                                              List<Integer> genreIds, String genreMode,
                                              List<Integer> subgenreIds, String subgenreMode,
                                              List<Integer> languageIds, String languageMode,
                                              List<Integer> genderIds, String genderMode,
                                              List<Integer> ethnicityIds, String ethnicityMode,
                                              List<String> countries, String countryMode,
                                              List<String> accounts, String accountMode,
                                              String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                              String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                              String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                              String listenedDateFrom, String listenedDateTo,
                                              String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                                              Integer ageMin, Integer ageMax, String ageMode,
                                              Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                              String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                              String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                              Integer playCountMin, Integer playCountMax,
                                              Integer lengthMin, Integer lengthMax, String lengthMode,
                                              Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                              Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                              Integer yearlyChartPeak, Integer yearlyChartYears,
                                              String sortBy, String sortDirection, int limit, int offset) {
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
        sql.append("""
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
                COALESCE(s.release_date, alb.release_date) as release_date,
                s.length_seconds,
                CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id) THEN 1 ELSE 0 END as has_image,
                gender.name as gender_name,
                COALESCE(play_stats.play_count, 0) as play_count,
                COALESCE(play_stats.vatito_play_count, 0) as vatito_play_count,
                COALESCE(play_stats.robertlover_play_count, 0) as robertlover_play_count,
                COALESCE(s.length_seconds * play_stats.play_count, 0) as time_listened,
                play_stats.first_listened,
                play_stats.last_listened,
                ar.country as country,
                s.organized,
                CASE WHEN alb.image IS NOT NULL THEN 1 ELSE 0 END as album_has_image,
                s.is_single
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
            """);
        
        // Use INNER JOIN when account filter is includes mode or when listened date filter is applied
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        String playStatsJoinType = ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) ? "INNER JOIN" : "LEFT JOIN";
        sql.append(playStatsJoinType).append(""" 
             (
                SELECT 
                    scr.song_id,
                    COUNT(*) as play_count,
                    SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as vatito_play_count,
                    SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_play_count,
                    MIN(scr.scrobble_date) as first_listened,
                    MAX(scr.scrobble_date) as last_listened
                FROM Scrobble scr
                WHERE 1=1 """);
        sql.append(accountFilterClause);
        sql.append(listenedDateFilterClause);
        sql.append("""
                GROUP BY scr.song_id
            ) play_stats ON play_stats.song_id = s.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        // Add account params only once now (play_stats subquery)
        params.addAll(accountParams);
        // Add listened date params
        params.addAll(listenedDateParams);
        
        // Name filters with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("s.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("ar.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(artistName) + "%");
        }
        
        // Album name filter
        if (albumName != null && !albumName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("alb.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(albumName) + "%");
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
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND s.organized = 1 ");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (s.organized = 0 OR s.organized IS NULL) ");
            }
        }
        
        // Image Count filter (counts primary image + gallery images)
        if (imageCountMin != null) {
            sql.append(" AND ((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id)) >= ? ");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id)) <= ? ");
            params.add(imageCountMax);
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
        
        // Has Featured Artists filter
        if (hasFeaturedArtists != null && !hasFeaturedArtists.isEmpty()) {
            if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id) ");
            } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id) ");
            }
        }
        
        // Is Band filter (from artist)
        if (isBand != null && !isBand.isEmpty()) {
            if ("true".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 1 ");
            } else if ("false".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 0 ");
            }
        }
        
        // Is Single filter
        if (isSingle != null && !isSingle.isEmpty()) {
            if ("true".equalsIgnoreCase(isSingle)) {
                sql.append(" AND s.is_single = 1 ");
            } else if ("false".equalsIgnoreCase(isSingle)) {
                sql.append(" AND s.is_single = 0 ");
            }
        }
        
        // Age filter (artist's current age, or age at death if deceased)
        if (ageMin != null || ageMax != null) {
            String ageExpr = "CAST((julianday(COALESCE(ar.death_date, DATE('now'))) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(ageExpr).append(" >= ? ");
                params.add(ageMin);
            }
            if (ageMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(ageExpr).append(" <= ? ");
                params.add(ageMax);
            }
        }
        
        // Age at Release filter (artist's age when song was released)
        if (ageAtReleaseMin != null || ageAtReleaseMax != null) {
            String ageAtReleaseExpr = "CAST((julianday(COALESCE(s.release_date, alb.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ? ");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ? ");
                params.add(ageAtReleaseMax);
            }
        }
        
        // Birth Date filter
        if (birthDateMode != null && !birthDateMode.isEmpty()) {
            switch (birthDateMode) {
                case "isnull":
                    sql.append(" AND ar.birth_date IS NULL");
                    break;
                case "isnotnull":
                    sql.append(" AND ar.birth_date IS NOT NULL");
                    break;
                case "exact":
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) = ?");
                        params.add(birthDate);
                    }
                    break;
                case "gte":
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) >= ?");
                        params.add(birthDate);
                    }
                    break;
                case "lte":
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) <= ?");
                        params.add(birthDate);
                    }
                    break;
                case "between":
                    if (birthDateFrom != null && !birthDateFrom.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) >= ?");
                        params.add(birthDateFrom);
                    }
                    if (birthDateTo != null && !birthDateTo.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) <= ?");
                        params.add(birthDateTo);
                    }
                    break;
            }
        }
        
        // Death Date filter
        if (deathDateMode != null && !deathDateMode.isEmpty()) {
            switch (deathDateMode) {
                case "isnull":
                    sql.append(" AND ar.death_date IS NULL");
                    break;
                case "isnotnull":
                    sql.append(" AND ar.death_date IS NOT NULL");
                    break;
                case "exact":
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) = ?");
                        params.add(deathDate);
                    }
                    break;
                case "gte":
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) >= ?");
                        params.add(deathDate);
                    }
                    break;
                case "lte":
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) <= ?");
                        params.add(deathDate);
                    }
                    break;
                case "between":
                    if (deathDateFrom != null && !deathDateFrom.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) >= ?");
                        params.add(deathDateFrom);
                    }
                    if (deathDateTo != null && !deathDateTo.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) <= ?");
                        params.add(deathDateTo);
                    }
                    break;
            }
        }
        
        // Length filter (song length_seconds)
        if (lengthMode != null && !lengthMode.isEmpty()) {
            if ("null".equalsIgnoreCase(lengthMode) || "zero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (s.length_seconds IS NULL OR s.length_seconds = 0) ");
            } else if ("notnull".equalsIgnoreCase(lengthMode) || "nonzero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (s.length_seconds IS NOT NULL AND s.length_seconds > 0) ");
            } else if ("lt".equalsIgnoreCase(lengthMode) && lengthMax != null) {
                sql.append(" AND s.length_seconds < ? ");
                params.add(lengthMax);
            } else if ("gt".equalsIgnoreCase(lengthMode) && lengthMin != null) {
                sql.append(" AND s.length_seconds > ? ");
                params.add(lengthMin);
            } else {
                // Default "range" mode
                if (lengthMin != null) {
                    sql.append(" AND s.length_seconds >= ? ");
                    params.add(lengthMin);
                }
                if (lengthMax != null) {
                    sql.append(" AND s.length_seconds <= ? ");
                    params.add(lengthMax);
                }
            }
        }
        
        // Weekly chart filter (peak position <= specified, total weeks >= specified)
        if (weeklyChartPeak != null || weeklyChartWeeks != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as weeks ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly'");
            sql.append(") chart_stats WHERE 1=1");
            if (weeklyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(weeklyChartPeak);
            }
            if (weeklyChartWeeks != null) {
                sql.append(" AND chart_stats.weeks >= ?");
                params.add(weeklyChartWeeks);
            }
            sql.append(")");
        }
        
        // Seasonal chart filter (peak position <= specified, total seasons >= specified)
        if (seasonalChartPeak != null || seasonalChartSeasons != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as seasons ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal'");
            sql.append(") chart_stats WHERE 1=1");
            if (seasonalChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(seasonalChartPeak);
            }
            if (seasonalChartSeasons != null) {
                sql.append(" AND chart_stats.seasons >= ?");
                params.add(seasonalChartSeasons);
            }
            sql.append(")");
        }
        
        // Yearly chart filter (peak position <= specified, total years >= specified)
        if (yearlyChartPeak != null || yearlyChartYears != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as years ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'yearly'");
            sql.append(") chart_stats WHERE 1=1");
            if (yearlyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(yearlyChartPeak);
            }
            if (yearlyChartYears != null) {
                sql.append(" AND chart_stats.years >= ?");
                params.add(yearlyChartYears);
            }
            sql.append(")");
        }
        
        // Determine sort direction
        String dir = "desc".equalsIgnoreCase(sortDirection) ? "DESC" : "ASC";
        String nullsOrder = "desc".equalsIgnoreCase(sortDirection) ? "NULLS LAST" : "NULLS FIRST";
        
        // Sorting with direction
        switch (sortBy != null ? sortBy : "name") {
            case "artist" -> sql.append(" ORDER BY ar.name ").append(dir).append(", s.name");
            case "album" -> sql.append(" ORDER BY alb.name ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "release_date" -> sql.append(" ORDER BY COALESCE(s.release_date, alb.release_date) ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "length" -> sql.append(" ORDER BY s.length_seconds ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "plays" -> sql.append(" ORDER BY play_count ").append(dir).append(", s.name");
            case "time" -> sql.append(" ORDER BY (s.length_seconds * play_count) ").append(dir).append(", s.name");
            case "first_listened" -> sql.append(" ORDER BY first_listened ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "last_listened" -> sql.append(" ORDER BY last_listened ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            case "age_at_release" -> sql.append(" ORDER BY CAST((julianday(COALESCE(s.release_date, alb.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER) ").append(dir).append(" ").append(nullsOrder).append(", s.name");
            default -> sql.append(" ORDER BY s.name ").append(dir);
        }
        
        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            Object[] row = new Object[29];
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
            row[15] = rs.getString("release_date");
            row[16] = rs.getObject("length_seconds");
            row[17] = rs.getInt("has_image");
            row[18] = rs.getString("gender_name");
            row[19] = rs.getInt("play_count");
            row[20] = rs.getInt("vatito_play_count");
            row[21] = rs.getInt("robertlover_play_count");
            row[22] = rs.getLong("time_listened");
            row[23] = rs.getString("first_listened");
            row[24] = rs.getString("last_listened");
            row[25] = rs.getString("country");
            row[26] = rs.getObject("organized");
            row[27] = rs.getInt("album_has_image");
            row[28] = rs.getInt("is_single");
            return row;
        }, params.toArray());
    }
    
    public long countSongsWithFilters(String name, String artistName, String albumName,
                                      List<Integer> genreIds, String genreMode,
                                      List<Integer> subgenreIds, String subgenreMode,
                                      List<Integer> languageIds, String languageMode,
                                      List<Integer> genderIds, String genderMode,
                                      List<Integer> ethnicityIds, String ethnicityMode,
                                      List<String> countries, String countryMode,
                                      List<String> accounts, String accountMode,
                                      String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                      String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                      String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                      String listenedDateFrom, String listenedDateTo,
                                      String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                                      Integer ageMin, Integer ageMax, String ageMode,
                                      Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                      String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                      String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                      Integer playCountMin, Integer playCountMax,
                                      Integer lengthMin, Integer lengthMax, String lengthMode,
                                      Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                      Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                      Integer yearlyChartPeak, Integer yearlyChartYears) {
        // Build account filter subquery for play_stats if we need play count filter
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
        
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        
        StringBuilder sql = new StringBuilder();
        sql.append("""
            SELECT COUNT(*)
            FROM Song s
            LEFT JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            """);
        
        // Add play_stats JOIN if we need to filter by play count or listened date
        if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
            String joinType = hasListenedDateFilter ? "INNER JOIN" : "LEFT JOIN";
            sql.append(joinType).append("""
                 (
                    SELECT scr.song_id, COUNT(*) as play_count
                    FROM Scrobble scr
                    WHERE 1=1 """);
            sql.append(accountFilterClause);
            sql.append(listenedDateFilterClause);
            sql.append("""
                    GROUP BY scr.song_id
                ) play_stats ON play_stats.song_id = s.id
                """);
        }
        
        sql.append(" WHERE 1=1 ");
        
        List<Object> params = new ArrayList<>();
        // Add account params for play_stats subquery
        if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
        }
        
        // Name filters with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("s.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("ar.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(artistName) + "%");
        }
        
        // Album name filter
        if (albumName != null && !albumName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("alb.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(albumName) + "%");
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
        
        // Account filter - filter to songs that have plays from the selected account(s)
        if (accounts != null && !accounts.isEmpty()) {
            if ("includes".equalsIgnoreCase(accountMode)) {
                sql.append(" AND EXISTS (SELECT 1 FROM Scrobble scr WHERE scr.song_id = s.id AND scr.account IN (");
                for (int i = 0; i < accounts.size(); i++) {
                    if (i > 0) sql.append(",");
                    sql.append("?");
                    params.add(accounts.get(i));
                }
                sql.append("))");
            } else if ("excludes".equalsIgnoreCase(accountMode)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM Scrobble scr WHERE scr.song_id = s.id AND scr.account IN (");
                for (int i = 0; i < accounts.size(); i++) {
                    if (i > 0) sql.append(",");
                    sql.append("?");
                    params.add(accounts.get(i));
                }
                sql.append("))");
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
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND s.organized = 1 ");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (s.organized = 0 OR s.organized IS NULL) ");
            }
        }
        
        // Image Count filter (counts primary image + gallery images)
        if (imageCountMin != null) {
            sql.append(" AND ((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id)) >= ? ");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id)) <= ? ");
            params.add(imageCountMax);
        }
        
        // Has Featured Artists filter
        if (hasFeaturedArtists != null && !hasFeaturedArtists.isEmpty()) {
            if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id) ");
            } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id) ");
            }
        }
        
        // Is Band filter (from artist)
        if (isBand != null && !isBand.isEmpty()) {
            if ("true".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 1 ");
            } else if ("false".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 0 ");
            }
        }
        
        // Is Single filter
        if (isSingle != null && !isSingle.isEmpty()) {
            if ("true".equalsIgnoreCase(isSingle)) {
                sql.append(" AND s.is_single = 1 ");
            } else if ("false".equalsIgnoreCase(isSingle)) {
                sql.append(" AND s.is_single = 0 ");
            }
        }
        
        // Age filter (artist's current age, or age at death if deceased)
        if (ageMin != null || ageMax != null) {
            String ageExpr = "CAST((julianday(COALESCE(ar.death_date, DATE('now'))) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(ageExpr).append(" >= ? ");
                params.add(ageMin);
            }
            if (ageMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(ageExpr).append(" <= ? ");
                params.add(ageMax);
            }
        }
        
        // Age at Release filter (artist's age when song was released)
        if (ageAtReleaseMin != null || ageAtReleaseMax != null) {
            String ageAtReleaseExpr = "CAST((julianday(COALESCE(s.release_date, alb.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ? ");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ? ");
                params.add(ageAtReleaseMax);
            }
        }
        
        // Birth Date filter
        if (birthDateMode != null && !birthDateMode.isEmpty()) {
            switch (birthDateMode) {
                case "isnull":
                    sql.append(" AND ar.birth_date IS NULL");
                    break;
                case "isnotnull":
                    sql.append(" AND ar.birth_date IS NOT NULL");
                    break;
                case "exact":
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) = ?");
                        params.add(birthDate);
                    }
                    break;
                case "gte":
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) >= ?");
                        params.add(birthDate);
                    }
                    break;
                case "lte":
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) <= ?");
                        params.add(birthDate);
                    }
                    break;
                case "between":
                    if (birthDateFrom != null && !birthDateFrom.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) >= ?");
                        params.add(birthDateFrom);
                    }
                    if (birthDateTo != null && !birthDateTo.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) <= ?");
                        params.add(birthDateTo);
                    }
                    break;
            }
        }
        
        // Death Date filter
        if (deathDateMode != null && !deathDateMode.isEmpty()) {
            switch (deathDateMode) {
                case "isnull":
                    sql.append(" AND ar.death_date IS NULL");
                    break;
                case "isnotnull":
                    sql.append(" AND ar.death_date IS NOT NULL");
                    break;
                case "exact":
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) = ?");
                        params.add(deathDate);
                    }
                    break;
                case "gte":
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) >= ?");
                        params.add(deathDate);
                    }
                    break;
                case "lte":
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) <= ?");
                        params.add(deathDate);
                    }
                    break;
                case "between":
                    if (deathDateFrom != null && !deathDateFrom.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) >= ?");
                        params.add(deathDateFrom);
                    }
                    if (deathDateTo != null && !deathDateTo.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) <= ?");
                        params.add(deathDateTo);
                    }
                    break;
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
        
        // Length filter (song length_seconds)
        if (lengthMode != null && !lengthMode.isEmpty()) {
            if ("null".equalsIgnoreCase(lengthMode) || "zero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (s.length_seconds IS NULL OR s.length_seconds = 0) ");
            } else if ("notnull".equalsIgnoreCase(lengthMode) || "nonzero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (s.length_seconds IS NOT NULL AND s.length_seconds > 0) ");
            } else if ("lt".equalsIgnoreCase(lengthMode) && lengthMax != null) {
                sql.append(" AND s.length_seconds < ? ");
                params.add(lengthMax);
            } else if ("gt".equalsIgnoreCase(lengthMode) && lengthMin != null) {
                sql.append(" AND s.length_seconds > ? ");
                params.add(lengthMin);
            } else {
                // Default "range" mode
                if (lengthMin != null) {
                    sql.append(" AND s.length_seconds >= ? ");
                    params.add(lengthMin);
                }
                if (lengthMax != null) {
                    sql.append(" AND s.length_seconds <= ? ");
                    params.add(lengthMax);
                }
            }
        }
        
        // Weekly chart filter (peak position <= specified, total weeks >= specified)
        if (weeklyChartPeak != null || weeklyChartWeeks != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as weeks ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly'");
            sql.append(") chart_stats WHERE 1=1");
            if (weeklyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(weeklyChartPeak);
            }
            if (weeklyChartWeeks != null) {
                sql.append(" AND chart_stats.weeks >= ?");
                params.add(weeklyChartWeeks);
            }
            sql.append(")");
        }
        
        // Seasonal chart filter (peak position <= specified, total seasons >= specified)
        if (seasonalChartPeak != null || seasonalChartSeasons != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as seasons ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal'");
            sql.append(") chart_stats WHERE 1=1");
            if (seasonalChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(seasonalChartPeak);
            }
            if (seasonalChartSeasons != null) {
                sql.append(" AND chart_stats.seasons >= ?");
                params.add(seasonalChartSeasons);
            }
            sql.append(")");
        }
        
        // Yearly chart filter (peak position <= specified, total years >= specified)
        if (yearlyChartPeak != null || yearlyChartYears != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as years ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'yearly'");
            sql.append(") chart_stats WHERE 1=1");
            if (yearlyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(yearlyChartPeak);
            }
            if (yearlyChartYears != null) {
                sql.append(" AND chart_stats.years >= ?");
                params.add(yearlyChartYears);
            }
            sql.append(")");
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0;
    }
    
    /**
     * Count songs grouped by gender for the filtered dataset.
     * Returns a Map with gender_id as key and count as value.
     * More efficient than loading all songs and counting in memory.
     * Uses COALESCE(s.override_gender_id, ar.gender_id) as the effective gender.
     */
    public Map<Integer, Long> countSongsByGenderWithFilters(String name, String artistName, String albumName,
                                              List<Integer> genreIds, String genreMode,
                                              List<Integer> subgenreIds, String subgenreMode,
                                              List<Integer> languageIds, String languageMode,
                                              List<Integer> genderIds, String genderMode,
                                              List<Integer> ethnicityIds, String ethnicityMode,
                                              List<String> countries, String countryMode,
                                              List<String> accounts, String accountMode,
                                              String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                              String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                              String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                              String listenedDateFrom, String listenedDateTo,
                                              String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                                              Integer ageMin, Integer ageMax, String ageMode,
                                              Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                              String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                              String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                              String inItunes,
                                              Integer playCountMin, Integer playCountMax,
                                              Integer lengthMin, Integer lengthMax, String lengthMode,
                                              Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                              Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                              Integer yearlyChartPeak, Integer yearlyChartYears) {
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
        boolean hasListenedDateFilter = listenedDateFilterClause.length() > 0;
        
        StringBuilder sql = new StringBuilder();
        
        // Build base query with effective gender for grouping
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            sql.append(
                "SELECT COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id, COUNT(DISTINCT s.id) as cnt " +
                "FROM Song s " +
                "LEFT JOIN Artist ar ON s.artist_id = ar.id " +
                "LEFT JOIN Album al ON s.album_id = al.id ");
            
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble scr WHERE 1=1 ");
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append(" GROUP BY song_id) play_stats ON play_stats.song_id = s.id ");
            }
            
            sql.append("INNER JOIN Scrobble scr ON scr.song_id = s.id " +
                "WHERE scr.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
            }
            sql.append(") ");
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            sql.append(
                "SELECT COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id, COUNT(DISTINCT s.id) as cnt " +
                "FROM Song s " +
                "LEFT JOIN Artist ar ON s.artist_id = ar.id " +
                "LEFT JOIN Album al ON s.album_id = al.id ");
            
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble scr WHERE 1=1 ");
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append(" GROUP BY song_id) play_stats ON play_stats.song_id = s.id ");
            }
            
            sql.append("WHERE NOT EXISTS (" +
                "SELECT 1 FROM Scrobble scr WHERE scr.song_id = s.id AND scr.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
            }
            sql.append(")) AND 1=1 ");
        } else {
            sql.append(
                "SELECT COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id, COUNT(*) as cnt " +
                "FROM Song s " +
                "LEFT JOIN Artist ar ON s.artist_id = ar.id " +
                "LEFT JOIN Album al ON s.album_id = al.id ");
            
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Scrobble scr WHERE 1=1 ");
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append(" GROUP BY song_id) play_stats ON play_stats.song_id = s.id ");
            }
            
            sql.append("WHERE 1=1 ");
        }
        
        List<Object> params = new ArrayList<>();
        
        // Add account params for play_stats subquery
        if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
        }
        
        // Account params for main query
        if (accounts != null && !accounts.isEmpty()) {
            params.addAll(accounts);
        }
        
        // Name filter with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("s.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("ar.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(artistName) + "%");
        }
        
        // Album name filter
        if (albumName != null && !albumName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("al.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(albumName) + "%");
        }
        
        // Gender filter (uses effective gender)
        String genderExpr = "COALESCE(s.override_gender_id, ar.gender_id)";
        library.util.SqlFilterHelper.appendIdFilter(sql, params, genderExpr, genderIds, genderMode);
        
        // Genre filter
        String genreExpr = "COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id)";
        library.util.SqlFilterHelper.appendIdFilter(sql, params, genreExpr, genreIds, genreMode);
        
        // Subgenre filter
        String subgenreExpr = "COALESCE(s.override_subgenre_id, al.override_subgenre_id, ar.subgenre_id)";
        library.util.SqlFilterHelper.appendIdFilter(sql, params, subgenreExpr, subgenreIds, subgenreMode);
        
        // Language filter
        String languageExpr = "COALESCE(s.override_language_id, al.override_language_id, ar.language_id)";
        library.util.SqlFilterHelper.appendIdFilter(sql, params, languageExpr, languageIds, languageMode);
        
        // Ethnicity filter
        library.util.SqlFilterHelper.appendIdFilter(sql, params, "ar.ethnicity_id", ethnicityIds, ethnicityMode);
        
        // Country filter
        library.util.SqlFilterHelper.appendStringFilter(sql, params, "ar.country", countries, countryMode);
        
        // Release date filter
        library.util.SqlFilterHelper.appendDateFilter(sql, params, "s.release_date", releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        // First listened date filter
        String firstListenedSubquery = "(SELECT MIN(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id = s.id)";
        library.util.SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last listened date filter
        String lastListenedSubquery = "(SELECT MAX(scr.scrobble_date) FROM Scrobble scr WHERE scr.song_id = s.id)";
        library.util.SqlFilterHelper.appendDateFilter(sql, params, lastListenedSubquery, lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode);
        
        // Birth date filter
        library.util.SqlFilterHelper.appendDateFilter(sql, params, "ar.birth_date", birthDate, birthDateFrom, birthDateTo, birthDateMode);
        
        // Death date filter
        library.util.SqlFilterHelper.appendDateFilter(sql, params, "ar.death_date", deathDate, deathDateFrom, deathDateTo, deathDateMode);
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND s.organized = 1");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (s.organized = 0 OR s.organized IS NULL)");
            }
        }
        
        // Is single filter
        if (isSingle != null && !isSingle.isEmpty()) {
            if ("true".equalsIgnoreCase(isSingle)) {
                sql.append(" AND s.is_single = 1");
            } else if ("false".equalsIgnoreCase(isSingle)) {
                sql.append(" AND (s.is_single = 0 OR s.is_single IS NULL)");
            }
        }
        
        // Is band filter
        if (isBand != null && !isBand.isEmpty()) {
            if ("true".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 1");
            } else if ("false".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 0");
            }
        }
        
        // In iTunes filter - NOT implemented in SQL
        // iTunes status is determined by checking against an in-memory map from iTunes XML
        // This filter must be applied in the service layer after fetching data
        
        // Play count filter
        if (playCountMin != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) >= ?");
            params.add(playCountMin);
        }
        if (playCountMax != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) <= ?");
            params.add(playCountMax);
        }
        
        // Length filter
        if (lengthMin != null) {
            sql.append(" AND s.length_seconds >= ?");
            params.add(lengthMin);
        }
        if (lengthMax != null) {
            sql.append(" AND s.length_seconds <= ?");
            params.add(lengthMax);
        }
        
        // Add GROUP BY
        sql.append(" GROUP BY effective_gender_id");
        
        Map<Integer, Long> result = new HashMap<>();
        jdbcTemplate.query(sql.toString(), rs -> {
            Integer genderId = rs.getObject("effective_gender_id") != null ? rs.getInt("effective_gender_id") : null;
            Long cnt = rs.getLong("cnt");
            result.put(genderId, cnt);
        }, params.toArray());
        
        return result;
    }
    
    // Get filtered chart data for gender breakdown
    public java.util.Map<String, Object> getFilteredChartData(
            String name, java.util.List<Integer> artistIds, java.util.List<Integer> albumIds, java.util.List<Integer> songIds,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
            String listenedDateFrom, String listenedDateTo) {

        // Build the filter clause that will be reused
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();

        buildFilterClause(filterClause, params, name, artistIds, albumIds, songIds,
            genreIds, genreMode, subgenreIds, subgenreMode,
            languageIds, languageMode, genderIds, genderMode,
            ethnicityIds, ethnicityMode, countries, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo);

        // Determine if Scrobble join is needed for non-scrobble queries
        boolean needsScrobbleJoin = (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty()) || 
                                     (listenedDateTo != null && !listenedDateTo.trim().isEmpty());

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        // Get plays by gender
        data.put("playsByGender", getPlaysByGenderFiltered(filterClause.toString(), params, null));

        // Get songs by gender
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params, needsScrobbleJoin, null));

        // Get artists by gender (from filtered songs)
        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params, needsScrobbleJoin, null));

        // Get albums by gender (from filtered songs)
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params, needsScrobbleJoin, null));

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
    
    /**
     * Overloaded getFilteredChartData that uses ChartFilterDTO.
     * Uses the DTO-specific buildFilterClause which supports all filters including entity-aware ones.
     */
    public java.util.Map<String, Object> getFilteredChartData(ChartFilterDTO filter) {
        // Build the filter clause using the DTO method (includes entity-aware filters)
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();

        buildFilterClause(filterClause, params, filter);

        // Determine if Scrobble join is needed for non-scrobble queries
        boolean needsScrobbleJoin = needsScrobbleJoin(filter);

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        // Get plays by gender
        data.put("playsByGender", getPlaysByGenderFiltered(filterClause.toString(), params, null));

        // Get songs by gender
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params, needsScrobbleJoin, null));

        // Get artists by gender (from filtered songs)
        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params, needsScrobbleJoin, null));

        // Get albums by gender (from filtered songs)
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params, needsScrobbleJoin, null));

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
            String name, java.util.List<Integer> artistIds, java.util.List<Integer> albumIds, java.util.List<Integer> songIds,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
            String listenedDateFrom, String listenedDateTo) {
        
        // Name filters with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("s.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist ID filter - supports multiple IDs (OR logic with exact matching)
        // Use s.artist_id for indexed lookup
        if (artistIds != null && !artistIds.isEmpty()) {
            String placeholders = String.join(",", artistIds.stream().map(id -> "?").toList());
            sql.append(" AND s.artist_id IN (").append(placeholders).append(")");
            params.addAll(artistIds);
        }
        
        // Album ID filter - supports multiple IDs (OR logic with exact matching)
        // Use s.album_id for indexed lookup
        if (albumIds != null && !albumIds.isEmpty()) {
            String placeholders = String.join(",", albumIds.stream().map(id -> "?").toList());
            sql.append(" AND s.album_id IN (").append(placeholders).append(")");
            params.addAll(albumIds);
        }
        
        // Song ID filter - supports multiple IDs (OR logic with exact matching)
        if (songIds != null && !songIds.isEmpty()) {
            String placeholders = String.join(",", songIds.stream().map(id -> "?").toList());
            sql.append(" AND s.id IN (").append(placeholders).append(")");
            params.addAll(songIds);
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
        
        // Listened date filter (scrobble_date range)
        if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty() && 
            listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(scr.scrobble_date) >= DATE(?) AND DATE(scr.scrobble_date) <= DATE(?)");
            params.add(listenedDateFrom);
            params.add(listenedDateTo);
        } else if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty()) {
            sql.append(" AND DATE(scr.scrobble_date) >= DATE(?)");
            params.add(listenedDateFrom);
        } else if (listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(scr.scrobble_date) <= DATE(?)");
            params.add(listenedDateTo);
        }
    }
    
    /**
     * Builds an early filter clause for scrobble queries.
     * This filters on scr.song_id before expensive joins, dramatically improving performance
     * when filtering by artist, album, or song IDs.
     */
    private void buildScrobbleEarlyFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        // Artist ID filter - filter scrobbles to only songs by these artists
        // When includeGroups/includeFeatured is set, skip this early filter and let buildFilterClause handle it
        // (buildFilterClause will add the expanded artist filter with proper OR conditions)
        java.util.List<Integer> artistIds = filter.getArtistIds();
        boolean includeGroups = filter.isIncludeGroups();
        boolean includeFeatured = filter.isIncludeFeatured();
        
        if (artistIds != null && !artistIds.isEmpty() && !includeGroups && !includeFeatured) {
            // Simple filter - only main artist songs (no expansion needed)
            String placeholders = String.join(",", artistIds.stream().map(id -> "?").toList());
            sql.append(" AND scr.song_id IN (SELECT id FROM Song WHERE artist_id IN (").append(placeholders).append("))");
            params.addAll(artistIds);
        }
        // When includeGroups or includeFeatured is set, don't add early filter - buildFilterClause will handle it
        
        // Album ID filter - filter scrobbles to only songs from these albums
        java.util.List<Integer> albumIds = filter.getAlbumIds();
        if (albumIds != null && !albumIds.isEmpty()) {
            String placeholders = String.join(",", albumIds.stream().map(id -> "?").toList());
            sql.append(" AND scr.song_id IN (SELECT id FROM Song WHERE album_id IN (").append(placeholders).append("))");
            params.addAll(albumIds);
        }
        
        // Song ID filter - filter scrobbles to these specific songs
        java.util.List<Integer> songIds = filter.getSongIds();
        if (songIds != null && !songIds.isEmpty()) {
            String placeholders = String.join(",", songIds.stream().map(id -> "?").toList());
            sql.append(" AND scr.song_id IN (").append(placeholders).append(")");
            params.addAll(songIds);
        }
    }
    
    /**
     * Overloaded buildFilterClause that uses ChartFilterDTO.
     * Delegates to the existing method for basic filters and adds new entity-aware filters.
     */
    private void buildFilterClause(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        boolean includeGroups = filter.isIncludeGroups();
        boolean includeFeatured = filter.isIncludeFeatured();
        java.util.List<Integer> artistIds = filter.getArtistIds();
        boolean hasArtistFilter = artistIds != null && !artistIds.isEmpty();
        
        // When includeGroups or includeFeatured is set AND we have artist filter,
        // we handle artist filter specially below, so pass null to basic method
        java.util.List<Integer> artistIdsForBasicFilter = (hasArtistFilter && (includeGroups || includeFeatured)) 
            ? null : artistIds;
        
        // Delegate basic filters to existing method
        // Note: release date is handled separately with entity-awareness below, so pass nulls here
        buildFilterClause(sql, params,
            filter.getName(), artistIdsForBasicFilter, filter.getAlbumIds(), filter.getSongIds(),
            filter.getGenreIds(), filter.getGenreMode(),
            filter.getSubgenreIds(), filter.getSubgenreMode(),
            filter.getLanguageIds(), filter.getLanguageMode(),
            filter.getGenderIds(), filter.getGenderMode(),
            filter.getEthnicityIds(), filter.getEthnicityMode(),
            filter.getCountries(), filter.getCountryMode(),
            null, null, null, null, // release date handled with entity awareness below
            filter.getListenedDateFrom(), filter.getListenedDateTo());
        
        // Handle artist filter with includeGroups/includeFeatured expansion
        if (hasArtistFilter && (includeGroups || includeFeatured)) {
            String placeholders = String.join(",", artistIds.stream().map(id -> "?").toList());
            StringBuilder artistCondition = new StringBuilder();
            
            // Always include songs where artist_id matches (main artist songs)
            artistCondition.append("(s.artist_id IN (").append(placeholders).append(")");
            params.addAll(artistIds);
            
            if (includeGroups) {
                // Include songs where the song's artist is a GROUP that the selected artist(s) are members of
                // i.e., the song's artist has the selected artists as members
                artistCondition.append(" OR s.artist_id IN (SELECT am.group_artist_id FROM ArtistMember am WHERE am.member_artist_id IN (").append(placeholders).append("))");
                params.addAll(artistIds);
            }
            
            if (includeFeatured) {
                // Include songs where the selected artist(s) are featured
                artistCondition.append(" OR s.id IN (SELECT sfa.song_id FROM SongFeaturedArtist sfa WHERE sfa.artist_id IN (").append(placeholders).append("))");
                params.addAll(artistIds);
            }
            
            artistCondition.append(")");
            sql.append(" AND ").append(artistCondition);
        }
        
        // Handle Account filter
        java.util.List<String> accounts = filter.getAccounts();
        String accountMode = filter.getAccountMode();
        if (accountMode != null && accounts != null && !accounts.isEmpty()) {
            String placeholders = String.join(",", accounts.stream().map(a -> "?").toList());
            if ("includes".equals(accountMode)) {
                sql.append(" AND scr.account IN (").append(placeholders).append(")");
                params.addAll(accounts);
            } else if ("excludes".equals(accountMode)) {
                sql.append(" AND scr.account NOT IN (").append(placeholders).append(")");
                params.addAll(accounts);
            }
        }
        
        // Handle isBand filter
        String isBand = filter.getIsBand();
        if (isBand != null) {
            if ("true".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 1");
            } else if ("false".equalsIgnoreCase(isBand)) {
                sql.append(" AND ar.is_band = 0");
            }
        }
        
        // Handle hasFeaturedArtists filter
        String hasFeaturedArtists = filter.getHasFeaturedArtists();
        if (hasFeaturedArtists != null) {
            if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id)");
            } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id)");
            }
        }
        
        // Handle isSingle filter
        String isSingle = filter.getIsSingle();
        if (isSingle != null) {
            if ("true".equalsIgnoreCase(isSingle)) {
                sql.append(" AND s.is_single = 1");
            } else if ("false".equalsIgnoreCase(isSingle)) {
                sql.append(" AND s.is_single = 0");
            }
        }
        
        // Note: inItunes filter is NOT handled here - it's done in memory via ItunesService
        // because iTunes status is determined by checking against an in-memory map from iTunes XML
        
        // Handle entity-aware filters: First/Last Listened Date, Play Count
        // These use subqueries to aggregate scrobble data by the selected entity type
        
        // First Listened Date filter
        String firstListenedEntity = filter.getFirstListenedDateEntity();
        if (firstListenedEntity == null || firstListenedEntity.isEmpty()) firstListenedEntity = "song";
        String firstListenedMode = filter.getFirstListenedDateMode();
        
        if (firstListenedMode != null) {
            String dateExpr = buildEntityMinDateSubquery(firstListenedEntity, "scr");
            
            String firstListenedDate = filter.getFirstListenedDate();
            if (firstListenedDate != null && !firstListenedDate.trim().isEmpty()) {
                switch (firstListenedMode) {
                    case "exact" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") = DATE(?)");
                        params.add(firstListenedDate);
                    }
                    case "gte" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") >= DATE(?)");
                        params.add(firstListenedDate);
                    }
                    case "lte" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") <= DATE(?)");
                        params.add(firstListenedDate);
                    }
                }
            }
            
            if ("between".equals(firstListenedMode)) {
                String from = filter.getFirstListenedDateFrom();
                String to = filter.getFirstListenedDateTo();
                if (from != null && !from.trim().isEmpty() && to != null && !to.trim().isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpr).append(") >= DATE(?) AND DATE(").append(dateExpr).append(") <= DATE(?)");
                    params.add(from);
                    params.add(to);
                }
            }
        }
        
        // Last Listened Date filter
        String lastListenedEntity = filter.getLastListenedDateEntity();
        if (lastListenedEntity == null || lastListenedEntity.isEmpty()) lastListenedEntity = "song";
        String lastListenedMode = filter.getLastListenedDateMode();
        
        if (lastListenedMode != null) {
            String dateExpr = buildEntityMaxDateSubquery(lastListenedEntity, "scr");
            
            String lastListenedDate = filter.getLastListenedDate();
            if (lastListenedDate != null && !lastListenedDate.trim().isEmpty()) {
                switch (lastListenedMode) {
                    case "exact" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") = DATE(?)");
                        params.add(lastListenedDate);
                    }
                    case "gte" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") >= DATE(?)");
                        params.add(lastListenedDate);
                    }
                    case "lte" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") <= DATE(?)");
                        params.add(lastListenedDate);
                    }
                }
            }
            
            if ("between".equals(lastListenedMode)) {
                String from = filter.getLastListenedDateFrom();
                String to = filter.getLastListenedDateTo();
                if (from != null && !from.trim().isEmpty() && to != null && !to.trim().isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpr).append(") >= DATE(?) AND DATE(").append(dateExpr).append(") <= DATE(?)");
                    params.add(from);
                    params.add(to);
                }
            }
        }
        
        // Play Count filter
        String playCountEntity = filter.getPlayCountEntity();
        if (playCountEntity == null || playCountEntity.isEmpty()) playCountEntity = "song";
        Integer playCountMin = filter.getPlayCountMin();
        Integer playCountMax = filter.getPlayCountMax();
        
        if (playCountMin != null || playCountMax != null) {
            String countExpr = buildEntityPlayCountSubquery(playCountEntity, "scr");
            
            if (playCountMin != null) {
                sql.append(" AND ").append(countExpr).append(" >= ?");
                params.add(playCountMin);
            }
            if (playCountMax != null) {
                sql.append(" AND ").append(countExpr).append(" <= ?");
                params.add(playCountMax);
            }
        }
        
        // Release Date filter with entity awareness
        String releaseDateEntity = filter.getReleaseDateEntity();
        if (releaseDateEntity == null || releaseDateEntity.isEmpty()) releaseDateEntity = "song";
        String releaseDateMode = filter.getReleaseDateMode();
        
        if (releaseDateMode != null) {
            String dateExpr = buildEntityReleaseDateExpr(releaseDateEntity);
            
            String releaseDate = filter.getReleaseDate();
            if (releaseDate != null && !releaseDate.trim().isEmpty()) {
                switch (releaseDateMode) {
                    case "exact" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") = DATE(?)");
                        params.add(releaseDate);
                    }
                    case "gt" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") > DATE(?)");
                        params.add(releaseDate);
                    }
                    case "lt" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") < DATE(?)");
                        params.add(releaseDate);
                    }
                    case "gte" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") >= DATE(?)");
                        params.add(releaseDate);
                    }
                    case "lte" -> {
                        sql.append(" AND DATE(").append(dateExpr).append(") <= DATE(?)");
                        params.add(releaseDate);
                    }
                }
            }
            
            if ("between".equals(releaseDateMode)) {
                String from = filter.getReleaseDateFrom();
                String to = filter.getReleaseDateTo();
                if (from != null && !from.trim().isEmpty() && to != null && !to.trim().isEmpty()) {
                    sql.append(" AND DATE(").append(dateExpr).append(") >= DATE(?) AND DATE(").append(dateExpr).append(") <= DATE(?)");
                    params.add(from);
                    params.add(to);
                }
            }
        }
        
        // ==================== CHART PERFORMANCE FILTERS ====================
        
        // Albums Weekly Chart filter (peak position <= specified, total weeks >= specified)
        Integer albumsWeeklyChartPeak = filter.getAlbumsWeeklyChartPeak();
        Integer albumsWeeklyChartWeeks = filter.getAlbumsWeeklyChartWeeks();
        if (albumsWeeklyChartPeak != null || albumsWeeklyChartWeeks != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as weeks ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.album_id = alb.id AND c.chart_type = 'album' AND c.period_type = 'weekly'");
            sql.append(") chart_stats WHERE 1=1");
            if (albumsWeeklyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(albumsWeeklyChartPeak);
            }
            if (albumsWeeklyChartWeeks != null) {
                sql.append(" AND chart_stats.weeks >= ?");
                params.add(albumsWeeklyChartWeeks);
            }
            sql.append(")");
        }
        
        // Albums Seasonal Chart filter (peak position <= specified, total seasons >= specified)
        Integer albumsSeasonalChartPeak = filter.getAlbumsSeasonalChartPeak();
        Integer albumsSeasonalChartSeasons = filter.getAlbumsSeasonalChartSeasons();
        if (albumsSeasonalChartPeak != null || albumsSeasonalChartSeasons != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as seasons ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.album_id = alb.id AND c.chart_type = 'album' AND c.period_type = 'seasonal'");
            sql.append(") chart_stats WHERE 1=1");
            if (albumsSeasonalChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(albumsSeasonalChartPeak);
            }
            if (albumsSeasonalChartSeasons != null) {
                sql.append(" AND chart_stats.seasons >= ?");
                params.add(albumsSeasonalChartSeasons);
            }
            sql.append(")");
        }
        
        // Albums Yearly Chart filter (peak position <= specified, total years >= specified)
        Integer albumsYearlyChartPeak = filter.getAlbumsYearlyChartPeak();
        Integer albumsYearlyChartYears = filter.getAlbumsYearlyChartYears();
        if (albumsYearlyChartPeak != null || albumsYearlyChartYears != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as years ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.album_id = alb.id AND c.chart_type = 'album' AND c.period_type = 'yearly'");
            sql.append(") chart_stats WHERE 1=1");
            if (albumsYearlyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(albumsYearlyChartPeak);
            }
            if (albumsYearlyChartYears != null) {
                sql.append(" AND chart_stats.years >= ?");
                params.add(albumsYearlyChartYears);
            }
            sql.append(")");
        }
        
        // Songs Weekly Chart filter (peak position <= specified, total weeks >= specified)
        Integer songsWeeklyChartPeak = filter.getSongsWeeklyChartPeak();
        Integer songsWeeklyChartWeeks = filter.getSongsWeeklyChartWeeks();
        if (songsWeeklyChartPeak != null || songsWeeklyChartWeeks != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as weeks ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly'");
            sql.append(") chart_stats WHERE 1=1");
            if (songsWeeklyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(songsWeeklyChartPeak);
            }
            if (songsWeeklyChartWeeks != null) {
                sql.append(" AND chart_stats.weeks >= ?");
                params.add(songsWeeklyChartWeeks);
            }
            sql.append(")");
        }
        
        // Songs Seasonal Chart filter (peak position <= specified, total seasons >= specified)
        Integer songsSeasonalChartPeak = filter.getSongsSeasonalChartPeak();
        Integer songsSeasonalChartSeasons = filter.getSongsSeasonalChartSeasons();
        if (songsSeasonalChartPeak != null || songsSeasonalChartSeasons != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as seasons ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal'");
            sql.append(") chart_stats WHERE 1=1");
            if (songsSeasonalChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(songsSeasonalChartPeak);
            }
            if (songsSeasonalChartSeasons != null) {
                sql.append(" AND chart_stats.seasons >= ?");
                params.add(songsSeasonalChartSeasons);
            }
            sql.append(")");
        }
        
        // Songs Yearly Chart filter (peak position <= specified, total years >= specified)
        Integer songsYearlyChartPeak = filter.getSongsYearlyChartPeak();
        Integer songsYearlyChartYears = filter.getSongsYearlyChartYears();
        if (songsYearlyChartPeak != null || songsYearlyChartYears != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT c.id) as years ");
            sql.append("FROM ChartEntry ce ");
            sql.append("INNER JOIN Chart c ON ce.chart_id = c.id ");
            sql.append("WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'yearly'");
            sql.append(") chart_stats WHERE 1=1");
            if (songsYearlyChartPeak != null) {
                sql.append(" AND chart_stats.peak <= ?");
                params.add(songsYearlyChartPeak);
            }
            if (songsYearlyChartYears != null) {
                sql.append(" AND chart_stats.years >= ?");
                params.add(songsYearlyChartYears);
            }
            sql.append(")");
        }

        // Age filter (artist's current age, or age at death if deceased)
        Integer ageMin = filter.getAgeMin();
        Integer ageMax = filter.getAgeMax();
        if (ageMin != null || ageMax != null) {
            String ageExpr = "CAST((julianday(COALESCE(ar.death_date, DATE('now'))) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(ageExpr).append(" >= ?");
                params.add(ageMin);
            }
            if (ageMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(ageExpr).append(" <= ?");
                params.add(ageMax);
            }
        }
        
        // Age at Release filter (artist's age when song/album was released)
        Integer ageAtReleaseMin = filter.getAgeAtReleaseMin();
        Integer ageAtReleaseMax = filter.getAgeAtReleaseMax();
        if (ageAtReleaseMin != null || ageAtReleaseMax != null) {
            String ageAtReleaseExpr = "CAST((julianday(COALESCE(s.release_date, alb.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ?");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ?");
                params.add(ageAtReleaseMax);
            }
        }
        
        // Birth Date filter
        String birthDateMode = filter.getBirthDateMode();
        if (birthDateMode != null && !birthDateMode.isEmpty()) {
            String birthDate = filter.getBirthDate();
            String birthDateFrom = filter.getBirthDateFrom();
            String birthDateTo = filter.getBirthDateTo();
            switch (birthDateMode) {
                case "isnull" -> sql.append(" AND ar.birth_date IS NULL");
                case "isnotnull" -> sql.append(" AND ar.birth_date IS NOT NULL");
                case "exact" -> {
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) = DATE(?)");
                        params.add(birthDate);
                    }
                }
                case "gte" -> {
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) >= DATE(?)");
                        params.add(birthDate);
                    }
                }
                case "lte" -> {
                    if (birthDate != null && !birthDate.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) <= DATE(?)");
                        params.add(birthDate);
                    }
                }
                case "between" -> {
                    if (birthDateFrom != null && !birthDateFrom.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) >= DATE(?)");
                        params.add(birthDateFrom);
                    }
                    if (birthDateTo != null && !birthDateTo.isEmpty()) {
                        sql.append(" AND DATE(ar.birth_date) <= DATE(?)");
                        params.add(birthDateTo);
                    }
                }
            }
        }
        
        // Death Date filter
        String deathDateMode = filter.getDeathDateMode();
        if (deathDateMode != null && !deathDateMode.isEmpty()) {
            String deathDate = filter.getDeathDate();
            String deathDateFrom = filter.getDeathDateFrom();
            String deathDateTo = filter.getDeathDateTo();
            switch (deathDateMode) {
                case "isnull" -> sql.append(" AND ar.death_date IS NULL");
                case "isnotnull" -> sql.append(" AND ar.death_date IS NOT NULL");
                case "exact" -> {
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) = DATE(?)");
                        params.add(deathDate);
                    }
                }
                case "gte" -> {
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) >= DATE(?)");
                        params.add(deathDate);
                    }
                }
                case "lte" -> {
                    if (deathDate != null && !deathDate.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) <= DATE(?)");
                        params.add(deathDate);
                    }
                }
                case "between" -> {
                    if (deathDateFrom != null && !deathDateFrom.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) >= DATE(?)");
                        params.add(deathDateFrom);
                    }
                    if (deathDateTo != null && !deathDateTo.isEmpty()) {
                        sql.append(" AND DATE(ar.death_date) <= DATE(?)");
                        params.add(deathDateTo);
                    }
                }
            }
        }
    }

    /**
     * Builds a subquery expression for MIN(scrobble_date) based on entity type.
     */
    private String buildEntityMinDateSubquery(String entity, String scrAlias) {
        return switch (entity) {
            case "artist" -> "(SELECT MIN(scr2.scrobble_date) FROM Scrobble scr2 " +
                             "JOIN Song s2 ON scr2.song_id = s2.id " +
                             "WHERE s2.artist_id = ar.id)";
            case "album" -> "(SELECT MIN(scr2.scrobble_date) FROM Scrobble scr2 " +
                            "JOIN Song s2 ON scr2.song_id = s2.id " +
                            "WHERE s2.album_id = alb.id)";
            default -> "(SELECT MIN(scr2.scrobble_date) FROM Scrobble scr2 WHERE scr2.song_id = s.id)"; // song level
        };
    }
    
    /**
     * Builds a subquery expression for MAX(scrobble_date) based on entity type.
     */
    private String buildEntityMaxDateSubquery(String entity, String scrAlias) {
        return switch (entity) {
            case "artist" -> "(SELECT MAX(scr2.scrobble_date) FROM Scrobble scr2 " +
                             "JOIN Song s2 ON scr2.song_id = s2.id " +
                             "WHERE s2.artist_id = ar.id)";
            case "album" -> "(SELECT MAX(scr2.scrobble_date) FROM Scrobble scr2 " +
                            "JOIN Song s2 ON scr2.song_id = s2.id " +
                            "WHERE s2.album_id = alb.id)";
            default -> "(SELECT MAX(scr2.scrobble_date) FROM Scrobble scr2 WHERE scr2.song_id = s.id)"; // song level
        };
    }
    
    /**
     * Builds a subquery expression for COUNT(*) based on entity type.
     */
    private String buildEntityPlayCountSubquery(String entity, String scrAlias) {
        return switch (entity) {
            case "artist" -> "(SELECT COUNT(*) FROM Scrobble scr2 " +
                             "JOIN Song s2 ON scr2.song_id = s2.id " +
                             "WHERE s2.artist_id = ar.id)";
            case "album" -> "(SELECT COUNT(*) FROM Scrobble scr2 " +
                            "JOIN Song s2 ON scr2.song_id = s2.id " +
                            "WHERE s2.album_id = alb.id)";
            default -> "(SELECT COUNT(*) FROM Scrobble scr2 WHERE scr2.song_id = s.id)"; // song level
        };
    }
    
    /**
     * Builds a date expression for release_date based on entity type.
     */
    private String buildEntityReleaseDateExpr(String entity) {
        return switch (entity) {
            case "artist" -> "(SELECT MIN(COALESCE(s2.release_date, alb2.release_date)) FROM Song s2 " +
                             "LEFT JOIN Album alb2 ON s2.album_id = alb2.id " +
                             "WHERE s2.artist_id = ar.id)";
            case "album" -> "alb.release_date";
            default -> "COALESCE(s.release_date, alb.release_date)"; // song level
        };
    }
    
    /**
     * Check if any entity-aware filter is active (requires special query handling)
     */
    private boolean hasEntityAwareFilters(ChartFilterDTO filter) {
        return (filter.getFirstListenedDate() != null && !filter.getFirstListenedDate().isEmpty()) ||
               (filter.getFirstListenedDateFrom() != null && !filter.getFirstListenedDateFrom().isEmpty()) ||
               (filter.getFirstListenedDateTo() != null && !filter.getFirstListenedDateTo().isEmpty()) ||
               (filter.getLastListenedDate() != null && !filter.getLastListenedDate().isEmpty()) ||
               (filter.getLastListenedDateFrom() != null && !filter.getLastListenedDateFrom().isEmpty()) ||
               (filter.getLastListenedDateTo() != null && !filter.getLastListenedDateTo().isEmpty()) ||
               filter.getPlayCountMin() != null ||
               filter.getPlayCountMax() != null;
    }
    
    /**
     * Check if Scrobble join is needed (for listened date, account, or entity-aware filters)
     */
    private boolean needsScrobbleJoin(ChartFilterDTO filter) {
        return (filter.getListenedDateFrom() != null && !filter.getListenedDateFrom().isEmpty()) ||
               (filter.getListenedDateTo() != null && !filter.getListenedDateTo().isEmpty()) ||
               (filter.getAccounts() != null && !filter.getAccounts().isEmpty()) ||
               hasEntityAwareFilters(filter);
    }

    private java.util.Map<String, Long> getPlaysByGenderFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, restrict to top N songs by play count within the filtered set
        if (limit != null) {
            java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
            params.add(limit);

            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    SUM(top.play_count) as play_count
                FROM (
                    SELECT s.id as song_id, COUNT(*) as play_count
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                ) top
                INNER JOIN Song s ON top.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("play_count");
                result.put(gender, count);
            }, params.toArray());

            return result;
        }

        // Fallback: all scrobbles within the filtered set
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

        jdbcTemplate.query(sql, rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("play_count");
            result.put(gender, count);
        }, filterParams.toArray());

        return result;
    }
    
    private java.util.Map<String, Long> getSongsByGenderFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // Optimize for no filters - use direct Song table count
        if (filterClause.trim().isEmpty() && !needsScrobbleJoin && limit == null) {
            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    COUNT(*) as song_count
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("song_count");
                result.put(gender, count);
            });

            return result;
        }

        // If limit is specified, restrict to top N songs by play count within the filtered set
        if (limit != null) {
            java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
            params.add(limit);

            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    COUNT(DISTINCT s.id) as song_count
                FROM (
                    SELECT s.id as song_id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                ) top
                INNER JOIN Song s ON top.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("song_count");
                result.put(gender, count);
            }, params.toArray());

            return result;
        }

        String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n            " : "";
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT s.id) as song_count
            FROM Song s
            """ + scrobbleJoin + """
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

        jdbcTemplate.query(sql, rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("song_count");
            result.put(gender, count);
        }, filterParams.toArray());

        return result;
    }
    
    private java.util.Map<String, Long> getArtistsByGenderFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // Optimize for no filters - use direct Artist table instead of going through Song
        if (filterClause.trim().isEmpty() && !needsScrobbleJoin && limit == null) {
            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    COUNT(*) as artist_count
                FROM Artist ar
                LEFT JOIN Gender g ON ar.gender_id = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("artist_count");
                result.put(gender, count);
            });

            return result;
        }

        // If limit is specified, restrict to top N artists by play count within the filtered set
        if (limit != null) {
            java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
            params.add(limit);

            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    COUNT(DISTINCT ar.id) as artist_count
                FROM (
                    SELECT s.artist_id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.artist_id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                ) top
                INNER JOIN Artist ar ON top.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("artist_count");
                result.put(gender, count);
            }, params.toArray());

            return result;
        }

        String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n            " : "";
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT ar.id) as artist_count
            FROM Song s
            """ + scrobbleJoin + """
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

        jdbcTemplate.query(sql, rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("artist_count");
            result.put(gender, count);
        }, filterParams.toArray());

        return result;
    }
    
    private java.util.Map<String, Long> getAlbumsByGenderFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // Optimize for no filters - use direct Album table count
        if (filterClause.trim().isEmpty() && !needsScrobbleJoin && limit == null) {
            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    COUNT(*) as album_count
                FROM Album alb
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("album_count");
                result.put(gender, count);
            });

            return result;
        }

        // If limit is specified, restrict to top N albums by play count within the filtered set
        if (limit != null) {
            java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
            params.add(limit);

            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    COUNT(DISTINCT alb.id) as album_count
                FROM (
                    SELECT s.album_id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                    GROUP BY s.album_id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                ) top
                INNER JOIN Album alb ON top.album_id = alb.id
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("album_count");
                result.put(gender, count);
            }, params.toArray());

            return result;
        }

        String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n            " : "";
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT alb.id) as album_count
            FROM Song s
            """ + scrobbleJoin + """
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

        jdbcTemplate.query(sql, rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("album_count");
            result.put(gender, count);
        }, filterParams.toArray());

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
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, filterParams.toArray());
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
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, filterParams.toArray());
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
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, filterParams.toArray());
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
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, filterParams.toArray());
    }
    
    // ==================== NEW TAB-SPECIFIC CHART DATA METHODS ====================
    
    // Get General tab chart data (5 pie charts: Artists, Albums, Songs, Plays, Listening Time by gender)
    public java.util.Map<String, Object> getGeneralChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();

        buildFilterClause(filterClause, params, filter);
        
        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);

        boolean scrobbleJoinNeeded = needsScrobbleJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        String limitEntity = filter.getLimitEntity();
        if (limitEntity == null || limitEntity.isBlank()) {
            limitEntity = "song";
        }

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        // General tab limit should apply consistently across all pies, based on the requested entity.
        // Plays/Listening Time are scrobble-derived metrics, so their top-N should be computed by entity.
        // Use combined filter with early scrobble filter for performance
        data.put("playsByGender", getPlaysByGenderFiltered(limitEntity, combinedFilter, combinedParams, limit));
        data.put("listeningTimeByGender", getListeningTimeByGenderFiltered(limitEntity, combinedFilter, combinedParams, limit));

        return data;
    }

    private java.util.Map<String, Long> getListeningTimeByGenderFiltered(String entity, String filterClause, java.util.List<Object> filterParams, Integer limit) {
        if (entity == null || entity.isBlank()) {
            entity = "song";
        }

        // If limit is specified, restrict to top N *entities* by play count within the filtered set
        if (limit != null) {
            java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
            params.add(limit);

            // NOTE: "entities" means artist/album/song, depending on the requested chart.
            // This keeps General tab behavior consistent with the other pies.
            final String sql;
            if ("artist".equalsIgnoreCase(entity)) {
                sql = """
                    SELECT
                        CASE
                            WHEN g.name LIKE '%Female%' THEN 'female'
                            WHEN g.name LIKE '%Male%' THEN 'male'
                            ELSE 'other'
                        END as gender,
                        SUM(top.total_seconds) as total_seconds
                    FROM (
                        SELECT s.artist_id as entity_id,
                               SUM(COALESCE(s.length_seconds, 0)) as total_seconds,
                               COUNT(*) as play_count
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.artist_id
                        ORDER BY play_count DESC
                        LIMIT ?
                    ) top
                    INNER JOIN Artist ar ON top.entity_id = ar.id
                    LEFT JOIN Gender g ON ar.gender_id = g.id
                    GROUP BY gender
                    """;
            } else if ("album".equalsIgnoreCase(entity)) {
                sql = """
                    SELECT
                        CASE
                            WHEN g.name LIKE '%Female%' THEN 'female'
                            WHEN g.name LIKE '%Male%' THEN 'male'
                            ELSE 'other'
                        END as gender,
                        SUM(top.total_seconds) as total_seconds
                    FROM (
                        SELECT s.album_id as entity_id,
                               SUM(COALESCE(s.length_seconds, 0)) as total_seconds,
                               COUNT(*) as play_count
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY play_count DESC
                        LIMIT ?
                    ) top
                    INNER JOIN Album alb ON top.entity_id = alb.id
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    LEFT JOIN Gender g ON ar.gender_id = g.id
                    GROUP BY gender
                    """;
            } else {
                // song (default)
                sql = """
                    SELECT
                        CASE
                            WHEN g.name LIKE '%Female%' THEN 'female'
                            WHEN g.name LIKE '%Male%' THEN 'male'
                            ELSE 'other'
                        END as gender,
                        SUM(COALESCE(s.length_seconds, 0) * top.play_count) as total_seconds
                    FROM (
                        SELECT s.id as song_id, COUNT(*) as play_count
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    ) top
                    INNER JOIN Song s ON top.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                    GROUP BY gender
                    """;
            }

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long seconds = rs.getLong("total_seconds");
                result.put(gender, seconds);
            }, params.toArray());

            return result;
        }

        // Fallback: all scrobbles within the filtered set
        final String sql;
        if ("artist".equalsIgnoreCase(entity)) {
            sql = """
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
                LEFT JOIN Gender g ON ar.gender_id = g.id
                WHERE 1=1 """ + " " + filterClause + """
                GROUP BY gender
                """;
        } else if ("album".equalsIgnoreCase(entity)) {
            sql = """
                SELECT
                    CASE
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    SUM(COALESCE(s.length_seconds, 0)) as total_seconds
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist arSong ON s.artist_id = arSong.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                GROUP BY gender
                """;
        } else {
            // song (default)
            sql = """
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
        }

        java.util.Map<String, Long> result = new java.util.HashMap<>();
        result.put("male", 0L);
        result.put("female", 0L);
        result.put("other", 0L);

        jdbcTemplate.query(sql, rs -> {
            String gender = rs.getString("gender");
            long seconds = rs.getLong("total_seconds");
            result.put(gender, seconds);
        }, filterParams.toArray());

        return result;
    }

    private java.util.Map<String, Long> getPlaysByGenderFiltered(String entity, String filterClause, java.util.List<Object> filterParams, Integer limit) {
        if (entity == null || entity.isBlank()) {
            entity = "song";
        }

        // If limit is specified, restrict to top N *entities* by play count within the filtered set
        if (limit != null) {
            java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
            params.add(limit);

            final String sql;
            if ("artist".equalsIgnoreCase(entity)) {
                sql = """
                    SELECT
                        CASE
                            WHEN g.name LIKE '%Female%' THEN 'female'
                            WHEN g.name LIKE '%Male%' THEN 'male'
                            ELSE 'other'
                        END as gender,
                        SUM(top.play_count) as play_count
                    FROM (
                        SELECT s.artist_id as entity_id, COUNT(*) as play_count
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.artist_id
                        ORDER BY play_count DESC
                        LIMIT ?
                    ) top
                    INNER JOIN Artist ar ON top.entity_id = ar.id
                    LEFT JOIN Gender g ON ar.gender_id = g.id
                    GROUP BY gender
                    """;
            } else if ("album".equalsIgnoreCase(entity)) {
                sql = """
                    SELECT
                        CASE
                            WHEN g.name LIKE '%Female%' THEN 'female'
                            WHEN g.name LIKE '%Male%' THEN 'male'
                            ELSE 'other'
                        END as gender,
                        SUM(top.play_count) as play_count
                    FROM (
                        SELECT s.album_id as entity_id, COUNT(*) as play_count
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist arSong ON s.artist_id = arSong.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY play_count DESC
                        LIMIT ?
                    ) top
                    INNER JOIN Album alb ON top.entity_id = alb.id
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    LEFT JOIN Gender g ON ar.gender_id = g.id
                    GROUP BY gender
                    """;
            } else {
                // song (default)
                sql = """
                    SELECT
                        CASE
                            WHEN g.name LIKE '%Female%' THEN 'female'
                            WHEN g.name LIKE '%Male%' THEN 'male'
                            ELSE 'other'
                        END as gender,
                        SUM(top.play_count) as play_count
                    FROM (
                        SELECT s.id as song_id, COUNT(*) as play_count
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    ) top
                    INNER JOIN Song s ON top.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                    GROUP BY gender
                    """;
            }

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long count = rs.getLong("play_count");
                result.put(gender, count);
            }, params.toArray());

            return result;
        }

        // Fallback: all scrobbles within the filtered set
        final String sql;
        if ("artist".equalsIgnoreCase(entity)) {
            sql = """
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
                LEFT JOIN Gender g ON ar.gender_id = g.id
                WHERE 1=1 """ + " " + filterClause + """
                GROUP BY gender
                """;
        } else if ("album".equalsIgnoreCase(entity)) {
            sql = """
                SELECT
                    CASE
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    COUNT(*) as play_count
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist arSong ON s.artist_id = arSong.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                GROUP BY gender
                """;
        } else {
            // song (default)
            sql = """
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
        }

        java.util.Map<String, Long> result = new java.util.HashMap<>();
        result.put("male", 0L);
        result.put("female", 0L);
        result.put("other", 0L);

        jdbcTemplate.query(sql, rs -> {
            String gender = rs.getString("gender");
            long count = rs.getLong("play_count");
            result.put(gender, count);
        }, filterParams.toArray());

        return result;
    }

    private java.util.Map<String, Long> getListeningTimeByGenderFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, restrict to top N songs by play count within the filtered set
        if (limit != null) {
            java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
            params.add(limit);

            String sql = """
                SELECT 
                    CASE 
                        WHEN g.name LIKE '%Female%' THEN 'female'
                        WHEN g.name LIKE '%Male%' THEN 'male'
                        ELSE 'other'
                    END as gender,
                    SUM(COALESCE(s.length_seconds, 0) * top.play_count) as total_seconds
                FROM (
                    SELECT s.id as song_id, COUNT(*) as play_count
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                ) top
                INNER JOIN Song s ON top.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                GROUP BY gender
                """;

            java.util.Map<String, Long> result = new java.util.HashMap<>();
            result.put("male", 0L);
            result.put("female", 0L);
            result.put("other", 0L);

            jdbcTemplate.query(sql, rs -> {
                String gender = rs.getString("gender");
                long seconds = rs.getLong("total_seconds");
                result.put(gender, seconds);
            }, params.toArray());

            return result;
        }

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

        jdbcTemplate.query(sql, rs -> {
            String gender = rs.getString("gender");
            long seconds = rs.getLong("total_seconds");
            result.put(gender, seconds);
        }, filterParams.toArray());

        return result;
    }
    
    // Get Genre tab chart data (5 bar charts grouped by genre)
    public java.util.Map<String, Object> getGenreChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, filter);
        
        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);
        
        boolean scrobbleJoinNeeded = needsScrobbleJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByGenre", getArtistsByGenreFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("albumsByGenre", getAlbumsByGenreFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("songsByGenre", getSongsByGenreFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("playsByGenre", getPlaysByGenreFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByGenre", getListeningTimeByGenreFiltered(combinedFilter, combinedParams, limit));

        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByGenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N artists by play count, then aggregate by genre
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            // Parameterize LIMIT to avoid accidental token merging (e.g., "LIMIT50") and keep SQL injection-safe
            params.add(limit);
            sql = """
                SELECT 
                    COALESCE(gr.name, 'Unknown') as genre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT s.artist_id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.artist_id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                ) sub
                INNER JOIN Artist ar ON sub.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                LEFT JOIN Genre gr ON ar.genre_id = gr.id
                GROUP BY COALESCE(gr.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(gr.name, 'Unknown') as genre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.genre_id, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getAlbumsByGenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N albums by play count, then aggregate by genre
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            // Parameterize LIMIT to avoid accidental token merging (e.g., "LIMIT50")
            params.add(limit);
            sql = """
                SELECT 
                    COALESCE(gr.name, 'Unknown') as genre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT alb.id as album_id, 
                           COALESCE(alb.override_genre_id, ar.genre_id) as effective_genre_id,
                           ar.gender_id
                    FROM Album alb
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.gender_id = g.id
                LEFT JOIN Genre gr ON sub.effective_genre_id = gr.id
                GROUP BY COALESCE(gr.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
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
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByGenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N songs by play count, then aggregate by genre
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(gr.name, 'Unknown') as genre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT s.id,
                        COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) as effective_genre_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                            WHERE 1=1 """ + " " + filterClause + """
                            GROUP BY s.id
                            ORDER BY COUNT(*) DESC
                            LIMIT ?
                        )
                    ) sub
                    LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                    LEFT JOIN Genre gr ON sub.effective_genre_id = gr.id
                    GROUP BY COALESCE(gr.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(gr.name, 'Unknown') as genre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT s.id,
                        COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) as effective_genre_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                LEFT JOIN Genre gr ON sub.effective_genre_id = gr.id
                GROUP BY COALESCE(gr.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getPlaysByGenreFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, only count plays for the top N songs by play count
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                    GROUP BY COALESCE(gr.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByGenreFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, only count listening time for the top N songs by play count
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                    GROUP BY COALESCE(gr.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("genre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    // Get Subgenre tab chart data (5 bar charts grouped by subgenre)
    public java.util.Map<String, Object> getSubgenreChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();

        buildFilterClause(filterClause, params, filter);

        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);

        boolean scrobbleJoinNeeded = needsScrobbleJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        data.put("artistsBySubgenre", getArtistsBySubgenreFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("albumsBySubgenre", getAlbumsBySubgenreFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("songsBySubgenre", getSongsBySubgenreFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("playsBySubgenre", getPlaysBySubgenreFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeBySubgenre", getListeningTimeBySubgenreFiltered(combinedFilter, combinedParams, limit));

        return data;
    }

    private java.util.List<java.util.Map<String, Object>> getArtistsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N artists by play count, then aggregate by subgenre
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(sg.name, 'Unknown') as subgenre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT ar.id as artist_id, ar.subgenre_id, ar.gender_id
                    FROM Artist ar
                    WHERE ar.id IN (
                        SELECT s.artist_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                            WHERE 1=1 """ + " " + filterClause + """
                            GROUP BY s.artist_id
                            ORDER BY COUNT(*) DESC
                            LIMIT ?
                        )
                    ) sub
                    INNER JOIN Artist ar ON sub.artist_id = ar.id
                    LEFT JOIN Gender g ON ar.gender_id = g.id
                    LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
                    GROUP BY COALESCE(sg.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(sg.name, 'Unknown') as subgenre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.subgenre_id, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getAlbumsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N albums by play count, then aggregate by subgenre
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(sg.name, 'Unknown') as subgenre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT alb.id as album_id,
                        COALESCE(alb.override_subgenre_id, ar.subgenre_id) as effective_subgenre_id,
                        ar.gender_id
                    FROM Album alb
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                            WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                            GROUP BY s.album_id
                            ORDER BY COUNT(*) DESC
                            LIMIT ?
                        )
                    ) sub
                    LEFT JOIN Gender g ON sub.gender_id = g.id
                    LEFT JOIN SubGenre sg ON sub.effective_subgenre_id = sg.id
                    GROUP BY COALESCE(sg.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
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
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N songs by play count, then aggregate by subgenre
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(sg.name, 'Unknown') as subgenre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT s.id,
                        COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) as effective_subgenre_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                            WHERE 1=1 """ + " " + filterClause + """
                            GROUP BY s.id
                            ORDER BY COUNT(*) DESC
                            LIMIT ?
                        )
                    ) sub
                    LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                    LEFT JOIN SubGenre sg ON sub.effective_subgenre_id = sg.id
                    GROUP BY COALESCE(sg.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(sg.name, 'Unknown') as subgenre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT s.id,
                        COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) as effective_subgenre_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                LEFT JOIN SubGenre sg ON sub.effective_subgenre_id = sg.id
                GROUP BY COALESCE(sg.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getPlaysBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, only count plays for the top N songs by play count
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                    GROUP BY COALESCE(sg.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getListeningTimeBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, only count listening time for the top N songs by play count
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                    GROUP BY COALESCE(sg.name, 'Unknown')
                    HAVING (male_count + female_count + other_count) > 0
                    ORDER BY (male_count + female_count + other_count) DESC
                    """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("subgenre_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    // Get Ethnicity tab chart data (5 bar charts grouped by ethnicity)
    public java.util.Map<String, Object> getEthnicityChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, filter);
        
        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);
        
        boolean scrobbleJoinNeeded = needsScrobbleJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByEthnicity", getArtistsByEthnicityFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("albumsByEthnicity", getAlbumsByEthnicityFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("songsByEthnicity", getSongsByEthnicityFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("playsByEthnicity", getPlaysByEthnicityFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByEthnicity", getListeningTimeByEthnicityFiltered(combinedFilter, combinedParams, limit));

        return data;
    }

    private java.util.List<java.util.Map<String, Object>> getArtistsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT ar.id as artist_id, ar.ethnicity_id, ar.gender_id
                    FROM Artist ar
                    WHERE ar.id IN (
                        SELECT s.artist_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.artist_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                INNER JOIN Artist ar ON sub.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                LEFT JOIN Ethnicity e ON ar.ethnicity_id = e.id
                GROUP BY COALESCE(e.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.ethnicity_id, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getAlbumsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT alb.id as album_id, ar.ethnicity_id, ar.gender_id
                    FROM Album alb
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.gender_id = g.id
                LEFT JOIN Ethnicity e ON sub.ethnicity_id = e.id
                GROUP BY COALESCE(e.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT alb.id as album_id, ar.ethnicity_id, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getSongsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT s.id,
                        COALESCE(s.override_ethnicity_id, ar.ethnicity_id) as effective_ethnicity_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                LEFT JOIN Ethnicity e ON sub.effective_ethnicity_id = e.id
                GROUP BY COALESCE(e.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT s.id,
                        COALESCE(s.override_ethnicity_id, ar.ethnicity_id) as effective_ethnicity_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                LEFT JOIN Ethnicity e ON sub.effective_ethnicity_id = e.id
                GROUP BY COALESCE(e.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getPlaysByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(e.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(e.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("ethnicity_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    // Get Language tab chart data (5 bar charts grouped by language)
    public java.util.Map<String, Object> getLanguageChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, filter);

        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);

        boolean scrobbleJoinNeeded = needsScrobbleJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByLanguage", getArtistsByLanguageFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("albumsByLanguage", getAlbumsByLanguageFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("songsByLanguage", getSongsByLanguageFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("playsByLanguage", getPlaysByLanguageFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByLanguage", getListeningTimeByLanguageFiltered(combinedFilter, combinedParams, limit));

        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N artists by play count, then aggregate by language
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(l.name, 'Unknown') as language_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT ar.id as artist_id, ar.language_id, ar.gender_id
                    FROM Artist ar
                    WHERE ar.id IN (
                        SELECT s.artist_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.artist_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                INNER JOIN Artist ar ON sub.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                LEFT JOIN Language l ON ar.language_id = l.id
                GROUP BY COALESCE(l.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(l.name, 'Unknown') as language_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.language_id, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getAlbumsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N albums by play count, then aggregate by language
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            params.add(limit);
            sql = """
                SELECT 
                    COALESCE(l.name, 'Unknown') as language_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT alb.id as album_id, 
                           COALESCE(alb.override_language_id, ar.language_id) as lang_id, 
                           ar.gender_id
                    FROM Album alb
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.gender_id = g.id
                LEFT JOIN Language l ON sub.lang_id = l.id
                GROUP BY COALESCE(l.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
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
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getSongsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        // If limit is specified, first get top N songs by play count, then aggregate by language
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(l.name, 'Unknown') as language_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT s.id,
                        COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) as effective_language_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                LEFT JOIN Language l ON sub.effective_language_id = l.id
                GROUP BY COALESCE(l.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(l.name, 'Unknown') as language_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT s.id,
                        COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) as effective_language_id,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                LEFT JOIN Language l ON sub.effective_language_id = l.id
                GROUP BY COALESCE(l.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getPlaysByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, only count plays for the top N songs by play count
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            params.add(limit);
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(l.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        // If limit is specified, only count listening time for the top N songs by play count
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            params.add(limit);
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(l.name, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("language_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    // Get Country tab chart data (5 bar charts grouped by country)
    public java.util.Map<String, Object> getCountryChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();

        buildFilterClause(filterClause, params, filter);

        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);

        boolean scrobbleJoinNeeded = needsScrobbleJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        data.put("artistsByCountry", getArtistsByCountryFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("albumsByCountry", getAlbumsByCountryFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("songsByCountry", getSongsByCountryFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("playsByCountry", getPlaysByCountryFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByCountry", getListeningTimeByCountryFiltered(combinedFilter, combinedParams, limit));

        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByCountryFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(ar.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT ar.id as artist_id, ar.country, ar.gender_id
                    FROM Artist ar
                    WHERE ar.id IN (
                        SELECT s.artist_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.artist_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                INNER JOIN Artist ar ON sub.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                GROUP BY COALESCE(ar.country, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(ar.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.country, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByCountryFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(sub.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT alb.id as album_id, ar.country, ar.gender_id
                    FROM Album alb
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.gender_id = g.id
                GROUP BY COALESCE(sub.country, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(sub.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT alb.id as album_id, ar.country, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE alb.id IS NOT NULL """ + " " + filterClause + """
                ) sub
                LEFT JOIN Gender g ON sub.gender_id = g.id
                GROUP BY COALESCE(sub.country, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getSongsByCountryFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(sub.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT s.id,
                        ar.country as country,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                GROUP BY COALESCE(sub.country, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(sub.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT s.id,
                        ar.country as country,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                GROUP BY COALESCE(sub.country, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getPlaysByCountryFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(ar.country, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getListeningTimeByCountryFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(ar.country, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY (male_count + female_count + other_count) DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("country_name"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    // Get Release Year tab chart data (4 bar charts grouped by release year - no Artists)
    public java.util.Map<String, Object> getReleaseYearChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();

        buildFilterClause(filterClause, params, filter);

        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);

        boolean scrobbleJoinNeeded = needsScrobbleJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        // No artists by release year - artists don't have release dates
        data.put("albumsByReleaseYear", getAlbumsByReleaseYearFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("songsByReleaseYear", getSongsByReleaseYearFiltered(filterClause.toString(), params, scrobbleJoinNeeded, limit));
        data.put("playsByReleaseYear", getPlaysByReleaseYearFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByReleaseYear", getListeningTimeByReleaseYearFiltered(combinedFilter, combinedParams, limit));

        return data;
    }

    private java.util.List<java.util.Map<String, Object>> getAlbumsByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown') as release_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT alb.id as album_id, alb.release_date, ar.gender_id
                    FROM Album alb
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                INNER JOIN Album alb ON sub.album_id = alb.id
                INNER JOIN Artist ar ON alb.artist_id = ar.id
                LEFT JOIN Gender g ON sub.gender_id = g.id
                GROUP BY COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY release_year DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown') as release_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT alb.id as album_id, alb.release_date, ar.gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getSongsByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsScrobbleJoin, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
                SELECT 
                    COALESCE(sub.release_year, 'Unknown') as release_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT s.id,
                        STRFTIME('%Y', alb.release_date) as release_year,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                GROUP BY COALESCE(sub.release_year, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY release_year DESC
                """;
            params.add(limit);
        } else {
            String scrobbleJoin = needsScrobbleJoin ? "INNER JOIN Scrobble scr ON scr.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(sub.release_year, 'Unknown') as release_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT s.id,
                        STRFTIME('%Y', alb.release_date) as release_year,
                        COALESCE(s.override_gender_id, ar.gender_id) as effective_gender_id
                    FROM Song s
                    """ + scrobbleJoin + """
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                ) sub
                LEFT JOIN Gender g ON sub.effective_gender_id = g.id
                GROUP BY COALESCE(sub.release_year, 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY release_year DESC
                """;
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getPlaysByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY release_year DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getListeningTimeByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY release_year DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("release_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    // Get Listen Year tab chart data (5 bar charts grouped by scrobble/listen year)
    public java.util.Map<String, Object> getListenYearChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, filter);
        
        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter: early scrobble filter + regular filter, with combined params
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(params);
        
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByListenYear", getArtistsByListenYearFiltered(combinedFilter, combinedParams, limit));
        data.put("albumsByListenYear", getAlbumsByListenYearFiltered(combinedFilter, combinedParams, limit));
        data.put("songsByListenYear", getSongsByListenYearFiltered(combinedFilter, combinedParams, limit));
        data.put("playsByListenYear", getPlaysByListenYearFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByListenYear", getListeningTimeByListenYearFiltered(combinedFilter, combinedParams, limit));

        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByListenYearFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                    WHERE ar.id IN (
                        SELECT s.artist_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.artist_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                INNER JOIN Artist ar ON sub.artist_id = ar.id
                LEFT JOIN Gender g ON ar.gender_id = g.id
                GROUP BY sub.year
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY sub.year DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByListenYearFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE s.album_id IS NOT NULL """ + " " + filterClause + """
                        GROUP BY s.album_id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.gender_id = g.id
                GROUP BY sub.year
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY sub.year DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getSongsByListenYearFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Scrobble scr
                        INNER JOIN Song s ON scr.song_id = s.id
                        INNER JOIN Artist ar ON s.artist_id = ar.id
                        LEFT JOIN Album alb ON s.album_id = alb.id
                        WHERE 1=1 """ + " " + filterClause + """
                        GROUP BY s.id
                        ORDER BY COUNT(*) DESC
                        LIMIT ?
                    )
                ) sub
                LEFT JOIN Gender g ON sub.gender_id = g.id
                GROUP BY sub.year
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY sub.year DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    private java.util.List<java.util.Map<String, Object>> getPlaysByListenYearFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(STRFTIME('%Y', scr.scrobble_date), 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY listen_year DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getListeningTimeByListenYearFiltered(String filterClause, java.util.List<Object> filterParams, Integer limit) {
        String sql;
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        if (limit != null) {
            sql = """
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
                WHERE s.id IN (
                    SELECT s.id
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(STRFTIME('%Y', scr.scrobble_date), 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY listen_year DESC
                """;
            params.add(limit);
        } else {
            sql = """
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
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("name", rs.getString("listen_year"));
            row.put("male", rs.getLong("male_count"));
            row.put("female", rs.getLong("female_count"));
            row.put("other", rs.getLong("other_count"));
            return row;
        }, params.toArray());
    }

    // Get Top chart data (top artists, albums, songs by play count)
    public java.util.Map<String, Object> getTopChartData(ChartFilterDTO filter) {
        int limit = filter.getTopLimit() != null ? filter.getTopLimit() : 10;
        
        // Build filter clause for songs (used by getTopSongsFiltered)
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> filterParams = new java.util.ArrayList<>();

        buildFilterClause(filterClause, filterParams, filter);
        
        // Build early scrobble filter for performance (filters on scr.song_id before expensive joins)
        StringBuilder scrobbleEarlyFilter = new StringBuilder();
        java.util.List<Object> scrobbleEarlyParams = new java.util.ArrayList<>();
        buildScrobbleEarlyFilter(scrobbleEarlyFilter, scrobbleEarlyParams, filter);
        
        // Combined filter for scrobble-based queries
        String combinedFilter = scrobbleEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(scrobbleEarlyParams);
        combinedParams.addAll(filterParams);

        java.util.Map<String, Object> result = new java.util.HashMap<>();
        // Use full filter for all entity types - filter based on songs that match, then aggregate
        result.put("topArtists", getTopArtistsFilteredByDTO(filter, limit));
        result.put("topAlbums", getTopAlbumsFilteredByDTO(filter, limit));
        // Use DTO-based song query to support featured/group indicators
        result.put("topSongs", getTopSongsFilteredByDTO(filter, limit));

        return result;
    }
    
    /**
     * Builds a filter clause for direct artist queries (no song/album override logic).
     */
    private void buildArtistDirectFilterClause(StringBuilder sql, java.util.List<Object> params,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode) {

        // Genre filter - direct on artist
        if (genreIds != null && !genreIds.isEmpty()) {
            appendSimpleFilter(sql, params, "ar.genre_id", genreIds, genreMode);
        }

        // Subgenre filter
        if (subgenreIds != null && !subgenreIds.isEmpty()) {
            appendSimpleFilter(sql, params, "ar.subgenre_id", subgenreIds, subgenreMode);
        }

        // Language filter
        if (languageIds != null && !languageIds.isEmpty()) {
            appendSimpleFilter(sql, params, "ar.language_id", languageIds, languageMode);
        }

        // Gender filter
        if (genderIds != null && !genderIds.isEmpty()) {
            appendSimpleFilter(sql, params, "ar.gender_id", genderIds, genderMode);
        }

        // Ethnicity filter
        if (ethnicityIds != null && !ethnicityIds.isEmpty()) {
            appendSimpleFilter(sql, params, "ar.ethnicity_id", ethnicityIds, ethnicityMode);
        }

        // Country filter
        if (countries != null && !countries.isEmpty()) {
            appendSimpleFilterStrings(sql, params, "ar.country", countries, countryMode);
        }
    }

    /**
     * Builds a filter clause for direct album queries (album override -> artist fallback).
     */
    private void buildAlbumDirectFilterClause(StringBuilder sql, java.util.List<Object> params,
            java.util.List<Integer> genreIds, String genreMode,
            java.util.List<Integer> subgenreIds, String subgenreMode,
            java.util.List<Integer> languageIds, String languageMode,
            java.util.List<Integer> genderIds, String genderMode,
            java.util.List<Integer> ethnicityIds, String ethnicityMode,
            java.util.List<String> countries, String countryMode) {
        
        // Genre filter - album override or artist
        if (genreIds != null && !genreIds.isEmpty()) {
            appendSimpleFilter(sql, params, "COALESCE(alb.override_genre_id, ar.genre_id)", genreIds, genreMode);
        }
        
        // Subgenre filter
        if (subgenreIds != null && !subgenreIds.isEmpty()) {
            appendSimpleFilter(sql, params, "COALESCE(alb.override_subgenre_id, ar.subgenre_id)", subgenreIds, subgenreMode);
        }
        
        // Language filter
        if (languageIds != null && !languageIds.isEmpty()) {
            appendSimpleFilter(sql, params, "COALESCE(alb.override_language_id, ar.language_id)", languageIds, languageMode);
        }
        
        // Gender filter - no album override, direct on artist
        if (genderIds != null && !genderIds.isEmpty()) {
            appendSimpleFilter(sql, params, "ar.gender_id", genderIds, genderMode);
        }
        
        // Ethnicity filter
        if (ethnicityIds != null && !ethnicityIds.isEmpty()) {
            appendSimpleFilter(sql, params, "ar.ethnicity_id", ethnicityIds, ethnicityMode);
        }
        
        // Country filter
        if (countries != null && !countries.isEmpty()) {
            appendSimpleFilterStrings(sql, params, "ar.country", countries, countryMode);
        }
    }

    /**
     * Appends a simple filter condition for integer IDs.
     */
    private void appendSimpleFilter(StringBuilder sql, java.util.List<Object> params, String column, java.util.List<Integer> ids, String mode) {
        if ("isnull".equalsIgnoreCase(mode)) {
            sql.append(" AND ").append(column).append(" IS NULL");
        } else if ("isnotnull".equalsIgnoreCase(mode)) {
            sql.append(" AND ").append(column).append(" IS NOT NULL");
        } else if ("excludes".equalsIgnoreCase(mode)) {
            sql.append(" AND (").append(column).append(" IS NULL OR ").append(column).append(" NOT IN (");
            for (int i = 0; i < ids.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
                params.add(ids.get(i));
            }
            sql.append("))");
        } else {
            // includes (default)
            sql.append(" AND ").append(column).append(" IN (");
            for (int i = 0; i < ids.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
                params.add(ids.get(i));
            }
            sql.append(")");
        }
    }

    /**
     * Appends a simple filter condition for string values.
     */
    private void appendSimpleFilterStrings(StringBuilder sql, java.util.List<Object> params, String column, java.util.List<String> values, String mode) {
        if ("isnull".equalsIgnoreCase(mode)) {
            sql.append(" AND ").append(column).append(" IS NULL");
        } else if ("isnotnull".equalsIgnoreCase(mode)) {
            sql.append(" AND ").append(column).append(" IS NOT NULL");
        } else if ("excludes".equalsIgnoreCase(mode)) {
            sql.append(" AND (").append(column).append(" IS NULL OR ").append(column).append(" NOT IN (");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
                params.add(values.get(i));
            }
            sql.append("))");
        } else {
            // includes (default)
            sql.append(" AND ").append(column).append(" IN (");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
                params.add(values.get(i));
            }
            sql.append(")");
        }
    }

    /**
     * Builds a filter clause for scrobble-level queries (only scrobble_date filters).
     * This is separate from song-level filters so it can be applied in subqueries where scr alias exists.
     */
    private void buildScrobbleDateFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        String listenedDateFrom = filter.getListenedDateFrom();
        String listenedDateTo = filter.getListenedDateTo();
        
        if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty() && 
            listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(scr.scrobble_date) >= DATE(?) AND DATE(scr.scrobble_date) <= DATE(?)");
            params.add(listenedDateFrom);
            params.add(listenedDateTo);
        } else if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty()) {
            sql.append(" AND DATE(scr.scrobble_date) >= DATE(?)");
            params.add(listenedDateFrom);
        } else if (listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(scr.scrobble_date) <= DATE(?)");
            params.add(listenedDateTo);
        }
    }
    
    /**
     * Get top artists filtered using full ChartFilterDTO.
     * Filters songs first, then aggregates by artist.
     * Supports includeGroups (include plays from artist's groups) and includeFeatured (include featured song plays).
     */
    private java.util.List<java.util.Map<String, Object>> getTopArtistsFilteredByDTO(ChartFilterDTO filter, int limit) {
        // Build song-level filter clause  
        StringBuilder songFilterClause = new StringBuilder();
        java.util.List<Object> songParams = new java.util.ArrayList<>();
        // Temporarily store and clear listened date filters to build song-level filter without them
        String savedListenedDateFrom = filter.getListenedDateFrom();
        String savedListenedDateTo = filter.getListenedDateTo();
        filter.setListenedDateFrom(null);
        filter.setListenedDateTo(null);
        buildFilterClause(songFilterClause, songParams, filter);
        // Restore listened date filters
        filter.setListenedDateFrom(savedListenedDateFrom);
        filter.setListenedDateTo(savedListenedDateTo);
        
        // Build scrobble date filter separately
        StringBuilder scrobbleDateFilter = new StringBuilder();
        java.util.List<Object> scrobbleDateParams = new java.util.ArrayList<>();
        buildScrobbleDateFilter(scrobbleDateFilter, scrobbleDateParams, filter);
        
        // Combine parameters in the right order
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        boolean includeGroups = filter.isIncludeGroups();
        boolean includeFeatured = filter.isIncludeFeatured();
        boolean hasArtistFilter = filter.getArtistIds() != null && !filter.getArtistIds().isEmpty();
        
        String sql;
        
        // When there's an artist filter with includeGroups/includeFeatured, the filter clause already
        // expands the artist filter to include group and featured songs. We just need to aggregate
        // by the song's actual artist_id (no UNION attribution needed).
        // The UNION logic is only needed when there's NO artist filter and we want to attribute
        // group/featured plays to individual artists for aggregation purposes.
        if (!includeGroups && !includeFeatured) {
            // Standard query - no includes
            sql = """
                SELECT 
                    ar.id,
                    ar.name,
                    ar.gender_id,
                    ar.genre_id,
                    gen.name as genre,
                    ar.subgenre_id,
                    sg.name as subgenre,
                    ar.ethnicity_id,
                    eth.name as ethnicity,
                    ar.language_id,
                    lang.name as language,
                    ar.country,
                    COALESCE(agg.plays, 0) as plays,
                    COALESCE(agg.primary_plays, 0) as primary_plays,
                    COALESCE(agg.legacy_plays, 0) as legacy_plays,
                    COALESCE(agg.time_listened, 0) as time_listened,
                    agg.first_listened,
                    agg.last_listened
                FROM Artist ar
                LEFT JOIN Genre gen ON ar.genre_id = gen.id
                LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
                LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
                LEFT JOIN Language lang ON ar.language_id = lang.id
                INNER JOIN (
                    SELECT 
                        s.artist_id,
                        SUM(COALESCE(play_stats.plays, 0)) as plays,
                        SUM(COALESCE(play_stats.primary_plays, 0)) as primary_plays,
                        SUM(COALESCE(play_stats.legacy_plays, 0)) as legacy_plays,
                        SUM(COALESCE(play_stats.time_listened, 0)) as time_listened,
                        MIN(play_stats.first_listened) as first_listened,
                        MAX(play_stats.last_listened) as last_listened
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    LEFT JOIN (
                        SELECT 
                            scr.song_id,
                            COUNT(*) as plays,
                            SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                            SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                            SUM(s2.length_seconds) as time_listened,
                            MIN(scr.scrobble_date) as first_listened,
                            MAX(scr.scrobble_date) as last_listened
                        FROM Scrobble scr
                        INNER JOIN Song s2 ON scr.song_id = s2.id
                        WHERE 1=1""" + scrobbleDateFilter.toString() + """
                        GROUP BY scr.song_id
                    ) play_stats ON play_stats.song_id = s.id
                    WHERE 1=1 """ + songFilterClause.toString() + """
                    GROUP BY s.artist_id
                ) agg ON ar.id = agg.artist_id
                ORDER BY plays DESC, agg.last_listened ASC
                LIMIT ?
                """;
            // Add scrobble date params first, then song params, then limit
            params.addAll(scrobbleDateParams);
            params.addAll(songParams);
            params.add(limit);
        } else if (hasArtistFilter) {
            // When there's an artist filter with includeGroups/includeFeatured, use the simple query.
            // The filter clause already expands to include group and featured songs.
            sql = """
                SELECT 
                    ar.id,
                    ar.name,
                    ar.gender_id,
                    ar.genre_id,
                    gen.name as genre,
                    ar.subgenre_id,
                    sg.name as subgenre,
                    ar.ethnicity_id,
                    eth.name as ethnicity,
                    ar.language_id,
                    lang.name as language,
                    ar.country,
                    COALESCE(agg.plays, 0) as plays,
                    COALESCE(agg.primary_plays, 0) as primary_plays,
                    COALESCE(agg.legacy_plays, 0) as legacy_plays,
                    COALESCE(agg.time_listened, 0) as time_listened,
                    agg.first_listened,
                    agg.last_listened
                FROM Artist ar
                LEFT JOIN Genre gen ON ar.genre_id = gen.id
                LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
                LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
                LEFT JOIN Language lang ON ar.language_id = lang.id
                INNER JOIN (
                    SELECT 
                        s.artist_id,
                        SUM(COALESCE(play_stats.plays, 0)) as plays,
                        SUM(COALESCE(play_stats.primary_plays, 0)) as primary_plays,
                        SUM(COALESCE(play_stats.legacy_plays, 0)) as legacy_plays,
                        SUM(COALESCE(play_stats.time_listened, 0)) as time_listened,
                        MIN(play_stats.first_listened) as first_listened,
                        MAX(play_stats.last_listened) as last_listened
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    LEFT JOIN (
                        SELECT 
                            scr.song_id,
                            COUNT(*) as plays,
                            SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                            SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                            SUM(s2.length_seconds) as time_listened,
                            MIN(scr.scrobble_date) as first_listened,
                            MAX(scr.scrobble_date) as last_listened
                        FROM Scrobble scr
                        INNER JOIN Song s2 ON scr.song_id = s2.id
                        WHERE 1=1""" + scrobbleDateFilter.toString() + """
                        GROUP BY scr.song_id
                    ) play_stats ON play_stats.song_id = s.id
                    WHERE 1=1 """ + songFilterClause.toString() + """
                    GROUP BY s.artist_id
                ) agg ON ar.id = agg.artist_id
                ORDER BY plays DESC, agg.last_listened ASC
                LIMIT ?
                """;
            // Add scrobble date params first, then song params, then limit
            params.addAll(scrobbleDateParams);
            params.addAll(songParams);
            params.add(limit);
        } else {
            // No artist filter, but includeGroups/includeFeatured is set.
            // Build union query to attribute group/featured plays to individual artists.
            StringBuilder unionParts = new StringBuilder();

            // Part 1: Direct artist plays (always included)
            unionParts.append("""
                SELECT 
                    s.artist_id as attributed_artist_id,
                    scr.id as scrobble_id,
                    scr.account,
                    s.length_seconds,
                    scr.scrobble_date
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + scrobbleDateFilter.toString() + songFilterClause.toString());
            // Add scrobble date params and song params for Part 1
            params.addAll(scrobbleDateParams);
            params.addAll(songParams);

            if (includeGroups) {
                // Part 2: Group plays - attribute to group members
                // Clone scrobble date params and song params for Part 2
                params.addAll(scrobbleDateParams);
                params.addAll(songParams);
                unionParts.append("""
                    
                    UNION ALL
                    
                    SELECT 
                        am.member_artist_id as attributed_artist_id,
                        scr.id as scrobble_id,
                        scr.account,
                        s.length_seconds,
                        scr.scrobble_date
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    INNER JOIN ArtistMember am ON s.artist_id = am.group_artist_id
                    WHERE 1=1 """ + scrobbleDateFilter.toString() + songFilterClause.toString());
            }
            
            if (includeFeatured) {
                // Part 3: Featured plays - attribute to featured artists
                // Clone scrobble date params and song params for Part 3
                params.addAll(scrobbleDateParams);
                params.addAll(songParams);
                unionParts.append("""
                    
                    UNION ALL
                    
                    SELECT 
                        sfa.artist_id as attributed_artist_id,
                        scr.id as scrobble_id,
                        scr.account,
                        s.length_seconds,
                        scr.scrobble_date
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    INNER JOIN SongFeaturedArtist sfa ON s.id = sfa.song_id
                    WHERE 1=1 """ + scrobbleDateFilter.toString() + songFilterClause.toString());
            }
            
            sql = """
                SELECT 
                    ar.id,
                    ar.name,
                    ar.gender_id,
                    ar.genre_id,
                    gen.name as genre,
                    ar.subgenre_id,
                    sg.name as subgenre,
                    ar.ethnicity_id,
                    eth.name as ethnicity,
                    ar.language_id,
                    lang.name as language,
                    ar.country,
                    agg.plays,
                    agg.primary_plays,
                    agg.legacy_plays,
                    agg.time_listened,
                    agg.first_listened,
                    agg.last_listened
                FROM Artist ar
                LEFT JOIN Genre gen ON ar.genre_id = gen.id
                LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
                LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
                LEFT JOIN Language lang ON ar.language_id = lang.id
                INNER JOIN (
                    SELECT 
                        attributed_artist_id,
                        COUNT(*) as plays,
                        SUM(CASE WHEN account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                        SUM(CASE WHEN account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                        SUM(length_seconds) as time_listened,
                        MIN(scrobble_date) as first_listened,
                        MAX(scrobble_date) as last_listened
                    FROM (
                        """ + unionParts.toString() + """
                    )
                    GROUP BY attributed_artist_id
                ) agg ON ar.id = agg.attributed_artist_id
                ORDER BY agg.plays DESC, agg.last_listened ASC
                LIMIT ?
                """;
            // Add limit parameter for the union query
            params.add(limit);
        }

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("genderId", rs.getObject("gender_id"));
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
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        }, params.toArray());
    }

    /**
     * Get top albums filtered using full ChartFilterDTO.
     * Filters songs first, then aggregates by album.
     */
    private java.util.List<java.util.Map<String, Object>> getTopAlbumsFilteredByDTO(ChartFilterDTO filter, int limit) {
        // Build song-level filter clause  
        StringBuilder songFilterClause = new StringBuilder();
        java.util.List<Object> songParams = new java.util.ArrayList<>();
        // Temporarily store and clear listened date filters to build song-level filter without them
        String savedListenedDateFrom = filter.getListenedDateFrom();
        String savedListenedDateTo = filter.getListenedDateTo();
        filter.setListenedDateFrom(null);
        filter.setListenedDateTo(null);
        buildFilterClause(songFilterClause, songParams, filter);
        // Restore listened date filters
        filter.setListenedDateFrom(savedListenedDateFrom);
        filter.setListenedDateTo(savedListenedDateTo);
        
        // Build scrobble date filter separately
        StringBuilder scrobbleDateFilter = new StringBuilder();
        java.util.List<Object> scrobbleDateParams = new java.util.ArrayList<>();
        buildScrobbleDateFilter(scrobbleDateFilter, scrobbleDateParams, filter);
        
        // Combine parameters: scrobble date params, then song params, then limit
        java.util.List<Object> params = new java.util.ArrayList<>();
        params.addAll(scrobbleDateParams);
        params.addAll(songParams);
        params.add(limit);

        String sql = """
            SELECT 
                alb.id,
                alb.name,
                alb.release_date,
                ar.id as artist_id,
                ar.name as artist_name,
                ar.gender_id,
                COALESCE(alb.override_genre_id, ar.genre_id) as effective_genre_id,
                gen.name as genre,
                COALESCE(alb.override_subgenre_id, ar.subgenre_id) as effective_subgenre_id,
                sg.name as subgenre,
                ar.ethnicity_id,
                eth.name as ethnicity,
                COALESCE(alb.override_language_id, ar.language_id) as effective_language_id,
                lang.name as language,
                ar.country,
                album_len.album_length,
                COALESCE(agg.plays, 0) as plays,
                COALESCE(agg.primary_plays, 0) as primary_plays,
                COALESCE(agg.legacy_plays, 0) as legacy_plays,
                COALESCE(agg.time_listened, 0) as time_listened,
                agg.first_listened,
                agg.last_listened
            FROM Album alb
            INNER JOIN Artist ar ON alb.artist_id = ar.id
            LEFT JOIN Genre gen ON COALESCE(alb.override_genre_id, ar.genre_id) = gen.id
            LEFT JOIN SubGenre sg ON COALESCE(alb.override_subgenre_id, ar.subgenre_id) = sg.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            LEFT JOIN Language lang ON COALESCE(alb.override_language_id, ar.language_id) = lang.id
            LEFT JOIN (
                SELECT album_id, SUM(length_seconds) as album_length
                FROM Song
                WHERE album_id IS NOT NULL
                GROUP BY album_id
            ) album_len ON alb.id = album_len.album_id
            LEFT JOIN (
                SELECT 
                    s.album_id,
                    SUM(COALESCE(play_stats.plays, 0)) as plays,
                    SUM(COALESCE(play_stats.primary_plays, 0)) as primary_plays,
                    SUM(COALESCE(play_stats.legacy_plays, 0)) as legacy_plays,
                    SUM(COALESCE(play_stats.time_listened, 0)) as time_listened,
                    MIN(play_stats.first_listened) as first_listened,
                    MAX(play_stats.last_listened) as last_listened
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN (
                    SELECT 
                        scr.song_id,
                        COUNT(*) as plays,
                        SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                        SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                        SUM(s2.length_seconds) as time_listened,
                        MIN(scr.scrobble_date) as first_listened,
                        MAX(scr.scrobble_date) as last_listened
                    FROM Scrobble scr
                    INNER JOIN Song s2 ON scr.song_id = s2.id
                    WHERE 1=1""" + scrobbleDateFilter.toString() + """
                    GROUP BY scr.song_id
                ) play_stats ON play_stats.song_id = s.id
                WHERE s.album_id IS NOT NULL """ + songFilterClause.toString() + """
                GROUP BY s.album_id
            ) agg ON alb.id = agg.album_id
            WHERE agg.album_id IS NOT NULL
            ORDER BY plays DESC, agg.last_listened ASC
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("releaseDate", formatDate(rs.getString("release_date")));
            row.put("artistId", rs.getInt("artist_id"));
            row.put("artistName", rs.getString("artist_name"));
            row.put("genderId", rs.getObject("gender_id"));
            row.put("genreId", rs.getObject("effective_genre_id"));
            row.put("genre", rs.getString("genre"));
            row.put("subgenreId", rs.getObject("effective_subgenre_id"));
            row.put("subgenre", rs.getString("subgenre"));
            row.put("ethnicityId", rs.getObject("ethnicity_id"));
            row.put("ethnicity", rs.getString("ethnicity"));
            row.put("languageId", rs.getObject("effective_language_id"));
            row.put("language", rs.getString("language"));
            row.put("country", rs.getString("country"));
            // Add album length
            int albumLength = rs.getInt("album_length");
            if (!rs.wasNull() && albumLength > 0) {
                row.put("length", albumLength);
                int hours = albumLength / 3600;
                int mins = (albumLength % 3600) / 60;
                int secs = albumLength % 60;
                if (hours > 0) {
                    row.put("lengthFormatted", String.format("%d:%02d:%02d", hours, mins, secs));
                } else {
                    row.put("lengthFormatted", String.format("%d:%02d", mins, secs));
                }
            } else {
                row.put("length", null);
                row.put("lengthFormatted", null);
            }
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        }, params.toArray());
    }
    
    /**
     * Get top songs filtered using full ChartFilterDTO.
     * When includeGroups/includeFeatured is set with an artist filter, marks songs as fromGroup/featuredOn.
     */
    private java.util.List<java.util.Map<String, Object>> getTopSongsFilteredByDTO(ChartFilterDTO filter, int limit) {
        // Build song-level filter clause  
        StringBuilder songFilterClause = new StringBuilder();
        java.util.List<Object> songParams = new java.util.ArrayList<>();
        // Temporarily store and clear listened date filters to build song-level filter without them
        String savedListenedDateFrom = filter.getListenedDateFrom();
        String savedListenedDateTo = filter.getListenedDateTo();
        filter.setListenedDateFrom(null);
        filter.setListenedDateTo(null);
        buildFilterClause(songFilterClause, songParams, filter);
        // Restore listened date filters
        filter.setListenedDateFrom(savedListenedDateFrom);
        filter.setListenedDateTo(savedListenedDateTo);
        
        // Build scrobble date filter separately
        StringBuilder scrobbleDateFilter = new StringBuilder();
        java.util.List<Object> scrobbleDateParams = new java.util.ArrayList<>();
        buildScrobbleDateFilter(scrobbleDateFilter, scrobbleDateParams, filter);
        
        java.util.List<Integer> artistIds = filter.getArtistIds();
        boolean includeGroups = filter.isIncludeGroups();
        boolean includeFeatured = filter.isIncludeFeatured();
        boolean hasArtistFilter = artistIds != null && !artistIds.isEmpty();
        
        // Build the SQL with optional columns for featured/group detection
        String featuredOnColumn = "";
        String fromGroupColumn = "";
        String primaryArtistColumns = "";
        String sourceArtistColumns = "";
        
        // Lists to hold feature/group params separately
        java.util.List<Object> featuredParams = new java.util.ArrayList<>();
        java.util.List<Object> groupParams = new java.util.ArrayList<>();
        
        if (hasArtistFilter && includeFeatured) {
            // Add column to detect if the song features the selected artist(s)
            String artistPlaceholders = String.join(",", artistIds.stream().map(id -> "?").toList());
            featuredOnColumn = ", CASE WHEN EXISTS (SELECT 1 FROM SongFeaturedArtist sfa2 WHERE sfa2.song_id = s.id AND sfa2.artist_id IN (" + artistPlaceholders + ")) THEN 1 ELSE 0 END as featured_on";
            featuredParams.addAll(artistIds);
            
            // Add primary artist info (the main artist of featured songs)
            primaryArtistColumns = ", s.artist_id as primary_artist_id, ar.name as primary_artist_name";
        }
        
        if (hasArtistFilter && includeGroups) {
            // Add column to detect if the song is from a group the selected artist(s) belong to
            String artistPlaceholders = String.join(",", artistIds.stream().map(id -> "?").toList());
            fromGroupColumn = ", CASE WHEN s.artist_id NOT IN (" + artistPlaceholders + ") AND EXISTS (SELECT 1 FROM ArtistMember am WHERE am.group_artist_id = s.artist_id AND am.member_artist_id IN (" + artistPlaceholders + ")) THEN 1 ELSE 0 END as from_group";
            groupParams.addAll(artistIds); // First IN clause
            groupParams.addAll(artistIds); // Second IN clause
            
            // Add source artist info (the group)
            sourceArtistColumns = ", s.artist_id as source_artist_id, ar.name as source_artist_name";
        }
        
        String sql = """
            SELECT 
                s.id,
                s.name,
                s.artist_id,
                ar.name as artist_name,
                s.album_id,
                alb.name as album_name,
                COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
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
                s.length_seconds,
                s.is_single,
                CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END as has_image,
                CASE WHEN alb.image IS NOT NULL THEN 1 ELSE 0 END as album_has_image,
                COALESCE(play_stats.plays, 0) as plays,
                COALESCE(play_stats.primary_plays, 0) as primary_plays,
                COALESCE(play_stats.legacy_plays, 0) as legacy_plays,
                COALESCE(s.length_seconds, 0) * COALESCE(play_stats.plays, 0) as time_listened,
                play_stats.first_listened,
                play_stats.last_listened
                """ + featuredOnColumn + fromGroupColumn + primaryArtistColumns + sourceArtistColumns + " " + """
            FROM Song s
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
            LEFT JOIN (
                SELECT 
                    scr.song_id,
                    COUNT(*) as plays,
                    SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                    SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                    MIN(scr.scrobble_date) as first_listened,
                    MAX(scr.scrobble_date) as last_listened
                FROM Scrobble scr
                WHERE 1=1""" + scrobbleDateFilter.toString() + """
                GROUP BY scr.song_id
            ) play_stats ON play_stats.song_id = s.id
            WHERE 1=1 """ + songFilterClause.toString() + """
            
            ORDER BY plays DESC, play_stats.last_listened ASC
            LIMIT ?
            """;
        
        // Build final params list in the order they appear in the SQL:
        // 1. scrobble date params (in play_stats WHERE)
        // 2. featured params (in featuredOnColumn CASE WHEN)
        // 3. group params (in fromGroupColumn CASE WHEN)
        // 4. song filter params (in main WHERE)
        // 5. limit
        java.util.List<Object> finalParams = new java.util.ArrayList<>();
        finalParams.addAll(scrobbleDateParams);
        finalParams.addAll(featuredParams);
        finalParams.addAll(groupParams);
        finalParams.addAll(songParams);
        finalParams.add(limit);
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("artistId", rs.getInt("artist_id"));
            row.put("artistName", rs.getString("artist_name"));
            row.put("albumId", rs.getObject("album_id"));
            row.put("albumName", rs.getString("album_name"));
            row.put("genderId", rs.getObject("gender_id"));
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
            int lengthSeconds = rs.getInt("length_seconds");
            row.put("length", rs.wasNull() ? null : lengthSeconds);
            if (!rs.wasNull() && lengthSeconds > 0) {
                int mins = lengthSeconds / 60;
                int secs = lengthSeconds % 60;
                row.put("lengthFormatted", String.format("%d:%02d", mins, secs));
            } else {
                row.put("lengthFormatted", null);
            }
            row.put("isSingle", rs.getInt("is_single") == 1);
            row.put("hasImage", rs.getInt("has_image") == 1);
            row.put("albumHasImage", rs.getInt("album_has_image") == 1);
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            
            // Add featured/group flags if applicable
            if (hasArtistFilter && includeFeatured) {
                try {
                    row.put("featuredOn", rs.getInt("featured_on") == 1);
                    row.put("primaryArtistId", rs.getInt("primary_artist_id"));
                    row.put("primaryArtistName", rs.getString("primary_artist_name"));
                } catch (java.sql.SQLException e) {
                    row.put("featuredOn", false);
                }
            } else {
                row.put("featuredOn", false);
            }
            
            if (hasArtistFilter && includeGroups) {
                try {
                    row.put("fromGroup", rs.getInt("from_group") == 1);
                    row.put("sourceArtistId", rs.getInt("source_artist_id"));
                    row.put("sourceArtistName", rs.getString("source_artist_name"));
                } catch (java.sql.SQLException e) {
                    row.put("fromGroup", false);
                }
            } else {
                row.put("fromGroup", false);
            }
            
            return row;
        }, finalParams.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getTopArtistsFiltered(String filterClause, java.util.List<Object> filterParams, String listenedDateFrom, String listenedDateTo, int limit) {
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        
        // Build date filter clause for scrobble filtering
        StringBuilder dateFilter = new StringBuilder();
        java.util.List<Object> dateParams = new java.util.ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            dateFilter.append(" AND DATE(scr.scrobble_date) >= ?");
            dateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            dateFilter.append(" AND DATE(scr.scrobble_date) <= ?");
            dateParams.add(listenedDateTo);
        }
        
        // Add date params before limit
        params.addAll(0, dateParams); // Add at beginning for WHERE clause
        params.add(limit);
        
        String sql = """
            SELECT 
                ar.id,
                ar.name,
                ar.gender_id,
                ar.genre_id,
                gen.name as genre,
                ar.subgenre_id,
                sg.name as subgenre,
                ar.ethnicity_id,
                eth.name as ethnicity,
                ar.language_id,
                lang.name as language,
                ar.country,
                COALESCE(agg.plays, 0) as plays,
                COALESCE(agg.primary_plays, 0) as primary_plays,
                COALESCE(agg.legacy_plays, 0) as legacy_plays,
                COALESCE(agg.time_listened, 0) as time_listened,
                agg.first_listened,
                agg.last_listened
            FROM Artist ar
            LEFT JOIN Genre gen ON ar.genre_id = gen.id
            LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            LEFT JOIN Language lang ON ar.language_id = lang.id
            INNER JOIN (
                SELECT 
                    s.artist_id,
                    COUNT(*) as plays,
                    SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                    SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                    SUM(s.length_seconds) as time_listened,
                    MIN(scr.scrobble_date) as first_listened,
                    MAX(scr.scrobble_date) as last_listened
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                WHERE 1=1 """ + dateFilter.toString() + """
                GROUP BY s.artist_id
            ) agg ON ar.id = agg.artist_id
            WHERE 1=1 """ + " " + filterClause + """
            ORDER BY plays DESC, last_listened ASC
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("genderId", rs.getObject("gender_id"));
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
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        }, params.toArray());
    }
    
    private java.util.List<java.util.Map<String, Object>> getTopAlbumsFiltered(String filterClause, java.util.List<Object> filterParams, String listenedDateFrom, String listenedDateTo, int limit) {
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);

        // Build date filter clause for scrobble filtering
        StringBuilder dateFilter = new StringBuilder();
        java.util.List<Object> dateParams = new java.util.ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            dateFilter.append(" AND DATE(scr.scrobble_date) >= ?");
            dateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            dateFilter.append(" AND DATE(scr.scrobble_date) <= ?");
            dateParams.add(listenedDateTo);
        }
        
        // Add date params at beginning for WHERE clause
        params.addAll(0, dateParams);
        params.add(limit);
        
        String sql = """
            SELECT 
                alb.id,
                alb.name,
                ar.id as artist_id,
                ar.name as artist_name,
                ar.gender_id,
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
                COALESCE(agg.plays, 0) as plays,
                COALESCE(agg.primary_plays, 0) as primary_plays,
                COALESCE(agg.legacy_plays, 0) as legacy_plays,
                COALESCE(agg.time_listened, 0) as time_listened,
                agg.first_listened,
                agg.last_listened
            FROM Album alb
            INNER JOIN Artist ar ON alb.artist_id = ar.id
            LEFT JOIN Genre g_override ON alb.override_genre_id = g_override.id
            LEFT JOIN Genre g_artist ON ar.genre_id = g_artist.id
            LEFT JOIN SubGenre sg_override ON alb.override_subgenre_id = sg_override.id
            LEFT JOIN SubGenre sg_artist ON ar.subgenre_id = sg_artist.id
            LEFT JOIN Language l_override ON alb.override_language_id = l_override.id
            LEFT JOIN Language l_artist ON ar.language_id = l_artist.id
            LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
            INNER JOIN (
                SELECT 
                    s.album_id,
                    COUNT(*) as plays,
                    SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                    SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                    SUM(s.length_seconds) as time_listened,
                    MIN(scr.scrobble_date) as first_listened,
                    MAX(scr.scrobble_date) as last_listened
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                WHERE s.album_id IS NOT NULL """ + dateFilter.toString() + """
                GROUP BY s.album_id
            ) agg ON alb.id = agg.album_id
            WHERE 1=1 """ + " " + filterClause + """
            ORDER BY plays DESC, last_listened ASC
            LIMIT ?
            """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("artistId", rs.getInt("artist_id"));
            row.put("artistName", rs.getString("artist_name"));
            row.put("genderId", rs.getObject("gender_id"));
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
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        }, params.toArray());
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
                COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
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
                s.length_seconds,
                s.is_single,
                MAX(CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END) as has_image,
                MAX(CASE WHEN alb.image IS NOT NULL THEN 1 ELSE 0 END) as album_has_image,
                COUNT(*) as plays,
                SUM(CASE WHEN scr.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                SUM(CASE WHEN scr.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                COALESCE(s.length_seconds, 0) * COUNT(*) as time_listened,
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
            GROUP BY s.id
            ORDER BY plays DESC, last_listened ASC
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("id", rs.getInt("id"));
            row.put("name", rs.getString("name"));
            row.put("artistId", rs.getInt("artist_id"));
            row.put("artistName", rs.getString("artist_name"));
            row.put("albumId", rs.getObject("album_id"));
            row.put("albumName", rs.getString("album_name"));
            row.put("genderId", rs.getObject("gender_id"));
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
            row.put("hasImage", rs.getInt("has_image") == 1);
            row.put("albumHasImage", rs.getInt("album_has_image") == 1);
            row.put("isSingle", rs.getBoolean("is_single"));
            // Add song length
            int lengthSeconds = rs.getInt("length_seconds");
            if (!rs.wasNull() && lengthSeconds > 0) {
                row.put("length", lengthSeconds);
                int mins = lengthSeconds / 60;
                int secs = lengthSeconds % 60;
                row.put("lengthFormatted", String.format("%d:%02d", mins, secs));
            } else {
                row.put("length", null);
                row.put("lengthFormatted", null);
            }
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            return row;
        }, params.toArray());
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
