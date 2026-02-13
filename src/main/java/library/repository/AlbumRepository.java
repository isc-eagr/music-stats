package library.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class AlbumRepository {
    
    private final JdbcTemplate jdbcTemplate;
    
    public AlbumRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public List<Object[]> findAlbumsWithStats(String name, String artistName,
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
                                               String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand,
                                               Integer ageMin, Integer ageMax, String ageMode,
                                               Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                               String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                               String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                               Integer playCountMin, Integer playCountMax, Integer songCountMin, Integer songCountMax,
                                               Integer lengthMin, Integer lengthMax, String lengthMode,
                                               Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                               Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                               Integer yearlyChartPeak, Integer yearlyChartYears,
                                               String sortBy, String sortDir, int limit, int offset) {
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
        sql.append("""
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
                a.release_date as release_date,
                COALESCE(song_stats.song_count, 0) as song_count,
                COALESCE(song_stats.album_length, 0) as album_length,
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                gender.name as gender_name,
                COALESCE(play_stats.play_count, 0) as play_count,
                COALESCE(play_stats.vatito_play_count, 0) as vatito_play_count,
                COALESCE(play_stats.robertlover_play_count, 0) as robertlover_play_count,
                COALESCE(play_stats.time_listened, 0) as time_listened,
                play_stats.first_listened,
                play_stats.last_listened,
                ar.country as country,
                a.organized
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
            LEFT JOIN (SELECT album_id, COUNT(*) as song_count, SUM(length_seconds) as album_length FROM Song GROUP BY album_id) song_stats ON song_stats.album_id = a.id
            """);
        
        // Use INNER JOIN when account filter is includes mode OR when listened date filter is active (for better performance)
        boolean hasListenedDateFilter = listenedDateFilterClause.length() > 0;
        String playStatsJoinType = ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) ? "INNER JOIN" : "LEFT JOIN";
        sql.append(playStatsJoinType).append(""" 
             (
                SELECT 
                    song.album_id,
                    COUNT(*) as play_count,
                    SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as vatito_play_count,
                    SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_play_count,
                    SUM(song.length_seconds) as time_listened,
                    MIN(p.play_date) as first_listened,
                    MAX(p.play_date) as last_listened
                FROM Play p
                JOIN Song song ON p.song_id = song.id
                WHERE song.album_id IS NOT NULL """);
        sql.append(accountFilterClause);
        sql.append(listenedDateFilterClause);
        sql.append("""
                GROUP BY song.album_id
            ) play_stats ON play_stats.album_id = a.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        // Add account params and listened date params for play_stats subquery
        params.addAll(accountParams);
        params.addAll(listenedDateParams);
        
        // Name filters with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("a.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("ar.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(artistName) + "%");
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
                case "isnull":
                    sql.append(" AND a.release_date IS NULL");
                    break;
                case "isnotnull":
                    sql.append(" AND a.release_date IS NOT NULL");
                    break;
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
            String subquery = "(SELECT MIN(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
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
            String subquery = "(SELECT MAX(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
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
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM AlbumImage WHERE album_id = a.id)) >= ? ");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM AlbumImage WHERE album_id = a.id)) <= ? ");
            params.add(imageCountMax);
        }
        
        // Has Featured Artists filter (check if any song in the album has featured artists)
        if (hasFeaturedArtists != null && !hasFeaturedArtists.isEmpty()) {
            if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND EXISTS (SELECT 1 FROM SongFeaturedArtist sfa JOIN Song s ON sfa.song_id = s.id WHERE s.album_id = a.id) ");
            } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa JOIN Song s ON sfa.song_id = s.id WHERE s.album_id = a.id) ");
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

        // Age at Release filter (artist's age when album was released)
        if (ageAtReleaseMin != null || ageAtReleaseMax != null) {
            String ageAtReleaseExpr = "CAST((julianday(a.release_date) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND a.release_date IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ? ");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND a.release_date IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ? ");
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
        
        // Song count filter
        if (songCountMin != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song WHERE album_id = a.id) >= ? ");
            params.add(songCountMin);
        }
        if (songCountMax != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song WHERE album_id = a.id) <= ? ");
            params.add(songCountMax);
        }
        
        // Length filter (album_length in seconds)
        if (lengthMode != null && !lengthMode.isEmpty()) {
            if ("null".equalsIgnoreCase(lengthMode) || "zero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (COALESCE(song_stats.album_length, 0) = 0) ");
            } else if ("notnull".equalsIgnoreCase(lengthMode) || "nonzero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (COALESCE(song_stats.album_length, 0) > 0) ");
            } else if ("lt".equalsIgnoreCase(lengthMode) && lengthMax != null) {
                sql.append(" AND COALESCE(song_stats.album_length, 0) < ? ");
                params.add(lengthMax);
            } else if ("gt".equalsIgnoreCase(lengthMode) && lengthMin != null) {
                sql.append(" AND COALESCE(song_stats.album_length, 0) > ? ");
                params.add(lengthMin);
            } else {
                // Default "range" mode
                if (lengthMin != null) {
                    sql.append(" AND COALESCE(song_stats.album_length, 0) >= ? ");
                    params.add(lengthMin);
                }
                if (lengthMax != null) {
                    sql.append(" AND COALESCE(song_stats.album_length, 0) <= ? ");
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
            sql.append("WHERE ce.album_id = a.id AND c.chart_type = 'album' AND c.period_type = 'weekly'");
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
            sql.append("WHERE ce.album_id = a.id AND c.chart_type = 'album' AND c.period_type = 'seasonal'");
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
            sql.append("WHERE ce.album_id = a.id AND c.chart_type = 'album' AND c.period_type = 'yearly'");
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
        
        // Sorting
        String direction = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        switch (sortBy != null ? sortBy : "name") {
            case "artist" -> sql.append(" ORDER BY ar.name " + direction + ", a.name");
            case "release_date" -> sql.append(" ORDER BY a.release_date " + direction + " NULLS LAST, a.name");
            case "song_count" -> sql.append(" ORDER BY song_count " + direction + ", a.name");
            case "album_length" -> sql.append(" ORDER BY album_length " + direction + ", a.name");
            case "plays" -> sql.append(" ORDER BY play_count " + direction + ", a.name");
            case "time" -> sql.append(" ORDER BY time_listened " + direction + ", a.name");
            case "first_listened" -> sql.append(" ORDER BY first_listened " + direction + " NULLS LAST, a.name");
            case "last_listened" -> sql.append(" ORDER BY last_listened " + direction + " NULLS LAST, a.name");
            case "age_at_release" -> sql.append(" ORDER BY CAST((julianday(a.release_date) - julianday(ar.birth_date)) / 365.25 AS INTEGER) " + direction + " NULLS LAST, a.name");
            default -> sql.append(" ORDER BY a.name " + direction);
        }
        
        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            Object[] row = new Object[26];
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
            row[13] = rs.getString("release_date");
            row[14] = rs.getInt("song_count");
            row[15] = rs.getLong("album_length");
            row[16] = rs.getInt("has_image");
            row[17] = rs.getString("gender_name");
            row[18] = rs.getInt("play_count");
            row[19] = rs.getInt("vatito_play_count");
            row[20] = rs.getInt("robertlover_play_count");
            row[21] = rs.getLong("time_listened");
            row[22] = rs.getString("first_listened");
            row[23] = rs.getString("last_listened");
            row[24] = rs.getString("country");
            row[25] = rs.getObject("organized");
            return row;
        }, params.toArray());
    }
    
    public long countAlbumsWithFilters(String name, String artistName,
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
                                       String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand,
                                       Integer ageMin, Integer ageMax, String ageMode,
                                       Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                       String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                       String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                       Integer playCountMin, Integer playCountMax, Integer songCountMin, Integer songCountMax,
                                       Integer lengthMin, Integer lengthMax, String lengthMode,
                                       Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                       Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                       Integer yearlyChartPeak, Integer yearlyChartYears) {
        // Build account filter subquery for play_stats if we need play count filter
        StringBuilder accountFilterClause = new StringBuilder();
        List<Object> accountParams = new ArrayList<>();
        
        // Build listened date filter clause for play_stats subquery
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
        boolean hasListenedDateFilter = listenedDateFilterClause.length() > 0;
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
        
        StringBuilder sql = new StringBuilder();
        
        // Use a more efficient approach with JOIN for account filtering
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            sql.append(
                "SELECT COUNT(DISTINCT a.id) " +
                "FROM Album a " +
                "LEFT JOIN Artist ar ON a.artist_id = ar.id ");
            
            // Add play_stats JOIN if we need to filter by play count or listened date
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("""
                    LEFT JOIN (
                        SELECT s.album_id, COUNT(*) as play_count
                        FROM Play p
                        JOIN Song s ON p.song_id = s.id
                        WHERE 1=1 """);
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append("""
                        GROUP BY s.album_id
                    ) play_stats ON play_stats.album_id = a.id
                    """);
            }
            
            sql.append("INNER JOIN Song s ON s.album_id = a.id " +
                "INNER JOIN Play p ON p.song_id = s.id " +
                "WHERE p.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
            }
            sql.append(") ");
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            sql.append(
                "SELECT COUNT(DISTINCT a.id) " +
                "FROM Album a " +
                "LEFT JOIN Artist ar ON a.artist_id = ar.id ");
            
            // Add play_stats JOIN if we need to filter by play count or listened date
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("""
                    LEFT JOIN (
                        SELECT s.album_id, COUNT(*) as play_count
                        FROM Play p
                        JOIN Song s ON p.song_id = s.id
                        WHERE 1=1 """);
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append("""
                        GROUP BY s.album_id
                    ) play_stats ON play_stats.album_id = a.id
                    """);
            }
            
            sql.append("WHERE NOT EXISTS ( " +
                "    SELECT 1 FROM Play p " +
                "    JOIN Song song ON p.song_id = song.id " +
                "    WHERE song.album_id = a.id AND p.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
            }
            sql.append(") ) AND 1=1 ");
        } else {
            sql.append(
                "SELECT COUNT(*) " +
                "FROM Album a " +
                "LEFT JOIN Artist ar ON a.artist_id = ar.id ");
            
            // Add song_stats JOIN if we need to filter by length
            if (lengthMin != null || lengthMax != null || (lengthMode != null && !lengthMode.isEmpty())) {
                sql.append("LEFT JOIN (SELECT album_id, COUNT(*) as song_count, SUM(length_seconds) as album_length FROM Song GROUP BY album_id) song_stats ON song_stats.album_id = a.id ");
            }
            
            // Add play_stats JOIN if we need to filter by play count or listened date
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("""
                    LEFT JOIN (
                        SELECT s.album_id, COUNT(*) as play_count
                        FROM Play p
                        JOIN Song s ON p.song_id = s.id
                        WHERE 1=1 """);
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append("""
                        GROUP BY s.album_id
                    ) play_stats ON play_stats.album_id = a.id
                    """);
            }
            
            sql.append("WHERE 1=1 ");
        }
        
        List<Object> params = new ArrayList<>();
        
        // Add account params for play_stats subquery
        if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
        }
        
        // Account params for main query if using includes or excludes mode
        if (accounts != null && !accounts.isEmpty()) {
            params.addAll(accounts);
        }
        
        // Name filters with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("a.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("ar.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(artistName) + "%");
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
                case "isnull":
                    sql.append(" AND a.release_date IS NULL");
                    break;
                case "isnotnull":
                    sql.append(" AND a.release_date IS NOT NULL");
                    break;
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
            String subquery = "(SELECT MIN(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
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
            String subquery = "(SELECT MAX(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
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
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM AlbumImage WHERE album_id = a.id)) >= ? ");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM AlbumImage WHERE album_id = a.id)) <= ? ");
            params.add(imageCountMax);
        }
        
        // Has Featured Artists filter (check if any song in the album has featured artists)
        if (hasFeaturedArtists != null && !hasFeaturedArtists.isEmpty()) {
            if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND EXISTS (SELECT 1 FROM SongFeaturedArtist sfa JOIN Song s ON sfa.song_id = s.id WHERE s.album_id = a.id) ");
            } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa JOIN Song s ON sfa.song_id = s.id WHERE s.album_id = a.id) ");
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

        // Age at Release filter (artist's age when album was released)
        if (ageAtReleaseMin != null || ageAtReleaseMax != null) {
            String ageAtReleaseExpr = "CAST((julianday(a.release_date) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND a.release_date IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ? ");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND a.release_date IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ? ");
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
        
        // Song count filter
        if (songCountMin != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song WHERE album_id = a.id) >= ? ");
            params.add(songCountMin);
        }
        if (songCountMax != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song WHERE album_id = a.id) <= ? ");
            params.add(songCountMax);
        }
        
        // Length filter (album_length in seconds)
        if (lengthMode != null && !lengthMode.isEmpty()) {
            if ("null".equalsIgnoreCase(lengthMode) || "zero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (COALESCE(song_stats.album_length, 0) = 0) ");
            } else if ("notnull".equalsIgnoreCase(lengthMode) || "nonzero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (COALESCE(song_stats.album_length, 0) > 0) ");
            } else if ("lt".equalsIgnoreCase(lengthMode) && lengthMax != null) {
                sql.append(" AND COALESCE(song_stats.album_length, 0) < ? ");
                params.add(lengthMax);
            } else if ("gt".equalsIgnoreCase(lengthMode) && lengthMin != null) {
                sql.append(" AND COALESCE(song_stats.album_length, 0) > ? ");
                params.add(lengthMin);
            } else {
                // Default "range" mode
                if (lengthMin != null) {
                    sql.append(" AND COALESCE(song_stats.album_length, 0) >= ? ");
                    params.add(lengthMin);
                }
                if (lengthMax != null) {
                    sql.append(" AND COALESCE(song_stats.album_length, 0) <= ? ");
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
            sql.append("WHERE ce.album_id = a.id AND c.chart_type = 'album' AND c.period_type = 'weekly'");
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
            sql.append("WHERE ce.album_id = a.id AND c.chart_type = 'album' AND c.period_type = 'seasonal'");
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
            sql.append("WHERE ce.album_id = a.id AND c.chart_type = 'album' AND c.period_type = 'yearly'");
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
     * Count albums grouped by gender (from artist) for the filtered dataset.
     * Returns a Map with gender_id as key and count as value.
     * More efficient than loading all albums and counting in memory.
     */
    public Map<Integer, Long> countAlbumsByGenderWithFilters(String name, String artistName,
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
                                       String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand,
                                       Integer ageMin, Integer ageMax, String ageMode,
                                       Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                       String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                       String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                       String inItunes,
                                       Integer playCountMin, Integer playCountMax, Integer songCountMin, Integer songCountMax,
                                       Integer lengthMin, Integer lengthMax, String lengthMode,
                                       Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                       Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                       Integer yearlyChartPeak, Integer yearlyChartYears) {
        // Build account filter subquery for play_stats if we need play count filter
        StringBuilder accountFilterClause = new StringBuilder();
        List<Object> accountParams = new ArrayList<>();
        
        // Build listened date filter clause for play_stats subquery
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
        boolean hasListenedDateFilter = listenedDateFilterClause.length() > 0;
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
        
        StringBuilder sql = new StringBuilder();
        
        // Use a more efficient approach with JOIN for account filtering
        if (accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) {
            sql.append(
                "SELECT ar.gender_id, COUNT(DISTINCT a.id) as cnt " +
                "FROM Album a " +
                "LEFT JOIN Artist ar ON a.artist_id = ar.id ");
            
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("""
                    LEFT JOIN (
                        SELECT s.album_id, COUNT(*) as play_count
                        FROM Play p
                        JOIN Song s ON p.song_id = s.id
                        WHERE 1=1 """);
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append("""
                        GROUP BY s.album_id
                    ) play_stats ON play_stats.album_id = a.id
                    """);
            }
            
            sql.append("INNER JOIN Song s ON s.album_id = a.id " +
                "INNER JOIN Play p ON p.song_id = s.id " +
                "WHERE p.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
            }
            sql.append(") ");
        } else if (accounts != null && !accounts.isEmpty() && "excludes".equalsIgnoreCase(accountMode)) {
            sql.append(
                "SELECT ar.gender_id, COUNT(DISTINCT a.id) as cnt " +
                "FROM Album a " +
                "LEFT JOIN Artist ar ON a.artist_id = ar.id ");
            
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("""
                    LEFT JOIN (
                        SELECT s.album_id, COUNT(*) as play_count
                        FROM Play p
                        JOIN Song s ON p.song_id = s.id
                        WHERE 1=1 """);
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append("""
                        GROUP BY s.album_id
                    ) play_stats ON play_stats.album_id = a.id
                    """);
            }
            
            sql.append("WHERE NOT EXISTS ( " +
                "    SELECT 1 FROM Play p " +
                "    JOIN Song song ON p.song_id = song.id " +
                "    WHERE song.album_id = a.id AND p.account IN (");
            for (int i = 0; i < accounts.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append("?");
            }
            sql.append(") ) AND 1=1 ");
        } else {
            sql.append(
                "SELECT ar.gender_id, COUNT(*) as cnt " +
                "FROM Album a " +
                "LEFT JOIN Artist ar ON a.artist_id = ar.id ");
            
            if (lengthMin != null || lengthMax != null || (lengthMode != null && !lengthMode.isEmpty())) {
                sql.append("LEFT JOIN (SELECT album_id, COUNT(*) as song_count, SUM(length_seconds) as album_length FROM Song GROUP BY album_id) song_stats ON song_stats.album_id = a.id ");
            }
            
            if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
                sql.append("""
                    LEFT JOIN (
                        SELECT s.album_id, COUNT(*) as play_count
                        FROM Play p
                        JOIN Song s ON p.song_id = s.id
                        WHERE 1=1 """);
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append("""
                        GROUP BY s.album_id
                    ) play_stats ON play_stats.album_id = a.id
                    """);
            }
            
            sql.append("WHERE 1=1 ");
        }
        
        List<Object> params = new ArrayList<>();
        
        // Add account params for play_stats subquery
        if (playCountMin != null || playCountMax != null || hasListenedDateFilter) {
            params.addAll(accountParams);
            params.addAll(listenedDateParams);
        }
        
        // Account params for main query if using includes or excludes mode
        if (accounts != null && !accounts.isEmpty()) {
            params.addAll(accounts);
        }
        
        // Name filters with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("a.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist name filter
        if (artistName != null && !artistName.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("ar.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(artistName) + "%");
        }
        
        // Gender filter (filter on artist gender)
        library.util.SqlFilterHelper.appendIdFilter(sql, params, "ar.gender_id", genderIds, genderMode);
        
        // Genre filter (using COALESCE to check album override first, then artist)
        String genreExpr = "COALESCE(a.override_genre_id, ar.genre_id)";
        library.util.SqlFilterHelper.appendIdFilter(sql, params, genreExpr, genreIds, genreMode);
        
        // Subgenre filter
        String subgenreExpr = "COALESCE(a.override_subgenre_id, ar.subgenre_id)";
        library.util.SqlFilterHelper.appendIdFilter(sql, params, subgenreExpr, subgenreIds, subgenreMode);
        
        // Language filter
        String languageExpr = "COALESCE(a.override_language_id, ar.language_id)";
        library.util.SqlFilterHelper.appendIdFilter(sql, params, languageExpr, languageIds, languageMode);
        
        // Ethnicity filter
        library.util.SqlFilterHelper.appendIdFilter(sql, params, "ar.ethnicity_id", ethnicityIds, ethnicityMode);
        
        // Country filter
        library.util.SqlFilterHelper.appendStringFilter(sql, params, "ar.country", countries, countryMode);
        
        // Release date filter
        library.util.SqlFilterHelper.appendDateFilter(sql, params, "a.release_date", releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        // First listened date filter
        String firstListenedSubquery = "(SELECT MIN(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
        library.util.SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last listened date filter
        String lastListenedSubquery = "(SELECT MAX(p.play_date) FROM Play p WHERE p.song_id IN (SELECT id FROM Song WHERE album_id = a.id))";
        library.util.SqlFilterHelper.appendDateFilter(sql, params, lastListenedSubquery, lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode);
        
        // Birth date filter
        library.util.SqlFilterHelper.appendDateFilter(sql, params, "ar.birth_date", birthDate, birthDateFrom, birthDateTo, birthDateMode);
        
        // Death date filter
        library.util.SqlFilterHelper.appendDateFilter(sql, params, "ar.death_date", deathDate, deathDateFrom, deathDateTo, deathDateMode);
        
        // Age filter
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
        
        // Age at release filter
        if (ageAtReleaseMin != null || ageAtReleaseMax != null) {
            String ageAtReleaseExpr = "CAST((julianday(a.release_date) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND a.release_date IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ?");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND a.release_date IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ?");
                params.add(ageAtReleaseMax);
            }
        }
        
        // Organized filter
        if (organized != null && !organized.isEmpty()) {
            if ("true".equalsIgnoreCase(organized)) {
                sql.append(" AND a.organized = 1");
            } else if ("false".equalsIgnoreCase(organized)) {
                sql.append(" AND (a.organized = 0 OR a.organized IS NULL)");
            }
        }
        
        // Image count filter
        if (imageCountMin != null) {
            sql.append(" AND ((CASE WHEN a.cover_image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM AlbumImage WHERE album_id = a.id)) >= ?");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN a.cover_image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM AlbumImage WHERE album_id = a.id)) <= ?");
            params.add(imageCountMax);
        }
        
        // Has featured artists filter
        if (hasFeaturedArtists != null && !hasFeaturedArtists.isEmpty()) {
            if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND EXISTS (SELECT 1 FROM FeaturedArtist fa JOIN Song fs ON fa.song_id = fs.id WHERE fs.album_id = a.id)");
            } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM FeaturedArtist fa JOIN Song fs ON fa.song_id = fs.id WHERE fs.album_id = a.id)");
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
        
        // In iTunes filter
        if (inItunes != null && !inItunes.isEmpty()) {
            if ("true".equalsIgnoreCase(inItunes)) {
                sql.append(" AND EXISTS (SELECT 1 FROM Song s2 WHERE s2.album_id = a.id AND s2.in_itunes = 1)");
            } else if ("false".equalsIgnoreCase(inItunes)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM Song s2 WHERE s2.album_id = a.id AND s2.in_itunes = 1)");
            }
        }
        
        // Play count filter
        if (playCountMin != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) >= ?");
            params.add(playCountMin);
        }
        if (playCountMax != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) <= ?");
            params.add(playCountMax);
        }
        
        // Song count filter
        if (songCountMin != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song WHERE album_id = a.id) >= ?");
            params.add(songCountMin);
        }
        if (songCountMax != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song WHERE album_id = a.id) <= ?");
            params.add(songCountMax);
        }
        
        // Length filter
        if (lengthMin != null || lengthMax != null || (lengthMode != null && !lengthMode.isEmpty())) {
            String lengthExpr = "COALESCE(song_stats.album_length, 0)";
            if ("isnull".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND ").append(lengthExpr).append(" = 0");
            } else if ("isnotnull".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND ").append(lengthExpr).append(" > 0");
            } else {
                if (lengthMin != null) {
                    sql.append(" AND ").append(lengthExpr).append(" >= ?");
                    params.add(lengthMin);
                }
                if (lengthMax != null) {
                    sql.append(" AND ").append(lengthExpr).append(" <= ?");
                    params.add(lengthMax);
                }
            }
        }
        
        // Add GROUP BY
        sql.append(" GROUP BY ar.gender_id");
        
        Map<Integer, Long> result = new HashMap<>();
        jdbcTemplate.query(sql.toString(), rs -> {
            Integer genderId = rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null;
            Long cnt = rs.getLong("cnt");
            result.put(genderId, cnt);
        }, params.toArray());
        
        return result;
    }
}
