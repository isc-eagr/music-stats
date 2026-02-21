package library.service;

import library.dto.TimeframeCardDTO;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.*;

@Service
public class TimeframeService {
    
    private final JdbcTemplate jdbcTemplate;
    
    public TimeframeService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * Get timeframe cards with aggregated stats
     */
    public List<TimeframeCardDTO> getTimeframeCards(String periodType, 
            List<Integer> winningGender, String winningGenderMode,
            List<Integer> winningGenre, String winningGenreMode,
            List<Integer> winningEthnicity, String winningEthnicityMode,
            List<Integer> winningLanguage, String winningLanguageMode,
            List<String> winningCountry, String winningCountryMode,
            Integer artistCountMin, Integer artistCountMax,
            Integer albumCountMin, Integer albumCountMax,
            Integer songCountMin, Integer songCountMax,
            Integer playsMin, Integer playsMax,
            Long timeMin, Long timeMax,
            Double maleArtistPctMin, Double maleArtistPctMax,
            Double maleAlbumPctMin, Double maleAlbumPctMax,
            Double maleSongPctMin, Double maleSongPctMax,
            Double malePlayPctMin, Double malePlayPctMax,
            Double maleTimePctMin, Double maleTimePctMax,
            String dateFrom, String dateTo,
            Integer maleDaysMin, Integer maleDaysMax,
            String sortBy, String sortDir, int page, int perPage) {
        
        int offset = page * perPage;
        String sortDirection = "asc".equalsIgnoreCase(sortDir) ? "ASC" : "DESC";
        
        // Check if we need to merge with all periods (for 0-play period support)
        // If so, skip SQL pagination and do it in Java after the merge
        // Note: decades are NOT included here since we don't generate all decade keys - they use the regular path
        boolean needsMergeWithAllPeriods = ("days".equals(periodType) || "weeks".equals(periodType) || 
                "months".equals(periodType) || "seasons".equals(periodType) || "years".equals(periodType)) 
                && !hasRestrictiveFilters(
                    winningGender, winningGenderMode, winningGenre, winningGenreMode,
                    winningEthnicity, winningEthnicityMode, winningLanguage, winningLanguageMode,
                    winningCountry, winningCountryMode, artistCountMin, albumCountMin, songCountMin,
                    playsMin, timeMin,
                    maleArtistPctMin, maleAlbumPctMin, maleSongPctMin, malePlayPctMin, maleTimePctMin);
        
        // Check if Java-side post-processing is needed (for filters/sorts not computable in SQL)
        boolean needsJavaPostFilter = (maleDaysMin != null || maleDaysMax != null || 
                                        dateFrom != null || dateTo != null);
        boolean needsJavaSorting = "maledays".equalsIgnoreCase(sortBy);
        boolean skipSqlPagination = needsMergeWithAllPeriods || needsJavaPostFilter || needsJavaSorting;
        
        // Build period key expression based on type
        String periodKeyExpr = getPeriodKeyExpression(periodType);
        
        // Build sort column
        String sortColumn = getSortColumn(sortBy, periodType);
        
        // Build the main query
        // OPTIMIZATION: Reduce work by filtering/sorting period_stats FIRST, then only compute winning attrs for visible rows
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        sql.append(String.format("""
            WITH period_summary AS (
                SELECT 
                    %s as period_key,
                    COUNT(*) as play_count,
                    COALESCE(SUM(s.length_seconds), 0) as time_listened,
                    COUNT(DISTINCT ar.id) as artist_count,
                    COUNT(DISTINCT CASE WHEN s.album_id IS NOT NULL THEN s.album_id END) as album_count,
                    COUNT(DISTINCT s.id) as song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN s.id END) as male_song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN s.id END) as female_song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN s.id END) as other_song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN ar.id END) as male_artist_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN ar.id END) as female_artist_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN ar.id END) as other_artist_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 AND s.album_id IS NOT NULL THEN s.album_id END) as male_album_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 AND s.album_id IS NOT NULL THEN s.album_id END) as female_album_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL AND s.album_id IS NOT NULL THEN s.album_id END) as other_album_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN 1 ELSE 0 END) as male_play_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN 1 ELSE 0 END) as female_play_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN 1 ELSE 0 END) as other_play_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_time_listened,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_time_listened,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_time_listened
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                WHERE p.play_date IS NOT NULL
                GROUP BY period_key
                HAVING period_key IS NOT NULL
            ),
            filtered_periods AS (
                SELECT 
                    period_key,
                    play_count,
                    time_listened,
                    artist_count,
                    album_count,
                    song_count,
                    male_song_count,
                    female_song_count,
                    other_song_count,
                    male_artist_count,
                    female_artist_count,
                    other_artist_count,
                    male_album_count,
                    female_album_count,
                    other_album_count,
                    male_play_count,
                    female_play_count,
                    other_play_count,
                    male_time_listened,
                    female_time_listened,
                    other_time_listened,
                    CASE 
                        WHEN (male_artist_count + female_artist_count + other_artist_count) > 0 
                        THEN CAST(male_artist_count AS REAL) * 100.0 / (male_artist_count + female_artist_count + other_artist_count)
                        ELSE NULL 
                    END as male_artist_pct,
                    CASE 
                        WHEN (male_album_count + female_album_count + other_album_count) > 0 
                        THEN CAST(male_album_count AS REAL) * 100.0 / (male_album_count + female_album_count + other_album_count)
                        ELSE NULL 
                    END as male_album_pct,
                    CASE 
                        WHEN (male_song_count + female_song_count + other_song_count) > 0 
                        THEN CAST(male_song_count AS REAL) * 100.0 / (male_song_count + female_song_count + other_song_count)
                        ELSE NULL 
                    END as male_song_pct,
                    CASE 
                        WHEN (male_play_count + female_play_count + other_play_count) > 0 
                        THEN CAST(male_play_count AS REAL) * 100.0 / (male_play_count + female_play_count + other_play_count)
                        ELSE NULL 
                    END as male_play_pct,
                    CASE 
                        WHEN (male_time_listened + female_time_listened + other_time_listened) > 0 
                        THEN CAST(male_time_listened AS REAL) * 100.0 / (male_time_listened + female_time_listened + other_time_listened)
                        ELSE NULL 
                    END as male_time_pct
                FROM period_summary
                WHERE 1=1""", periodKeyExpr));
        
        // Append filter conditions inline here (before ORDER BY/LIMIT)
        appendInlineSummaryFilters(sql, params,
            artistCountMin, artistCountMax,
            albumCountMin, albumCountMax,
            songCountMin, songCountMax,
            playsMin, playsMax,
            timeMin, timeMax,
            maleArtistPctMin, maleArtistPctMax,
            maleAlbumPctMin, maleAlbumPctMax,
            maleSongPctMin, maleSongPctMax,
            malePlayPctMin, malePlayPctMax,
            maleTimePctMin, maleTimePctMax);
        
        sql.append("\n                ORDER BY ").append(sortColumn.replace("ps.", "")).append(" ").append(sortDirection);

        // Only apply SQL pagination if we're NOT going to do Java post-processing
        // When merging/filtering/sorting in Java, we need ALL results from DB, then do pagination in Java
        if (!skipSqlPagination) {
            sql.append("\n                LIMIT ? OFFSET ?");
            params.add(perPage);
            params.add(offset);
        }
        sql.append("\n            ),");
        
        // Now compute winning attributes ONLY for the filtered/paginated periods
        sql.append("""
            
            winning_gender AS (
                SELECT 
                    %s as period_key,
                    gn.id as gender_id,
                    gn.name as gender_name,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE p.play_date IS NOT NULL AND gn.id IS NOT NULL
                GROUP BY period_key, gn.id, gn.name
            ),
            winning_genre AS (
                SELECT 
                    %s as period_key,
                    gr.id as genre_id,
                    gr.name as genre_name,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(al.override_genre_id, ar.genre_id)) = gr.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE p.play_date IS NOT NULL AND gr.id IS NOT NULL
                GROUP BY period_key, gr.id, gr.name
            ),
            winning_ethnicity AS (
                SELECT 
                    %s as period_key,
                    eth.id as ethnicity_id,
                    eth.name as ethnicity_name,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Ethnicity eth ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = eth.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE p.play_date IS NOT NULL AND eth.id IS NOT NULL
                GROUP BY period_key, eth.id, eth.name
            ),
            winning_language AS (
                SELECT 
                    %s as period_key,
                    lang.id as language_id,
                    lang.name as language_name,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Language lang ON COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = lang.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE p.play_date IS NOT NULL AND lang.id IS NOT NULL
                GROUP BY period_key, lang.id, lang.name
            ),
            winning_country AS (
                SELECT 
                    %s as period_key,
                    ar.country as country,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE p.play_date IS NOT NULL AND ar.country IS NOT NULL
                GROUP BY period_key, ar.country
            )
            SELECT 
                fp.period_key,
                fp.play_count,
                fp.time_listened,
                fp.artist_count,
                fp.album_count,
                fp.song_count,
                fp.male_song_count,
                fp.female_song_count,
                fp.other_song_count,
                fp.male_artist_count,
                fp.female_artist_count,
                fp.other_artist_count,
                fp.male_album_count,
                fp.female_album_count,
                fp.other_album_count,
                fp.male_play_count,
                fp.female_play_count,
                fp.other_play_count,
                fp.male_time_listened,
                fp.female_time_listened,
                fp.other_time_listened,
                wgn.gender_id as winning_gender_id,
                wgn.gender_name as winning_gender_name,
                wgr.genre_id as winning_genre_id,
                wgr.genre_name as winning_genre_name,
                weth.ethnicity_id as winning_ethnicity_id,
                weth.ethnicity_name as winning_ethnicity_name,
                wlang.language_id as winning_language_id,
                wlang.language_name as winning_language_name,
                wcty.country as winning_country,
                fp.male_artist_pct,
                fp.male_album_pct,
                fp.male_song_pct,
                fp.male_play_pct,
                fp.male_time_pct
            FROM filtered_periods fp
            LEFT JOIN winning_gender wgn ON fp.period_key = wgn.period_key AND wgn.rn = 1
            LEFT JOIN winning_genre wgr ON fp.period_key = wgr.period_key AND wgr.rn = 1
            LEFT JOIN winning_ethnicity weth ON fp.period_key = weth.period_key AND weth.rn = 1
            LEFT JOIN winning_language wlang ON fp.period_key = wlang.period_key AND wlang.rn = 1
            LEFT JOIN winning_country wcty ON fp.period_key = wcty.period_key AND wcty.rn = 1
            ORDER BY %s %s
            """.formatted(
                periodKeyExpr, periodKeyExpr, periodKeyExpr, // winning_gender
                periodKeyExpr, periodKeyExpr, periodKeyExpr, // winning_genre
                periodKeyExpr, periodKeyExpr, periodKeyExpr, // winning_ethnicity
                periodKeyExpr, periodKeyExpr, periodKeyExpr, // winning_language
                periodKeyExpr, periodKeyExpr, periodKeyExpr, // winning_country
                sortColumn.replace("ps.", "fp."), sortDirection
            ));
        
        // Apply winning attribute filters (if any)
        appendWinningFilters(sql, params,
            winningGender, winningGenderMode,
            winningGenre, winningGenreMode,
            winningEthnicity, winningEthnicityMode,
            winningLanguage, winningLanguageMode,
            winningCountry, winningCountryMode);
        
        // Execute query and map results
        List<TimeframeCardDTO> results = jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            TimeframeCardDTO dto = new TimeframeCardDTO();
            String periodKey = rs.getString("period_key");
            dto.setPeriodKey(periodKey);
            dto.setPeriodType(periodType);
            dto.setPeriodDisplayName(formatPeriodDisplayName(periodType, periodKey));
            
            // Calculate date range
            String[] dateRange = calculateDateRange(periodType, periodKey);
            dto.setListenedDateFrom(dateRange[0]);
            dto.setListenedDateTo(dateRange[1]);
            
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setTimeListened(rs.getLong("time_listened"));
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(rs.getLong("time_listened")));
            dto.setArtistCount(rs.getInt("artist_count"));
            dto.setAlbumCount(rs.getInt("album_count"));
            dto.setSongCount(rs.getInt("song_count"));
            dto.setMaleCount(rs.getInt("male_song_count"));
            dto.setFemaleCount(rs.getInt("female_song_count"));
            dto.setOtherCount(rs.getInt("other_song_count"));
            dto.setMaleArtistCount(rs.getInt("male_artist_count"));
            dto.setFemaleArtistCount(rs.getInt("female_artist_count"));
            dto.setOtherArtistCount(rs.getInt("other_artist_count"));
            dto.setMaleAlbumCount(rs.getInt("male_album_count"));
            dto.setFemaleAlbumCount(rs.getInt("female_album_count"));
            dto.setOtherAlbumCount(rs.getInt("other_album_count"));
            dto.setMalePlayCount(rs.getInt("male_play_count"));
            dto.setFemalePlayCount(rs.getInt("female_play_count"));
            dto.setOtherPlayCount(rs.getInt("other_play_count"));
            dto.setMaleTimeListened(rs.getLong("male_time_listened"));
            dto.setFemaleTimeListened(rs.getLong("female_time_listened"));
            dto.setOtherTimeListened(rs.getLong("other_time_listened"));
            
            // Winning attributes
            dto.setWinningGenderId(rs.getObject("winning_gender_id") != null ? rs.getInt("winning_gender_id") : null);
            dto.setWinningGenderName(rs.getString("winning_gender_name"));
            dto.setWinningGenreId(rs.getObject("winning_genre_id") != null ? rs.getInt("winning_genre_id") : null);
            dto.setWinningGenreName(rs.getString("winning_genre_name"));
            dto.setWinningEthnicityId(rs.getObject("winning_ethnicity_id") != null ? rs.getInt("winning_ethnicity_id") : null);
            dto.setWinningEthnicityName(rs.getString("winning_ethnicity_name"));
            dto.setWinningLanguageId(rs.getObject("winning_language_id") != null ? rs.getInt("winning_language_id") : null);
            dto.setWinningLanguageName(rs.getString("winning_language_name"));
            dto.setWinningCountry(rs.getString("winning_country"));
            
            return dto;
        }, params.toArray());
        
        // Unified post-processing: merge, filter, sort, paginate
        if (skipSqlPagination) {
            // Step 1: Merge with all periods if needed (includes date overlap filtering)
            if (needsMergeWithAllPeriods) {
                // For weeks, first merge Week 00 data
                if ("weeks".equals(periodType)) {
                    results = mergeWeekZeroIntoPreviousYear(results);
                }
                
                // Generate all possible period keys
                List<String> allPeriodKeys = switch (periodType) {
                    case "days" -> generateAllDayKeys();
                    case "weeks" -> generateAllWeekKeysWithoutWeekZero();
                    case "months" -> generateAllMonthKeys();
                    case "seasons" -> generateAllSeasonKeys();
                    case "years" -> generateAllYearKeys();
                    default -> null;
                };
                
                if (allPeriodKeys != null) {
                    // Filter period keys by date overlap if date range is set
                    if (dateFrom != null || dateTo != null) {
                        allPeriodKeys = filterPeriodKeysByDateOverlap(periodType, allPeriodKeys, dateFrom, dateTo);
                    }
                    
                    // Build full list with existing data + empty DTOs for missing periods
                    Map<String, TimeframeCardDTO> existingMap = new HashMap<>();
                    for (TimeframeCardDTO dto : results) {
                        existingMap.put(dto.getPeriodKey(), dto);
                    }
                    
                    List<TimeframeCardDTO> fullList = new ArrayList<>();
                    for (String periodKey : allPeriodKeys) {
                        TimeframeCardDTO dto = existingMap.get(periodKey);
                        if (dto == null) {
                            dto = createEmptyTimeframeCard(periodType, periodKey);
                        }
                        fullList.add(dto);
                    }
                    results = fullList;
                }
            } else if (dateFrom != null || dateTo != null) {
                // Non-merge path: filter results by date overlap
                results = filterByDateOverlap(results, dateFrom, dateTo);
            }
            
            // Step 2: Compute maleDays (for non-days types, before filtering)
            if (!"days".equals(periodType)) {
                populateMaleDays(results, periodType);
            }
            
            // Step 3: Apply maleDays filter
            if (maleDaysMin != null || maleDaysMax != null) {
                results = results.stream().filter(tf -> {
                    int md = tf.getMaleDays() != null ? tf.getMaleDays() : 0;
                    if (maleDaysMin != null && md < maleDaysMin) return false;
                    if (maleDaysMax != null && md > maleDaysMax) return false;
                    return true;
                }).collect(java.util.stream.Collectors.toList());
            }
            
            // Step 4: Sort
            results.sort(getComparator(sortBy, periodType, sortDir));
            
            // Step 5: Paginate
            int start = page * perPage;
            int end = Math.min(start + perPage, results.size());
            if (start >= results.size()) {
                results = new ArrayList<>();
            } else {
                results = new ArrayList<>(results.subList(start, end));
            }
            
            // Step 6: Populate top items for the paginated page
            if (!results.isEmpty()) {
                populateTopItems(results, periodType);
            }
        } else {
            // SQL-paginated path (no Java processing needed)
            if (!results.isEmpty()) {
                populateTopItems(results, periodType);
                if (!"days".equals(periodType)) {
                    populateMaleDays(results, periodType);
                }
            }
        }

        return results;
    }
    
    /**
     * Populates the top artist, album, and song for each timeframe in the list.
     */
    private void populateTopItems(List<TimeframeCardDTO> timeframes, String periodType) {
        List<String> periodKeys = timeframes.stream()
            .filter(t -> t.getPlayCount() != null && t.getPlayCount() > 0)
            .map(TimeframeCardDTO::getPeriodKey)
            .toList();
        if (periodKeys.isEmpty()) return;

        String periodKeyExpr = getPeriodKeyExpression(periodType);
        String placeholders = String.join(",", periodKeys.stream().map(pk -> "?").toList());

        // Query for top artist per period
        // Note: Using periodKeyExpr in GROUP BY instead of alias for SQLite compatibility with complex expressions
        String topArtistSql =
            "WITH artist_plays AS ( " +
            "    SELECT " +
            "        " + periodKeyExpr + " as period_key, " +
            "        ar.id as artist_id, " +
            "        ar.name as artist_name, " +
            "        ar.gender_id as gender_id, " +
            "        COUNT(*) as play_count, " +
            "        MAX(p.play_date) as max_play_date, " +
            "        ROW_NUMBER() OVER (PARTITION BY " + periodKeyExpr + " ORDER BY COUNT(*) DESC, MAX(p.play_date) ASC) as rn " +
            "    FROM Play p " +
            "    JOIN Song s ON p.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    WHERE " + periodKeyExpr + " IN (" + placeholders + ") " +
            "    GROUP BY " + periodKeyExpr + ", ar.id, ar.name, ar.gender_id " +
            ") " +
            "SELECT period_key, artist_id, artist_name, gender_id FROM artist_plays WHERE rn = 1";

        List<Object[]> artistResults = jdbcTemplate.query(topArtistSql, (rs, rowNum) ->
            new Object[]{rs.getString("period_key"), rs.getInt("artist_id"), rs.getString("artist_name"),
                        rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null},
            periodKeys.toArray()
        );

        // Query for top album per period
        String topAlbumSql =
            "WITH album_plays AS ( " +
            "    SELECT " +
            "        " + periodKeyExpr + " as period_key, " +
            "        al.id as album_id, " +
            "        al.name as album_name, " +
            "        ar.name as artist_name, " +
            "        ar.gender_id as gender_id, " +
            "        COUNT(*) as play_count, " +
            "        MAX(p.play_date) as max_play_date, " +
            "        ROW_NUMBER() OVER (PARTITION BY " + periodKeyExpr + " ORDER BY COUNT(*) DESC, MAX(p.play_date) ASC) as rn " +
            "    FROM Play p " +
            "    JOIN Song s ON p.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE al.id IS NOT NULL AND " + periodKeyExpr + " IN (" + placeholders + ") " +
            "    GROUP BY " + periodKeyExpr + ", al.id, al.name, ar.name, ar.gender_id " +
            ") " +
            "SELECT period_key, album_id, album_name, artist_name, gender_id FROM album_plays WHERE rn = 1";

        List<Object[]> albumResults = jdbcTemplate.query(topAlbumSql, (rs, rowNum) ->
            new Object[]{rs.getString("period_key"), rs.getInt("album_id"), rs.getString("album_name"), rs.getString("artist_name"),
                        rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null},
            periodKeys.toArray()
        );

        // Query for top song per period
        String topSongSql =
            "WITH song_plays AS ( " +
            "    SELECT " +
            "        " + periodKeyExpr + " as period_key, " +
            "        s.id as song_id, " +
            "        s.name as song_name, " +
            "        ar.name as artist_name, " +
            "        ar.gender_id as gender_id, " +
            "        COUNT(*) as play_count, " +
            "        MAX(p.play_date) as max_play_date, " +
            "        ROW_NUMBER() OVER (PARTITION BY " + periodKeyExpr + " ORDER BY COUNT(*) DESC, MAX(p.play_date) ASC) as rn " +
            "    FROM Play p " +
            "    JOIN Song s ON p.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    WHERE " + periodKeyExpr + " IN (" + placeholders + ") " +
            "    GROUP BY " + periodKeyExpr + ", s.id, s.name, ar.name, ar.gender_id " +
            ") " +
            "SELECT period_key, song_id, song_name, artist_name, gender_id FROM song_plays WHERE rn = 1";

        List<Object[]> songResults = jdbcTemplate.query(topSongSql, (rs, rowNum) ->
            new Object[]{rs.getString("period_key"), rs.getInt("song_id"), rs.getString("song_name"), rs.getString("artist_name"),
                        rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null},
            periodKeys.toArray()
        );

        // Map results to timeframes
        for (TimeframeCardDTO tf : timeframes) {
            for (Object[] row : artistResults) {
                if (tf.getPeriodKey() != null && tf.getPeriodKey().equals(row[0])) {
                    tf.setTopArtistId((Integer) row[1]);
                    tf.setTopArtistName((String) row[2]);
                    tf.setTopArtistGenderId((Integer) row[3]);
                    break;
                }
            }
            for (Object[] row : albumResults) {
                if (tf.getPeriodKey() != null && tf.getPeriodKey().equals(row[0])) {
                    tf.setTopAlbumId((Integer) row[1]);
                    tf.setTopAlbumName((String) row[2]);
                    tf.setTopAlbumArtistName((String) row[3]);
                    tf.setTopAlbumGenderId((Integer) row[4]);
                    break;
                }
            }
            for (Object[] row : songResults) {
                if (tf.getPeriodKey() != null && tf.getPeriodKey().equals(row[0])) {
                    tf.setTopSongId((Integer) row[1]);
                    tf.setTopSongName((String) row[2]);
                    tf.setTopSongArtistName((String) row[3]);
                    tf.setTopSongGenderId((Integer) row[4]);
                    break;
                }
            }
        }
    }

    /**
     * Populates maleDays (days where male plays > female plays) and totalDays for each timeframe.
     * Only meaningful for non-"days" period types (weeks, months, seasons, years, decades).
     */
    private void populateMaleDays(List<TimeframeCardDTO> timeframes, String periodType) {
        // Get the first ever scrobble date so that the opening period's totalDays starts there
        // (e.g., 2005 should not be counted as a full 365-day year if scrobbling started in Feb 2005)
        LocalDate firstScrobbleDate = null;
        try {
            String firstDateStr = jdbcTemplate.queryForObject(
                "SELECT DATE(MIN(play_date)) FROM Play", String.class);
            if (firstDateStr != null) {
                firstScrobbleDate = LocalDate.parse(firstDateStr);
            }
        } catch (Exception ignored) {}

        final LocalDate effectiveFirstDay = firstScrobbleDate;

        // Compute totalDays for all timeframes using the nominal period boundaries,
        // but clamp the start to the first scrobble date for periods that begin before it,
        // and clamp the end to today for ongoing (current) periods.
        LocalDate today = LocalDate.now();
        for (TimeframeCardDTO tf : timeframes) {
            if (tf.getListenedDateFrom() != null && tf.getListenedDateTo() != null) {
                try {
                    LocalDate from = LocalDate.parse(tf.getListenedDateFrom());
                    LocalDate to = LocalDate.parse(tf.getListenedDateTo());
                    // If this period started before the first scrobble, use the first scrobble as effective start
                    if (effectiveFirstDay != null && from.isBefore(effectiveFirstDay)) {
                        from = effectiveFirstDay;
                    }
                    // If this period hasn't ended yet, use today as effective end (elapsed days only)
                    if (to.isAfter(today)) {
                        to = today;
                    }
                    int total = (int) java.time.temporal.ChronoUnit.DAYS.between(from, to) + 1;
                    tf.setTotalDays(total);
                } catch (Exception ignored) {}
            }
            if (tf.getMaleDays() == null) {
                tf.setMaleDays(0);
            }
        }

        // Only need to query DB for timeframes that have plays
        List<TimeframeCardDTO> withPlays = timeframes.stream()
            .filter(t -> t.getPlayCount() != null && t.getPlayCount() > 0)
            .toList();
        if (withPlays.isEmpty()) return;

        List<String> periodKeys = withPlays.stream()
            .map(TimeframeCardDTO::getPeriodKey)
            .toList();

        String periodKeyExpr = getPeriodKeyExpression(periodType);
        String placeholders = String.join(",", periodKeys.stream().map(pk -> "?").toList());

        String sql =
            "WITH day_gender AS ( " +
            "    SELECT " +
            "        " + periodKeyExpr + " as period_key, " +
            "        DATE(p.play_date) as play_day, " +
            "        SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN 1 ELSE 0 END) as male_plays, " +
            "        SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN 1 ELSE 0 END) as female_plays " +
            "    FROM Play p " +
            "    JOIN Song s ON p.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    WHERE " + periodKeyExpr + " IN (" + placeholders + ") " +
            "    GROUP BY " + periodKeyExpr + ", DATE(p.play_date) " +
            ") " +
            "SELECT period_key, SUM(CASE WHEN male_plays > female_plays THEN 1 ELSE 0 END) as male_days " +
            "FROM day_gender " +
            "GROUP BY period_key";

        List<Object[]> results = jdbcTemplate.query(sql, (rs, rowNum) ->
            new Object[]{rs.getString("period_key"), rs.getInt("male_days")},
            periodKeys.toArray()
        );

        for (TimeframeCardDTO tf : timeframes) {
            for (Object[] row : results) {
                if (tf.getPeriodKey() != null && tf.getPeriodKey().equals(row[0])) {
                    tf.setMaleDays((Integer) row[1]);
                    break;
                }
            }
        }
    }

    private static final DateTimeFormatter DATE_FILTER_FORMAT = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    /**
     * Filter period keys by date overlap with the given date range.
     * A period overlaps if periodStart <= dateTo AND periodEnd >= dateFrom.
     */
    private List<String> filterPeriodKeysByDateOverlap(String periodType, List<String> periodKeys, String dateFrom, String dateTo) {
        LocalDate from = dateFrom != null ? LocalDate.parse(dateFrom, DATE_FILTER_FORMAT) : null;
        LocalDate to = dateTo != null ? LocalDate.parse(dateTo, DATE_FILTER_FORMAT) : null;
        
        return periodKeys.stream().filter(key -> {
            String[] range = calculateDateRange(periodType, key);
            if (range[0].isEmpty() || range[1].isEmpty()) return false;
            try {
                LocalDate periodStart = LocalDate.parse(range[0]);
                LocalDate periodEnd = LocalDate.parse(range[1]);
                if (from != null && periodEnd.isBefore(from)) return false;
                if (to != null && periodStart.isAfter(to)) return false;
                return true;
            } catch (Exception e) {
                return false;
            }
        }).collect(java.util.stream.Collectors.toList());
    }

    /**
     * Filter timeframe results by date overlap with the given date range.
     */
    private List<TimeframeCardDTO> filterByDateOverlap(List<TimeframeCardDTO> results, String dateFrom, String dateTo) {
        LocalDate from = dateFrom != null ? LocalDate.parse(dateFrom, DATE_FILTER_FORMAT) : null;
        LocalDate to = dateTo != null ? LocalDate.parse(dateTo, DATE_FILTER_FORMAT) : null;
        
        return results.stream().filter(tf -> {
            if (tf.getListenedDateFrom() == null || tf.getListenedDateTo() == null) return false;
            try {
                LocalDate periodStart = LocalDate.parse(tf.getListenedDateFrom());
                LocalDate periodEnd = LocalDate.parse(tf.getListenedDateTo());
                if (from != null && periodEnd.isBefore(from)) return false;
                if (to != null && periodStart.isAfter(to)) return false;
                return true;
            } catch (Exception e) {
                return false;
            }
        }).collect(java.util.stream.Collectors.toList());
    }

    /**
     * Check if any filters are set that would exclude 0-play periods
     */
    private boolean hasRestrictiveFilters(
            List<Integer> winningGender, String winningGenderMode,
            List<Integer> winningGenre, String winningGenreMode,
            List<Integer> winningEthnicity, String winningEthnicityMode,
            List<Integer> winningLanguage, String winningLanguageMode,
            List<String> winningCountry, String winningCountryMode,
            Integer artistCountMin, Integer albumCountMin, Integer songCountMin,
            Integer playsMin, Long timeMin,
            Double maleArtistPctMin, Double maleAlbumPctMin, Double maleSongPctMin,
            Double malePlayPctMin, Double maleTimePctMin) {
        
        // Winning attribute filters would exclude 0-play periods
        if (winningGenderMode != null && !"excludes".equals(winningGenderMode) && winningGender != null && !winningGender.isEmpty()) return true;
        if (winningGenreMode != null && !"excludes".equals(winningGenreMode) && winningGenre != null && !winningGenre.isEmpty()) return true;
        if (winningEthnicityMode != null && !"excludes".equals(winningEthnicityMode) && winningEthnicity != null && !winningEthnicity.isEmpty()) return true;
        if (winningLanguageMode != null && !"excludes".equals(winningLanguageMode) && winningLanguage != null && !winningLanguage.isEmpty()) return true;
        if (winningCountryMode != null && !"excludes".equals(winningCountryMode) && winningCountry != null && !winningCountry.isEmpty()) return true;
        
        // Min count filters would exclude 0-play periods
        if (artistCountMin != null && artistCountMin > 0) return true;
        if (albumCountMin != null && albumCountMin > 0) return true;
        if (songCountMin != null && songCountMin > 0) return true;
        if (playsMin != null && playsMin > 0) return true;
        if (timeMin != null && timeMin > 0) return true;
        
        // Male percentage filters would exclude periods that don't meet the criteria
        if (maleArtistPctMin != null && maleArtistPctMin > 0) return true;
        if (maleAlbumPctMin != null && maleAlbumPctMin > 0) return true;
        if (maleSongPctMin != null && maleSongPctMin > 0) return true;
        if (malePlayPctMin != null && malePlayPctMin > 0) return true;
        if (maleTimePctMin != null && maleTimePctMin > 0) return true;
        
        return false;
    }
    
    /**
     * Count total timeframes matching filters
     */
    public long countTimeframes(String periodType,
            List<Integer> winningGender, String winningGenderMode,
            List<Integer> winningGenre, String winningGenreMode,
            List<Integer> winningEthnicity, String winningEthnicityMode,
            List<Integer> winningLanguage, String winningLanguageMode,
            List<String> winningCountry, String winningCountryMode,
            Integer artistCountMin, Integer artistCountMax,
            Integer albumCountMin, Integer albumCountMax,
            Integer songCountMin, Integer songCountMax,
            Integer playsMin, Integer playsMax,
            Long timeMin, Long timeMax,
            Double maleArtistPctMin, Double maleArtistPctMax,
            Double maleAlbumPctMin, Double maleAlbumPctMax,
            Double maleSongPctMin, Double maleSongPctMax,
            Double malePlayPctMin, Double malePlayPctMax,
            Double maleTimePctMin, Double maleTimePctMax,
            String dateFrom, String dateTo,
            Integer maleDaysMin, Integer maleDaysMax) {
        // TODO: maleDaysMin/maleDaysMax not yet factored into count query (minor pagination inaccuracy)
        
        String periodKeyExpr = getPeriodKeyExpression(periodType);
        
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        // Optimize count query: only compute winning CTEs if they're actually filtered on
        boolean needWinningGender = winningGenderMode != null && winningGender != null && !winningGender.isEmpty();
        boolean needWinningGenre = winningGenreMode != null && winningGenre != null && !winningGenre.isEmpty();
        boolean needWinningEthnicity = winningEthnicityMode != null && winningEthnicity != null && !winningEthnicity.isEmpty();
        boolean needWinningLanguage = winningLanguageMode != null && winningLanguage != null && !winningLanguage.isEmpty();
        boolean needWinningCountry = winningCountryMode != null && winningCountry != null && !winningCountry.isEmpty();
        
        sql.append(String.format("""
            WITH period_summary AS (
                SELECT 
                    %s as period_key,
                    COUNT(*) as play_count,
                    COALESCE(SUM(s.length_seconds), 0) as time_listened,
                    COUNT(DISTINCT ar.id) as artist_count,
                    COUNT(DISTINCT CASE WHEN s.album_id IS NOT NULL THEN s.album_id END) as album_count,
                    COUNT(DISTINCT s.id) as song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN s.id END) as male_song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN s.id END) as female_song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN s.id END) as other_song_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN ar.id END) as male_artist_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN ar.id END) as female_artist_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN ar.id END) as other_artist_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 AND s.album_id IS NOT NULL THEN s.album_id END) as male_album_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 AND s.album_id IS NOT NULL THEN s.album_id END) as female_album_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL AND s.album_id IS NOT NULL THEN s.album_id END) as other_album_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN 1 ELSE 0 END) as male_play_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN 1 ELSE 0 END) as female_play_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN 1 ELSE 0 END) as other_play_count,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_time_listened,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_time_listened,
                    SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1,2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_time_listened
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                WHERE p.play_date IS NOT NULL
                GROUP BY period_key
                HAVING period_key IS NOT NULL
            ),
            filtered_periods AS (
                SELECT 
                    period_key,
                    play_count,
                    time_listened,
                    artist_count,
                    album_count,
                    song_count,
                    male_song_count,
                    female_song_count,
                    other_song_count,
                    male_artist_count,
                    female_artist_count,
                    other_artist_count,
                    male_album_count,
                    female_album_count,
                    other_album_count,
                    male_play_count,
                    female_play_count,
                    other_play_count,
                    male_time_listened,
                    female_time_listened,
                    other_time_listened,
                    CASE 
                        WHEN (male_artist_count + female_artist_count + other_artist_count) > 0 
                        THEN CAST(male_artist_count AS REAL) * 100.0 / (male_artist_count + female_artist_count + other_artist_count)
                        ELSE NULL 
                    END as male_artist_pct,
                    CASE 
                        WHEN (male_album_count + female_album_count + other_album_count) > 0 
                        THEN CAST(male_album_count AS REAL) * 100.0 / (male_album_count + female_album_count + other_album_count)
                        ELSE NULL 
                    END as male_album_pct,
                    CASE 
                        WHEN (male_song_count + female_song_count + other_song_count) > 0 
                        THEN CAST(male_song_count AS REAL) * 100.0 / (male_song_count + female_song_count + other_song_count)
                        ELSE NULL 
                    END as male_song_pct,
                    CASE 
                        WHEN (male_play_count + female_play_count + other_play_count) > 0 
                        THEN CAST(male_play_count AS REAL) * 100.0 / (male_play_count + female_play_count + other_play_count)
                        ELSE NULL 
                    END as male_play_pct,
                    CASE 
                        WHEN (male_time_listened + female_time_listened + other_time_listened) > 0 
                        THEN CAST(male_time_listened AS REAL) * 100.0 / (male_time_listened + female_time_listened + other_time_listened)
                        ELSE NULL 
                    END as male_time_pct
                FROM period_summary
                WHERE 1=1""", periodKeyExpr));
        
        appendInlineSummaryFilters(sql, params,
            artistCountMin, artistCountMax,
            albumCountMin, albumCountMax,
            songCountMin, songCountMax,
            playsMin, playsMax,
            timeMin, timeMax,
            maleArtistPctMin, maleArtistPctMax,
            maleAlbumPctMin, maleAlbumPctMax,
            maleSongPctMin, maleSongPctMax,
            malePlayPctMin, malePlayPctMax,
            maleTimePctMin, maleTimePctMax);
        
        sql.append("\n            )");
        
        // Only add winning CTEs if needed for filtering
        if (needWinningGender) {
            sql.append("""
                winning_gender AS (
                    SELECT 
                        %s as period_key,
                        gn.id as gender_id,
                        ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE p.play_date IS NOT NULL AND gn.id IS NOT NULL
                    GROUP BY period_key, gn.id
                )""".formatted(periodKeyExpr, periodKeyExpr, periodKeyExpr));
        }
        
        if (needWinningGenre) {
            sql.append("""
                winning_genre AS (
                    SELECT 
                        %s as period_key,
                        gr.id as genre_id,
                        ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album al ON s.album_id = al.id
                    LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(al.override_genre_id, ar.genre_id)) = gr.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE p.play_date IS NOT NULL AND gr.id IS NOT NULL
                    GROUP BY period_key, gr.id
                )""".formatted(periodKeyExpr, periodKeyExpr, periodKeyExpr));
        }
        
        if (needWinningEthnicity) {
            sql.append("""
                winning_ethnicity AS (
                    SELECT 
                        %s as period_key,
                        eth.id as ethnicity_id,
                        ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Ethnicity eth ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = eth.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE p.play_date IS NOT NULL AND eth.id IS NOT NULL
                    GROUP BY period_key, eth.id
                )""".formatted(periodKeyExpr, periodKeyExpr, periodKeyExpr));
        }
        
        if (needWinningLanguage) {
            sql.append("""
                winning_language AS (
                    SELECT 
                        %s as period_key,
                        lang.id as language_id,
                        ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album al ON s.album_id = al.id
                    LEFT JOIN Language lang ON COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = lang.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE p.play_date IS NOT NULL AND lang.id IS NOT NULL
                    GROUP BY period_key, lang.id
                )""".formatted(periodKeyExpr, periodKeyExpr, periodKeyExpr));
        }
        
        if (needWinningCountry) {
            sql.append("""
                winning_country AS (
                    SELECT 
                        %s as period_key,
                        ar.country as country,
                        ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE p.play_date IS NOT NULL AND ar.country IS NOT NULL
                    GROUP BY period_key, ar.country
                )""".formatted(periodKeyExpr, periodKeyExpr, periodKeyExpr));
        }
        
        sql.append("\n            SELECT COUNT(*) as cnt\n            FROM filtered_periods fp");
        
        // Add JOINs only for winning CTEs that were created
        if (needWinningGender) {
            sql.append("\n            LEFT JOIN winning_gender wgn ON fp.period_key = wgn.period_key AND wgn.rn = 1");
        }
        if (needWinningGenre) {
            sql.append("\n            LEFT JOIN winning_genre wgr ON fp.period_key = wgr.period_key AND wgr.rn = 1");
        }
        if (needWinningEthnicity) {
            sql.append("\n            LEFT JOIN winning_ethnicity weth ON fp.period_key = weth.period_key AND weth.rn = 1");
        }
        if (needWinningLanguage) {
            sql.append("\n            LEFT JOIN winning_language wlang ON fp.period_key = wlang.period_key AND wlang.rn = 1");
        }
        if (needWinningCountry) {
            sql.append("\n            LEFT JOIN winning_country wcty ON fp.period_key = wcty.period_key AND wcty.rn = 1");
        }
        
        // Apply winning filters  
        if (needWinningGender || needWinningGenre || needWinningEthnicity || needWinningLanguage || needWinningCountry) {
            StringBuilder whereClause = new StringBuilder("\n            WHERE 1=1");
            
            if (needWinningGender && winningGender != null) {
                String placeholders = String.join(",", winningGender.stream().map(id -> "?").toList());
                if ("includes".equals(winningGenderMode)) {
                    whereClause.append(" AND wgn.gender_id IN (").append(placeholders).append(")");
                    params.addAll(winningGender);
                } else if ("excludes".equals(winningGenderMode)) {
                    whereClause.append(" AND (wgn.gender_id NOT IN (").append(placeholders).append(") OR wgn.gender_id IS NULL)");
                    params.addAll(winningGender);
                }
            }
            
            if (needWinningGenre && winningGenre != null) {
                String placeholders = String.join(",", winningGenre.stream().map(id -> "?").toList());
                if ("includes".equals(winningGenreMode)) {
                    whereClause.append(" AND wgr.genre_id IN (").append(placeholders).append(")");
                    params.addAll(winningGenre);
                } else if ("excludes".equals(winningGenreMode)) {
                    whereClause.append(" AND (wgr.genre_id NOT IN (").append(placeholders).append(") OR wgr.genre_id IS NULL)");
                    params.addAll(winningGenre);
                }
            }
            
            if (needWinningEthnicity && winningEthnicity != null) {
                String placeholders = String.join(",", winningEthnicity.stream().map(id -> "?").toList());
                if ("includes".equals(winningEthnicityMode)) {
                    whereClause.append(" AND weth.ethnicity_id IN (").append(placeholders).append(")");
                    params.addAll(winningEthnicity);
                } else if ("excludes".equals(winningEthnicityMode)) {
                    whereClause.append(" AND (weth.ethnicity_id NOT IN (").append(placeholders).append(") OR weth.ethnicity_id IS NULL)");
                    params.addAll(winningEthnicity);
                }
            }
            
            if (needWinningLanguage && winningLanguage != null) {
                String placeholders = String.join(",", winningLanguage.stream().map(id -> "?").toList());
                if ("includes".equals(winningLanguageMode)) {
                    whereClause.append(" AND wlang.language_id IN (").append(placeholders).append(")");
                    params.addAll(winningLanguage);
                } else if ("excludes".equals(winningLanguageMode)) {
                    whereClause.append(" AND (wlang.language_id NOT IN (").append(placeholders).append(") OR wlang.language_id IS NULL)");
                    params.addAll(winningLanguage);
                }
            }
            
            if (needWinningCountry && winningCountry != null) {
                String placeholders = String.join(",", winningCountry.stream().map(c -> "?").toList());
                if ("includes".equals(winningCountryMode)) {
                    whereClause.append(" AND wcty.country IN (").append(placeholders).append(")");
                    params.addAll(winningCountry);
                } else if ("excludes".equals(winningCountryMode)) {
                    whereClause.append(" AND (wcty.country NOT IN (").append(placeholders).append(") OR wcty.country IS NULL)");
                    params.addAll(winningCountry);
                }
            }
            
            sql.append(whereClause);
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, params.toArray());
        long baseCount = count != null ? count : 0;
        
        // For days, weeks, months, seasons, and years with no restrictive filters, include 0-play periods in the count
        if (("days".equals(periodType) || "weeks".equals(periodType) || "months".equals(periodType) ||
             "seasons".equals(periodType) || "years".equals(periodType)) && !hasRestrictiveFilters(
                winningGender, winningGenderMode, winningGenre, winningGenreMode,
                winningEthnicity, winningEthnicityMode, winningLanguage, winningLanguageMode,
                winningCountry, winningCountryMode, artistCountMin, albumCountMin, songCountMin,
                playsMin, timeMin,
                maleArtistPctMin, maleAlbumPctMin, maleSongPctMin, malePlayPctMin, maleTimePctMin)) {
            // Return count of all possible periods
            List<String> allPeriods = switch (periodType) {
                case "days" -> generateAllDayKeys();
                case "weeks" -> generateAllWeekKeysWithoutWeekZero(); // Exclude Week 00 since it's merged
                case "months" -> generateAllMonthKeys();
                case "seasons" -> generateAllSeasonKeys();
                case "years" -> generateAllYearKeys();
                default -> null;
            };
            if (allPeriods != null) {
                // Filter by date overlap if date range is set
                if (dateFrom != null || dateTo != null) {
                    allPeriods = filterPeriodKeysByDateOverlap(periodType, allPeriods, dateFrom, dateTo);
                }
                return allPeriods.size();
            }
            return baseCount;
        }
        
        return baseCount;
    }
    
    /**
     * Get the earliest play date from the database
     */
    private LocalDate getEarliestPlayDate() {
        String sql = "SELECT MIN(DATE(play_date)) FROM Play WHERE play_date IS NOT NULL";
        String dateStr = jdbcTemplate.queryForObject(sql, String.class);
        return dateStr != null ? LocalDate.parse(dateStr) : LocalDate.now();
    }
    
    /**
     * Generate all possible season period keys from earliest play to current season
     */
    private List<String> generateAllSeasonKeys() {
        LocalDate earliest = getEarliestPlayDate();
        LocalDate now = LocalDate.now();
        
        List<String> seasons = new ArrayList<>();
        String[] seasonNames = {"Winter", "Spring", "Summer", "Fall"};
        
        // Determine the season for the earliest date
        int startYear = earliest.getYear();
        int startMonth = earliest.getMonthValue();
        int startSeasonIdx;
        if (startMonth == 12) {
            startSeasonIdx = 0; // Winter of next year
            startYear++;
        } else if (startMonth >= 1 && startMonth <= 2) {
            startSeasonIdx = 0; // Winter
        } else if (startMonth >= 3 && startMonth <= 5) {
            startSeasonIdx = 1; // Spring
        } else if (startMonth >= 6 && startMonth <= 8) {
            startSeasonIdx = 2; // Summer
        } else {
            startSeasonIdx = 3; // Fall
        }
        
        // Determine current season
        int endYear = now.getYear();
        int endMonth = now.getMonthValue();
        int endSeasonIdx;
        if (endMonth == 12) {
            endSeasonIdx = 0;
            endYear++;
        } else if (endMonth >= 1 && endMonth <= 2) {
            endSeasonIdx = 0;
        } else if (endMonth >= 3 && endMonth <= 5) {
            endSeasonIdx = 1;
        } else if (endMonth >= 6 && endMonth <= 8) {
            endSeasonIdx = 2;
        } else {
            endSeasonIdx = 3;
        }
        
        // Generate all seasons from start to current
        int year = startYear;
        int seasonIdx = startSeasonIdx;
        while (year < endYear || (year == endYear && seasonIdx <= endSeasonIdx)) {
            seasons.add(year + "-" + seasonNames[seasonIdx]);
            seasonIdx++;
            if (seasonIdx > 3) {
                seasonIdx = 0;
                year++;
            }
        }
        
        return seasons;
    }
    
    /**
     * Generate all possible year period keys from earliest play to current year
     */
    private List<String> generateAllYearKeys() {
        LocalDate earliest = getEarliestPlayDate();
        LocalDate now = LocalDate.now();
        
        List<String> years = new ArrayList<>();
        for (int year = earliest.getYear(); year <= now.getYear(); year++) {
            years.add(String.valueOf(year));
        }
        
        return years;
    }
    
    /**
     * Generate all possible week period keys from earliest play to current week.
     * Uses SQLite's %W week numbering convention.
     */
    private List<String> generateAllWeekKeys() {
        LocalDate earliest = getEarliestPlayDate();
        LocalDate now = LocalDate.now();
        
        List<String> weeks = new ArrayList<>();
        LocalDate current = earliest;
        
        while (!current.isAfter(now)) {
            // Calculate week number using SQLite's %W convention
            // Week 01 starts from first Monday of the year
            int year = current.getYear();
            LocalDate jan1 = LocalDate.of(year, 1, 1);
            LocalDate firstMonday = jan1.with(java.time.temporal.TemporalAdjusters.nextOrSame(java.time.DayOfWeek.MONDAY));

            int weekNum;
            if (current.isBefore(firstMonday)) {
                weekNum = 0;
            } else {
                long daysSinceFirstMonday = java.time.temporal.ChronoUnit.DAYS.between(firstMonday, current);
                weekNum = (int) (daysSinceFirstMonday / 7) + 1;
            }

            String weekKey = String.format("%d-W%02d", year, weekNum);
            if (weeks.isEmpty() || !weeks.get(weeks.size() - 1).equals(weekKey)) {
                weeks.add(weekKey);
            }
            
            current = current.plusDays(1);
        }
        
        return weeks;
    }
    
    /**
     * Generate all possible week period keys, but exclude Week 00 entries.
     * Week 00 data should be merged into the last week of the previous year.
     */
    private List<String> generateAllWeekKeysWithoutWeekZero() {
        List<String> allWeeks = generateAllWeekKeys();
        // Filter out Week 00 entries
        return allWeeks.stream()
            .filter(key -> !key.endsWith("-W00"))
            .toList();
    }

    /**
     * Merge Week 00 data into the last week of the previous year.
     * Week 00 is the partial week (days before first Monday) which should be combined
     * with the last week of the previous year for a complete 7-day display.
     */
    private List<TimeframeCardDTO> mergeWeekZeroIntoPreviousYear(List<TimeframeCardDTO> results) {
        Map<String, TimeframeCardDTO> resultMap = new HashMap<>();
        List<TimeframeCardDTO> weekZeros = new ArrayList<>();

        for (TimeframeCardDTO dto : results) {
            String key = dto.getPeriodKey();
            if (key != null && key.endsWith("-W00")) {
                weekZeros.add(dto);
            } else {
                resultMap.put(key, dto);
            }
        }

        // For each Week 00, find the last week of the previous year and merge
        for (TimeframeCardDTO week0 : weekZeros) {
            String key = week0.getPeriodKey(); // e.g., "2025-W00"
            int year = Integer.parseInt(key.split("-W")[0]);
            int prevYear = year - 1;

            // Find the last week of the previous year
            // December 31 of prev year tells us what week that is
            LocalDate dec31 = LocalDate.of(prevYear, 12, 31);
            LocalDate jan1PrevYear = LocalDate.of(prevYear, 1, 1);
            LocalDate firstMondayPrevYear = jan1PrevYear.with(java.time.temporal.TemporalAdjusters.nextOrSame(java.time.DayOfWeek.MONDAY));

            int lastWeekNum;
            if (dec31.isBefore(firstMondayPrevYear)) {
                lastWeekNum = 0;
            } else {
                long daysSinceFirstMonday = java.time.temporal.ChronoUnit.DAYS.between(firstMondayPrevYear, dec31);
                lastWeekNum = (int) (daysSinceFirstMonday / 7) + 1;
            }

            String lastWeekKey = String.format("%d-W%02d", prevYear, lastWeekNum);

            TimeframeCardDTO targetWeek = resultMap.get(lastWeekKey);
            if (targetWeek != null) {
                // Merge Week 00 counts into the target week
                mergeTimeframeCounts(targetWeek, week0);
                // Update the display name to show the full range
                updateWeekDisplayNameForMerge(targetWeek, week0);
            } else {
                // No data for last week of prev year - just use week 0 data but relabel it
                week0.setPeriodKey(lastWeekKey);
                week0.setPeriodDisplayName(formatPeriodDisplayName("weeks", lastWeekKey));
                String[] dateRange = calculateDateRange("weeks", lastWeekKey);
                week0.setListenedDateFrom(dateRange[0]);
                week0.setListenedDateTo(dateRange[1]);
                resultMap.put(lastWeekKey, week0);
            }
        }

        return new ArrayList<>(resultMap.values());
    }

    /**
     * Merge counts from source into target TimeframeCardDTO.
     */
    private void mergeTimeframeCounts(TimeframeCardDTO target, TimeframeCardDTO source) {
        target.setPlayCount(safeAdd(target.getPlayCount(), source.getPlayCount()));
        target.setTimeListened(safeAddLong(target.getTimeListened(), source.getTimeListened()));
        target.setTimeListenedFormatted(TimeFormatUtils.formatTime(target.getTimeListened()));
        target.setArtistCount(safeAdd(target.getArtistCount(), source.getArtistCount()));
        target.setAlbumCount(safeAdd(target.getAlbumCount(), source.getAlbumCount()));
        target.setSongCount(safeAdd(target.getSongCount(), source.getSongCount()));
        target.setMaleCount(safeAdd(target.getMaleCount(), source.getMaleCount()));
        target.setFemaleCount(safeAdd(target.getFemaleCount(), source.getFemaleCount()));
        target.setOtherCount(safeAdd(target.getOtherCount(), source.getOtherCount()));
        target.setMaleArtistCount(safeAdd(target.getMaleArtistCount(), source.getMaleArtistCount()));
        target.setFemaleArtistCount(safeAdd(target.getFemaleArtistCount(), source.getFemaleArtistCount()));
        target.setOtherArtistCount(safeAdd(target.getOtherArtistCount(), source.getOtherArtistCount()));
        target.setMaleAlbumCount(safeAdd(target.getMaleAlbumCount(), source.getMaleAlbumCount()));
        target.setFemaleAlbumCount(safeAdd(target.getFemaleAlbumCount(), source.getFemaleAlbumCount()));
        target.setOtherAlbumCount(safeAdd(target.getOtherAlbumCount(), source.getOtherAlbumCount()));
        target.setMalePlayCount(safeAdd(target.getMalePlayCount(), source.getMalePlayCount()));
        target.setFemalePlayCount(safeAdd(target.getFemalePlayCount(), source.getFemalePlayCount()));
        target.setOtherPlayCount(safeAdd(target.getOtherPlayCount(), source.getOtherPlayCount()));
        target.setMaleTimeListened(safeAddLong(target.getMaleTimeListened(), source.getMaleTimeListened()));
        target.setFemaleTimeListened(safeAddLong(target.getFemaleTimeListened(), source.getFemaleTimeListened()));
        target.setOtherTimeListened(safeAddLong(target.getOtherTimeListened(), source.getOtherTimeListened()));
    }

    private Integer safeAdd(Integer a, Integer b) {
        return (a != null ? a : 0) + (b != null ? b : 0);
    }

    private Long safeAddLong(Long a, Long b) {
        return (a != null ? a : 0L) + (b != null ? b : 0L);
    }

    /**
     * Update the target week's display name and date range to include the merged Week 00 days.
     */
    private void updateWeekDisplayNameForMerge(TimeframeCardDTO target, TimeframeCardDTO week0) {
        // The merged week should show the full range from the target week's start
        // to the end of Week 00's range (which is the day before first Monday of next year)
        // Actually, the target week already has the correct date range for its period key
        // We just need to make sure the date range includes both
        String[] targetRange = calculateDateRange("weeks", target.getPeriodKey());
        target.setListenedDateFrom(targetRange[0]);
        target.setListenedDateTo(targetRange[1]);
        target.setPeriodDisplayName(formatPeriodDisplayName("weeks", target.getPeriodKey()));
    }

    /**
     * Generate all possible day period keys from earliest play to today
     */
    private List<String> generateAllDayKeys() {
        LocalDate earliest = getEarliestPlayDate();
        LocalDate now = LocalDate.now();
        
        List<String> days = new ArrayList<>();
        LocalDate current = earliest;
        
        while (!current.isAfter(now)) {
            days.add(current.toString());
            current = current.plusDays(1);
        }
        
        return days;
    }
    
    /**
     * Generate all possible month period keys from earliest play to current month
     */
    private List<String> generateAllMonthKeys() {
        LocalDate earliest = getEarliestPlayDate();
        LocalDate now = LocalDate.now();
        
        List<String> months = new ArrayList<>();
        YearMonth current = YearMonth.from(earliest);
        YearMonth end = YearMonth.from(now);
        
        while (!current.isAfter(end)) {
            months.add(current.toString());
            current = current.plusMonths(1);
        }
        
        return months;
    }
    
    /**
     * Create an empty TimeframeCardDTO for a period with no plays
     */
    private TimeframeCardDTO createEmptyTimeframeCard(String periodType, String periodKey) {
        TimeframeCardDTO dto = new TimeframeCardDTO();
        dto.setPeriodKey(periodKey);
        dto.setPeriodType(periodType);
        dto.setPeriodDisplayName(formatPeriodDisplayName(periodType, periodKey));
        
        String[] dateRange = calculateDateRange(periodType, periodKey);
        dto.setListenedDateFrom(dateRange[0]);
        dto.setListenedDateTo(dateRange[1]);
        
        // All counts are 0
        dto.setPlayCount(0);
        dto.setTimeListened(0L);
        dto.setTimeListenedFormatted("0m");
        dto.setArtistCount(0);
        dto.setAlbumCount(0);
        dto.setSongCount(0);
        dto.setMaleCount(0);
        dto.setFemaleCount(0);
        dto.setOtherCount(0);
        dto.setMaleArtistCount(0);
        dto.setFemaleArtistCount(0);
        dto.setOtherArtistCount(0);
        dto.setMaleAlbumCount(0);
        dto.setFemaleAlbumCount(0);
        dto.setOtherAlbumCount(0);
        dto.setMalePlayCount(0);
        dto.setFemalePlayCount(0);
        dto.setOtherPlayCount(0);
        dto.setMaleTimeListened(0L);
        dto.setFemaleTimeListened(0L);
        dto.setOtherTimeListened(0L);
        
        return dto;
    }
    
    /**
     * Merge existing results with all possible period keys, filling in empty DTOs for missing periods.
     * Applies to days, weeks, months, seasons, and years period types.
     *
     * For weeks: Merges "Week 00" (partial week before first Monday) into the last week of the previous year.
     */
    private List<TimeframeCardDTO> mergeWithAllPeriods(String periodType, List<TimeframeCardDTO> existingResults, 
            String sortBy, String sortDir, int page, int perPage) {
        
        // For weeks, first merge any Week 00 data into the last week of the previous year
        if ("weeks".equals(periodType)) {
            existingResults = mergeWeekZeroIntoPreviousYear(existingResults);
        }

        // Generate all possible period keys based on period type
        List<String> allPeriodKeys = switch (periodType) {
            case "days" -> generateAllDayKeys();
            case "weeks" -> generateAllWeekKeysWithoutWeekZero(); // Exclude Week 00 since we merged it
            case "months" -> generateAllMonthKeys();
            case "seasons" -> generateAllSeasonKeys();
            case "years" -> generateAllYearKeys();
            default -> null;
        };
        
        if (allPeriodKeys == null) {
            return existingResults;
        }
        
        // Create a map of existing results
        Map<String, TimeframeCardDTO> existingMap = new HashMap<>();
        for (TimeframeCardDTO dto : existingResults) {
            existingMap.put(dto.getPeriodKey(), dto);
        }
        
        // Build full list with empty DTOs for missing periods
        List<TimeframeCardDTO> fullList = new ArrayList<>();
        for (String periodKey : allPeriodKeys) {
            TimeframeCardDTO dto = existingMap.get(periodKey);
            if (dto == null) {
                dto = createEmptyTimeframeCard(periodType, periodKey);
            }
            fullList.add(dto);
        }
        
        // Sort the full list
        Comparator<TimeframeCardDTO> comparator = getComparator(sortBy, periodType, sortDir);
        fullList.sort(comparator);
        
        // Apply pagination
        int start = page * perPage;
        int end = Math.min(start + perPage, fullList.size());
        if (start >= fullList.size()) {
            return new ArrayList<>();
        }
        
        return new ArrayList<>(fullList.subList(start, end));
    }
    
    /**
     * Get a comparator for sorting TimeframeCardDTOs.
     * Zero-play periods are always sorted to the END regardless of sort direction.
     */
    private Comparator<TimeframeCardDTO> getComparator(String sortBy, String periodType, String sortDir) {
        boolean isDesc = "desc".equalsIgnoreCase(sortDir);

        // Helper to check if a timeframe has zero plays (should always be last)
        java.util.function.Function<TimeframeCardDTO, Boolean> hasPlays =
            dto -> dto.getPlayCount() != null && dto.getPlayCount() > 0;

        if (sortBy == null || sortBy.isEmpty()) {
            // Default: sort by period key (chronological for seasons, lexicographic for years)
            Comparator<TimeframeCardDTO> defaultComparator;
            if ("seasons".equals(periodType)) {
                defaultComparator = (a, b) -> {
                    // Parse season key for chronological order
                    int orderA = getSeasonOrder(a.getPeriodKey());
                    int orderB = getSeasonOrder(b.getPeriodKey());
                    return Integer.compare(orderA, orderB);
                };
            } else {
                defaultComparator = Comparator.comparing(TimeframeCardDTO::getPeriodKey);
            }
            return isDesc ? defaultComparator.reversed() : defaultComparator;
        }

        return switch (sortBy.toLowerCase()) {
            case "plays" -> {
                // Zero plays always last, then sort by play count
                Comparator<TimeframeCardDTO> c = Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(TimeframeCardDTO::getPlayCount);
                yield isDesc ? Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(Comparator.comparing(TimeframeCardDTO::getPlayCount).reversed()) : c;
            }
            case "time" -> {
                Comparator<TimeframeCardDTO> c = Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(TimeframeCardDTO::getTimeListened);
                yield isDesc ? Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(Comparator.comparing(TimeframeCardDTO::getTimeListened).reversed()) : c;
            }
            case "artists" -> {
                Comparator<TimeframeCardDTO> c = Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(TimeframeCardDTO::getArtistCount);
                yield isDesc ? Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(Comparator.comparing(TimeframeCardDTO::getArtistCount).reversed()) : c;
            }
            case "albums" -> {
                Comparator<TimeframeCardDTO> c = Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(TimeframeCardDTO::getAlbumCount);
                yield isDesc ? Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(Comparator.comparing(TimeframeCardDTO::getAlbumCount).reversed()) : c;
            }
            case "songs" -> {
                Comparator<TimeframeCardDTO> c = Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(TimeframeCardDTO::getSongCount);
                yield isDesc ? Comparator.comparing(hasPlays, Comparator.reverseOrder())
                    .thenComparing(Comparator.comparing(TimeframeCardDTO::getSongCount).reversed()) : c;
            }
            case "maleartistpct" -> {
                // Sort by male artist percentage. Nulls (from zero-play cards) always go last.
                yield (a, b) -> {
                    Double pctA = a.getMaleArtistPercentage();
                    Double pctB = b.getMaleArtistPercentage();
                    if (pctA == null && pctB == null) return 0;
                    if (pctA == null) return 1;  // nulls last
                    if (pctB == null) return -1; // nulls last
                    return isDesc ? Double.compare(pctB, pctA) : Double.compare(pctA, pctB);
                };
            }
            case "malealbumpct" -> {
                yield (a, b) -> {
                    Double pctA = a.getMaleAlbumPercentage();
                    Double pctB = b.getMaleAlbumPercentage();
                    if (pctA == null && pctB == null) return 0;
                    if (pctA == null) return 1;
                    if (pctB == null) return -1;
                    return isDesc ? Double.compare(pctB, pctA) : Double.compare(pctA, pctB);
                };
            }
            case "malesongpct" -> {
                yield (a, b) -> {
                    Double pctA = a.getMaleSongPercentage();
                    Double pctB = b.getMaleSongPercentage();
                    if (pctA == null && pctB == null) return 0;
                    if (pctA == null) return 1;
                    if (pctB == null) return -1;
                    return isDesc ? Double.compare(pctB, pctA) : Double.compare(pctA, pctB);
                };
            }
            case "maleplaypct" -> {
                yield (a, b) -> {
                    Double pctA = a.getMalePlayPercentage();
                    Double pctB = b.getMalePlayPercentage();
                    if (pctA == null && pctB == null) return 0;
                    if (pctA == null) return 1;
                    if (pctB == null) return -1;
                    return isDesc ? Double.compare(pctB, pctA) : Double.compare(pctA, pctB);
                };
            }
            case "maletimepct" -> {
                yield (a, b) -> {
                    Double pctA = a.getMaleTimePercentage();
                    Double pctB = b.getMaleTimePercentage();
                    if (pctA == null && pctB == null) return 0;
                    if (pctA == null) return 1;
                    if (pctB == null) return -1;
                    return isDesc ? Double.compare(pctB, pctA) : Double.compare(pctA, pctB);
                };
            }
            case "maledays" -> {
                yield (a, b) -> {
                    Integer mdA = a.getMaleDays();
                    Integer mdB = b.getMaleDays();
                    if (mdA == null && mdB == null) return 0;
                    if (mdA == null) return 1;  // nulls last
                    if (mdB == null) return -1; // nulls last
                    return isDesc ? Integer.compare(mdB, mdA) : Integer.compare(mdA, mdB);
                };
            }
            default -> {
                Comparator<TimeframeCardDTO> defaultComparator;
                if ("seasons".equals(periodType)) {
                    defaultComparator = (a, b) -> {
                        int orderA = getSeasonOrder(a.getPeriodKey());
                        int orderB = getSeasonOrder(b.getPeriodKey());
                        return Integer.compare(orderA, orderB);
                    };
                } else {
                    defaultComparator = Comparator.comparing(TimeframeCardDTO::getPeriodKey);
                }
                yield isDesc ? defaultComparator.reversed() : defaultComparator;
            }
        };
    }

    /**
     * Convert season period key to a sortable integer (e.g., "2024-Spring" -> 20242)
     */
    private int getSeasonOrder(String periodKey) {
        if (periodKey == null) return 0;
        String[] parts = periodKey.split("-");
        if (parts.length != 2) return 0;
        
        int year = Integer.parseInt(parts[0]);
        int seasonNum = switch (parts[1]) {
            case "Winter" -> 1;
            case "Spring" -> 2;
            case "Summer" -> 3;
            case "Fall" -> 4;
            default -> 0;
        };
        
        return year * 10 + seasonNum;
    }
    
    /**
     * Get SQLite expression for period key based on type
     */
    private String getPeriodKeyExpression(String periodType) {
        return switch (periodType) {
            case "days" -> "DATE(p.play_date)";
            case "weeks" -> "strftime('%Y-W%W', p.play_date)";
            case "months" -> "strftime('%Y-%m', p.play_date)";
            case "seasons" -> """
                CASE 
                    WHEN CAST(strftime('%m', p.play_date) AS INTEGER) = 12 
                        THEN (CAST(strftime('%Y', p.play_date) AS INTEGER) + 1) || '-Winter'
                    WHEN CAST(strftime('%m', p.play_date) AS INTEGER) IN (1, 2) 
                        THEN strftime('%Y', p.play_date) || '-Winter'
                    WHEN CAST(strftime('%m', p.play_date) AS INTEGER) IN (3, 4, 5) 
                        THEN strftime('%Y', p.play_date) || '-Spring'
                    WHEN CAST(strftime('%m', p.play_date) AS INTEGER) IN (6, 7, 8) 
                        THEN strftime('%Y', p.play_date) || '-Summer'
                    WHEN CAST(strftime('%m', p.play_date) AS INTEGER) IN (9, 10, 11) 
                        THEN strftime('%Y', p.play_date) || '-Fall'
                END
                """;
            case "years" -> "strftime('%Y', p.play_date)";
            case "decades" -> "CAST((CAST(strftime('%Y', p.play_date) AS INTEGER) / 10) * 10 AS TEXT) || 's'";
            default -> "strftime('%Y', p.play_date)";
        };
    }
    
    /**
     * Get sort column based on sortBy parameter.
     * Returns expression with 'ps.' prefix which gets replaced as needed:
     * - For filtered_periods CTE: .replace("ps.", "")
     * - For final SELECT: .replace("ps.", "fp.")
     */
    private String getSortColumn(String sortBy, String periodType) {
        // For seasons, use chronological order (year * 10 + season_number)
        // Use ps.period_key so it can be replaced with correct prefix
        String seasonSortExpr = """
            (CAST(SUBSTR(ps.period_key, 1, 4) AS INTEGER) * 10 + 
             CASE SUBSTR(ps.period_key, 6)
                 WHEN 'Winter' THEN 1
                 WHEN 'Spring' THEN 2
                 WHEN 'Summer' THEN 3
                 WHEN 'Fall' THEN 4
                 ELSE 0
             END)""";
        
        if (sortBy == null) {
            return "seasons".equals(periodType) ? seasonSortExpr : "ps.period_key";
        }
        return switch (sortBy.toLowerCase()) {
            case "plays" -> "ps.play_count";
            case "time" -> "ps.time_listened";
            case "artists" -> "ps.artist_count";
            case "albums" -> "ps.album_count";
            case "songs" -> "ps.song_count";
            case "maleartistpct" -> "ps.male_artist_pct";
            case "malealbumpct" -> "ps.male_album_pct";
            case "malesongpct" -> "ps.male_song_pct";
            case "maleplaypct" -> "ps.male_play_pct";
            case "maletimepct" -> "ps.male_time_pct";
            case "maledays" -> "ps.period_key"; // Sorted in Java, use period_key as SQL fallback
            default -> "seasons".equals(periodType) ? seasonSortExpr : "ps.period_key";
        };
    }
    
    /**
     * Append inline summary filters (applied BEFORE pagination in filtered_periods CTE)
     */
    private void appendInlineSummaryFilters(StringBuilder sql, List<Object> params,
            Integer artistCountMin, Integer artistCountMax,
            Integer albumCountMin, Integer albumCountMax,
            Integer songCountMin, Integer songCountMax,
            Integer playsMin, Integer playsMax,
            Long timeMin, Long timeMax,
            Double maleArtistPctMin, Double maleArtistPctMax,
            Double maleAlbumPctMin, Double maleAlbumPctMax,
            Double maleSongPctMin, Double maleSongPctMax,
            Double malePlayPctMin, Double malePlayPctMax,
            Double maleTimePctMin, Double maleTimePctMax) {
        
        // Count range filters
        if (artistCountMin != null) {
            sql.append(" AND artist_count >= ?");
            params.add(artistCountMin);
        }
        if (artistCountMax != null) {
            sql.append(" AND artist_count <= ?");
            params.add(artistCountMax);
        }
        if (albumCountMin != null) {
            sql.append(" AND album_count >= ?");
            params.add(albumCountMin);
        }
        if (albumCountMax != null) {
            sql.append(" AND album_count <= ?");
            params.add(albumCountMax);
        }
        if (songCountMin != null) {
            sql.append(" AND song_count >= ?");
            params.add(songCountMin);
        }
        if (songCountMax != null) {
            sql.append(" AND song_count <= ?");
            params.add(songCountMax);
        }
        if (playsMin != null) {
            sql.append(" AND play_count >= ?");
            params.add(playsMin);
        }
        if (playsMax != null) {
            sql.append(" AND play_count <= ?");
            params.add(playsMax);
        }
        if (timeMin != null) {
            sql.append(" AND time_listened >= ?");
            params.add(timeMin);
        }
        if (timeMax != null) {
            sql.append(" AND time_listened <= ?");
            params.add(timeMax);
        }
        
        // Male percentage filters
        if (maleArtistPctMin != null) {
            sql.append(" AND male_artist_pct >= ?");
            params.add(maleArtistPctMin);
        }
        if (maleArtistPctMax != null) {
            sql.append(" AND male_artist_pct <= ?");
            params.add(maleArtistPctMax);
        }
        if (maleAlbumPctMin != null) {
            sql.append(" AND male_album_pct >= ?");
            params.add(maleAlbumPctMin);
        }
        if (maleAlbumPctMax != null) {
            sql.append(" AND male_album_pct <= ?");
            params.add(maleAlbumPctMax);
        }
        if (maleSongPctMin != null) {
            sql.append(" AND male_song_pct >= ?");
            params.add(maleSongPctMin);
        }
        if (maleSongPctMax != null) {
            sql.append(" AND male_song_pct <= ?");
            params.add(maleSongPctMax);
        }
        if (malePlayPctMin != null) {
            sql.append(" AND male_play_pct >= ?");
            params.add(malePlayPctMin);
        }
        if (malePlayPctMax != null) {
            sql.append(" AND male_play_pct <= ?");
            params.add(malePlayPctMax);
        }
        if (maleTimePctMin != null) {
            sql.append(" AND male_time_pct >= ?");
            params.add(maleTimePctMin);
        }
        if (maleTimePctMax != null) {
            sql.append(" AND male_time_pct <= ?");
            params.add(maleTimePctMax);
        }
    }
    
    /**
     * Append winning attribute filters (applied after main query)
     */
    private void appendWinningFilters(StringBuilder sql, List<Object> params,
            List<Integer> winningGender, String winningGenderMode,
            List<Integer> winningGenre, String winningGenreMode,
            List<Integer> winningEthnicity, String winningEthnicityMode,
            List<Integer> winningLanguage, String winningLanguageMode,
            List<String> winningCountry, String winningCountryMode) {
        
        // Winning gender filter
        if (winningGenderMode != null && winningGender != null && !winningGender.isEmpty()) {
            String placeholders = String.join(",", winningGender.stream().map(id -> "?").toList());
            if ("includes".equals(winningGenderMode)) {
                sql.append(" WHERE winning_gender_id IN (").append(placeholders).append(")");
                params.addAll(winningGender);
            } else if ("excludes".equals(winningGenderMode)) {
                sql.append(" WHERE (winning_gender_id NOT IN (").append(placeholders).append(") OR winning_gender_id IS NULL)");
                params.addAll(winningGender);
            }
        }
        
        // Winning genre filter
        if (winningGenreMode != null && winningGenre != null && !winningGenre.isEmpty()) {
            String placeholders = String.join(",", winningGenre.stream().map(id -> "?").toList());
            String connector = sql.indexOf(" WHERE ") > 0 ? " AND " : " WHERE ";
            if ("includes".equals(winningGenreMode)) {
                sql.append(connector).append("winning_genre_id IN (").append(placeholders).append(")");
                params.addAll(winningGenre);
            } else if ("excludes".equals(winningGenreMode)) {
                sql.append(connector).append("(winning_genre_id NOT IN (").append(placeholders).append(") OR winning_genre_id IS NULL)");
                params.addAll(winningGenre);
            }
        }
        
        // Winning ethnicity filter
        if (winningEthnicityMode != null && winningEthnicity != null && !winningEthnicity.isEmpty()) {
            String placeholders = String.join(",", winningEthnicity.stream().map(id -> "?").toList());
            String connector = sql.indexOf(" WHERE ") > 0 ? " AND " : " WHERE ";
            if ("includes".equals(winningEthnicityMode)) {
                sql.append(connector).append("winning_ethnicity_id IN (").append(placeholders).append(")");
                params.addAll(winningEthnicity);
            } else if ("excludes".equals(winningEthnicityMode)) {
                sql.append(connector).append("(winning_ethnicity_id NOT IN (").append(placeholders).append(") OR winning_ethnicity_id IS NULL)");
                params.addAll(winningEthnicity);
            }
        }
        
        // Winning language filter
        if (winningLanguageMode != null && winningLanguage != null && !winningLanguage.isEmpty()) {
            String placeholders = String.join(",", winningLanguage.stream().map(id -> "?").toList());
            String connector = sql.indexOf(" WHERE ") > 0 ? " AND " : " WHERE ";
            if ("includes".equals(winningLanguageMode)) {
                sql.append(connector).append("winning_language_id IN (").append(placeholders).append(")");
                params.addAll(winningLanguage);
            } else if ("excludes".equals(winningLanguageMode)) {
                sql.append(connector).append("(winning_language_id NOT IN (").append(placeholders).append(") OR winning_language_id IS NULL)");
                params.addAll(winningLanguage);
            }
        }
        
        // Winning country filter
        if (winningCountryMode != null && winningCountry != null && !winningCountry.isEmpty()) {
            String placeholders = String.join(",", winningCountry.stream().map(c -> "?").toList());
            String connector = sql.indexOf(" WHERE ") > 0 ? " AND " : " WHERE ";
            if ("includes".equals(winningCountryMode)) {
                sql.append(connector).append("winning_country IN (").append(placeholders).append(")");
                params.addAll(winningCountry);
            } else if ("excludes".equals(winningCountryMode)) {
                sql.append(connector).append("(winning_country NOT IN (").append(placeholders).append(") OR winning_country IS NULL)");
                params.addAll(winningCountry);
            }
        }
    }
    
    /**
     * Format period key to human-readable display name
     */
    private String formatPeriodDisplayName(String periodType, String periodKey) {
        if (periodKey == null) return "";
        
        try {
            return switch (periodType) {
                case "days" -> {
                    LocalDate date = LocalDate.parse(periodKey);
                    yield date.format(DateTimeFormatter.ofPattern("MMM d, yyyy"));
                }
                case "weeks" -> {
                    // Format: "2024-W01" -> "Jan 6 - Jan 12, 2024"
                    // Using SQLite's %W convention: week 01 starts from first Monday of year
                    String[] parts = periodKey.split("-W");
                    if (parts.length == 2) {
                        int year = Integer.parseInt(parts[0]);
                        int week = Integer.parseInt(parts[1]);

                        // Find the first Monday of the year (matches SQLite's %W week 01)
                        LocalDate jan1 = LocalDate.of(year, 1, 1);
                        LocalDate firstMonday = jan1.with(java.time.temporal.TemporalAdjusters.nextOrSame(java.time.DayOfWeek.MONDAY));

                        LocalDate firstDay;
                        LocalDate lastDay;

                        if (week == 0) {
                            // Week 00: Jan 1 to the day before the first Monday
                            firstDay = jan1;
                            lastDay = firstMonday.minusDays(1);
                            if (lastDay.isBefore(firstDay)) {
                                yield periodKey; // Week 00 is empty
                            }
                        } else {
                            // Week N: first Monday + (N-1)*7 days, for 7 days
                            firstDay = firstMonday.plusWeeks(week - 1);
                            lastDay = firstDay.plusDays(6);
                        }

                        String startStr = firstDay.format(DateTimeFormatter.ofPattern("MMM d"));
                        String endStr = lastDay.format(DateTimeFormatter.ofPattern("MMM d"));
                        // If same month, only show month once
                        if (firstDay.getMonth() == lastDay.getMonth()) {
                            endStr = String.valueOf(lastDay.getDayOfMonth());
                        }
                        // Use the calendar year of the last day for display
                        int displayYear = lastDay.getYear();
                        yield startStr + " - " + endStr + ", " + displayYear;
                    }
                    yield periodKey;
                }
                case "months" -> {
                    // Format: "2024-01" -> "January 2024"
                    YearMonth ym = YearMonth.parse(periodKey);
                    yield ym.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH) + " " + ym.getYear();
                }
                case "seasons" -> {
                    // Format: "2024-Winter" -> "Winter 2024"
                    String[] parts = periodKey.split("-");
                    if (parts.length == 2) {
                        yield parts[1] + " " + parts[0];
                    }
                    yield periodKey;
                }
                case "years" -> periodKey;
                case "decades" -> periodKey; // Already formatted as "2020s"
                default -> periodKey;
            };
        } catch (Exception e) {
            return periodKey;
        }
    }
    
    /**
     * Calculate date range for a period (for chart filtering)
     * 
     * For weeks: Uses SQLite's %W week numbering convention:
     * - Week 00: Days before the first Monday of the year
     * - Week 01: Starts from the first Monday of the year
     * - Week N: Starts from first Monday + (N-1)*7 days
     */
    private String[] calculateDateRange(String periodType, String periodKey) {
        if (periodKey == null) return new String[]{"", ""};
        
        try {
            return switch (periodType) {
                case "days" -> new String[]{periodKey, periodKey};
                case "weeks" -> {
                    // "2024-W01" - calculate first and last day of that week
                    // Using SQLite's %W convention: week 01 starts from first Monday of year
                    String[] parts = periodKey.split("-W");
                    if (parts.length == 2) {
                        int year = Integer.parseInt(parts[0]);
                        int week = Integer.parseInt(parts[1]);

                        // Find the first Monday of the year (matches SQLite's %W week 01)
                        LocalDate jan1 = LocalDate.of(year, 1, 1);
                        LocalDate firstMonday = jan1.with(java.time.temporal.TemporalAdjusters.nextOrSame(java.time.DayOfWeek.MONDAY));

                        LocalDate firstDay;
                        LocalDate lastDay;

                        if (week == 0) {
                            // Week 00: Jan 1 to the day before the first Monday
                            firstDay = jan1;
                            lastDay = firstMonday.minusDays(1);
                            // If first Monday IS Jan 1, week 00 is empty - return empty range
                            if (lastDay.isBefore(firstDay)) {
                                yield new String[]{"", ""};
                            }
                        } else {
                            // Week N: first Monday + (N-1)*7 days, for 7 days
                            firstDay = firstMonday.plusWeeks(week - 1);
                            lastDay = firstDay.plusDays(6);
                        }

                        yield new String[]{firstDay.toString(), lastDay.toString()};
                    }
                    yield new String[]{"", ""};
                }
                case "months" -> {
                    YearMonth ym = YearMonth.parse(periodKey);
                    yield new String[]{ym.atDay(1).toString(), ym.atEndOfMonth().toString()};
                }
                case "seasons" -> {
                    // "2024-Winter" means Dec 2023 + Jan-Feb 2024
                    String[] parts = periodKey.split("-");
                    if (parts.length == 2) {
                        int year = Integer.parseInt(parts[0]);
                        String season = parts[1];
                        yield switch (season) {
                            case "Winter" -> new String[]{(year - 1) + "-12-01", year + "-02-28"};
                            case "Spring" -> new String[]{year + "-03-01", year + "-05-31"};
                            case "Summer" -> new String[]{year + "-06-01", year + "-08-31"};
                            case "Fall" -> new String[]{year + "-09-01", year + "-11-30"};
                            default -> new String[]{"", ""};
                        };
                    }
                    yield new String[]{"", ""};
                }
                case "years" -> new String[]{periodKey + "-01-01", periodKey + "-12-31"};
                case "decades" -> {
                    // "2020s" -> 2020-01-01 to 2029-12-31
                    int decadeStart = Integer.parseInt(periodKey.replace("s", ""));
                    yield new String[]{decadeStart + "-01-01", (decadeStart + 9) + "-12-31"};
                }
                default -> new String[]{"", ""};
            };
        } catch (Exception e) {
            return new String[]{"", ""};
        }
    }

}
