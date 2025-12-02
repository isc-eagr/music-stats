package library.service;

import library.dto.TimeframeCardDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
            String sortBy, String sortDir, int page, int perPage) {
        
        int offset = page * perPage;
        String sortDirection = "asc".equalsIgnoreCase(sortDir) ? "ASC" : "DESC";
        
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
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                WHERE scr.scrobble_date IS NOT NULL
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
                        WHEN (male_artist_count + female_artist_count) > 0 
                        THEN CAST(male_artist_count AS REAL) * 100.0 / (male_artist_count + female_artist_count)
                        ELSE NULL 
                    END as male_artist_pct,
                    CASE 
                        WHEN (male_album_count + female_album_count) > 0 
                        THEN CAST(male_album_count AS REAL) * 100.0 / (male_album_count + female_album_count)
                        ELSE NULL 
                    END as male_album_pct,
                    CASE 
                        WHEN (male_song_count + female_song_count) > 0 
                        THEN CAST(male_song_count AS REAL) * 100.0 / (male_song_count + female_song_count)
                        ELSE NULL 
                    END as male_song_pct,
                    CASE 
                        WHEN (male_play_count + female_play_count) > 0 
                        THEN CAST(male_play_count AS REAL) * 100.0 / (male_play_count + female_play_count)
                        ELSE NULL 
                    END as male_play_pct,
                    CASE 
                        WHEN (male_time_listened + female_time_listened) > 0 
                        THEN CAST(male_time_listened AS REAL) * 100.0 / (male_time_listened + female_time_listened)
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
        sql.append("\n                LIMIT ? OFFSET ?");
        params.add(perPage);
        params.add(offset);
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
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE scr.scrobble_date IS NOT NULL AND gn.id IS NOT NULL
                GROUP BY period_key, gn.id, gn.name
            ),
            winning_genre AS (
                SELECT 
                    %s as period_key,
                    gr.id as genre_id,
                    gr.name as genre_name,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(al.override_genre_id, ar.genre_id)) = gr.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE scr.scrobble_date IS NOT NULL AND gr.id IS NOT NULL
                GROUP BY period_key, gr.id, gr.name
            ),
            winning_ethnicity AS (
                SELECT 
                    %s as period_key,
                    eth.id as ethnicity_id,
                    eth.name as ethnicity_name,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Ethnicity eth ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = eth.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE scr.scrobble_date IS NOT NULL AND eth.id IS NOT NULL
                GROUP BY period_key, eth.id, eth.name
            ),
            winning_language AS (
                SELECT 
                    %s as period_key,
                    lang.id as language_id,
                    lang.name as language_name,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Language lang ON COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = lang.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE scr.scrobble_date IS NOT NULL AND lang.id IS NOT NULL
                GROUP BY period_key, lang.id, lang.name
            ),
            winning_country AS (
                SELECT 
                    %s as period_key,
                    ar.country as country,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COUNT(*) DESC) as rn
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                INNER JOIN filtered_periods fp ON %s = fp.period_key
                WHERE scr.scrobble_date IS NOT NULL AND ar.country IS NOT NULL
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
        List<TimeframeCardDTO> results = jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
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
            dto.setTimeListenedFormatted(formatTime(rs.getLong("time_listened")));
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
        });
        
        return results;
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
            Double maleTimePctMin, Double maleTimePctMax) {
        
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
                FROM Scrobble scr
                INNER JOIN Song s ON scr.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                WHERE scr.scrobble_date IS NOT NULL
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
                        WHEN (male_artist_count + female_artist_count) > 0 
                        THEN CAST(male_artist_count AS REAL) * 100.0 / (male_artist_count + female_artist_count)
                        ELSE NULL 
                    END as male_artist_pct,
                    CASE 
                        WHEN (male_album_count + female_album_count) > 0 
                        THEN CAST(male_album_count AS REAL) * 100.0 / (male_album_count + female_album_count)
                        ELSE NULL 
                    END as male_album_pct,
                    CASE 
                        WHEN (male_song_count + female_song_count) > 0 
                        THEN CAST(male_song_count AS REAL) * 100.0 / (male_song_count + female_song_count)
                        ELSE NULL 
                    END as male_song_pct,
                    CASE 
                        WHEN (male_play_count + female_play_count) > 0 
                        THEN CAST(male_play_count AS REAL) * 100.0 / (male_play_count + female_play_count)
                        ELSE NULL 
                    END as male_play_pct,
                    CASE 
                        WHEN (male_time_listened + female_time_listened) > 0 
                        THEN CAST(male_time_listened AS REAL) * 100.0 / (male_time_listened + female_time_listened)
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
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE scr.scrobble_date IS NOT NULL AND gn.id IS NOT NULL
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
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album al ON s.album_id = al.id
                    LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(al.override_genre_id, ar.genre_id)) = gr.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE scr.scrobble_date IS NOT NULL AND gr.id IS NOT NULL
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
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Ethnicity eth ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = eth.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE scr.scrobble_date IS NOT NULL AND eth.id IS NOT NULL
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
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album al ON s.album_id = al.id
                    LEFT JOIN Language lang ON COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id)) = lang.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE scr.scrobble_date IS NOT NULL AND lang.id IS NOT NULL
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
                    FROM Scrobble scr
                    INNER JOIN Song s ON scr.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    INNER JOIN filtered_periods fp ON %s = fp.period_key
                    WHERE scr.scrobble_date IS NOT NULL AND ar.country IS NOT NULL
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
            
            if (needWinningGender) {
                String placeholders = String.join(",", winningGender.stream().map(id -> "?").toList());
                if ("includes".equals(winningGenderMode)) {
                    whereClause.append(" AND wgn.gender_id IN (").append(placeholders).append(")");
                    params.addAll(winningGender);
                } else if ("excludes".equals(winningGenderMode)) {
                    whereClause.append(" AND (wgn.gender_id NOT IN (").append(placeholders).append(") OR wgn.gender_id IS NULL)");
                    params.addAll(winningGender);
                }
            }
            
            if (needWinningGenre) {
                String placeholders = String.join(",", winningGenre.stream().map(id -> "?").toList());
                if ("includes".equals(winningGenreMode)) {
                    whereClause.append(" AND wgr.genre_id IN (").append(placeholders).append(")");
                    params.addAll(winningGenre);
                } else if ("excludes".equals(winningGenreMode)) {
                    whereClause.append(" AND (wgr.genre_id NOT IN (").append(placeholders).append(") OR wgr.genre_id IS NULL)");
                    params.addAll(winningGenre);
                }
            }
            
            if (needWinningEthnicity) {
                String placeholders = String.join(",", winningEthnicity.stream().map(id -> "?").toList());
                if ("includes".equals(winningEthnicityMode)) {
                    whereClause.append(" AND weth.ethnicity_id IN (").append(placeholders).append(")");
                    params.addAll(winningEthnicity);
                } else if ("excludes".equals(winningEthnicityMode)) {
                    whereClause.append(" AND (weth.ethnicity_id NOT IN (").append(placeholders).append(") OR weth.ethnicity_id IS NULL)");
                    params.addAll(winningEthnicity);
                }
            }
            
            if (needWinningLanguage) {
                String placeholders = String.join(",", winningLanguage.stream().map(id -> "?").toList());
                if ("includes".equals(winningLanguageMode)) {
                    whereClause.append(" AND wlang.language_id IN (").append(placeholders).append(")");
                    params.addAll(winningLanguage);
                } else if ("excludes".equals(winningLanguageMode)) {
                    whereClause.append(" AND (wlang.language_id NOT IN (").append(placeholders).append(") OR wlang.language_id IS NULL)");
                    params.addAll(winningLanguage);
                }
            }
            
            if (needWinningCountry) {
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
        return count != null ? count : 0;
    }
    
    /**
     * Get SQLite expression for period key based on type
     */
    private String getPeriodKeyExpression(String periodType) {
        return switch (periodType) {
            case "days" -> "DATE(scr.scrobble_date)";
            case "weeks" -> "strftime('%Y-W%W', scr.scrobble_date)";
            case "months" -> "strftime('%Y-%m', scr.scrobble_date)";
            case "seasons" -> """
                CASE 
                    WHEN CAST(strftime('%m', scr.scrobble_date) AS INTEGER) = 12 
                        THEN (CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) + 1) || '-Winter'
                    WHEN CAST(strftime('%m', scr.scrobble_date) AS INTEGER) IN (1, 2) 
                        THEN strftime('%Y', scr.scrobble_date) || '-Winter'
                    WHEN CAST(strftime('%m', scr.scrobble_date) AS INTEGER) IN (3, 4, 5) 
                        THEN strftime('%Y', scr.scrobble_date) || '-Spring'
                    WHEN CAST(strftime('%m', scr.scrobble_date) AS INTEGER) IN (6, 7, 8) 
                        THEN strftime('%Y', scr.scrobble_date) || '-Summer'
                    WHEN CAST(strftime('%m', scr.scrobble_date) AS INTEGER) IN (9, 10, 11) 
                        THEN strftime('%Y', scr.scrobble_date) || '-Fall'
                END
                """;
            case "years" -> "strftime('%Y', scr.scrobble_date)";
            case "decades" -> "(CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) / 10) * 10 || 's'";
            default -> "strftime('%Y', scr.scrobble_date)";
        };
    }
    
    /**
     * Get sort column based on sortBy parameter
     */
    private String getSortColumn(String sortBy, String periodType) {
        if (sortBy == null) {
            return "ps.period_key";
        }
        return switch (sortBy.toLowerCase()) {
            case "plays" -> "ps.play_count";
            case "time" -> "ps.time_listened";
            case "artists" -> "ps.artist_count";
            case "albums" -> "ps.album_count";
            case "songs" -> "ps.song_count";
            case "maleartistpct" -> "male_artist_pct";
            case "malealbumpct" -> "male_album_pct";
            case "malesongpct" -> "male_song_pct";
            case "maleplaypct" -> "male_play_pct";
            case "maletimepct" -> "male_time_pct";
            default -> "ps.period_key";
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
     * Append filter conditions to SQL
     */
    private void appendFilters(StringBuilder sql, List<Object> params,
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
            Double maleTimePctMin, Double maleTimePctMax) {
        
        // Winning gender filter
        if (winningGenderMode != null && winningGender != null && !winningGender.isEmpty()) {
            String placeholders = String.join(",", winningGender.stream().map(id -> "?").toList());
            if ("includes".equals(winningGenderMode)) {
                sql.append(" AND wgn.gender_id IN (").append(placeholders).append(")");
                params.addAll(winningGender);
            } else if ("excludes".equals(winningGenderMode)) {
                sql.append(" AND (wgn.gender_id NOT IN (").append(placeholders).append(") OR wgn.gender_id IS NULL)");
                params.addAll(winningGender);
            }
        }
        
        // Winning genre filter
        if (winningGenreMode != null && winningGenre != null && !winningGenre.isEmpty()) {
            String placeholders = String.join(",", winningGenre.stream().map(id -> "?").toList());
            if ("includes".equals(winningGenreMode)) {
                sql.append(" AND wgr.genre_id IN (").append(placeholders).append(")");
                params.addAll(winningGenre);
            } else if ("excludes".equals(winningGenreMode)) {
                sql.append(" AND (wgr.genre_id NOT IN (").append(placeholders).append(") OR wgr.genre_id IS NULL)");
                params.addAll(winningGenre);
            }
        }
        
        // Winning ethnicity filter
        if (winningEthnicityMode != null && winningEthnicity != null && !winningEthnicity.isEmpty()) {
            String placeholders = String.join(",", winningEthnicity.stream().map(id -> "?").toList());
            if ("includes".equals(winningEthnicityMode)) {
                sql.append(" AND weth.ethnicity_id IN (").append(placeholders).append(")");
                params.addAll(winningEthnicity);
            } else if ("excludes".equals(winningEthnicityMode)) {
                sql.append(" AND (weth.ethnicity_id NOT IN (").append(placeholders).append(") OR weth.ethnicity_id IS NULL)");
                params.addAll(winningEthnicity);
            }
        }
        
        // Winning language filter
        if (winningLanguageMode != null && winningLanguage != null && !winningLanguage.isEmpty()) {
            String placeholders = String.join(",", winningLanguage.stream().map(id -> "?").toList());
            if ("includes".equals(winningLanguageMode)) {
                sql.append(" AND wlang.language_id IN (").append(placeholders).append(")");
                params.addAll(winningLanguage);
            } else if ("excludes".equals(winningLanguageMode)) {
                sql.append(" AND (wlang.language_id NOT IN (").append(placeholders).append(") OR wlang.language_id IS NULL)");
                params.addAll(winningLanguage);
            }
        }
        
        // Winning country filter
        if (winningCountryMode != null && winningCountry != null && !winningCountry.isEmpty()) {
            String placeholders = String.join(",", winningCountry.stream().map(c -> "?").toList());
            if ("includes".equals(winningCountryMode)) {
                sql.append(" AND wcty.country IN (").append(placeholders).append(")");
                params.addAll(winningCountry);
            } else if ("excludes".equals(winningCountryMode)) {
                sql.append(" AND (wcty.country NOT IN (").append(placeholders).append(") OR wcty.country IS NULL)");
                params.addAll(winningCountry);
            }
        }
        
        // Count range filters
        if (artistCountMin != null) {
            sql.append(" AND ps.artist_count >= ?");
            params.add(artistCountMin);
        }
        if (artistCountMax != null) {
            sql.append(" AND ps.artist_count <= ?");
            params.add(artistCountMax);
        }
        if (albumCountMin != null) {
            sql.append(" AND ps.album_count >= ?");
            params.add(albumCountMin);
        }
        if (albumCountMax != null) {
            sql.append(" AND ps.album_count <= ?");
            params.add(albumCountMax);
        }
        if (songCountMin != null) {
            sql.append(" AND ps.song_count >= ?");
            params.add(songCountMin);
        }
        if (songCountMax != null) {
            sql.append(" AND ps.song_count <= ?");
            params.add(songCountMax);
        }
        if (playsMin != null) {
            sql.append(" AND ps.play_count >= ?");
            params.add(playsMin);
        }
        if (playsMax != null) {
            sql.append(" AND ps.play_count <= ?");
            params.add(playsMax);
        }
        if (timeMin != null) {
            sql.append(" AND ps.time_listened >= ?");
            params.add(timeMin);
        }
        if (timeMax != null) {
            sql.append(" AND ps.time_listened <= ?");
            params.add(timeMax);
        }
        
        // Male percentage filters
        if (maleArtistPctMin != null) {
            sql.append(" AND (CAST(ps.male_artist_count AS REAL) * 100.0 / NULLIF(ps.male_artist_count + ps.female_artist_count, 0)) >= ?");
            params.add(maleArtistPctMin);
        }
        if (maleArtistPctMax != null) {
            sql.append(" AND (CAST(ps.male_artist_count AS REAL) * 100.0 / NULLIF(ps.male_artist_count + ps.female_artist_count, 0)) <= ?");
            params.add(maleArtistPctMax);
        }
        if (maleAlbumPctMin != null) {
            sql.append(" AND (CAST(ps.male_album_count AS REAL) * 100.0 / NULLIF(ps.male_album_count + ps.female_album_count, 0)) >= ?");
            params.add(maleAlbumPctMin);
        }
        if (maleAlbumPctMax != null) {
            sql.append(" AND (CAST(ps.male_album_count AS REAL) * 100.0 / NULLIF(ps.male_album_count + ps.female_album_count, 0)) <= ?");
            params.add(maleAlbumPctMax);
        }
        if (maleSongPctMin != null) {
            sql.append(" AND (CAST(ps.male_song_count AS REAL) * 100.0 / NULLIF(ps.male_song_count + ps.female_song_count, 0)) >= ?");
            params.add(maleSongPctMin);
        }
        if (maleSongPctMax != null) {
            sql.append(" AND (CAST(ps.male_song_count AS REAL) * 100.0 / NULLIF(ps.male_song_count + ps.female_song_count, 0)) <= ?");
            params.add(maleSongPctMax);
        }
        if (malePlayPctMin != null) {
            sql.append(" AND (CAST(ps.male_play_count AS REAL) * 100.0 / NULLIF(ps.male_play_count + ps.female_play_count, 0)) >= ?");
            params.add(malePlayPctMin);
        }
        if (malePlayPctMax != null) {
            sql.append(" AND (CAST(ps.male_play_count AS REAL) * 100.0 / NULLIF(ps.male_play_count + ps.female_play_count, 0)) <= ?");
            params.add(malePlayPctMax);
        }
        if (maleTimePctMin != null) {
            sql.append(" AND (CAST(ps.male_time_listened AS REAL) * 100.0 / NULLIF(ps.male_time_listened + ps.female_time_listened, 0)) >= ?");
            params.add(maleTimePctMin);
        }
        if (maleTimePctMax != null) {
            sql.append(" AND (CAST(ps.male_time_listened AS REAL) * 100.0 / NULLIF(ps.male_time_listened + ps.female_time_listened, 0)) <= ?");
            params.add(maleTimePctMax);
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
                    // Format: "2024-W01" -> "Jan 1 - Jan 7, 2024"
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
                        yield startStr + " - " + endStr + ", " + year;
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
    
    /**
     * Format seconds to human-readable time
     */
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
}
