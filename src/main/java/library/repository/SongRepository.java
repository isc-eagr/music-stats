package library.repository;

import library.dto.ChartFilterDTO;
import library.dto.SongStatsQuery;
import library.dto.SongStatsRow;
import library.service.AppConfigService;
import library.util.RandomSortUtils;
import library.util.TimeFormatUtils;
import library.util.SqlFilterHelper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class SongRepository {
    
    private final JdbcTemplate jdbcTemplate;
    private final AppConfigService appConfigService;
    
    public SongRepository(JdbcTemplate jdbcTemplate, AppConfigService appConfigService) {
        this.jdbcTemplate = jdbcTemplate;
        this.appConfigService = appConfigService;
    }
    
    public List<SongStatsRow> findSongsWithStats(SongStatsQuery query) {
        String name = query.name();
        List<Integer> artistName = query.artistName();
        String albumName = query.albumName();
        List<Integer> genreIds = query.genreIds();
        String genreMode = query.genreMode();
        List<Integer> subgenreIds = query.subgenreIds();
        String subgenreMode = query.subgenreMode();
        List<Integer> languageIds = query.languageIds();
        String languageMode = query.languageMode();
        List<Integer> genderIds = query.genderIds();
        String genderMode = query.genderMode();
        List<Integer> ethnicityIds = query.ethnicityIds();
        String ethnicityMode = query.ethnicityMode();
        List<String> countries = query.countries();
        String countryMode = query.countryMode();
        List<Integer> tagIds = query.tagIds();
        String tagMode = query.tagMode();
        List<String> accounts = query.accounts();
        String accountMode = query.accountMode();
        String releaseDate = query.releaseDate();
        String releaseDateFrom = query.releaseDateFrom();
        String releaseDateTo = query.releaseDateTo();
        String releaseDateMode = query.releaseDateMode();
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
        String hasFeaturedArtists = query.hasFeaturedArtists();
        String isBand = query.isBand();
        String isSingle = query.isSingle();
        String itunesIdsJson = query.itunesIdsJson();
        String inItunes = query.inItunes();
        Integer ageMin = query.ageMin();
        Integer ageMax = query.ageMax();
        Integer ageAtReleaseMin = query.ageAtReleaseMin();
        Integer ageAtReleaseMax = query.ageAtReleaseMax();
        String birthDate = query.birthDate();
        String birthDateFrom = query.birthDateFrom();
        String birthDateTo = query.birthDateTo();
        String birthDateMode = query.birthDateMode();
        String deathDate = query.deathDate();
        String deathDateFrom = query.deathDateFrom();
        String deathDateTo = query.deathDateTo();
        String deathDateMode = query.deathDateMode();
        Integer playCountMin = query.playCountMin();
        Integer playCountMax = query.playCountMax();
        Integer trackNumber = query.trackNumber();
        String trackNumberMode = query.trackNumberMode();
        Integer lengthMin = query.lengthMin();
        Integer lengthMax = query.lengthMax();
        String lengthMode = query.lengthMode();
        Integer weeklyChartPeak = query.weeklyChartPeak();
        String weeklyChartPeakMode = query.weeklyChartPeakMode();
        Integer weeklyChartWeeks = query.weeklyChartWeeks();
        Integer weeklyChartPeakWeeks = query.weeklyChartPeakWeeks();
        String weeklyChartPeakWeeksMode = query.weeklyChartPeakWeeksMode();
        String weeklyChartDateFrom = query.weeklyChartDateFrom();
        String weeklyChartDateTo = query.weeklyChartDateTo();
        String weeklyChartSeason = query.weeklyChartSeason();
        Integer trlPeak = query.trlPeak();
        String trlPeakMode = query.trlPeakMode();
        Integer trlDays = query.trlDays();
        Integer trlDaysAtPeak = query.trlDaysAtPeak();
        String trlDaysAtPeakMode = query.trlDaysAtPeakMode();
        String trlDateFrom = query.trlDateFrom();
        String trlDateTo = query.trlDateTo();
        Integer vatosCuntdownPeak = query.vatosCuntdownPeak();
        String vatosCuntdownPeakMode = query.vatosCuntdownPeakMode();
        Integer vatosCuntdownDays = query.vatosCuntdownDays();
        Integer vatosCuntdownDaysAtPeak = query.vatosCuntdownDaysAtPeak();
        String vatosCuntdownDaysAtPeakMode = query.vatosCuntdownDaysAtPeakMode();
        String vatosCuntdownDateFrom = query.vatosCuntdownDateFrom();
        String vatosCuntdownDateTo = query.vatosCuntdownDateTo();
        Integer billboardPeak = query.billboardPeak();
        String billboardPeakMode = query.billboardPeakMode();
        Integer billboardWeeks = query.billboardWeeks();
        Integer billboardWeeksAtPeak = query.billboardWeeksAtPeak();
        String billboardWeeksAtPeakMode = query.billboardWeeksAtPeakMode();
        String billboardDateFrom = query.billboardDateFrom();
        String billboardDateTo = query.billboardDateTo();
        Integer seasonalChartPeak = query.seasonalChartPeak();
        Integer seasonalChartSeasons = query.seasonalChartSeasons();
        String seasonalChartDateFrom = query.seasonalChartDateFrom();
        String seasonalChartDateTo = query.seasonalChartDateTo();
        String seasonalChartSeason = query.seasonalChartSeason();
        Integer yearlyChartPeak = query.yearlyChartPeak();
        Integer yearlyChartYears = query.yearlyChartYears();
        String yearlyChartDateFrom = query.yearlyChartDateFrom();
        String yearlyChartDateTo = query.yearlyChartDateTo();
        String sortBy = query.sortBy();
        String sortDirection = query.sortDirection();
        String sortBy2 = query.sortBy2();
        String sortDirection2 = query.sortDirection2();
        String sortBy3 = query.sortBy3();
        String sortDirection3 = query.sortDirection3();
        int limit = query.limit();
        int offset = query.offset();
        boolean includeExpensiveStats = query.includeExpensiveStats();
        List<Integer> songIdsFilter = query.songIdsFilter();
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

        StringBuilder playStatsSongFilterClause = new StringBuilder();
        List<Object> playStatsSongParams = new ArrayList<>();
        if (songIdsFilter != null && !songIdsFilter.isEmpty()) {
            String placeholders = String.join(",", songIdsFilter.stream().map(id -> "?").toList());
            playStatsSongFilterClause.append(" AND p.song_id IN (").append(placeholders).append(") ");
            playStatsSongParams.addAll(songIdsFilter);
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
                COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
                COALESCE(play_stats.play_count, 0) as play_count,
                COALESCE(play_stats.vatito_play_count, 0) as vatito_play_count,
                COALESCE(play_stats.robertlover_play_count, 0) as robertlover_play_count,
                COALESCE(s.length_seconds * play_stats.play_count, 0) as time_listened,
                play_stats.first_listened,
                play_stats.last_listened,
                COALESCE(play_stats.days_listened, 0) as days_listened,
                COALESCE(play_stats.weeks_listened, 0) as weeks_listened,
                COALESCE(play_stats.months_listened, 0) as months_listened,
                COALESCE(play_stats.years_listened, 0) as years_listened,
                ar.country as country,
                s.organized,
                CASE WHEN alb.image IS NOT NULL THEN 1 ELSE 0 END as album_has_image,
                s.is_single,
                ar.birth_date,
                ar.death_date,
                ((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id)) as image_count,
                """);
        if (includeExpensiveStats) {
            sql.append("""
                (SELECT MIN(b.peak_position) FROM billboard_hot100_debut b WHERE b.song_id = s.id) as billboard_peak,
                (SELECT MAX(b.weeks_on_chart) FROM billboard_hot100_debut b WHERE b.song_id = s.id) as billboard_weeks,
                (SELECT MAX(b.weeks_at_peak) FROM billboard_hot100_debut b WHERE b.song_id = s.id) as billboard_weeks_at_peak,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal') as seasonal_chart_peak,
                (SELECT COUNT(DISTINCT ce.chart_date) FROM trl_chart_entry ce INNER JOIN trl_debut td ON td.id = ce.debut_id WHERE td.song_id = s.id) as trl_days,
                (SELECT COUNT(DISTINCT ce2.chart_date) FROM trl_chart_entry ce2 INNER JOIN trl_debut td2 ON td2.id = ce2.debut_id WHERE td2.song_id = s.id AND ce2.position = (SELECT MIN(ce3.position) FROM trl_chart_entry ce3 INNER JOIN trl_debut td3 ON td3.id = ce3.debut_id WHERE td3.song_id = s.id)) as trl_days_at_peak,
                (SELECT MIN(ce.position) FROM trl_chart_entry ce INNER JOIN trl_debut td ON td.id = ce.debut_id WHERE td.song_id = s.id) as trl_peak,
                (SELECT COUNT(DISTINCT e.chart_date) FROM vatos_cuntdown_entry e WHERE e.song_id = s.id AND e.is_close_call = 0) as vatos_cuntdown_days,
                (SELECT COUNT(DISTINCT e2.chart_date) FROM vatos_cuntdown_entry e2 WHERE e2.song_id = s.id AND e2.is_close_call = 0 AND e2.position = (SELECT MIN(e3.position) FROM vatos_cuntdown_entry e3 WHERE e3.song_id = s.id AND e3.is_close_call = 0)) as vatos_cuntdown_days_at_peak,
                (SELECT MIN(e.position) FROM vatos_cuntdown_entry e WHERE e.song_id = s.id AND e.is_close_call = 0) as vatos_cuntdown_peak,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly') as weekly_chart_peak,
                (SELECT COUNT(DISTINCT c.id) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly') as weekly_chart_weeks,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'yearly') as yearly_chart_peak,
                (SELECT c.period_start_date FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly' AND ce.position = (SELECT MIN(ce2.position) FROM ChartEntry ce2 INNER JOIN Chart c2 ON ce2.chart_id = c2.id WHERE ce2.song_id = s.id AND c2.chart_type = 'song' AND c2.period_type = 'weekly') ORDER BY c.period_start_date ASC LIMIT 1) as weekly_chart_peak_start_date,
                (SELECT c.period_start_date FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal' AND ce.position = (SELECT MIN(ce2.position) FROM ChartEntry ce2 INNER JOIN Chart c2 ON ce2.chart_id = c2.id WHERE ce2.song_id = s.id AND c2.chart_type = 'song' AND c2.period_type = 'seasonal') ORDER BY c.period_start_date ASC LIMIT 1) as seasonal_chart_peak_start_date,
                (SELECT c.period_key FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal' AND ce.position = (SELECT MIN(ce2.position) FROM ChartEntry ce2 INNER JOIN Chart c2 ON ce2.chart_id = c2.id WHERE ce2.song_id = s.id AND c2.chart_type = 'song' AND c2.period_type = 'seasonal') ORDER BY c.period_start_date ASC LIMIT 1) as seasonal_chart_peak_period,
                (SELECT c.period_key FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'yearly' AND ce.position = (SELECT MIN(ce2.position) FROM ChartEntry ce2 INNER JOIN Chart c2 ON ce2.chart_id = c2.id WHERE ce2.song_id = s.id AND c2.chart_type = 'song' AND c2.period_type = 'yearly') ORDER BY c.period_start_date ASC LIMIT 1) as yearly_chart_peak_period,
                (SELECT COUNT(*) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly' AND ce.position = (SELECT MIN(ce2.position) FROM ChartEntry ce2 INNER JOIN Chart c2 ON ce2.chart_id = c2.id WHERE ce2.song_id = s.id AND c2.chart_type = 'song' AND c2.period_type = 'weekly')) as weekly_chart_peak_weeks,
                (SELECT COUNT(*) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal' AND ce.position = (SELECT MIN(ce2.position) FROM ChartEntry ce2 INNER JOIN Chart c2 ON ce2.chart_id = c2.id WHERE ce2.song_id = s.id AND c2.chart_type = 'song' AND c2.period_type = 'seasonal')) as seasonal_chart_peak_seasons,
                (SELECT COUNT(*) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'yearly' AND ce.position = (SELECT MIN(ce2.position) FROM ChartEntry ce2 INNER JOIN Chart c2 ON ce2.chart_id = c2.id WHERE ce2.song_id = s.id AND c2.chart_type = 'song' AND c2.period_type = 'yearly')) as yearly_chart_peak_years,
                """);
        } else {
            sql.append("""
                NULL as billboard_peak,
                NULL as billboard_weeks,
                NULL as billboard_weeks_at_peak,
                NULL as seasonal_chart_peak,
                NULL as trl_days,
                NULL as trl_days_at_peak,
                NULL as trl_peak,
                NULL as vatos_cuntdown_days,
                NULL as vatos_cuntdown_days_at_peak,
                NULL as vatos_cuntdown_peak,
                NULL as weekly_chart_peak,
                NULL as weekly_chart_weeks,
                NULL as yearly_chart_peak,
                NULL as weekly_chart_peak_start_date,
                NULL as seasonal_chart_peak_start_date,
                NULL as seasonal_chart_peak_period,
                NULL as yearly_chart_peak_period,
                NULL as weekly_chart_peak_weeks,
                NULL as seasonal_chart_peak_seasons,
                NULL as yearly_chart_peak_years,
                """);
        }
        sql.append("""
                COALESCE(fac.featured_artist_count, 0) as featured_artist_count,
                CASE WHEN ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL THEN CAST((julianday(COALESCE(s.release_date, alb.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER) ELSE NULL END as age_at_release,
                s.track_number
            FROM Song s
            LEFT JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Gender gender ON COALESCE(s.override_gender_id, ar.gender_id) = gender.id
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
            LEFT JOIN (SELECT song_id, COUNT(*) as featured_artist_count FROM SongFeaturedArtist GROUP BY song_id) fac ON fac.song_id = s.id
            """);
        
        // Use INNER JOIN when account filter is includes mode or when listened date filter is applied
        boolean hasListenedDateFilter = (listenedDateFrom != null && !listenedDateFrom.isEmpty()) || 
                                        (listenedDateTo != null && !listenedDateTo.isEmpty());
        String playStatsJoinType = ((accounts != null && !accounts.isEmpty() && "includes".equalsIgnoreCase(accountMode)) || hasListenedDateFilter) ? "INNER JOIN" : "LEFT JOIN";
        sql.append(playStatsJoinType).append(""" 
             (
                SELECT 
                    p.song_id,
                    COUNT(*) as play_count,
                    SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as vatito_play_count,
                    SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as robertlover_play_count,
                    """);
        if (includeExpensiveStats) {
            sql.append("""
                    COUNT(DISTINCT DATE(p.play_date)) as days_listened,
                    COUNT(DISTINCT strftime('%Y-%W', p.play_date)) as weeks_listened,
                    COUNT(DISTINCT strftime('%Y-%m', p.play_date)) as months_listened,
                    COUNT(DISTINCT strftime('%Y', p.play_date)) as years_listened,
                    """);
        } else {
            sql.append("""
                    0 as days_listened,
                    0 as weeks_listened,
                    0 as months_listened,
                    0 as years_listened,
                    """);
        }
        sql.append("""
                    MIN(p.play_date) as first_listened,
                    MAX(p.play_date) as last_listened
                FROM Play p
                WHERE 1=1 """);
        sql.append(accountFilterClause);
        sql.append(listenedDateFilterClause);
        sql.append(playStatsSongFilterClause);
        sql.append("""
                GROUP BY p.song_id
            ) play_stats ON play_stats.song_id = s.id
            WHERE 1=1
            """);
        
        List<Object> params = new ArrayList<>();
        // Add account params only once now (play_stats subquery)
        params.addAll(accountParams);
        // Add listened date params
        params.addAll(listenedDateParams);
        // Add song-id params for the play_stats subquery before outer WHERE params.
        params.addAll(playStatsSongParams);

        if (songIdsFilter != null && !songIdsFilter.isEmpty()) {
            String placeholders = String.join(",", songIdsFilter.stream().map(id -> "?").toList());
            sql.append(" AND s.id IN (").append(placeholders).append(") ");
            params.addAll(songIdsFilter);
        }
        
        // Name filters with accent-insensitive search
        if (name != null && !name.trim().isEmpty()) {
            sql.append(" AND ").append(library.util.StringNormalizer.sqlNormalizeColumn("s.name")).append(" LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
        }
        
        // Artist filter
        if (artistName != null && !artistName.isEmpty()) {
            String artistPlaceholders = String.join(",", artistName.stream().map(id -> "?").toList());
            sql.append(" AND ar.id IN (").append(artistPlaceholders).append(")");
            params.addAll(artistName);
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
                    sql.append(" AND (COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) IS NULL)");
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
                    sql.append(" AND (COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) IS NULL)");
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
                    sql.append(" AND (COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) IS NULL)");
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

        // Tag filter
        SqlFilterHelper.appendTagFilter(sql, params, "s.id", "SongTag", "song_id", tagIds, tagMode);
        
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
        if ("isnull".equals(releaseDateMode)) {
            sql.append(" AND COALESCE(s.release_date, alb.release_date) IS NULL");
        } else if ("isnotnull".equals(releaseDateMode)) {
            sql.append(" AND COALESCE(s.release_date, alb.release_date) IS NOT NULL");
        }
        
        // First listened date filter
        if (firstListenedDate != null && !firstListenedDate.trim().isEmpty() && firstListenedDateMode != null) {
            switch (firstListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) = DATE(?)");
                    params.add(firstListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?)");
                    params.add(firstListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
                    params.add(firstListenedDate);
                }
            }
        }
        
        // First listened between filter
        if ("between".equals(firstListenedDateMode) && firstListenedDateFrom != null && !firstListenedDateFrom.trim().isEmpty()
                && firstListenedDateTo != null && !firstListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
            params.add(firstListenedDateFrom);
            params.add(firstListenedDateTo);
        }
        
        // Last listened date filter
        if (lastListenedDate != null && !lastListenedDate.trim().isEmpty() && lastListenedDateMode != null) {
            switch (lastListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) = DATE(?)");
                    params.add(lastListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?)");
                    params.add(lastListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
                    params.add(lastListenedDate);
                }
            }
        }
        
        // Last listened between filter
        if ("between".equals(lastListenedDateMode) && lastListenedDateFrom != null && !lastListenedDateFrom.trim().isEmpty()
                && lastListenedDateTo != null && !lastListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
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

        // Track number filter
        if (trackNumberMode != null && !trackNumberMode.isEmpty()) {
            if ("isnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NULL ");
            } else if ("isnotnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NOT NULL ");
            } else if (trackNumber != null) {
                sql.append(" AND s.track_number = ? ");
                params.add(trackNumber);
            }
        } else if (trackNumber != null) {
            sql.append(" AND s.track_number = ? ");
            params.add(trackNumber);
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
        
        // iTunes filter (pre-computed ID set via json_each)
        SqlFilterHelper.appendItunesIdFilter(sql, params, "s.id", itunesIdsJson, inItunes);
        
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
        
        SqlFilterHelper.appendChartStatsFilter(sql, params, "ce.song_id", "s.id", "song", "weekly", "weeks",
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                weeklyChartPeakWeeks, weeklyChartPeakWeeksMode);

        appendSongTrlFilter(sql, params, trlPeak, trlPeakMode, trlDays, trlDaysAtPeak, trlDaysAtPeakMode, trlDateFrom, trlDateTo);
        appendSongVatosCuntdownFilter(sql, params, vatosCuntdownPeak, vatosCuntdownPeakMode, vatosCuntdownDays, vatosCuntdownDaysAtPeak, vatosCuntdownDaysAtPeakMode, vatosCuntdownDateFrom, vatosCuntdownDateTo);
        appendSongBillboardFilter(sql, params, billboardPeak, billboardPeakMode, billboardWeeks, billboardWeeksAtPeak, billboardWeeksAtPeakMode, billboardDateFrom, billboardDateTo);

        SqlFilterHelper.appendChartStatsFilter(sql, params, "ce.song_id", "s.id", "song", "seasonal", "seasons",
                seasonalChartPeak, seasonalChartSeasons, seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason);

        SqlFilterHelper.appendChartStatsFilter(sql, params, "ce.song_id", "s.id", "song", "yearly", "years",
                yearlyChartPeak, yearlyChartYears, yearlyChartDateFrom, yearlyChartDateTo, null);
        
        appendSongSortOrder(sql, sortBy, sortDirection, sortBy2, sortDirection2, sortBy3, sortDirection3, query.randomSeed());
        
        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> SongStatsRow.from(rs), params.toArray());
    }

    private void appendSongSortOrder(StringBuilder sql, String sortBy, String sortDirection,
                                     String sortBy2, String sortDirection2,
                                     String sortBy3, String sortDirection3,
                                     Integer randomSeed) {
        List<String> clauses = new ArrayList<>();
        List<String> appliedSorts = new ArrayList<>();

        String effectiveSortBy = sortBy != null ? sortBy : "name";
        boolean hasSecondSort = sortBy2 != null && !sortBy2.isBlank();
        boolean hasThirdSort = sortBy3 != null && !sortBy3.isBlank();

        appendSongSortClause(clauses, appliedSorts, effectiveSortBy, sortDirection, !hasSecondSort, randomSeed);
        appendSongSortClause(clauses, appliedSorts, sortBy2, sortDirection2, !hasThirdSort, randomSeed);
        appendSongSortClause(clauses, appliedSorts, sortBy3, sortDirection3, true, randomSeed);

        clauses.add("play_count DESC");
        clauses.add("s.name");
        sql.append(" ORDER BY ").append(String.join(", ", clauses));
    }

    private void appendSongSortClause(List<String> clauses, List<String> appliedSorts, String sortBy, String sortDirection, boolean allowInternalTieBreakers, Integer randomSeed) {
        if (sortBy == null || sortBy.isBlank() || appliedSorts.contains(sortBy)) {
            return;
        }

        String dir = "desc".equalsIgnoreCase(sortDirection) ? "DESC" : "ASC";
        String nullsOrder = "desc".equalsIgnoreCase(sortDirection) ? "NULLS LAST" : "NULLS FIRST";
        String clause = switch (sortBy) {
            case "artist" -> "ar.name " + dir;
            case "album" -> "alb.name " + dir + " " + nullsOrder;
            case "country" -> "ar.country " + dir + " " + nullsOrder;
            case "ethnicity" -> "e.name " + dir + " " + nullsOrder;
            case "featured_artist_count" -> "featured_artist_count " + dir;
            case "release_date" -> "COALESCE(s.release_date, alb.release_date) " + dir + " " + nullsOrder;
            case "genre" -> "genre_name " + dir + " " + nullsOrder;
            case "language" -> "language_name " + dir + " " + nullsOrder;
            case "legacy_plays" -> "robertlover_play_count " + dir;
            case "length" -> "s.length_seconds " + dir + " " + nullsOrder;
            case "plays" -> "play_count " + dir;
            case "primary_plays" -> "vatito_play_count " + dir;
            case "track_number" -> "s.track_number " + dir + " " + nullsOrder;
            case "subgenre" -> "subgenre_name " + dir + " " + nullsOrder;
            case "time" -> "(s.length_seconds * play_count) " + dir;
            case "first_listened" -> "first_listened " + dir + " " + nullsOrder;
            case "last_listened" -> "last_listened " + dir + " " + nullsOrder;
            case "days_listened" -> "days_listened " + dir;
            case "weeks_listened" -> "weeks_listened " + dir;
            case "months_listened" -> "months_listened " + dir;
            case "years_listened" -> "years_listened " + dir;
            case "age_at_release" -> "CAST((julianday(COALESCE(s.release_date, alb.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER) " + dir + " " + nullsOrder;
            case "birth_date" -> "ar.birth_date " + dir + " " + nullsOrder;
            case "billboard_peak" -> allowInternalTieBreakers
                    ? "billboard_peak " + dir + " NULLS LAST, billboard_weeks DESC NULLS LAST"
                    : "billboard_peak " + dir + " NULLS LAST";
            case "billboard_weeks" -> "billboard_weeks " + dir;
            case "billboard_weeks_at_peak" -> "billboard_weeks_at_peak " + dir + " NULLS LAST";
            case "death_date" -> "ar.death_date " + dir + " " + nullsOrder;
            case "image_count" -> "image_count " + dir;
            case "random" -> RandomSortUtils.sqliteNumericExpression("s.id", randomSeed);
                case "seasonal_chart_peak" -> allowInternalTieBreakers
                    ? "seasonal_chart_peak " + dir + " NULLS LAST, seasonal_chart_peak_start_date DESC NULLS LAST"
                    : "seasonal_chart_peak " + dir + " NULLS LAST";
            case "trl_days" -> "trl_days " + dir;
            case "trl_days_at_peak" -> "trl_days_at_peak " + dir + " NULLS LAST";
            case "trl_peak" -> allowInternalTieBreakers
                    ? "trl_peak " + dir + " NULLS LAST, trl_days DESC NULLS LAST"
                    : "trl_peak " + dir + " NULLS LAST";
                case "weekly_chart_peak" -> allowInternalTieBreakers
                    ? "weekly_chart_peak " + dir + " NULLS LAST, weekly_chart_peak_start_date DESC NULLS LAST"
                    : "weekly_chart_peak " + dir + " NULLS LAST";
            case "weekly_chart_weeks" -> "weekly_chart_weeks " + dir;
            case "weekly_chart_peak_weeks" -> "weekly_chart_peak_weeks " + dir + " NULLS LAST";
            case "vatos_cuntdown_days" -> "vatos_cuntdown_days " + dir;
            case "vatos_cuntdown_days_at_peak" -> "vatos_cuntdown_days_at_peak " + dir + " NULLS LAST";
                case "vatos_cuntdown_peak" -> allowInternalTieBreakers
                    ? "vatos_cuntdown_peak " + dir + " NULLS LAST, vatos_cuntdown_days DESC NULLS LAST"
                    : "vatos_cuntdown_peak " + dir + " NULLS LAST";
                case "yearly_chart_peak" -> allowInternalTieBreakers
                    ? "yearly_chart_peak " + dir + " NULLS LAST, yearly_chart_peak_period DESC NULLS LAST"
                    : "yearly_chart_peak " + dir + " NULLS LAST";
            default -> "s.name " + dir;
        };

        clauses.add(clause);
        appliedSorts.add(sortBy);
    }
    
    public long countSongsWithFilters(String name, List<Integer> artistName, String albumName,
                                      List<Integer> genreIds, String genreMode,
                                      List<Integer> subgenreIds, String subgenreMode,
                                      List<Integer> languageIds, String languageMode,
                                      List<Integer> genderIds, String genderMode,
                                      List<Integer> ethnicityIds, String ethnicityMode,
                                      List<String> countries, String countryMode,
                                      List<Integer> tagIds, String tagMode,
                                      List<String> accounts, String accountMode,
                                      String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                      String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                      String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                      String listenedDateFrom, String listenedDateTo,
                                      String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                                      String itunesIdsJson, String inItunes,
                                      Integer ageMin, Integer ageMax, String ageMode,
                                      Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                      String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                      String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                      Integer playCountMin, Integer playCountMax,
                                      Integer trackNumber, String trackNumberMode,
                                      Integer lengthMin, Integer lengthMax, String lengthMode,
                                      Integer weeklyChartPeak, String weeklyChartPeakMode, Integer weeklyChartWeeks,
                                      Integer weeklyChartPeakWeeks, String weeklyChartPeakWeeksMode,
                                      String weeklyChartDateFrom, String weeklyChartDateTo, String weeklyChartSeason,
                                      Integer trlPeak, String trlPeakMode, Integer trlDays,
                                      Integer trlDaysAtPeak, String trlDaysAtPeakMode,
                                      String trlDateFrom, String trlDateTo,
                                      Integer vatosCuntdownPeak, String vatosCuntdownPeakMode, Integer vatosCuntdownDays,
                                      Integer vatosCuntdownDaysAtPeak, String vatosCuntdownDaysAtPeakMode,
                                      String vatosCuntdownDateFrom, String vatosCuntdownDateTo,
                                      Integer billboardPeak, String billboardPeakMode, Integer billboardWeeks,
                                      Integer billboardWeeksAtPeak, String billboardWeeksAtPeakMode,
                                      String billboardDateFrom, String billboardDateTo,
                                      Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                      String seasonalChartDateFrom, String seasonalChartDateTo, String seasonalChartSeason,
                                      Integer yearlyChartPeak, Integer yearlyChartYears,
                                      String yearlyChartDateFrom, String yearlyChartDateTo) {
        // Build account filter subquery for play_stats if we need play count filter
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
                    SELECT p.song_id, COUNT(*) as play_count
                    FROM Play p
                    WHERE 1=1 """);
            sql.append(accountFilterClause);
            sql.append(listenedDateFilterClause);
            sql.append("""
                    GROUP BY p.song_id
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
        
        // Artist filter
        if (artistName != null && !artistName.isEmpty()) {
            String artistPlaceholders = String.join(",", artistName.stream().map(id -> "?").toList());
            sql.append(" AND ar.id IN (").append(artistPlaceholders).append(")");
            params.addAll(artistName);
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
                    sql.append(" AND (COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) IS NULL)");
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
                    sql.append(" AND (COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) IS NULL)");
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
                    sql.append(" AND (COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) IS NULL)");
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
                sql.append(" AND EXISTS (SELECT 1 FROM Play p WHERE p.song_id = s.id AND p.account IN (");
                for (int i = 0; i < accounts.size(); i++) {
                    if (i > 0) sql.append(",");
                    sql.append("?");
                    params.add(accounts.get(i));
                }
                sql.append("))");
            } else if ("excludes".equalsIgnoreCase(accountMode)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM Play p WHERE p.song_id = s.id AND p.account IN (");
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
                case "isnull" -> sql.append(" AND COALESCE(s.release_date, alb.release_date) IS NULL");
                case "isnotnull" -> sql.append(" AND COALESCE(s.release_date, alb.release_date) IS NOT NULL");
            }
        }

        // Tag filter
        SqlFilterHelper.appendTagFilter(sql, params, "s.id", "SongTag", "song_id", tagIds, tagMode);
        
        // First listened date filter
        if (firstListenedDate != null && !firstListenedDate.trim().isEmpty() && firstListenedDateMode != null) {
            switch (firstListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) = DATE(?)");
                    params.add(firstListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?)");
                    params.add(firstListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
                    params.add(firstListenedDate);
                }
            }
        }
        
        // First listened between filter
        if ("between".equals(firstListenedDateMode) && firstListenedDateFrom != null && !firstListenedDateFrom.trim().isEmpty()
                && firstListenedDateTo != null && !firstListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MIN(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
            params.add(firstListenedDateFrom);
            params.add(firstListenedDateTo);
        }
        
        // Last listened date filter
        if (lastListenedDate != null && !lastListenedDate.trim().isEmpty() && lastListenedDateMode != null) {
            switch (lastListenedDateMode) {
                case "exact" -> {
                    sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) = DATE(?)");
                    params.add(lastListenedDate);
                }
                case "gte" -> {
                    sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?)");
                    params.add(lastListenedDate);
                }
                case "lte" -> {
                    sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
                    params.add(lastListenedDate);
                }
            }
        }
        
        // Last listened between filter
        if ("between".equals(lastListenedDateMode) && lastListenedDateFrom != null && !lastListenedDateFrom.trim().isEmpty()
                && lastListenedDateTo != null && !lastListenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) >= DATE(?) AND DATE((SELECT MAX(play_date) FROM Play WHERE song_id = s.id)) <= DATE(?)");
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
        
        // iTunes filter (pre-computed ID set via json_each)
        SqlFilterHelper.appendItunesIdFilter(sql, params, "s.id", itunesIdsJson, inItunes);
        
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

        // Track number filter
        if (trackNumberMode != null && !trackNumberMode.isEmpty()) {
            if ("isnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NULL ");
            } else if ("isnotnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NOT NULL ");
            } else if (trackNumber != null) {
                sql.append(" AND s.track_number = ? ");
                params.add(trackNumber);
            }
        } else if (trackNumber != null) {
            sql.append(" AND s.track_number = ? ");
            params.add(trackNumber);
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
        
        SqlFilterHelper.appendChartStatsFilter(sql, params, "ce.song_id", "s.id", "song", "weekly", "weeks",
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                weeklyChartPeakWeeks, weeklyChartPeakWeeksMode);

        appendSongTrlFilter(sql, params, trlPeak, trlPeakMode, trlDays, trlDaysAtPeak, trlDaysAtPeakMode, trlDateFrom, trlDateTo);
        appendSongVatosCuntdownFilter(sql, params, vatosCuntdownPeak, vatosCuntdownPeakMode, vatosCuntdownDays, vatosCuntdownDaysAtPeak, vatosCuntdownDaysAtPeakMode, vatosCuntdownDateFrom, vatosCuntdownDateTo);
        appendSongBillboardFilter(sql, params, billboardPeak, billboardPeakMode, billboardWeeks, billboardWeeksAtPeak, billboardWeeksAtPeakMode, billboardDateFrom, billboardDateTo);
        
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
    public Map<Integer, Long> countSongsByGenderWithFilters(String name, List<Integer> artistName, String albumName,
                                              List<Integer> genreIds, String genreMode,
                                              List<Integer> subgenreIds, String subgenreMode,
                                              List<Integer> languageIds, String languageMode,
                                              List<Integer> genderIds, String genderMode,
                                              List<Integer> ethnicityIds, String ethnicityMode,
                                              List<String> countries, String countryMode,
                                              List<Integer> tagIds, String tagMode,
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
                                              String itunesIdsJson, String inItunes,
                                              Integer playCountMin, Integer playCountMax,
                                              Integer trackNumber, String trackNumberMode,
                                              Integer lengthMin, Integer lengthMax, String lengthMode,
                                              Integer weeklyChartPeak, String weeklyChartPeakMode, Integer weeklyChartWeeks,
                                              Integer weeklyChartPeakWeeks, String weeklyChartPeakWeeksMode,
                                              String weeklyChartDateFrom, String weeklyChartDateTo, String weeklyChartSeason,
                                              Integer trlPeak, String trlPeakMode, Integer trlDays,
                                              Integer trlDaysAtPeak, String trlDaysAtPeakMode,
                                              String trlDateFrom, String trlDateTo,
                                              Integer vatosCuntdownPeak, String vatosCuntdownPeakMode, Integer vatosCuntdownDays,
                                              Integer vatosCuntdownDaysAtPeak, String vatosCuntdownDaysAtPeakMode,
                                              String vatosCuntdownDateFrom, String vatosCuntdownDateTo,
                                              Integer billboardPeak, String billboardPeakMode, Integer billboardWeeks,
                                              Integer billboardWeeksAtPeak, String billboardWeeksAtPeakMode,
                                              String billboardDateFrom, String billboardDateTo,
                                              Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                              String seasonalChartDateFrom, String seasonalChartDateTo, String seasonalChartSeason,
                                              Integer yearlyChartPeak, Integer yearlyChartYears,
                                              String yearlyChartDateFrom, String yearlyChartDateTo) {
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
                sql.append("LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Play p WHERE 1=1 ");
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append(" GROUP BY song_id) play_stats ON play_stats.song_id = s.id ");
            }
            
            sql.append("INNER JOIN Play p ON p.song_id = s.id " +
                "WHERE p.account IN (");
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
                sql.append("LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Play p WHERE 1=1 ");
                sql.append(accountFilterClause);
                sql.append(listenedDateFilterClause);
                sql.append(" GROUP BY song_id) play_stats ON play_stats.song_id = s.id ");
            }
            
            sql.append("WHERE NOT EXISTS (" +
                "SELECT 1 FROM Play p WHERE p.song_id = s.id AND p.account IN (");
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
                sql.append("LEFT JOIN (SELECT song_id, COUNT(*) as play_count FROM Play p WHERE 1=1 ");
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
        
        // Artist filter
        if (artistName != null && !artistName.isEmpty()) {
            String artistPlaceholders = String.join(",", artistName.stream().map(id -> "?").toList());
            sql.append(" AND ar.id IN (").append(artistPlaceholders).append(")");
            params.addAll(artistName);
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

        // Tag filter
        library.util.SqlFilterHelper.appendTagFilter(sql, params, "s.id", "SongTag", "song_id", tagIds, tagMode);
        
        // Release date filter
        library.util.SqlFilterHelper.appendDateFilter(sql, params, "COALESCE(s.release_date, al.release_date)", releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode);
        
        // First listened date filter
        String firstListenedSubquery = "(SELECT MIN(p.play_date) FROM Play p WHERE p.song_id = s.id)";
        library.util.SqlFilterHelper.appendDateFilter(sql, params, firstListenedSubquery, firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode);
        
        // Last listened date filter
        String lastListenedSubquery = "(SELECT MAX(p.play_date) FROM Play p WHERE p.song_id = s.id)";
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
        
        // Image count filter
        if (imageCountMin != null) {
            sql.append(" AND ((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id)) >= ?");
            params.add(imageCountMin);
        }
        if (imageCountMax != null) {
            sql.append(" AND ((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id)) <= ?");
            params.add(imageCountMax);
        }
        
        // Has Featured Artists filter
        if (hasFeaturedArtists != null && !hasFeaturedArtists.isEmpty()) {
            if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id)");
            } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id)");
            }
        }
        
        // Age filter (artist's current age, or age at death if deceased)
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
        
        // Age at Release filter (artist's age when song was released)
        if (ageAtReleaseMin != null || ageAtReleaseMax != null) {
            String ageAtReleaseExpr = "CAST((julianday(COALESCE(s.release_date, al.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, al.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ?");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND COALESCE(s.release_date, al.release_date) IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ?");
                params.add(ageAtReleaseMax);
            }
        }
        
        // iTunes filter (pre-computed ID set via json_each)
        SqlFilterHelper.appendItunesIdFilter(sql, params, "s.id", itunesIdsJson, inItunes);
        
        // Play count filter
        if (playCountMin != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) >= ?");
            params.add(playCountMin);
        }
        if (playCountMax != null) {
            sql.append(" AND COALESCE(play_stats.play_count, 0) <= ?");
            params.add(playCountMax);
        }

        // Track number filter
        if (trackNumberMode != null && !trackNumberMode.isEmpty()) {
            if ("isnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NULL");
            } else if ("isnotnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NOT NULL");
            } else if (trackNumber != null) {
                sql.append(" AND s.track_number = ?");
                params.add(trackNumber);
            }
        } else if (trackNumber != null) {
            sql.append(" AND s.track_number = ?");
            params.add(trackNumber);
        }
        
        // Length filter (song length_seconds)
        if (lengthMode != null && !lengthMode.isEmpty()) {
            if ("null".equalsIgnoreCase(lengthMode) || "zero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (s.length_seconds IS NULL OR s.length_seconds = 0)");
            } else if ("notnull".equalsIgnoreCase(lengthMode) || "nonzero".equalsIgnoreCase(lengthMode)) {
                sql.append(" AND (s.length_seconds IS NOT NULL AND s.length_seconds > 0)");
            } else if ("lt".equalsIgnoreCase(lengthMode) && lengthMax != null) {
                sql.append(" AND s.length_seconds < ?");
                params.add(lengthMax);
            } else if ("gt".equalsIgnoreCase(lengthMode) && lengthMin != null) {
                sql.append(" AND s.length_seconds > ?");
                params.add(lengthMin);
            } else {
                if (lengthMin != null) {
                    sql.append(" AND s.length_seconds >= ?");
                    params.add(lengthMin);
                }
                if (lengthMax != null) {
                    sql.append(" AND s.length_seconds <= ?");
                    params.add(lengthMax);
                }
            }
        }
        
        SqlFilterHelper.appendChartStatsFilter(sql, params, "ce.song_id", "s.id", "song", "weekly", "weeks",
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                weeklyChartPeakWeeks, weeklyChartPeakWeeksMode);

        appendSongTrlFilter(sql, params, trlPeak, trlPeakMode, trlDays, trlDaysAtPeak, trlDaysAtPeakMode, trlDateFrom, trlDateTo);
        appendSongVatosCuntdownFilter(sql, params, vatosCuntdownPeak, vatosCuntdownPeakMode, vatosCuntdownDays, vatosCuntdownDaysAtPeak, vatosCuntdownDaysAtPeakMode, vatosCuntdownDateFrom, vatosCuntdownDateTo);
        appendSongBillboardFilter(sql, params, billboardPeak, billboardPeakMode, billboardWeeks, billboardWeeksAtPeak, billboardWeeksAtPeakMode, billboardDateFrom, billboardDateTo);

        SqlFilterHelper.appendChartStatsFilter(sql, params, "ce.song_id", "s.id", "song", "seasonal", "seasons",
                seasonalChartPeak, seasonalChartSeasons, seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason);

        SqlFilterHelper.appendChartStatsFilter(sql, params, "ce.song_id", "s.id", "song", "yearly", "years",
                yearlyChartPeak, yearlyChartYears, yearlyChartDateFrom, yearlyChartDateTo, null);
        
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

        // Determine if Play join is needed for non-play queries
        boolean needsPlayJoin = (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty()) || 
                                     (listenedDateTo != null && !listenedDateTo.trim().isEmpty());

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        // Get plays by gender
        data.put("playsByGender", getPlaysByGenderFiltered(filterClause.toString(), params, null));

        // Get songs by gender
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params, needsPlayJoin, null));

        // Get artists by gender (from filtered songs)
        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params, needsPlayJoin, null));

        // Get albums by gender (from filtered songs)
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params, needsPlayJoin, null));

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

        // Determine if Play join is needed for non-play queries
        boolean needsPlayJoin = needsPlayJoin(filter);

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        // Get plays by gender
        data.put("playsByGender", getPlaysByGenderFiltered(filterClause.toString(), params, null));

        // Get songs by gender
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params, needsPlayJoin, null));

        // Get artists by gender (from filtered songs)
        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params, needsPlayJoin, null));

        // Get albums by gender (from filtered songs)
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params, needsPlayJoin, null));

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
                    sql.append(" AND (COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) IS NULL)");
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
                    sql.append(" AND (COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id) IS NULL)");
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
                    sql.append(" AND (COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) NOT IN (").append(placeholders).append(") OR COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) IS NULL)");
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
        
        // Listened date filter (play_date range)
        if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty() && 
            listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(p.play_date) >= DATE(?) AND DATE(p.play_date) <= DATE(?)");
            params.add(listenedDateFrom);
            params.add(listenedDateTo);
        } else if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty()) {
            sql.append(" AND DATE(p.play_date) >= DATE(?)");
            params.add(listenedDateFrom);
        } else if (listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(p.play_date) <= DATE(?)");
            params.add(listenedDateTo);
        }
    }
    
    /**
     * Builds an early filter clause for play queries.
     * This filters on p.song_id before expensive joins, dramatically improving performance
     * when filtering by artist, album, or song IDs.
     */
    private void buildplayEarlyFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        // Artist ID filter - filter plays to only songs by these artists
        // When includeGroups/includeFeatured is set, skip this early filter and let buildFilterClause handle it
        // (buildFilterClause will add the expanded artist filter with proper OR conditions)
        java.util.List<Integer> artistIds = filter.getArtistIds();
        boolean includeGroups = filter.isIncludeGroups();
        boolean includeFeatured = filter.isIncludeFeatured();
        
        if (artistIds != null && !artistIds.isEmpty() && !includeGroups && !includeFeatured) {
            // Simple filter - only main artist songs (no expansion needed)
            String placeholders = String.join(",", artistIds.stream().map(id -> "?").toList());
            sql.append(" AND p.song_id IN (SELECT id FROM Song WHERE artist_id IN (").append(placeholders).append("))");
            params.addAll(artistIds);
        }
        // When includeGroups or includeFeatured is set, don't add early filter - buildFilterClause will handle it
        
        // Album ID filter - filter plays to only songs from these albums
        java.util.List<Integer> albumIds = filter.getAlbumIds();
        if (albumIds != null && !albumIds.isEmpty()) {
            String placeholders = String.join(",", albumIds.stream().map(id -> "?").toList());
            sql.append(" AND p.song_id IN (SELECT id FROM Song WHERE album_id IN (").append(placeholders).append("))");
            params.addAll(albumIds);
        }
        
        // Song ID filter - filter plays to these specific songs
        java.util.List<Integer> songIds = filter.getSongIds();
        if (songIds != null && !songIds.isEmpty()) {
            String placeholders = String.join(",", songIds.stream().map(id -> "?").toList());
            sql.append(" AND p.song_id IN (").append(placeholders).append(")");
            params.addAll(songIds);
        }
    }
    
    /**
     * Overloaded buildFilterClause that uses ChartFilterDTO.
     * Delegates to the existing method for basic filters and adds new entity-aware filters.
     */
    private void buildFilterClause(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        String catalogType = normalizeCatalogType(filter.getCatalogType());
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
            null, artistIdsForBasicFilter, filter.getAlbumIds(), filter.getSongIds(),
            filter.getGenreIds(), filter.getGenreMode(),
            filter.getSubgenreIds(), filter.getSubgenreMode(),
            filter.getLanguageIds(), filter.getLanguageMode(),
            filter.getGenderIds(), filter.getGenderMode(),
            filter.getEthnicityIds(), filter.getEthnicityMode(),
            filter.getCountries(), filter.getCountryMode(),
            null, null, null, null, // release date handled with entity awareness below
            filter.getListenedDateFrom(), filter.getListenedDateTo());

        appendCatalogSearchFilter(sql, params, filter.getName(), catalogType);
        
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
                sql.append(" AND p.account IN (").append(placeholders).append(")");
                params.addAll(accounts);
            } else if ("excludes".equals(accountMode)) {
                sql.append(" AND p.account NOT IN (").append(placeholders).append(")");
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
            appendHasFeaturedArtistsFilter(sql, hasFeaturedArtists, catalogType);
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

        appendCatalogSpecificFilters(sql, params, filter, catalogType);
        
        // Note: inItunes filter is NOT handled here - it's done in memory via ItunesService
        // because iTunes status is determined by checking against an in-memory map from iTunes XML.
        appendItunesPresenceFilter(sql, params, filter, catalogType);
        
        // Handle entity-aware filters: First/Last Listened Date, Play Count
        // These use subqueries to aggregate play data by the selected entity type
        
        // First Listened Date filter
        String firstListenedEntity = filter.getFirstListenedDateEntity();
        if (firstListenedEntity == null || firstListenedEntity.isEmpty()) firstListenedEntity = catalogType;
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
        if (lastListenedEntity == null || lastListenedEntity.isEmpty()) lastListenedEntity = catalogType;
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
        if (playCountEntity == null || playCountEntity.isEmpty()) playCountEntity = catalogType;
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
        
        // Release Date filter (song release date, falling back to album release date)
        String releaseDateMode = filter.getReleaseDateMode();
        
        if (releaseDateMode != null) {
            String dateExpr = buildReleaseDateExpression(catalogType);
            
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
            if ("isnull".equals(releaseDateMode)) {
                sql.append(" AND ").append(dateExpr).append(" IS NULL");
            } else if ("isnotnull".equals(releaseDateMode)) {
                sql.append(" AND ").append(dateExpr).append(" IS NOT NULL");
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

        appendSongTrlFilter(sql, params, filter.getSongsTrlPeak(), filter.getSongsTrlDays());
        appendSongVatosCuntdownFilter(sql, params, filter.getSongsVatosCuntdownPeak(), filter.getSongsVatosCuntdownDays());
        appendSongBillboardFilter(sql, params, filter.getSongsBillboardPeak(), filter.getSongsBillboardWeeks());
        
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
            String ageAtReleaseExpr = "CAST((julianday(" + buildReleaseDateExpression(catalogType) + ") - julianday(ar.birth_date)) / 365.25 AS INTEGER)";
            if (ageAtReleaseMin != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(buildReleaseDateExpression(catalogType)).append(" IS NOT NULL AND ").append(ageAtReleaseExpr).append(" >= ?");
                params.add(ageAtReleaseMin);
            }
            if (ageAtReleaseMax != null) {
                sql.append(" AND ar.birth_date IS NOT NULL AND ").append(buildReleaseDateExpression(catalogType)).append(" IS NOT NULL AND ").append(ageAtReleaseExpr).append(" <= ?");
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

    private String normalizeCatalogType(String catalogType) {
        if (catalogType == null || catalogType.isBlank()) {
            return "song";
        }

        return switch (catalogType.trim().toLowerCase()) {
            case "artists", "artist" -> "artist";
            case "albums", "album" -> "album";
            default -> "song";
        };
    }

    private String buildReleaseDateExpression(String catalogType) {
        return "album".equals(catalogType) ? "alb.release_date" : "COALESCE(s.release_date, alb.release_date)";
    }

    private void appendCatalogSearchFilter(StringBuilder sql, java.util.List<Object> params, String name, String catalogType) {
        if (name == null || name.trim().isEmpty()) {
            return;
        }

        String column = switch (catalogType) {
            case "artist" -> library.util.StringNormalizer.sqlNormalizeColumn("ar.name");
            case "album" -> library.util.StringNormalizer.sqlNormalizeColumn("alb.name");
            default -> library.util.StringNormalizer.sqlNormalizeColumn("s.name");
        };

        sql.append(" AND ").append(column).append(" LIKE ?");
        params.add("%" + library.util.StringNormalizer.normalizeForSearch(name) + "%");
    }

    private void appendHasFeaturedArtistsFilter(StringBuilder sql, String hasFeaturedArtists, String catalogType) {
        String existsClause = switch (catalogType) {
            case "artist" -> "EXISTS (SELECT 1 FROM SongFeaturedArtist sfa JOIN Song sf ON sfa.song_id = sf.id WHERE sf.artist_id = ar.id)";
            case "album" -> "EXISTS (SELECT 1 FROM SongFeaturedArtist sfa JOIN Song sf ON sfa.song_id = sf.id WHERE sf.album_id = alb.id)";
            default -> "EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s.id)";
        };

        if ("true".equalsIgnoreCase(hasFeaturedArtists)) {
            sql.append(" AND ").append(existsClause);
        } else if ("false".equalsIgnoreCase(hasFeaturedArtists)) {
            sql.append(" AND NOT ").append(existsClause);
        }
    }

    private void appendCatalogSpecificFilters(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter, String catalogType) {
        appendOrganizedFilter(sql, filter.getOrganized(), catalogType);
        appendImageCountFilter(sql, params, filter, catalogType);

        if ("artist".equals(catalogType)) {
            appendImageThemeFilter(sql, params, filter);
            appendArtistCountFilters(sql, params, filter);
            return;
        }

        if ("album".equals(catalogType)) {
            appendAlbumCountFilters(sql, params, filter);
            appendLengthFilter(sql, params, filter, false);
            return;
        }

        appendSongAlbumNameFilter(sql, params, filter.getAlbumName());
        appendTrackNumberFilter(sql, params, filter);
        appendLengthFilter(sql, params, filter, true);
    }

    private void appendSongAlbumNameFilter(StringBuilder sql, java.util.List<Object> params, String albumName) {
        if (albumName == null || albumName.trim().isEmpty()) {
            return;
        }

        String normalizedAlbum = library.util.StringNormalizer.normalizeForSearch(albumName);
        sql.append(" AND ")
                .append(library.util.StringNormalizer.sqlNormalizeColumn("alb.name"))
                .append(" LIKE ?");
        params.add("%" + normalizedAlbum + "%");
    }

    private void appendOrganizedFilter(StringBuilder sql, String organized, String catalogType) {
        if (organized == null || organized.isBlank()) {
            return;
        }

        String column = switch (catalogType) {
            case "artist" -> "ar.organized";
            case "album" -> "alb.organized";
            default -> "s.organized";
        };

        if ("true".equalsIgnoreCase(organized)) {
            sql.append(" AND ").append(column).append(" = 1");
        } else if ("false".equalsIgnoreCase(organized)) {
            sql.append(" AND (").append(column).append(" = 0 OR ").append(column).append(" IS NULL)");
        }
    }

    private void appendImageCountFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter, String catalogType) {
        String expr = switch (catalogType) {
            case "artist" -> "((CASE WHEN ar.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM ArtistImage WHERE artist_id = ar.id))";
            case "album" -> "((CASE WHEN alb.image IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM AlbumImage WHERE album_id = alb.id))";
            default -> "((CASE WHEN s.single_cover IS NOT NULL THEN 1 ELSE 0 END) + (SELECT COUNT(*) FROM SongImage WHERE song_id = s.id))";
        };

        if (filter.getImageCountMin() != null) {
            sql.append(" AND ").append(expr).append(" >= ?");
            params.add(filter.getImageCountMin());
        }
        if (filter.getImageCountMax() != null) {
            sql.append(" AND ").append(expr).append(" <= ?");
            params.add(filter.getImageCountMax());
        }
    }

    private void appendImageThemeFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        Integer imageTheme = filter.getImageTheme();
        String imageThemeMode = filter.getImageThemeMode();
        if (imageTheme == null) {
            return;
        }

        if ("has".equalsIgnoreCase(imageThemeMode)) {
            sql.append(" AND EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = ar.id AND theme_id = ?)");
            params.add(imageTheme);
        } else if ("doesntHave".equalsIgnoreCase(imageThemeMode)) {
            sql.append(" AND NOT EXISTS (SELECT 1 FROM ArtistImageTheme WHERE artist_id = ar.id AND theme_id = ?)");
            params.add(imageTheme);
        }
    }

    private void appendArtistCountFilters(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        if (filter.getAlbumCountMin() != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Album alb_count WHERE alb_count.artist_id = ar.id) >= ?");
            params.add(filter.getAlbumCountMin());
        }
        if (filter.getAlbumCountMax() != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Album alb_count WHERE alb_count.artist_id = ar.id) <= ?");
            params.add(filter.getAlbumCountMax());
        }
        if (filter.getSongCountMin() != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song song_count WHERE song_count.artist_id = ar.id) >= ?");
            params.add(filter.getSongCountMin());
        }
        if (filter.getSongCountMax() != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song song_count WHERE song_count.artist_id = ar.id) <= ?");
            params.add(filter.getSongCountMax());
        }
    }

    private void appendAlbumCountFilters(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        if (filter.getSongCountMin() != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song song_count WHERE song_count.album_id = alb.id) >= ?");
            params.add(filter.getSongCountMin());
        }
        if (filter.getSongCountMax() != null) {
            sql.append(" AND (SELECT COUNT(*) FROM Song song_count WHERE song_count.album_id = alb.id) <= ?");
            params.add(filter.getSongCountMax());
        }
    }

    private void appendTrackNumberFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        String trackNumberMode = filter.getTrackNumberMode();
        Integer trackNumber = filter.getTrackNumber();
        if (trackNumberMode != null && !trackNumberMode.isEmpty()) {
            if ("isnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NULL");
            } else if ("isnotnull".equalsIgnoreCase(trackNumberMode)) {
                sql.append(" AND s.track_number IS NOT NULL");
            } else if (trackNumber != null) {
                sql.append(" AND s.track_number = ?");
                params.add(trackNumber);
            }
        } else if (trackNumber != null) {
            sql.append(" AND s.track_number = ?");
            params.add(trackNumber);
        }
    }

    private void appendLengthFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter, boolean songCatalog) {
        String lengthMode = filter.getLengthMode();
        Integer lengthMin = filter.getLengthMin();
        Integer lengthMax = filter.getLengthMax();
        if (lengthMode == null || lengthMode.isEmpty()) {
            if (lengthMin != null) {
                sql.append(" AND ").append(songCatalog ? "COALESCE(s.length_seconds, 0)" : "COALESCE((SELECT SUM(song_len.length_seconds) FROM Song song_len WHERE song_len.album_id = alb.id), 0)").append(" >= ?");
                params.add(lengthMin);
            }
            if (lengthMax != null) {
                sql.append(" AND ").append(songCatalog ? "COALESCE(s.length_seconds, 0)" : "COALESCE((SELECT SUM(song_len.length_seconds) FROM Song song_len WHERE song_len.album_id = alb.id), 0)").append(" <= ?");
                params.add(lengthMax);
            }
            return;
        }

        String expr = songCatalog
                ? "COALESCE(s.length_seconds, 0)"
                : "COALESCE((SELECT SUM(song_len.length_seconds) FROM Song song_len WHERE song_len.album_id = alb.id), 0)";

        if ("null".equalsIgnoreCase(lengthMode) || "zero".equalsIgnoreCase(lengthMode)) {
            sql.append(" AND ").append(expr).append(" = 0");
        } else if ("notnull".equalsIgnoreCase(lengthMode) || "nonzero".equalsIgnoreCase(lengthMode)) {
            sql.append(" AND ").append(expr).append(" > 0");
        } else if ("lt".equalsIgnoreCase(lengthMode) && lengthMax != null) {
            sql.append(" AND ").append(expr).append(" < ?");
            params.add(lengthMax);
        } else if ("gt".equalsIgnoreCase(lengthMode) && lengthMin != null) {
            sql.append(" AND ").append(expr).append(" > ?");
            params.add(lengthMin);
        } else {
            if (lengthMin != null) {
                sql.append(" AND ").append(expr).append(" >= ?");
                params.add(lengthMin);
            }
            if (lengthMax != null) {
                sql.append(" AND ").append(expr).append(" <= ?");
                params.add(lengthMax);
            }
        }
    }

    private void appendItunesPresenceFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter, String catalogType) {
        if ((filter.getItunesPresenceMin() == null && filter.getItunesPresenceMax() == null) || filter.getItunesSongIdsJson() == null || filter.getItunesSongIdsJson().isBlank()) {
            return;
        }

        String expr = switch (catalogType) {
            case "artist" -> "CAST(COALESCE((SELECT COUNT(*) FROM Song it_song WHERE it_song.artist_id = ar.id AND it_song.id IN (SELECT value FROM json_each(?))), 0) AS REAL) * 100.0 / NULLIF(COALESCE((SELECT COUNT(*) FROM Song song_total WHERE song_total.artist_id = ar.id), 0), 0)";
            case "album" -> "CAST(COALESCE((SELECT COUNT(*) FROM Song it_song WHERE it_song.album_id = alb.id AND it_song.id IN (SELECT value FROM json_each(?))), 0) AS REAL) * 100.0 / NULLIF(COALESCE((SELECT COUNT(*) FROM Song song_total WHERE song_total.album_id = alb.id), 0), 0)";
            default -> null;
        };

        if (expr == null) {
            return;
        }

        if (filter.getItunesPresenceMin() != null) {
            sql.append(" AND ").append(expr).append(" >= ?");
            params.add(filter.getItunesSongIdsJson());
            params.add(filter.getItunesPresenceMin());
        }
        if (filter.getItunesPresenceMax() != null) {
            sql.append(" AND ").append(expr).append(" <= ?");
            params.add(filter.getItunesSongIdsJson());
            params.add(filter.getItunesPresenceMax());
        }
    }

    /**
     * Builds a subquery expression for MIN(play_date) based on entity type.
     */
    private String buildEntityMinDateSubquery(String entity, String scrAlias) {
        return switch (entity) {
            case "artist" -> "(SELECT MIN(p2.play_date) FROM Play p2 " +
                             "JOIN Song s2 ON p2.song_id = s2.id " +
                             "WHERE s2.artist_id = ar.id)";
            case "album" -> "(SELECT MIN(p2.play_date) FROM Play p2 " +
                            "JOIN Song s2 ON p2.song_id = s2.id " +
                            "WHERE s2.album_id = alb.id)";
            default -> "(SELECT MIN(p2.play_date) FROM Play p2 WHERE p2.song_id = s.id)"; // song level
        };
    }
    
    /**
     * Builds a subquery expression for MAX(play_date) based on entity type.
     */
    private String buildEntityMaxDateSubquery(String entity, String scrAlias) {
        return switch (entity) {
            case "artist" -> "(SELECT MAX(p2.play_date) FROM Play p2 " +
                             "JOIN Song s2 ON p2.song_id = s2.id " +
                             "WHERE s2.artist_id = ar.id)";
            case "album" -> "(SELECT MAX(p2.play_date) FROM Play p2 " +
                            "JOIN Song s2 ON p2.song_id = s2.id " +
                            "WHERE s2.album_id = alb.id)";
            default -> "(SELECT MAX(p2.play_date) FROM Play p2 WHERE p2.song_id = s.id)"; // song level
        };
    }
    
    /**
     * Builds a subquery expression for COUNT(*) based on entity type.
     */
    private String buildEntityPlayCountSubquery(String entity, String scrAlias) {
        return switch (entity) {
            case "artist" -> "(SELECT COUNT(*) FROM Play p2 " +
                             "JOIN Song s2 ON p2.song_id = s2.id " +
                             "WHERE s2.artist_id = ar.id)";
            case "album" -> "(SELECT COUNT(*) FROM Play p2 " +
                            "JOIN Song s2 ON p2.song_id = s2.id " +
                            "WHERE s2.album_id = alb.id)";
            default -> "(SELECT COUNT(*) FROM Play p2 WHERE p2.song_id = s.id)"; // song level
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
     * Check if Play join is needed (for listened date, account, or entity-aware filters)
     */
    private boolean needsPlayJoin(ChartFilterDTO filter) {
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
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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

        // Fallback: all plays within the filtered set
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(*) as play_count
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
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
    
    private java.util.Map<String, Long> getSongsByGenderFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
        // Optimize for no filters - use direct Song table count
        if (filterClause.trim().isEmpty() && !needsPlayJoin && limit == null) {
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
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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

        String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n            " : "";
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT s.id) as song_count
            FROM Song s
            """ + playJoin + """
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
    
    private java.util.Map<String, Long> getArtistsByGenderFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
        // Optimize for no filters - use direct Artist table instead of going through Song
        if (filterClause.trim().isEmpty() && !needsPlayJoin && limit == null) {
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
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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

        String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n            " : "";
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT ar.id) as artist_count
            FROM Song s
            """ + playJoin + """
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
    
    private java.util.Map<String, Long> getAlbumsByGenderFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
        // Optimize for no filters - use direct Album table count
        if (filterClause.trim().isEmpty() && !needsPlayJoin && limit == null) {
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
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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

        String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n            " : "";
        String sql = """
            SELECT 
                CASE 
                    WHEN g.name LIKE '%Female%' THEN 'female'
                    WHEN g.name LIKE '%Male%' THEN 'male'
                    ELSE 'other'
                END as gender,
                COUNT(DISTINCT alb.id) as album_count
            FROM Song s
            """ + playJoin + """
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
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
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
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
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
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
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
                strftime('%Y', p.play_date) as year,
                SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
            WHERE p.play_date IS NOT NULL """ + " " + filterClause + """
            GROUP BY strftime('%Y', p.play_date)
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
        
        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
        combinedParams.addAll(params);

        boolean playJoinNeeded = needsPlayJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        String limitEntity = filter.getLimitEntity();
        if (limitEntity == null || limitEntity.isBlank()) {
            limitEntity = "song";
        }

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        data.put("artistsByGender", getArtistsByGenderFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("albumsByGender", getAlbumsByGenderFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("songsByGender", getSongsByGenderFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        // General tab limit should apply consistently across all pies, based on the requested entity.
        // Plays/Listening Time are play-derived metrics, so their top-N should be computed by entity.
        // Use combined filter with early play filter for performance
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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

        // Fallback: all plays within the filtered set
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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

        // Fallback: all plays within the filtered set
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
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
        
        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
        combinedParams.addAll(params);
        
        boolean playJoinNeeded = needsPlayJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByGenre", getArtistsByGenreFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("albumsByGenre", getAlbumsByGenreFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("songsByGenre", getSongsByGenreFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("playsByGenre", getPlaysByGenreFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByGenre", getListeningTimeByGenreFiltered(combinedFilter, combinedParams, limit));

        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByGenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(gr.name, 'Unknown') as genre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.genre_id, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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

    private java.util.List<java.util.Map<String, Object>> getAlbumsByGenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
    
    private java.util.List<java.util.Map<String, Object>> getSongsByGenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) = gr.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN Genre gr ON COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) = gr.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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

        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
        combinedParams.addAll(params);

        boolean playJoinNeeded = needsPlayJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        data.put("artistsBySubgenre", getArtistsBySubgenreFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("albumsBySubgenre", getAlbumsBySubgenreFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("songsBySubgenre", getSongsBySubgenreFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("playsBySubgenre", getPlaysBySubgenreFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeBySubgenre", getListeningTimeBySubgenreFiltered(combinedFilter, combinedParams, limit));

        return data;
    }

    private java.util.List<java.util.Map<String, Object>> getArtistsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(sg.name, 'Unknown') as subgenre_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.subgenre_id, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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

    private java.util.List<java.util.Map<String, Object>> getAlbumsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
    
    private java.util.List<java.util.Map<String, Object>> getSongsBySubgenreFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN SubGenre sg ON COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) = sg.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN SubGenre sg ON COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) = sg.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
        
        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
        combinedParams.addAll(params);
        
        boolean playJoinNeeded = needsPlayJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByEthnicity", getArtistsByEthnicityFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("albumsByEthnicity", getAlbumsByEthnicityFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("songsByEthnicity", getSongsByEthnicityFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("playsByEthnicity", getPlaysByEthnicityFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByEthnicity", getListeningTimeByEthnicityFiltered(combinedFilter, combinedParams, limit));

        return data;
    }

    private java.util.List<java.util.Map<String, Object>> getArtistsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.ethnicity_id, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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

    private java.util.List<java.util.Map<String, Object>> getAlbumsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(e.name, 'Unknown') as ethnicity_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT alb.id as album_id, ar.ethnicity_id, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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
    
    private java.util.List<java.util.Map<String, Object>> getSongsByEthnicityFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN Ethnicity e ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = e.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN Ethnicity e ON COALESCE(s.override_ethnicity_id, ar.ethnicity_id) = e.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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

        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
        combinedParams.addAll(params);

        boolean playJoinNeeded = needsPlayJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();
        
        data.put("artistsByLanguage", getArtistsByLanguageFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("albumsByLanguage", getAlbumsByLanguageFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("songsByLanguage", getSongsByLanguageFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("playsByLanguage", getPlaysByLanguageFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByLanguage", getListeningTimeByLanguageFiltered(combinedFilter, combinedParams, limit));

        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(l.name, 'Unknown') as language_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.language_id, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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

    private java.util.List<java.util.Map<String, Object>> getAlbumsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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

    private java.util.List<java.util.Map<String, Object>> getSongsByLanguageFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN Language l ON COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) = l.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                LEFT JOIN Language l ON COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) = l.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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

        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
        combinedParams.addAll(params);

        boolean playJoinNeeded = needsPlayJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        data.put("artistsByCountry", getArtistsByCountryFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("albumsByCountry", getAlbumsByCountryFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("songsByCountry", getSongsByCountryFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("playsByCountry", getPlaysByCountryFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByCountry", getListeningTimeByCountryFiltered(combinedFilter, combinedParams, limit));

        return data;
    }
    
    private java.util.List<java.util.Map<String, Object>> getArtistsByCountryFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(ar.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT ar.id as artist_id, ar.country, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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
    
    private java.util.List<java.util.Map<String, Object>> getAlbumsByCountryFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(sub.country, 'Unknown') as country_name,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT alb.id as album_id, ar.country, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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

    private java.util.List<java.util.Map<String, Object>> getSongsByCountryFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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

        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
        combinedParams.addAll(params);

        boolean playJoinNeeded = needsPlayJoin(filter);
        Integer limit = filter.getTopLimit() != null && filter.getTopLimit() > 0 ? filter.getTopLimit() : null;

        java.util.Map<String, Object> data = new java.util.HashMap<>();

        // No artists by release year - artists don't have release dates
        data.put("albumsByReleaseYear", getAlbumsByReleaseYearFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("songsByReleaseYear", getSongsByReleaseYearFiltered(filterClause.toString(), params, playJoinNeeded, limit));
        data.put("playsByReleaseYear", getPlaysByReleaseYearFiltered(combinedFilter, combinedParams, limit));
        data.put("listeningTimeByReleaseYear", getListeningTimeByReleaseYearFiltered(combinedFilter, combinedParams, limit));

        return data;
    }

    private java.util.List<java.util.Map<String, Object>> getAlbumsByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
            sql = """
                SELECT 
                    COALESCE(STRFTIME('%Y', alb.release_date), 'Unknown') as release_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM (
                    SELECT DISTINCT alb.id as album_id, alb.release_date, ar.gender_id
                    FROM Song s
                    """ + playJoin + """
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

    private java.util.List<java.util.Map<String, Object>> getSongsByReleaseYearFiltered(String filterClause, java.util.List<Object> filterParams, boolean needsPlayJoin, Integer limit) {
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
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
            String playJoin = needsPlayJoin ? "INNER JOIN Play p ON p.song_id = s.id\n                " : "";
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
                    """ + playJoin + """
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
    
    // Get Listen Year tab chart data (5 bar charts grouped by play/listen year)
    public java.util.Map<String, Object> getListenYearChartData(ChartFilterDTO filter) {
        StringBuilder filterClause = new StringBuilder();
        java.util.List<Object> params = new java.util.ArrayList<>();
        
        buildFilterClause(filterClause, params, filter);
        
        // Build early play filter for performance (filters on p.song_id before expensive joins)
        StringBuilder playEarlyFilter = new StringBuilder();
        java.util.List<Object> playEarlyParams = new java.util.ArrayList<>();
        buildplayEarlyFilter(playEarlyFilter, playEarlyParams, filter);
        
        // Combined filter: early play filter + regular filter, with combined params
        String combinedFilter = playEarlyFilter.toString() + " " + filterClause.toString();
        java.util.List<Object> combinedParams = new java.util.ArrayList<>();
        combinedParams.addAll(playEarlyParams);
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
                    SELECT DISTINCT ar.id as artist_id, ar.gender_id, STRFTIME('%Y', p.play_date) as year
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE ar.id IN (
                        SELECT s.artist_id
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
                    SELECT DISTINCT ar.id as artist_id, ar.gender_id, STRFTIME('%Y', p.play_date) as year
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                    SELECT DISTINCT alb.id as album_id, ar.gender_id, STRFTIME('%Y', p.play_date) as year
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE alb.id IN (
                        SELECT s.album_id
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
                    SELECT DISTINCT alb.id as album_id, ar.gender_id, STRFTIME('%Y', p.play_date) as year
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                    SELECT DISTINCT s.id as song_id, COALESCE(s.override_gender_id, ar.gender_id) as gender_id, STRFTIME('%Y', p.play_date) as year
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE s.id IN (
                        SELECT s.id
                        FROM Play p
                        INNER JOIN Song s ON p.song_id = s.id
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
                    SELECT DISTINCT s.id as song_id, COALESCE(s.override_gender_id, ar.gender_id) as gender_id, STRFTIME('%Y', p.play_date) as year
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
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
                    COALESCE(STRFTIME('%Y', p.play_date), 'Unknown') as listen_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(STRFTIME('%Y', p.play_date), 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY listen_year DESC
                """;
            params.add(limit);
        } else {
            sql = """
                SELECT 
                    COALESCE(STRFTIME('%Y', p.play_date), 'Unknown') as listen_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN 1 ELSE 0 END) as other_count
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE 1=1 """ + " " + filterClause + """
                GROUP BY COALESCE(STRFTIME('%Y', p.play_date), 'Unknown')
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
                    COALESCE(STRFTIME('%Y', p.play_date), 'Unknown') as listen_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE s.id IN (
                    SELECT s.id
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    WHERE 1=1 """ + " " + filterClause + """
                    GROUP BY s.id
                    ORDER BY COUNT(*) DESC
                    LIMIT ?
                )
                GROUP BY COALESCE(STRFTIME('%Y', p.play_date), 'Unknown')
                HAVING (male_count + female_count + other_count) > 0
                ORDER BY listen_year DESC
                """;
            params.add(limit);
        } else {
            sql = """
                SELECT 
                    COALESCE(STRFTIME('%Y', p.play_date), 'Unknown') as listen_year,
                    SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as male_count,
                    SUM(CASE WHEN g.name LIKE '%Female%' THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as female_count,
                    SUM(CASE WHEN (g.name IS NULL OR (g.name NOT LIKE '%Male%' AND g.name NOT LIKE '%Female%')) THEN COALESCE(s.length_seconds, 0) ELSE 0 END) as other_count
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Gender g ON COALESCE(s.override_gender_id, ar.gender_id) = g.id
                WHERE 1=1 """ + " " + filterClause + """
                GROUP BY COALESCE(STRFTIME('%Y', p.play_date), 'Unknown')
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
     * Builds a filter clause for play-level queries (only play_date filters).
     * This is separate from song-level filters so it can be applied in subqueries where scr alias exists.
     */
    private void buildPlayDateFilter(StringBuilder sql, java.util.List<Object> params, ChartFilterDTO filter) {
        String listenedDateFrom = filter.getListenedDateFrom();
        String listenedDateTo = filter.getListenedDateTo();
        
        if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty() && 
            listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(p.play_date) >= DATE(?) AND DATE(p.play_date) <= DATE(?)");
            params.add(listenedDateFrom);
            params.add(listenedDateTo);
        } else if (listenedDateFrom != null && !listenedDateFrom.trim().isEmpty()) {
            sql.append(" AND DATE(p.play_date) >= DATE(?)");
            params.add(listenedDateFrom);
        } else if (listenedDateTo != null && !listenedDateTo.trim().isEmpty()) {
            sql.append(" AND DATE(p.play_date) <= DATE(?)");
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
        
        // Build play date filter separately
        StringBuilder playDateFilter = new StringBuilder();
        java.util.List<Object> playDateParams = new java.util.ArrayList<>();
        buildPlayDateFilter(playDateFilter, playDateParams, filter);
        
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
                    ar.birth_date,
                    ar.death_date,
                    CASE WHEN ar.birth_date IS NOT NULL THEN CAST((julianday(COALESCE(ar.death_date, DATE('now'))) - julianday(ar.birth_date)) / 365.25 AS INTEGER) ELSE NULL END as age,
                    COALESCE(ac.album_count, 0) as album_count,
                    COALESCE(sc.song_count, 0) as all_song_count,
                    CASE WHEN COALESCE(sc.song_count, 0) > 0 THEN CAST(COALESCE(sc.total_length, 0) AS REAL) / sc.song_count ELSE NULL END as avg_length,
                    COALESCE(foc.featured_on_count, 0) as featured_on_count,
                    COALESCE(fac.featured_artist_count, 0) as featured_artist_count,
                    COALESCE(ssc.solo_song_count, 0) as solo_song_count,
                    COALESCE(swfc.songs_with_feat_count, 0) as songs_with_feat_count,
                    COALESCE(agg.plays, 0) as plays,
                    COALESCE(agg.primary_plays, 0) as primary_plays,
                    COALESCE(agg.legacy_plays, 0) as legacy_plays,
                    COALESCE(agg.time_listened, 0) as time_listened,
                    agg.first_listened,
                    agg.last_listened,
                    COALESCE(consistency_agg.days_listened, 0) as days_listened,
                    COALESCE(consistency_agg.weeks_listened, 0) as weeks_listened,
                    COALESCE(consistency_agg.months_listened, 0) as months_listened,
                    COALESCE(consistency_agg.years_listened, 0) as years_listened
                FROM Artist ar
                LEFT JOIN Genre gen ON ar.genre_id = gen.id
                LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
                LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
                LEFT JOIN Language lang ON ar.language_id = lang.id
                LEFT JOIN (SELECT artist_id, COUNT(*) as album_count FROM Album GROUP BY artist_id) ac ON ar.id = ac.artist_id
                LEFT JOIN (SELECT artist_id, COUNT(*) as song_count, SUM(length_seconds) as total_length FROM Song GROUP BY artist_id) sc ON ar.id = sc.artist_id
                LEFT JOIN (SELECT artist_id, COUNT(*) as featured_on_count FROM SongFeaturedArtist GROUP BY artist_id) foc ON ar.id = foc.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(DISTINCT sfa.artist_id) as featured_artist_count FROM Song s2 INNER JOIN SongFeaturedArtist sfa ON s2.id = sfa.song_id GROUP BY s2.artist_id) fac ON ar.id = fac.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(*) as solo_song_count FROM Song s2 WHERE NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s2.id) GROUP BY s2.artist_id) ssc ON ar.id = ssc.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(*) as songs_with_feat_count FROM Song s2 WHERE EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s2.id) GROUP BY s2.artist_id) swfc ON ar.id = swfc.artist_id
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
                            p.song_id,
                            COUNT(*) as plays,
                            SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                            SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                            SUM(s2.length_seconds) as time_listened,
                            MIN(p.play_date) as first_listened,
                            MAX(p.play_date) as last_listened
                        FROM Play p
                        INNER JOIN Song s2 ON p.song_id = s2.id
                        WHERE 1=1""" + playDateFilter.toString() + """
                        GROUP BY p.song_id
                    ) play_stats ON play_stats.song_id = s.id
                    WHERE 1=1 """ + songFilterClause.toString() + """
                    GROUP BY s.artist_id
                ) agg ON ar.id = agg.artist_id
                LEFT JOIN (
                    SELECT
                        s.artist_id,
                        COUNT(DISTINCT DATE(p.play_date)) as days_listened,
                        COUNT(DISTINCT strftime('%Y-%W', p.play_date)) as weeks_listened,
                        COUNT(DISTINCT strftime('%Y-%m', p.play_date)) as months_listened,
                        COUNT(DISTINCT strftime('%Y', p.play_date)) as years_listened
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    INNER JOIN Play p ON p.song_id = s.id
                    WHERE 1=1 """ + playDateFilter.toString() + songFilterClause.toString() + """
                    GROUP BY s.artist_id
                ) consistency_agg ON ar.id = consistency_agg.artist_id
                ORDER BY plays DESC, agg.last_listened ASC
                LIMIT ?
                """;
            // Add play date params first, then song params, then limit
            params.addAll(playDateParams);
            params.addAll(songParams);
            params.addAll(playDateParams);
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
                    ar.birth_date,
                    ar.death_date,
                    CASE WHEN ar.birth_date IS NOT NULL THEN CAST((julianday(COALESCE(ar.death_date, DATE('now'))) - julianday(ar.birth_date)) / 365.25 AS INTEGER) ELSE NULL END as age,
                    COALESCE(ac.album_count, 0) as album_count,
                    COALESCE(sc.song_count, 0) as all_song_count,
                    CASE WHEN COALESCE(sc.song_count, 0) > 0 THEN CAST(COALESCE(sc.total_length, 0) AS REAL) / sc.song_count ELSE NULL END as avg_length,
                    COALESCE(foc.featured_on_count, 0) as featured_on_count,
                    COALESCE(fac.featured_artist_count, 0) as featured_artist_count,
                    COALESCE(ssc.solo_song_count, 0) as solo_song_count,
                    COALESCE(swfc.songs_with_feat_count, 0) as songs_with_feat_count,
                    COALESCE(agg.plays, 0) as plays,
                    COALESCE(agg.primary_plays, 0) as primary_plays,
                    COALESCE(agg.legacy_plays, 0) as legacy_plays,
                    COALESCE(agg.time_listened, 0) as time_listened,
                    agg.first_listened,
                    agg.last_listened,
                    COALESCE(consistency_agg.days_listened, 0) as days_listened,
                    COALESCE(consistency_agg.weeks_listened, 0) as weeks_listened,
                    COALESCE(consistency_agg.months_listened, 0) as months_listened,
                    COALESCE(consistency_agg.years_listened, 0) as years_listened
                FROM Artist ar
                LEFT JOIN Genre gen ON ar.genre_id = gen.id
                LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
                LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
                LEFT JOIN Language lang ON ar.language_id = lang.id
                LEFT JOIN (SELECT artist_id, COUNT(*) as album_count FROM Album GROUP BY artist_id) ac ON ar.id = ac.artist_id
                LEFT JOIN (SELECT artist_id, COUNT(*) as song_count, SUM(length_seconds) as total_length FROM Song GROUP BY artist_id) sc ON ar.id = sc.artist_id
                LEFT JOIN (SELECT artist_id, COUNT(*) as featured_on_count FROM SongFeaturedArtist GROUP BY artist_id) foc ON ar.id = foc.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(DISTINCT sfa.artist_id) as featured_artist_count FROM Song s2 INNER JOIN SongFeaturedArtist sfa ON s2.id = sfa.song_id GROUP BY s2.artist_id) fac ON ar.id = fac.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(*) as solo_song_count FROM Song s2 WHERE NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s2.id) GROUP BY s2.artist_id) ssc ON ar.id = ssc.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(*) as songs_with_feat_count FROM Song s2 WHERE EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s2.id) GROUP BY s2.artist_id) swfc ON ar.id = swfc.artist_id
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
                            p.song_id,
                            COUNT(*) as plays,
                            SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                            SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                            SUM(s2.length_seconds) as time_listened,
                            MIN(p.play_date) as first_listened,
                            MAX(p.play_date) as last_listened
                        FROM Play p
                        INNER JOIN Song s2 ON p.song_id = s2.id
                        WHERE 1=1""" + playDateFilter.toString() + """
                        GROUP BY p.song_id
                    ) play_stats ON play_stats.song_id = s.id
                    WHERE 1=1 """ + songFilterClause.toString() + """
                    GROUP BY s.artist_id
                ) agg ON ar.id = agg.artist_id
                LEFT JOIN (
                    SELECT
                        s.artist_id,
                        COUNT(DISTINCT DATE(p.play_date)) as days_listened,
                        COUNT(DISTINCT strftime('%Y-%W', p.play_date)) as weeks_listened,
                        COUNT(DISTINCT strftime('%Y-%m', p.play_date)) as months_listened,
                        COUNT(DISTINCT strftime('%Y', p.play_date)) as years_listened
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    INNER JOIN Play p ON p.song_id = s.id
                    WHERE 1=1 """ + playDateFilter.toString() + songFilterClause.toString() + """
                    GROUP BY s.artist_id
                ) consistency_agg ON ar.id = consistency_agg.artist_id
                ORDER BY plays DESC, agg.last_listened ASC
                LIMIT ?
                """;
            // Add play date params first, then song params, then limit
            params.addAll(playDateParams);
            params.addAll(songParams);
            params.addAll(playDateParams);
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
                    p.id as play_id,
                    p.account,
                    s.length_seconds,
                    p.play_date
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                WHERE 1=1 """ + playDateFilter.toString() + songFilterClause.toString());
            // Add play date params and song params for Part 1
            params.addAll(playDateParams);
            params.addAll(songParams);

            if (includeGroups) {
                // Part 2: Group plays - attribute to group members
                // Clone play date params and song params for Part 2
                params.addAll(playDateParams);
                params.addAll(songParams);
                unionParts.append("""
                    
                    UNION ALL
                    
                    SELECT 
                        am.member_artist_id as attributed_artist_id,
                        p.id as play_id,
                        p.account,
                        s.length_seconds,
                        p.play_date
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    INNER JOIN ArtistMember am ON s.artist_id = am.group_artist_id
                    WHERE 1=1 """ + playDateFilter.toString() + songFilterClause.toString());
            }
            
            if (includeFeatured) {
                // Part 3: Featured plays - attribute to featured artists
                // Clone play date params and song params for Part 3
                params.addAll(playDateParams);
                params.addAll(songParams);
                unionParts.append("""
                    
                    UNION ALL
                    
                    SELECT 
                        sfa.artist_id as attributed_artist_id,
                        p.id as play_id,
                        p.account,
                        s.length_seconds,
                        p.play_date
                    FROM Play p
                    INNER JOIN Song s ON p.song_id = s.id
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    INNER JOIN SongFeaturedArtist sfa ON s.id = sfa.song_id
                    WHERE 1=1 """ + playDateFilter.toString() + songFilterClause.toString());
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
                    ar.birth_date,
                    ar.death_date,
                    CASE WHEN ar.birth_date IS NOT NULL THEN CAST((julianday(COALESCE(ar.death_date, DATE('now'))) - julianday(ar.birth_date)) / 365.25 AS INTEGER) ELSE NULL END as age,
                    COALESCE(ac.album_count, 0) as album_count,
                    COALESCE(sc.song_count, 0) as all_song_count,
                    CASE WHEN COALESCE(sc.song_count, 0) > 0 THEN CAST(COALESCE(sc.total_length, 0) AS REAL) / sc.song_count ELSE NULL END as avg_length,
                    COALESCE(foc.featured_on_count, 0) as featured_on_count,
                    COALESCE(fac.featured_artist_count, 0) as featured_artist_count,
                    COALESCE(ssc.solo_song_count, 0) as solo_song_count,
                    COALESCE(swfc.songs_with_feat_count, 0) as songs_with_feat_count,
                    agg.plays,
                    agg.primary_plays,
                    agg.legacy_plays,
                    agg.time_listened,
                    agg.first_listened,
                    agg.last_listened,
                    agg.days_listened,
                    agg.weeks_listened,
                    agg.months_listened,
                    agg.years_listened
                FROM Artist ar
                LEFT JOIN Genre gen ON ar.genre_id = gen.id
                LEFT JOIN SubGenre sg ON ar.subgenre_id = sg.id
                LEFT JOIN Ethnicity eth ON ar.ethnicity_id = eth.id
                LEFT JOIN Language lang ON ar.language_id = lang.id
                LEFT JOIN (SELECT artist_id, COUNT(*) as album_count FROM Album GROUP BY artist_id) ac ON ar.id = ac.artist_id
                LEFT JOIN (SELECT artist_id, COUNT(*) as song_count, SUM(length_seconds) as total_length FROM Song GROUP BY artist_id) sc ON ar.id = sc.artist_id
                LEFT JOIN (SELECT artist_id, COUNT(*) as featured_on_count FROM SongFeaturedArtist GROUP BY artist_id) foc ON ar.id = foc.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(DISTINCT sfa.artist_id) as featured_artist_count FROM Song s2 INNER JOIN SongFeaturedArtist sfa ON s2.id = sfa.song_id GROUP BY s2.artist_id) fac ON ar.id = fac.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(*) as solo_song_count FROM Song s2 WHERE NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s2.id) GROUP BY s2.artist_id) ssc ON ar.id = ssc.artist_id
                LEFT JOIN (SELECT s2.artist_id, COUNT(*) as songs_with_feat_count FROM Song s2 WHERE EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s2.id) GROUP BY s2.artist_id) swfc ON ar.id = swfc.artist_id
                INNER JOIN (
                    SELECT 
                        attributed_artist_id,
                        COUNT(*) as plays,
                        SUM(CASE WHEN account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                        SUM(CASE WHEN account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                        SUM(length_seconds) as time_listened,
                        MIN(play_date) as first_listened,
                        MAX(play_date) as last_listened,
                        COUNT(DISTINCT DATE(play_date)) as days_listened,
                        COUNT(DISTINCT strftime('%Y-%W', play_date)) as weeks_listened,
                        COUNT(DISTINCT strftime('%Y-%m', play_date)) as months_listened,
                        COUNT(DISTINCT strftime('%Y', play_date)) as years_listened
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
            row.put("birthDate", formatDate(rs.getString("birth_date")));
            row.put("deathDate", formatDate(rs.getString("death_date")));
            int age = rs.getInt("age");
            row.put("age", rs.wasNull() ? null : age);
            row.put("albumCount", rs.getInt("album_count"));
            int songCount = rs.getInt("all_song_count");
            row.put("songCount", songCount);
            double avgLength = rs.getDouble("avg_length");
            row.put("avgLength", rs.wasNull() ? null : avgLength);
            if (!rs.wasNull() && avgLength > 0) {
                int avgMins = (int) avgLength / 60;
                int avgSecs = (int) avgLength % 60;
                row.put("avgLengthFormatted", String.format("%d:%02d", avgMins, avgSecs));
            } else {
                row.put("avgLengthFormatted", null);
            }
            long plays2 = rs.getLong("plays");
            row.put("avgPlays", songCount > 0 ? Math.round((double) plays2 / songCount * 100.0) / 100.0 : null);
            int albumCount = rs.getInt("album_count");
            row.put("avgPlaysAlbum", albumCount > 0 ? Math.round((double) plays2 / albumCount * 100.0) / 100.0 : null);
            row.put("featuredOnCount", rs.getInt("featured_on_count"));
            row.put("featuredArtistCount", rs.getInt("featured_artist_count"));
            row.put("soloSongCount", rs.getInt("solo_song_count"));
            row.put("songsWithFeatCount", rs.getInt("songs_with_feat_count"));
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            row.put("daysListened", rs.getInt("days_listened"));
            row.put("weeksListened", rs.getInt("weeks_listened"));
            row.put("monthsListened", rs.getInt("months_listened"));
            row.put("yearsListened", rs.getInt("years_listened"));
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
        
        // Build play date filter separately
        StringBuilder playDateFilter = new StringBuilder();
        java.util.List<Object> playDateParams = new java.util.ArrayList<>();
        buildPlayDateFilter(playDateFilter, playDateParams, filter);
        
        // Last Full Listen Date filter for outer query
        String lastFullListenDateMode = filter.getLastFullListenDateMode();
        String lastFullListenDate = filter.getLastFullListenDate();
        String lastFullListenDateFrom = filter.getLastFullListenDateFrom();
        String lastFullListenDateTo = filter.getLastFullListenDateTo();
        boolean hasLastFullListenFilter = lastFullListenDateMode != null && !lastFullListenDateMode.isEmpty();
        StringBuilder lastFullListenClause = new StringBuilder();
        java.util.List<Object> lastFullListenParams = new java.util.ArrayList<>();
        if (hasLastFullListenFilter) {
            switch (lastFullListenDateMode) {
                case "exact" -> { if (lastFullListenDate != null && !lastFullListenDate.isEmpty()) { lastFullListenClause.append(" AND DATE(last_full_listen_date) = DATE(?)"); lastFullListenParams.add(lastFullListenDate); } }
                case "gte"   -> { if (lastFullListenDate != null && !lastFullListenDate.isEmpty()) { lastFullListenClause.append(" AND DATE(last_full_listen_date) >= DATE(?)"); lastFullListenParams.add(lastFullListenDate); } }
                case "lte"   -> { if (lastFullListenDate != null && !lastFullListenDate.isEmpty()) { lastFullListenClause.append(" AND DATE(last_full_listen_date) <= DATE(?)"); lastFullListenParams.add(lastFullListenDate); } }
                case "between" -> { if (lastFullListenDateFrom != null && !lastFullListenDateFrom.isEmpty() && lastFullListenDateTo != null && !lastFullListenDateTo.isEmpty()) { lastFullListenClause.append(" AND DATE(last_full_listen_date) >= DATE(?) AND DATE(last_full_listen_date) <= DATE(?)"); lastFullListenParams.add(lastFullListenDateFrom); lastFullListenParams.add(lastFullListenDateTo); } }
                case "isnull"    -> lastFullListenClause.append(" AND last_full_listen_date IS NULL");
                case "isnotnull" -> lastFullListenClause.append(" AND last_full_listen_date IS NOT NULL");
            }
        }

        // Combine parameters: play date params, then song params, optionally lastFullListen params, then limit
        java.util.List<Object> params = new java.util.ArrayList<>();
        params.addAll(playDateParams);
        params.addAll(songParams);
        params.addAll(playDateParams);
        params.addAll(songParams);
        if (hasLastFullListenFilter && lastFullListenClause.length() > 0) {
            params.addAll(lastFullListenParams);
        }
        params.add(limit);

        String innerSql = """
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
                CASE WHEN ar.birth_date IS NOT NULL AND alb.release_date IS NOT NULL THEN CAST((julianday(alb.release_date) - julianday(ar.birth_date)) / 365.25 AS INTEGER) ELSE NULL END as age_at_release,
                COALESCE(album_sc.song_count, 0) as song_count,
                CASE WHEN COALESCE(album_sc.song_count, 0) > 0 THEN CAST(COALESCE(album_len.album_length, 0) AS REAL) / album_sc.song_count ELSE NULL END as avg_length,
                COALESCE(album_fac.featured_artist_count, 0) as featured_artist_count,
                COALESCE(album_ssc.solo_song_count, 0) as solo_song_count,
                COALESCE(album_swfc.songs_with_feat_count, 0) as songs_with_feat_count,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.album_id = alb.id AND c.chart_type = 'album' AND c.period_type = 'seasonal') as seasonal_chart_peak,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.album_id = alb.id AND c.chart_type = 'album' AND c.period_type = 'weekly') as weekly_chart_peak,
                (SELECT COUNT(DISTINCT c.id) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.album_id = alb.id AND c.chart_type = 'album' AND c.period_type = 'weekly') as weekly_chart_weeks,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.album_id = alb.id AND c.chart_type = 'album' AND c.period_type = 'yearly') as yearly_chart_peak,
                COALESCE(agg.plays, 0) as plays,
                COALESCE(agg.primary_plays, 0) as primary_plays,
                COALESCE(agg.legacy_plays, 0) as legacy_plays,
                COALESCE(agg.time_listened, 0) as time_listened,
                agg.first_listened,
                agg.last_listened,
                COALESCE(consistency_agg.days_listened, 0) as days_listened,
                COALESCE(consistency_agg.weeks_listened, 0) as weeks_listened,
                COALESCE(consistency_agg.months_listened, 0) as months_listened,
                COALESCE(consistency_agg.years_listened, 0) as years_listened,
                (SELECT MAX(DATE(arc.run_end_date))
                 FROM (
                     SELECT rn2.album_id, rn2.run_id, MAX(rn2.play_date) AS run_end_date, COUNT(DISTINCT rn2.song_id) AS songs_played
                     FROM (
                         SELECT *, SUM(CASE WHEN lag_alb IS NOT album_id THEN 1 ELSE 0 END) OVER (ORDER BY rn_ord) AS run_id
                         FROM (
                             SELECT p2.id, p2.play_date, p2.song_id, s2.album_id,
                                    ROW_NUMBER() OVER (ORDER BY p2.play_date, p2.id) AS rn_ord,
                                    LAG(s2.album_id) OVER (ORDER BY p2.play_date, p2.id) AS lag_alb
                             FROM Play p2
                             LEFT JOIN Song s2 ON p2.song_id = s2.id
                         )
                     ) rn2
                     WHERE rn2.album_id IS NOT NULL
                     GROUP BY rn2.album_id, rn2.run_id
                 ) arc
                 WHERE arc.album_id = alb.id
                   AND arc.songs_played >= (
                                         SELECT %s
                     FROM (SELECT COUNT(*) AS cnt FROM Song WHERE album_id = alb.id)
                   )
                ) AS last_full_listen_date
                        FROM Album alb
                        """.formatted(buildRequiredSongsExpression("cnt")) + """
            LEFT JOIN Artist ar ON alb.artist_id = ar.id
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
            LEFT JOIN (SELECT album_id, COUNT(*) as song_count FROM Song WHERE album_id IS NOT NULL GROUP BY album_id) album_sc ON alb.id = album_sc.album_id
            LEFT JOIN (SELECT s3.album_id, COUNT(DISTINCT sfa.artist_id) as featured_artist_count FROM Song s3 INNER JOIN SongFeaturedArtist sfa ON s3.id = sfa.song_id WHERE s3.album_id IS NOT NULL GROUP BY s3.album_id) album_fac ON alb.id = album_fac.album_id
            LEFT JOIN (SELECT s3.album_id, COUNT(*) as solo_song_count FROM Song s3 WHERE s3.album_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s3.id) GROUP BY s3.album_id) album_ssc ON alb.id = album_ssc.album_id
            LEFT JOIN (SELECT s3.album_id, COUNT(*) as songs_with_feat_count FROM Song s3 WHERE s3.album_id IS NOT NULL AND EXISTS (SELECT 1 FROM SongFeaturedArtist sfa WHERE sfa.song_id = s3.id) GROUP BY s3.album_id) album_swfc ON alb.id = album_swfc.album_id
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
                        p.song_id,
                        COUNT(*) as plays,
                        SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                        SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                        SUM(s2.length_seconds) as time_listened,
                        MIN(p.play_date) as first_listened,
                        MAX(p.play_date) as last_listened
                    FROM Play p
                    INNER JOIN Song s2 ON p.song_id = s2.id
                    WHERE 1=1""" + playDateFilter.toString() + """
                    GROUP BY p.song_id
                ) play_stats ON play_stats.song_id = s.id
                WHERE s.album_id IS NOT NULL """ + songFilterClause.toString() + """
                GROUP BY s.album_id
            ) agg ON alb.id = agg.album_id
            LEFT JOIN (
                SELECT
                    s.album_id,
                    COUNT(DISTINCT DATE(p.play_date)) as days_listened,
                    COUNT(DISTINCT strftime('%Y-%W', p.play_date)) as weeks_listened,
                    COUNT(DISTINCT strftime('%Y-%m', p.play_date)) as months_listened,
                    COUNT(DISTINCT strftime('%Y', p.play_date)) as years_listened
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                INNER JOIN Play p ON p.song_id = s.id
                WHERE s.album_id IS NOT NULL """ + playDateFilter.toString() + songFilterClause.toString() + """
                GROUP BY s.album_id
            ) consistency_agg ON alb.id = consistency_agg.album_id
            WHERE agg.album_id IS NOT NULL
            ORDER BY plays DESC, agg.last_listened ASC
            """;

        String sql;
        if (hasLastFullListenFilter && lastFullListenClause.length() > 0) {
            sql = "SELECT * FROM (" + innerSql + ") alb_sub WHERE 1=1" + lastFullListenClause + " ORDER BY plays DESC, last_listened ASC LIMIT ?";
        } else {
            sql = innerSql + "LIMIT ?";
        }

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
            // Add new stats columns
            int ageAtRelease = rs.getInt("age_at_release");
            row.put("ageAtRelease", rs.wasNull() ? null : ageAtRelease);
            int songCount = rs.getInt("song_count");
            row.put("songCount", songCount);
            double avgLengthVal = rs.getDouble("avg_length");
            row.put("avgLength", rs.wasNull() ? null : avgLengthVal);
            if (!rs.wasNull() && avgLengthVal > 0) {
                int avgMins = (int) avgLengthVal / 60;
                int avgSecs = (int) avgLengthVal % 60;
                row.put("avgLengthFormatted", String.format("%d:%02d", avgMins, avgSecs));
            } else {
                row.put("avgLengthFormatted", null);
            }
            row.put("featuredArtistCount", rs.getInt("featured_artist_count"));
            row.put("soloSongCount", rs.getInt("solo_song_count"));
            row.put("songsWithFeatCount", rs.getInt("songs_with_feat_count"));
            int seasonalPeak = rs.getInt("seasonal_chart_peak");
            row.put("seasonalChartPeak", rs.wasNull() ? null : seasonalPeak);
            int weeklyPeak = rs.getInt("weekly_chart_peak");
            row.put("weeklyChartPeak", rs.wasNull() ? null : weeklyPeak);
            row.put("weeklyChartWeeks", rs.getInt("weekly_chart_weeks"));
            int yearlyPeak = rs.getInt("yearly_chart_peak");
            row.put("yearlyChartPeak", rs.wasNull() ? null : yearlyPeak);
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
            long albumPlays = rs.getLong("plays");
            row.put("plays", albumPlays);
            row.put("avgPlays", songCount > 0 ? Math.round((double) albumPlays / songCount * 100.0) / 100.0 : null);
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            row.put("daysListened", rs.getInt("days_listened"));
            row.put("weeksListened", rs.getInt("weeks_listened"));
            row.put("monthsListened", rs.getInt("months_listened"));
            row.put("yearsListened", rs.getInt("years_listened"));
            row.put("lastFullListen", formatDate(rs.getString("last_full_listen_date")));
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
        
        // Build play date filter separately
        StringBuilder playDateFilter = new StringBuilder();
        java.util.List<Object> playDateParams = new java.util.ArrayList<>();
        buildPlayDateFilter(playDateFilter, playDateParams, filter);
        
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
                CASE WHEN ar.birth_date IS NOT NULL AND COALESCE(s.release_date, alb.release_date) IS NOT NULL THEN CAST((julianday(COALESCE(s.release_date, alb.release_date)) - julianday(ar.birth_date)) / 365.25 AS INTEGER) ELSE NULL END as age_at_release,
                (SELECT COUNT(*) FROM SongFeaturedArtist sfa3 WHERE sfa3.song_id = s.id) as featured_artist_count,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'seasonal') as seasonal_chart_peak,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly') as weekly_chart_peak,
                (SELECT COUNT(DISTINCT c.id) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'weekly') as weekly_chart_weeks,
                (SELECT MIN(ce.position) FROM trl_chart_entry ce INNER JOIN trl_debut td ON td.id = ce.debut_id WHERE td.song_id = s.id) as trl_peak,
                (SELECT COUNT(DISTINCT ce.chart_date) FROM trl_chart_entry ce INNER JOIN trl_debut td ON td.id = ce.debut_id WHERE td.song_id = s.id) as trl_days,
                (SELECT COUNT(DISTINCT ce2.chart_date) FROM trl_chart_entry ce2 INNER JOIN trl_debut td2 ON td2.id = ce2.debut_id WHERE td2.song_id = s.id AND ce2.position = (SELECT MIN(ce3.position) FROM trl_chart_entry ce3 INNER JOIN trl_debut td3 ON td3.id = ce3.debut_id WHERE td3.song_id = s.id)) as trl_days_at_peak,
                (SELECT MIN(e.position) FROM vatos_cuntdown_entry e WHERE e.song_id = s.id AND e.is_close_call = 0) as vatos_cuntdown_peak,
                (SELECT COUNT(DISTINCT e.chart_date) FROM vatos_cuntdown_entry e WHERE e.song_id = s.id AND e.is_close_call = 0) as vatos_cuntdown_days,
                (SELECT COUNT(DISTINCT e2.chart_date) FROM vatos_cuntdown_entry e2 WHERE e2.song_id = s.id AND e2.is_close_call = 0 AND e2.position = (SELECT MIN(e3.position) FROM vatos_cuntdown_entry e3 WHERE e3.song_id = s.id AND e3.is_close_call = 0)) as vatos_cuntdown_days_at_peak,
                (SELECT MIN(b.peak_position) FROM billboard_hot100_debut b WHERE b.song_id = s.id) as billboard_peak,
                (SELECT MAX(b.weeks_on_chart) FROM billboard_hot100_debut b WHERE b.song_id = s.id) as billboard_weeks,
                (SELECT MAX(b.weeks_at_peak) FROM billboard_hot100_debut b WHERE b.song_id = s.id) as billboard_weeks_at_peak,
                (SELECT MIN(ce.position) FROM ChartEntry ce INNER JOIN Chart c ON ce.chart_id = c.id WHERE ce.song_id = s.id AND c.chart_type = 'song' AND c.period_type = 'yearly') as yearly_chart_peak,
                COALESCE(play_stats.plays, 0) as plays,
                COALESCE(play_stats.primary_plays, 0) as primary_plays,
                COALESCE(play_stats.legacy_plays, 0) as legacy_plays,
                COALESCE(s.length_seconds, 0) * COALESCE(play_stats.plays, 0) as time_listened,
                play_stats.first_listened,
                play_stats.last_listened,
                COALESCE(play_stats.days_listened, 0) as days_listened,
                COALESCE(play_stats.weeks_listened, 0) as weeks_listened,
                COALESCE(play_stats.months_listened, 0) as months_listened,
                COALESCE(play_stats.years_listened, 0) as years_listened
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
                    p.song_id,
                    COUNT(*) as plays,
                    SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                    SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                    COUNT(DISTINCT DATE(p.play_date)) as days_listened,
                    COUNT(DISTINCT strftime('%Y-%W', p.play_date)) as weeks_listened,
                    COUNT(DISTINCT strftime('%Y-%m', p.play_date)) as months_listened,
                    COUNT(DISTINCT strftime('%Y', p.play_date)) as years_listened,
                    MIN(p.play_date) as first_listened,
                    MAX(p.play_date) as last_listened
                FROM Play p
                WHERE 1=1""" + playDateFilter.toString() + """
                GROUP BY p.song_id
            ) play_stats ON play_stats.song_id = s.id
            WHERE 1=1 """ + songFilterClause.toString() + """
            
            ORDER BY plays DESC, play_stats.last_listened ASC
            LIMIT ?
            """;
        
        // Build final params list in the order they appear in the SQL:
        // 1. play date params (in play_stats WHERE)
        // 2. featured params (in featuredOnColumn CASE WHEN)
        // 3. group params (in fromGroupColumn CASE WHEN)
        // 4. song filter params (in main WHERE)
        // 5. limit
        java.util.List<Object> finalParams = new java.util.ArrayList<>();
        finalParams.addAll(playDateParams);
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
            int songAgeAtRelease = rs.getInt("age_at_release");
            row.put("ageAtRelease", rs.wasNull() ? null : songAgeAtRelease);
            row.put("featuredArtistCount", rs.getInt("featured_artist_count"));
            int songSeasonalPeak = rs.getInt("seasonal_chart_peak");
            row.put("seasonalChartPeak", rs.wasNull() ? null : songSeasonalPeak);
            int songWeeklyPeak = rs.getInt("weekly_chart_peak");
            row.put("weeklyChartPeak", rs.wasNull() ? null : songWeeklyPeak);
            row.put("weeklyChartWeeks", rs.getInt("weekly_chart_weeks"));
            int trlPeak = rs.getInt("trl_peak");
            row.put("trlPeak", rs.wasNull() ? null : trlPeak);
            row.put("trlDays", rs.getInt("trl_days"));
            row.put("trlDaysAtPeak", rs.getInt("trl_days_at_peak"));
            int vatosCuntdownPeak = rs.getInt("vatos_cuntdown_peak");
            row.put("vatosCuntdownPeak", rs.wasNull() ? null : vatosCuntdownPeak);
            row.put("vatosCuntdownDays", rs.getInt("vatos_cuntdown_days"));
            row.put("vatosCuntdownDaysAtPeak", rs.getInt("vatos_cuntdown_days_at_peak"));
            int billboardPeak = rs.getInt("billboard_peak");
            row.put("billboardPeak", rs.wasNull() ? null : billboardPeak);
            row.put("billboardWeeks", rs.getInt("billboard_weeks"));
            row.put("billboardWeeksAtPeak", rs.getInt("billboard_weeks_at_peak"));
            int songYearlyPeak = rs.getInt("yearly_chart_peak");
            row.put("yearlyChartPeak", rs.wasNull() ? null : songYearlyPeak);
            row.put("plays", rs.getLong("plays"));
            row.put("primaryPlays", rs.getLong("primary_plays"));
            row.put("legacyPlays", rs.getLong("legacy_plays"));
            long timeListened = rs.getLong("time_listened");
            row.put("timeListened", timeListened);
            row.put("timeListenedFormatted", TimeFormatUtils.formatTime(timeListened));
            row.put("firstListened", formatDate(rs.getString("first_listened")));
            row.put("lastListened", formatDate(rs.getString("last_listened")));
            row.put("daysListened", rs.getInt("days_listened"));
            row.put("weeksListened", rs.getInt("weeks_listened"));
            row.put("monthsListened", rs.getInt("months_listened"));
            row.put("yearsListened", rs.getInt("years_listened"));
            
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

    private void appendSongTrlFilter(StringBuilder sql, List<Object> params, Integer peak, Integer days) {
        appendSongTrlFilter(sql, params, peak, null, days, null, null, null, null);
    }

    private void appendSongTrlFilter(StringBuilder sql, List<Object> params, Integer peak, Integer days,
                                     String dateFrom, String dateTo) {
        appendSongTrlFilter(sql, params, peak, null, days, null, null, dateFrom, dateTo);
    }

    private void appendSongTrlFilter(StringBuilder sql, List<Object> params, Integer peak, String peakMode,
                                     Integer days, Integer daysAtPeak, String daysAtPeakMode,
                                     String dateFrom, String dateTo) {
        if (peak == null && days == null && daysAtPeak == null && !SqlFilterHelper.hasValue(dateFrom) && !SqlFilterHelper.hasValue(dateTo)) {
            return;
        }

        sql.append(" AND EXISTS (SELECT 1 FROM (");
        sql.append("SELECT MIN(ce.position) as peak, COUNT(DISTINCT ce.chart_date) as days ");
        sql.append("FROM trl_chart_entry ce ");
        sql.append("INNER JOIN trl_debut td ON td.id = ce.debut_id ");
        sql.append("WHERE td.song_id = s.id");
        SqlFilterHelper.appendDateRangeBounds(sql, params, "ce.chart_date", dateFrom, dateTo);
        sql.append(") countdown_stats WHERE countdown_stats.days > 0");
        if (peak != null) {
            SqlFilterHelper.appendChartPeakComparison(sql, params, "countdown_stats.peak", peak, peakMode);
        }
        if (days != null) {
            sql.append(" AND countdown_stats.days >= ?");
            params.add(days);
        }
        sql.append(")");
        if (daysAtPeak != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT ce.position, COUNT(DISTINCT ce.chart_date) as days_at_peak ");
            sql.append("FROM trl_chart_entry ce ");
            sql.append("INNER JOIN trl_debut td ON td.id = ce.debut_id ");
            sql.append("WHERE td.song_id = s.id");
            SqlFilterHelper.appendDateRangeBounds(sql, params, "ce.chart_date", dateFrom, dateTo);
            sql.append(" GROUP BY ce.position ORDER BY ce.position ASC LIMIT 1");
            sql.append(") peak_stats WHERE 1=1");
            SqlFilterHelper.appendNumericComparison(sql, params, "peak_stats.days_at_peak", daysAtPeak, daysAtPeakMode, ">=");
            sql.append(")");
        }
    }

    private void appendSongVatosCuntdownFilter(StringBuilder sql, List<Object> params, Integer peak, Integer days) {
        appendSongVatosCuntdownFilter(sql, params, peak, null, days, null, null, null, null);
    }

    private void appendSongVatosCuntdownFilter(StringBuilder sql, List<Object> params, Integer peak, Integer days,
                                               String dateFrom, String dateTo) {
        appendSongVatosCuntdownFilter(sql, params, peak, null, days, null, null, dateFrom, dateTo);
    }

    private void appendSongVatosCuntdownFilter(StringBuilder sql, List<Object> params, Integer peak, String peakMode,
                                               Integer days, Integer daysAtPeak, String daysAtPeakMode,
                                               String dateFrom, String dateTo) {
        if (peak == null && days == null && daysAtPeak == null && !SqlFilterHelper.hasValue(dateFrom) && !SqlFilterHelper.hasValue(dateTo)) {
            return;
        }

        sql.append(" AND EXISTS (SELECT 1 FROM (");
        sql.append("SELECT MIN(e.position) as peak, COUNT(DISTINCT e.chart_date) as days ");
        sql.append("FROM vatos_cuntdown_entry e ");
        sql.append("WHERE e.song_id = s.id AND e.is_close_call = 0");
        SqlFilterHelper.appendDateRangeBounds(sql, params, "e.chart_date", dateFrom, dateTo);
        sql.append(") countdown_stats WHERE countdown_stats.days > 0");
        if (peak != null) {
            SqlFilterHelper.appendChartPeakComparison(sql, params, "countdown_stats.peak", peak, peakMode);
        }
        if (days != null) {
            sql.append(" AND countdown_stats.days >= ?");
            params.add(days);
        }
        sql.append(")");
        if (daysAtPeak != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT e.position, COUNT(DISTINCT e.chart_date) as days_at_peak ");
            sql.append("FROM vatos_cuntdown_entry e ");
            sql.append("WHERE e.song_id = s.id AND e.is_close_call = 0");
            SqlFilterHelper.appendDateRangeBounds(sql, params, "e.chart_date", dateFrom, dateTo);
            sql.append(" GROUP BY e.position ORDER BY e.position ASC LIMIT 1");
            sql.append(") peak_stats WHERE 1=1");
            SqlFilterHelper.appendNumericComparison(sql, params, "peak_stats.days_at_peak", daysAtPeak, daysAtPeakMode, ">=");
            sql.append(")");
        }
    }

    private void appendSongBillboardFilter(StringBuilder sql, List<Object> params, Integer peak, Integer weeks) {
        appendSongBillboardFilter(sql, params, peak, null, weeks, null, null, null, null);
    }

    private void appendSongBillboardFilter(StringBuilder sql, List<Object> params, Integer peak, Integer weeks,
                                           String dateFrom, String dateTo) {
        appendSongBillboardFilter(sql, params, peak, null, weeks, null, null, dateFrom, dateTo);
    }

    private void appendSongBillboardFilter(StringBuilder sql, List<Object> params, Integer peak, String peakMode,
                                           Integer weeks, Integer weeksAtPeak, String weeksAtPeakMode,
                                           String dateFrom, String dateTo) {
        if (peak == null && weeks == null && weeksAtPeak == null && !SqlFilterHelper.hasValue(dateFrom) && !SqlFilterHelper.hasValue(dateTo)) {
            return;
        }

        sql.append(" AND EXISTS (SELECT 1 FROM (");
        sql.append("SELECT MIN(b.position) as peak, COUNT(DISTINCT b.chart_date) as weeks ");
        sql.append("FROM billboard_hot100_entry b ");
        sql.append("WHERE b.song_id = s.id");
        SqlFilterHelper.appendDateRangeBounds(sql, params, "b.chart_date", dateFrom, dateTo);
        sql.append(") countdown_stats WHERE countdown_stats.weeks > 0");
        if (peak != null) {
            SqlFilterHelper.appendChartPeakComparison(sql, params, "countdown_stats.peak", peak, peakMode);
        }
        if (weeks != null) {
            sql.append(" AND countdown_stats.weeks >= ?");
            params.add(weeks);
        }
        sql.append(")");
        if (weeksAtPeak != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM (");
            sql.append("SELECT b.position, COUNT(DISTINCT b.chart_date) as weeks_at_peak ");
            sql.append("FROM billboard_hot100_entry b ");
            sql.append("WHERE b.song_id = s.id");
            SqlFilterHelper.appendDateRangeBounds(sql, params, "b.chart_date", dateFrom, dateTo);
            sql.append(" GROUP BY b.position ORDER BY b.position ASC LIMIT 1");
            sql.append(") peak_stats WHERE 1=1");
            SqlFilterHelper.appendNumericComparison(sql, params, "peak_stats.weeks_at_peak", weeksAtPeak, weeksAtPeakMode, ">=");
            sql.append(")");
        }
    }
    
    private java.util.List<java.util.Map<String, Object>> getTopArtistsFiltered(String filterClause, java.util.List<Object> filterParams, String listenedDateFrom, String listenedDateTo, int limit) {
        java.util.List<Object> params = new java.util.ArrayList<>(filterParams);
        
        // Build date filter clause for play filtering
        StringBuilder dateFilter = new StringBuilder();
        java.util.List<Object> dateParams = new java.util.ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            dateFilter.append(" AND DATE(p.play_date) >= ?");
            dateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            dateFilter.append(" AND DATE(p.play_date) <= ?");
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
                    SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                    SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                    SUM(s.length_seconds) as time_listened,
                    MIN(p.play_date) as first_listened,
                    MAX(p.play_date) as last_listened
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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

        // Build date filter clause for play filtering
        StringBuilder dateFilter = new StringBuilder();
        java.util.List<Object> dateParams = new java.util.ArrayList<>();
        if (listenedDateFrom != null && !listenedDateFrom.isEmpty()) {
            dateFilter.append(" AND DATE(p.play_date) >= ?");
            dateParams.add(listenedDateFrom);
        }
        if (listenedDateTo != null && !listenedDateTo.isEmpty()) {
            dateFilter.append(" AND DATE(p.play_date) <= ?");
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
                    SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                    SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                    SUM(s.length_seconds) as time_listened,
                    MIN(p.play_date) as first_listened,
                    MAX(p.play_date) as last_listened
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
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
                SUM(CASE WHEN p.account = 'vatito' THEN 1 ELSE 0 END) as primary_plays,
                SUM(CASE WHEN p.account = 'robertlover' THEN 1 ELSE 0 END) as legacy_plays,
                COALESCE(s.length_seconds, 0) * COUNT(*) as time_listened,
                MIN(p.play_date) as first_listened,
                MAX(p.play_date) as last_listened
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
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
    
    // Helper: enrich top artists list with itunesPresence ratio
    private void enrichWithItunesPresenceByArtist(java.util.List<java.util.Map<String, Object>> artists, String itunesSongIdsJson) {
        if (artists == null || artists.isEmpty()) return;
        // Query: for each artist, count songs in iTunes
        java.util.Map<Integer, Integer> itunesCountByArtist = new java.util.HashMap<>();
        jdbcTemplate.query(
            "SELECT s.artist_id, COUNT(*) as itunes_count FROM Song s " +
            "WHERE s.id IN (SELECT value FROM json_each(?)) GROUP BY s.artist_id",
            rs -> { itunesCountByArtist.put(rs.getInt("artist_id"), rs.getInt("itunes_count")); },
            itunesSongIdsJson
        );
        for (java.util.Map<String, Object> row : artists) {
            int artistId = (Integer) row.get("id");
            int songCount = row.get("songCount") instanceof Integer ? (Integer) row.get("songCount") : 0;
            int itunesCount = itunesCountByArtist.getOrDefault(artistId, 0);
            Double ratio = songCount > 0 ? Math.round(itunesCount * 100.0 / songCount * 10.0) / 10.0 : null;
            row.put("itunesPresence", ratio);
        }
    }
    
    // Helper: enrich top albums list with itunesPresence ratio
    private void enrichWithItunesPresenceByAlbum(java.util.List<java.util.Map<String, Object>> albums, String itunesSongIdsJson) {
        if (albums == null || albums.isEmpty()) return;
        java.util.Map<Integer, Integer> itunesCountByAlbum = new java.util.HashMap<>();
        jdbcTemplate.query(
            "SELECT s.album_id, COUNT(*) as itunes_count FROM Song s " +
            "WHERE s.id IN (SELECT value FROM json_each(?)) AND s.album_id IS NOT NULL GROUP BY s.album_id",
            rs -> { itunesCountByAlbum.put(rs.getInt("album_id"), rs.getInt("itunes_count")); },
            itunesSongIdsJson
        );
        for (java.util.Map<String, Object> row : albums) {
            Object albumIdObj = row.get("id");
            if (albumIdObj == null) continue;
            int albumId = (Integer) albumIdObj;
            int songCount = row.get("songCount") instanceof Integer ? (Integer) row.get("songCount") : 0;
            int itunesCount = itunesCountByAlbum.getOrDefault(albumId, 0);
            Double ratio = songCount > 0 ? Math.round(itunesCount * 100.0 / songCount * 10.0) / 10.0 : null;
            row.put("itunesPresence", ratio);
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

    private String buildRequiredSongsExpression(String countExpression) {
        AppConfigService.AlbumFullListenConfig fullListenConfig = appConfigService.getAlbumFullListenConfig();
        return "CASE WHEN " + countExpression + " <= 6 THEN MAX(" + countExpression + " - " + fullListenConfig.allowedMissingUpTo6Tracks() + ", 1) "
                + "WHEN " + countExpression + " <= 10 THEN MAX(" + countExpression + " - " + fullListenConfig.allowedMissingUpTo10Tracks() + ", 1) "
                + "WHEN " + countExpression + " <= 20 THEN MAX(" + countExpression + " - " + fullListenConfig.allowedMissingUpTo20Tracks() + ", 1) "
                + "ELSE MAX(" + countExpression + " - " + fullListenConfig.allowedMissingOver20Tracks() + ", 1) END";
    }
}
