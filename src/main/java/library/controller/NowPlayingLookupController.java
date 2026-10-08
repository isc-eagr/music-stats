package library.controller;

import library.service.BillboardHot100Service;
import library.service.SongService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import library.service.ChartService;
import library.service.PcService;
import library.service.TrlService;
import library.dto.WeeklyChartStatsDTO;
import jakarta.servlet.http.HttpServletRequest;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class NowPlayingLookupController {
    private static final Logger logger = LoggerFactory.getLogger(NowPlayingLookupController.class);
    private static final int MAX_TRACK_CANDIDATES = 30;

    private final JdbcTemplate jdbcTemplate;
    private final SongService songService;
    private final ChartService chartService;
    private final BillboardHot100Service billboardHot100Service;
    private final PcService pcService;
    private final TrlService trlService;

    public NowPlayingLookupController(JdbcTemplate jdbcTemplate, SongService songService, ChartService chartService, BillboardHot100Service billboardHot100Service, PcService pcService, TrlService trlService) {
        this.jdbcTemplate = jdbcTemplate;
        this.songService = songService;
        this.chartService = chartService;
        this.billboardHot100Service = billboardHot100Service;
        this.pcService = pcService;
        this.trlService = trlService;
    }

    /**
     * JSON-body variant of {@link #lookup}. Values in a request body are never parsed as URL syntax,
     * so '#', '&' and '?' inside titles arrive intact. A form-encoded POST is handled by {@link #lookup} itself.
     */
    @PostMapping(value = "/lookup", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> lookupJson(@RequestBody Map<String, Object> body, HttpServletRequest request) {
        String songId = bodyText(body, "songId");
        return lookup(bodyText(body, "artist"), bodyText(body, "album"), bodyText(body, "song"),
                songId != null && songId.trim().matches("\\d+") ? Integer.valueOf(songId.trim()) : null, request);
    }

    /**
     * Same lookup as {@link #lookup}, served as a plain HTML page. The track picker links here
     * so a tapped candidate opens straight into its card.
     */
    @GetMapping(value = "/lookup/card", produces = MediaType.TEXT_HTML_VALUE)
    public String lookupCard(@RequestParam(required = false) String artist,
                             @RequestParam(required = false) String album,
                             @RequestParam(required = false) String song,
                             @RequestParam(name = "songId", required = false) Integer requestedSongId,
                             HttpServletRequest request) {
        return safe(lookup(artist, album, song, requestedSongId, request).get("html"));
    }

    @RequestMapping(value = "/lookup", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> lookup(@RequestParam(required = false) String artist,
                                      @RequestParam(required = false) String album,
                                      @RequestParam(required = false) String song,
                                      @RequestParam(name = "songId", required = false) Integer requestedSongId,
                                      HttpServletRequest request) {
        Map<String, Object> resp = new HashMap<>();

        // Reconstruct artist/song params that may contain an unencoded '&' character.
        // Apple Shortcuts often sends URLs without encoding '&' inside parameter values,
        // causing Spring to truncate e.g. "Wibal & Alex" to just "Wibal".
        // Strategy: find each param in the raw query string and determine its true end
        // by looking for the next known parameter delimiter (&song=, &artist=, &album=).
        String rawQuery = request.getQueryString();
        if (rawQuery != null) {
            int aIdx = rawQuery.indexOf("artist=");
            if (aIdx >= 0) {
                String afterA = rawQuery.substring(aIdx + "artist=".length());
                int endA = afterA.length();
                int s1 = afterA.indexOf("&song=");
                int s2 = afterA.indexOf("&album=");
                if (s1 >= 0 && s1 < endA) endA = s1;
                if (s2 >= 0 && s2 < endA) endA = s2;
                try {
                    String reconstructed = java.net.URLDecoder.decode(
                            afterA.substring(0, endA), java.nio.charset.StandardCharsets.UTF_8.name()).trim();
                    if (artist == null || reconstructed.length() > artist.trim().length()) {
                        artist = reconstructed;
                    }
                } catch (Exception ignored) {}
            }
            int sIdx = rawQuery.indexOf("song=");
            if (sIdx >= 0) {
                String afterS = rawQuery.substring(sIdx + "song=".length());
                int endS = afterS.length();
                int s1 = afterS.indexOf("&artist=");
                int s2 = afterS.indexOf("&album=");
                if (s1 >= 0 && s1 < endS) endS = s1;
                if (s2 >= 0 && s2 < endS) endS = s2;
                try {
                    String reconstructed = java.net.URLDecoder.decode(
                            afterS.substring(0, endS), java.nio.charset.StandardCharsets.UTF_8.name()).trim();
                    if (song == null || reconstructed.length() > song.trim().length()) {
                        song = reconstructed;
                    }
                } catch (Exception ignored) {}
            }
        }

        Integer songId = requestedSongId != null
                ? existingSongId(requestedSongId)
                : findSongId(artist, album, song, resp);
        if (songId == null && requestedSongId == null) {
            // A GET whose album contains an unencoded '#' arrives cut off at the '#', and every
            // parameter after it (usually the song) is gone. Log what arrived so that is visible.
            logger.info("Now playing lookup found no song. artist='{}', album='{}', song='{}', rawQuery='{}'",
                    artist, album, song, request.getQueryString());
        }

        resp.put("artistQuery", artist);
        resp.put("albumQuery", album);
        resp.put("songQuery", song);

        Integer albumId = null;
        Integer artistId = null;

        // If we found a song id, fetch its artist_id and album_id
        if (songId != null) {
            Map<String, Object> songRow = jdbcTemplate.queryForMap("SELECT id, artist_id, album_id FROM Song WHERE id = ?", songId);
            artistId = songRow.get("artist_id") != null ? ((Number) songRow.get("artist_id")).intValue() : null;
            albumId = songRow.get("album_id") != null ? ((Number) songRow.get("album_id")).intValue() : null;
        } else {
            // If no song match, try to find artist by name and album by (possibly truncated) name heuristically
            if (artist != null && !artist.isBlank()) {
                List<Map<String,Object>> artistMatches = jdbcTemplate.queryForList("SELECT id FROM Artist WHERE " + library.util.StringNormalizer.sqlNormalizeColumn("name") + " LIKE ? LIMIT 1", "%" + library.util.StringNormalizer.normalizeForSearch(artist) + "%");
                if (!artistMatches.isEmpty()) {
                    artistId = ((Number) artistMatches.get(0).get("id")).intValue();
                }
            }
            Map<String, Object> albumMatch = findAlbumByPrefix(artistId, album);
            if (albumMatch != null) {
                albumId = ((Number) albumMatch.get("id")).intValue();
                if (artistId == null && albumMatch.get("artist_id") != null) {
                    artistId = ((Number) albumMatch.get("artist_id")).intValue();
                }
            }
            List<Map<String, Object>> candidates = findTrackCandidates(artistId, albumId, song);
            if (!candidates.isEmpty()) resp.put("candidates", candidates);
        }

        // Compute counts
        Long songPlays = null;
        Long albumPlays = null;
        Long artistPlays = null;

        if (songId != null) {
            songPlays = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM Play WHERE song_id = ?", Long.class, songId);
            // song first/last listened and release date
            try {
                String sFirst = jdbcTemplate.queryForObject("SELECT MIN(p.play_date) FROM Play p WHERE p.song_id = ?", String.class, songId);
                String sLast = jdbcTemplate.queryForObject("SELECT MAX(p.play_date) FROM Play p WHERE p.song_id = ?", String.class, songId);
                String sRelease = jdbcTemplate.queryForObject("SELECT COALESCE(s.release_date, al.release_date) FROM Song s LEFT JOIN Album al ON s.album_id = al.id WHERE s.id = ?", String.class, songId);
                resp.put("songFirstListened", sFirst);
                resp.put("songLastListened", sLast);
                resp.put("songReleaseDate", sRelease);
            } catch (Exception ignored) {
            }
        }

        if (albumId != null) {
            albumPlays = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM Play p JOIN Song s ON p.song_id = s.id WHERE s.album_id = ?", Long.class, albumId);
        }

        if (artistId != null) {
            artistPlays = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM Play p JOIN Song s ON p.song_id = s.id WHERE s.artist_id = ?", Long.class, artistId);
        }

        resp.put("songId", songId);
        resp.put("albumId", albumId);
        resp.put("artistId", artistId);
        resp.put("songPlays", songPlays != null ? songPlays : 0);
        resp.put("albumPlays", albumPlays != null ? albumPlays : 0);
        resp.put("artistPlays", artistPlays != null ? artistPlays : 0);

        // Build detail URLs
        String base = request.getScheme() + "://" + request.getServerName();
        int port = request.getServerPort();
        if (!(request.getScheme().equals("http") && port == 80) && !(request.getScheme().equals("https") && port == 443)) {
            base += ":" + port;
        }

        if (artistId != null) resp.put("artistUrl", base + "/artists/" + artistId);
        if (albumId != null) resp.put("albumUrl", base + "/albums/" + albumId);
        if (songId != null) resp.put("songUrl", base + "/songs/" + songId);

        // Add ranking/chips data when we have a matched song
        if (songId != null) {
            try {
                try {
                    List<Map<String, Object>> yearlyChartHistory = chartService.getChartHistoryForItem(songId, "song", "yearly");
                    resp.put("yearlyChartHistory", yearlyChartHistory);
                } catch (Exception e) {}

                try {
                    Map<String, Object> billboardHot100Stats = billboardHot100Service.getStatsBySongId(songId);
                    resp.put("billboardHot100Stats", billboardHot100Stats);
                } catch (Exception e) {}

                try {
                    WeeklyChartStatsDTO weeklyStats = chartService.getSongWeeklyChartStats(songId);
                    if (weeklyStats != null && weeklyStats.hasCharted()) resp.put("weeklyChartStats", weeklyStats);
                } catch (Exception e) {}

                try {
                    Map<String, Object> pcStats = pcService.getPcStatsBySongId(songId);
                    if (pcStats != null) resp.put("pcStats", pcStats);
                } catch (Exception e) {}

                try {
                    Map<String, Object> trlStats = trlService.getTrlStatsBySongId(songId);
                    if (trlStats != null) resp.put("trlStats", trlStats);
                } catch (Exception e) {}

                Integer overall = songService.getSongOverallPosition(songId);
                Integer rankArtist = songService.getSongRankByArtist(songId);
                Integer rankAlbum = songService.getSongRankByAlbum(songId);
                resp.put("overallPosition", overall);
                resp.put("rankByArtist", rankArtist);
                resp.put("rankByAlbum", rankAlbum);

                Integer rankByReleaseYear = songService.getSongRankByReleaseYear(songId);
                Integer releaseYear = songService.getSongReleaseYear(songId);
                resp.put("rankByReleaseYear", rankByReleaseYear);
                resp.put("releaseYear", releaseYear);

                java.util.Map<Integer, Integer> ranksByYear = songService.getSongRanksByYear(songId);
                Integer currentYear = java.time.Year.now().getValue();
                if (ranksByYear != null && ranksByYear.containsKey(currentYear)) {
                    resp.put("rankByCurrentYear", ranksByYear.get(currentYear));
                }
                resp.put("currentYear", currentYear);

                java.util.Map<String, Integer> rankMap = songService.getAllSongRankings(songId);
                if (rankMap != null) {
                    if (rankMap.containsKey("gender")) resp.put("rankByGender", rankMap.get("gender"));
                    if (rankMap.containsKey("genre")) resp.put("rankByGenre", rankMap.get("genre"));
                    if (rankMap.containsKey("subgenre")) resp.put("rankBySubgenre", rankMap.get("subgenre"));
                    if (rankMap.containsKey("ethnicity")) resp.put("rankByEthnicity", rankMap.get("ethnicity"));
                    if (rankMap.containsKey("language")) resp.put("rankByLanguage", rankMap.get("language"));
                    if (rankMap.containsKey("country")) resp.put("rankByCountry", rankMap.get("country"));
                }
                if (songService.isSongSpanishRap(songId)) {
                    Integer rankSpRap = songService.getSongSpanishRapRank(songId);
                    resp.put("rankBySpanishRap", rankSpRap);
                }
                // Also compute projected rankings if this song had +1 play
                try {
                    // Overall projected position
                    try {
                        String projOverallSql = "SELECT rank FROM (" +
                                "SELECT s.id, ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(p.id), 0) + CASE WHEN s.id = ? THEN 1 ELSE 0 END DESC) as rank " +
                                "FROM Song s LEFT JOIN Play p ON p.song_id = s.id GROUP BY s.id) ranked WHERE id = ?";
                        Integer overallIfPlusOne = jdbcTemplate.queryForObject(projOverallSql, Integer.class, songId, songId);
                        resp.put("overallPositionIfPlusOne", overallIfPlusOne);
                    } catch (Exception ignored) {
                    }

                    // Artist projected
                    try {
                        String projArtistSql = "SELECT rank FROM (" +
                                "SELECT s.id, s.artist_id, ROW_NUMBER() OVER (PARTITION BY s.artist_id ORDER BY COALESCE(COUNT(p.id), 0) + CASE WHEN s.id = ? THEN 1 ELSE 0 END DESC) as rank " +
                                "FROM Song s LEFT JOIN Play p ON p.song_id = s.id GROUP BY s.id, s.artist_id) ranked WHERE id = ?";
                        Integer rankArtistIfPlusOne = jdbcTemplate.queryForObject(projArtistSql, Integer.class, songId, songId);
                        resp.put("rankByArtistIfPlusOne", rankArtistIfPlusOne);
                    } catch (Exception ignored) {
                    }

                    // Album projected
                    try {
                        String projAlbumSql = "SELECT rank FROM (" +
                                "SELECT s.id, s.album_id, ROW_NUMBER() OVER (PARTITION BY s.album_id ORDER BY COALESCE(COUNT(p.id), 0) + CASE WHEN s.id = ? THEN 1 ELSE 0 END DESC) as rank " +
                                "FROM Song s LEFT JOIN Play p ON p.song_id = s.id WHERE s.album_id IS NOT NULL GROUP BY s.id, s.album_id) ranked WHERE id = ?";
                        Integer rankAlbumIfPlusOne = jdbcTemplate.queryForObject(projAlbumSql, Integer.class, songId, songId);
                        resp.put("rankByAlbumIfPlusOne", rankAlbumIfPlusOne);
                    } catch (Exception ignored) {
                    }

                    // Demographic/genre projected ranks using adjusted play_count in CTE
                    try {
                        String projAllSql = "WITH song_play_counts AS (" +
                                " SELECT s.id, ar.gender_id, COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) as effective_genre_id, " +
                                " COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) as effective_subgenre_id, " +
                                " ar.ethnicity_id, COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) as effective_language_id, " +
                                " ar.country, COALESCE(COUNT(p.id), 0) + CASE WHEN s.id = ? THEN 1 ELSE 0 END as play_count, MIN(p.play_date) as first_play " +
                                " FROM Song s INNER JOIN Artist ar ON s.artist_id = ar.id LEFT JOIN Album alb ON s.album_id = alb.id LEFT JOIN Play p ON p.song_id = s.id " +
                                " GROUP BY s.id, ar.gender_id, effective_genre_id, effective_subgenre_id, ar.ethnicity_id, effective_language_id, ar.country" +
                                "), ranked_songs AS (" +
                                " SELECT id, gender_id, effective_genre_id, effective_subgenre_id, ethnicity_id, effective_language_id, country, play_count, first_play, " +
                                " CASE WHEN gender_id IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY gender_id ORDER BY play_count DESC, first_play ASC) END as gender_rank, " +
                                " CASE WHEN effective_genre_id IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY effective_genre_id ORDER BY play_count DESC, first_play ASC) END as genre_rank, " +
                                " CASE WHEN effective_subgenre_id IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY effective_subgenre_id ORDER BY play_count DESC, first_play ASC) END as subgenre_rank, " +
                                " CASE WHEN ethnicity_id IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY ethnicity_id ORDER BY play_count DESC, first_play ASC) END as ethnicity_rank, " +
                                " CASE WHEN effective_language_id IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY effective_language_id ORDER BY play_count DESC, first_play ASC) END as language_rank, " +
                                " CASE WHEN country IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY country ORDER BY play_count DESC, first_play ASC) END as country_rank " +
                                " FROM song_play_counts) SELECT gender_rank, genre_rank, subgenre_rank, ethnicity_rank, language_rank, country_rank FROM ranked_songs WHERE id = ?";

                        jdbcTemplate.query(projAllSql, rs -> {
                            Integer genderRank = (Integer) rs.getObject("gender_rank");
                            Integer genreRank = (Integer) rs.getObject("genre_rank");
                            Integer subgenreRank = (Integer) rs.getObject("subgenre_rank");
                            Integer ethnicityRank = (Integer) rs.getObject("ethnicity_rank");
                            Integer languageRank = (Integer) rs.getObject("language_rank");
                            Integer countryRank = (Integer) rs.getObject("country_rank");
                            if (genderRank != null) resp.put("rankByGenderIfPlusOne", genderRank);
                            if (genreRank != null) resp.put("rankByGenreIfPlusOne", genreRank);
                            if (subgenreRank != null) resp.put("rankBySubgenreIfPlusOne", subgenreRank);
                            if (ethnicityRank != null) resp.put("rankByEthnicityIfPlusOne", ethnicityRank);
                            if (languageRank != null) resp.put("rankByLanguageIfPlusOne", languageRank);
                            if (countryRank != null) resp.put("rankByCountryIfPlusOne", countryRank);
                        }, songId, songId);
                    } catch (Exception ignored) {
                    }
                    
                    // Release Year projected
                    if (releaseYear != null) {
                        try {
                            String projRelYearSql = "SELECT rank FROM (" +
                                "SELECT s.id, strftime('%Y', COALESCE(s.release_date, alb.release_date)) as rel_year, " +
                                "ROW_NUMBER() OVER (PARTITION BY strftime('%Y', COALESCE(s.release_date, alb.release_date)) " +
                                "ORDER BY COALESCE(COUNT(p.id), 0) + CASE WHEN s.id = ? THEN 1 ELSE 0 END DESC) as rank " +
                                "FROM Song s LEFT JOIN Album alb ON s.album_id = alb.id LEFT JOIN Play p ON p.song_id = s.id " +
                                "WHERE COALESCE(s.release_date, alb.release_date) IS NOT NULL " +
                                "GROUP BY s.id, rel_year) ranked WHERE id = ?";
                            Integer rankRelYearIfPlusOne = jdbcTemplate.queryForObject(projRelYearSql, Integer.class, songId, songId);
                            resp.put("rankByReleaseYearIfPlusOne", rankRelYearIfPlusOne);
                        } catch (Exception ignored) {}
                    }

                    // Current Year projected
                    try {
                        String cyStr = currentYear.toString();
                        String projCurrYearSql = "SELECT rank FROM (" +
                            "SELECT s.id, " +
                            "ROW_NUMBER() OVER (ORDER BY " +
                            "SUM(CASE WHEN strftime('%Y', p.play_date) = ? THEN 1 ELSE 0 END) + " +
                            "CASE WHEN s.id = ? THEN 1 ELSE 0 END DESC) as rank " +
                            "FROM Song s LEFT JOIN Play p ON p.song_id = s.id " +
                            "GROUP BY s.id) ranked WHERE id = ?";
                        Integer rankCurrYearIfPlusOne = jdbcTemplate.queryForObject(projCurrYearSql, Integer.class, cyStr, songId, songId);
                        resp.put("rankByCurrentYearIfPlusOne", rankCurrYearIfPlusOne);
                    } catch (Exception ignored) {}

                    // Spanish rap projected
                    if (songService.isSongSpanishRap(songId)) {
                        try {
                            String projSpRapSql = "SELECT rank FROM (" +
                                "SELECT s.id, ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(p.id), 0) + CASE WHEN s.id = ? THEN 1 ELSE 0 END DESC) as rank " +
                                "FROM Song s " +
                                "INNER JOIN Artist ar ON s.artist_id = ar.id " +
                                "LEFT JOIN Album alb ON s.album_id = alb.id " +
                                "INNER JOIN Genre g ON COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) = g.id " +
                                "INNER JOIN Language l ON COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) = l.id " +
                                "LEFT JOIN Play p ON p.song_id = s.id " +
                                "WHERE g.name = 'Rap' AND l.name = 'Spanish' " +
                                "GROUP BY s.id) ranked WHERE id = ?";
                            Integer rankSpRapIfPlusOne = jdbcTemplate.queryForObject(projSpRapSql, Integer.class, songId, songId);
                            resp.put("rankBySpanishRapIfPlusOne", rankSpRapIfPlusOne);
                        } catch (Exception ignored) {}
                    }
                } catch (Exception ignored) {
                }
            } catch (Exception ignored) {
            }
        }

        // Build HTML snippet so Shortcuts can Quick Look a single field
        try {
            String trueSong = ""; String trueArtist = ""; String trueAlbum = "";
            String genderName = ""; String genreName = ""; String subgenreName = ""; 
            String languageName = ""; String countryName = "";

            if (songId != null) {
                String namesSql = "SELECT s.name as song_true_name, ar.name as artist_true_name, alb.name as album_true_name, " +
                        "(SELECT name FROM Gender WHERE id = ar.gender_id) as gender_name, " +
                        "(SELECT name FROM Genre WHERE id = COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id)) as genre_name, " +
                        "(SELECT name FROM SubGenre WHERE id = COALESCE(s.override_subgenre_id, alb.override_subgenre_id, ar.subgenre_id)) as subgenre_name, " +
                        "(SELECT name FROM Language WHERE id = COALESCE(s.override_language_id, alb.override_language_id, ar.language_id)) as language_name, " +
                        "ar.country as country_name " +
                        "FROM Song s JOIN Artist ar ON s.artist_id = ar.id LEFT JOIN Album alb ON s.album_id = alb.id WHERE s.id = ?";
                try {
                    Map<String, Object> namesMap = jdbcTemplate.queryForMap(namesSql, songId);
                    trueSong = safe(namesMap.get("song_true_name"));
                    trueArtist = safe(namesMap.get("artist_true_name"));
                    trueAlbum = safe(namesMap.get("album_true_name"));
                    genderName = safe(namesMap.get("gender_name"));
                    genreName = safe(namesMap.get("genre_name"));
                    subgenreName = safe(namesMap.get("subgenre_name"));
                    languageName = safe(namesMap.get("language_name"));
                    countryName = safe(namesMap.get("country_name"));
                    
                    resp.put("songName", trueSong);
                    resp.put("artistName", trueArtist);
                    resp.put("albumName", trueAlbum);
                    resp.put("genderName", genderName);
                    resp.put("genreName", genreName);
                    resp.put("subgenreName", subgenreName);
                    resp.put("languageName", languageName);
                    resp.put("countryName", countryName);
                } catch (Exception ignored) {}
            } else {
                // No song, but the artist/album may still have resolved from what arrived
                try {
                    if (artistId != null) {
                        Map<String, Object> artistRow = jdbcTemplate.queryForMap("SELECT ar.name, (SELECT name FROM Gender WHERE id = ar.gender_id) as gender_name FROM Artist ar WHERE ar.id = ?", artistId);
                        trueArtist = safe(artistRow.get("name"));
                        genderName = safe(artistRow.get("gender_name"));
                    }
                    if (albumId != null) {
                        trueAlbum = safe(jdbcTemplate.queryForObject("SELECT name FROM Album WHERE id = ?", String.class, albumId));
                    }
                } catch (Exception ignored) {}
            }

            if (trueSong.isEmpty()) trueSong = safe(resp.get("songQuery"));
            if (trueSong.isEmpty() && resp.containsKey("candidates")) trueSong = "Which song?";
            if (trueArtist.isEmpty()) trueArtist = safe(resp.get("artistQuery"));
            if (trueAlbum.isEmpty()) trueAlbum = safe(resp.get("albumQuery"));

            // UI Color based on Gender
            String themeColor = "#1f8feb"; // Blue Default
            String themeBg = "rgba(31,143,235,0.1)";
            String themeBorder = "rgba(31,143,235,0.3)";
            
            if ("Female".equalsIgnoreCase(genderName)) {
                themeColor = "#f06292"; // Pink
                themeBg = "rgba(240,98,146,0.1)";
                themeBorder = "rgba(240,98,146,0.3)";
            } else if ("Male".equalsIgnoreCase(genderName) || genderName.isEmpty()) {
                // Keep default blue
            } else {
                themeColor = "#ba68c8"; // Purple for missing/mixed
                themeBg = "rgba(186,104,200,0.1)";
                themeBorder = "rgba(186,104,200,0.3)";
            }

            String overall = safe(resp.get("overallPosition"));
            String rankArtist = safe(resp.get("rankByArtist"));
            String rankAlbum = safe(resp.get("rankByAlbum"));
            String rankGender = safe(resp.get("rankByGender"));
            String rankGenre = safe(resp.get("rankByGenre"));
            String rankSubgenre = safe(resp.get("rankBySubgenre"));
            String rankLanguage = safe(resp.get("rankByLanguage"));
            String rankCountry = safe(resp.get("rankByCountry"));
            String rankSpanishRap = safe(resp.get("rankBySpanishRap"));

            String rankRelYear = safe(resp.get("rankByReleaseYear"));
            String relYearStr = safe(resp.get("releaseYear"));
            String rankCurYear = safe(resp.get("rankByCurrentYear"));
            String curYearStr = safe(resp.get("currentYear"));

            String overallIf = safe(resp.get("overallPositionIfPlusOne"));
            String rankArtistIf = safe(resp.get("rankByArtistIfPlusOne"));
            String rankAlbumIf = safe(resp.get("rankByAlbumIfPlusOne"));
            String rankGenderIf = safe(resp.get("rankByGenderIfPlusOne"));
            String rankGenreIf = safe(resp.get("rankByGenreIfPlusOne"));
            String rankSubgenreIf = safe(resp.get("rankBySubgenreIfPlusOne"));
            String rankLanguageIf = safe(resp.get("rankByLanguageIfPlusOne"));
            String rankCountryIf = safe(resp.get("rankByCountryIfPlusOne"));
            String rankSpanishRapIf = safe(resp.get("rankBySpanishRapIfPlusOne"));
            
            String rankRelYearIf = safe(resp.get("rankByReleaseYearIfPlusOne"));
            String rankCurYearIf = safe(resp.get("rankByCurrentYearIfPlusOne"));

            String release = safe(resp.get("songReleaseDate"));
            if (release.length() > 10) release = release.substring(0, 10);
            String first = safe(resp.get("songFirstListened"));
            if (first.length() > 10) first = first.substring(0, 10);
            String last = safe(resp.get("songLastListened"));
            if (last.length() > 10) last = last.substring(0, 10);
            String lastWithDaysSince = last + formatDaysSinceDate(last);
            
            String artistUrlS = safe(resp.get("artistUrl"));
            String albumUrlS = safe(resp.get("albumUrl"));
            String songUrlS = safe(resp.get("songUrl"));

            StringBuilder h = new StringBuilder();
            h.append("<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Now Playing</title>");
            h.append("<style>:root{--bg:#0b0f14;--card:#151b23;--muted:#8b949e;--text:#c9d1d9;");
            h.append("--accent:").append(themeColor).append(";");
            h.append("--chip-bg:").append(themeBg).append(";");
            h.append("--chip-border:").append(themeBorder).append(";}");
            h.append("body{margin:0;padding:12px;font-family:-apple-system,sans-serif;background:var(--bg);color:var(--text)}");
            h.append(".card{background:var(--card);border:1px solid rgba(255,255,255,.05);border-radius:12px;padding:16px;box-shadow:0 8px 24px rgba(0,0,0,.4)}");
            h.append(".title{font-size:20px;font-weight:700;color:#fff;margin:0 0 4px;line-height:1.2}");
            h.append(".sub{font-size:15px;color:var(--muted);margin:0 0 16px}");
            h.append(".chips{display:block;margin-bottom:16px}");
            h.append(".chip{background:var(--chip-bg);border:1px solid var(--chip-border);border-radius:6px;padding:6px 10px;font-size:13px;color:#fff;display:inline-flex;align-items:center;gap:6px;vertical-align:middle;margin:0 6px 6px 0}");
            h.append(".chip .val{font-weight:700;color:var(--accent)}");
            h.append(".chip-link{text-decoration:none}");
            h.append(".chip-proj{background:var(--chip-bg);border:1px dashed var(--accent);border-radius:6px;padding:6px 10px;font-size:13px;color:#fff;display:inline-flex;align-items:center;gap:6px;vertical-align:middle;margin:0 6px 6px 0}");
            h.append(".chip-proj .val{font-weight:600;color:var(--accent)}");
            h.append(".chip-yearly{color:#fff;background:linear-gradient(135deg, rgba(20,12,0,0.65), rgba(255,215,0,0.25))!important;border:1px solid rgba(255,215,0,0.8)!important;border-radius:6px;padding:6px 10px;font-size:13px;display:inline-flex;align-items:center;gap:6px;vertical-align:middle;font-weight:700!important;box-shadow:0 0 12px rgba(255,215,0,0.5)!important;margin:0 6px 6px 0;border-style:solid!important}");
            h.append(".chip-yearly .val{color:#ffe680!important;text-shadow:0 1px 3px rgba(0,0,0,0.6)}");
            h.append(".date-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:16px}");
            h.append(".date-tile{background:var(--chip-bg);padding:12px 8px;border-radius:8px;text-align:center;border:1px solid var(--chip-border)}");
            h.append(".date-tile .num{font-size:13px;font-weight:600;color:var(--accent);margin-bottom:4px}");
            h.append(".date-tile .lbl{font-size:11px;color:var(--muted);text-transform:uppercase}");
            h.append("a.header-link{color:var(--accent);text-decoration:none;transition:opacity 0.2s}");
            h.append("a.header-link:hover{opacity:0.8}");
            h.append(".small-muted{color:var(--muted);font-size:12px;margin:16px 0 8px}");
            h.append(".rank-arrow{color:var(--muted);font-size:11px;margin:0 2px}");
            h.append(".proj-val{font-weight:600;color:rgba(255,255,255,0.65);font-size:12px}");
            h.append(".rank-delta{font-size:11px;font-weight:700}");
            h.append(".rank-delta.up{color:#4ade80}");
            h.append(".rank-delta.down{color:#f87171}");
            h.append(".chip-new{background:var(--chip-bg);border:1px dashed var(--accent);border-radius:6px;padding:6px 10px;font-size:13px;color:#fff;display:inline-flex;align-items:center;gap:6px;vertical-align:middle;margin:0 6px 6px 0}");
            h.append(".new-badge{font-size:10px;color:var(--accent);font-weight:600;opacity:0.8}");
            h.append(".chip-peak-one{color:#fff!important;background:linear-gradient(135deg, rgba(20,12,0,0.65), rgba(255,215,0,0.25))!important;border:1px solid rgba(255,215,0,0.8)!important;font-weight:700!important;box-shadow:0 0 12px rgba(255,215,0,0.5)!important;border-style:solid!important}");
            h.append(".chip-peak-one .val{color:#ffe680!important;text-shadow:0 1px 3px rgba(0,0,0,0.6)}</style>");
            
            h.append("</head><body><div class=\"card\">\n");
            String songHtml = !songUrlS.isEmpty() ? "<a class=\"header-link\" href=\"" + songUrlS + "\">" + trueSong + "</a>" : trueSong;
            h.append("<p class=\"title\">").append(songHtml).append("</p>");
            String artistHtml = !artistUrlS.isEmpty() ? "<a class=\"header-link\" href=\"" + artistUrlS + "\">" + trueArtist + "</a>" : trueArtist;
            String albumHtml = !trueAlbum.isEmpty() ? (!albumUrlS.isEmpty() ? "<a class=\"header-link\" href=\"" + albumUrlS + "\">" + trueAlbum + "</a>" : trueAlbum) : "";
            String subLine = artistHtml;
            if (!albumHtml.isEmpty()) subLine += " • " + albumHtml;
            h.append("<p class=\"sub\">").append(subLine).append("</p>");

            // Track picker: the song title did not arrive, so offer the likely tracks as one-tap links
            if (resp.get("candidates") instanceof List<?> candidates) {
                h.append("<p class=\"small-muted\">The song title didn't make it to the server (usually a # in the album name). Tap the track that's playing:</p>");
                h.append("<div class=\"chips\">");
                for (Object item : candidates) {
                    if (!(item instanceof Map<?, ?> candidate)) continue;
                    String trackNumber = safe(candidate.get("track_number"));
                    h.append("<a class=\"chip chip-link\" href=\"").append(base).append("/api/lookup/card?songId=").append(safe(candidate.get("id"))).append("\">");
                    if (!trackNumber.isEmpty()) h.append("<span class=\"val\">").append(trackNumber).append("</span> ");
                    h.append(escapeHtml(safe(candidate.get("name"))))
                     .append(" <span class=\"proj-val\">").append(safe(candidate.get("plays"))).append(" plays</span></a>");
                }
                h.append("</div>");
            }

            // Stats & Dates section
            String sPlays = safe(resp.get("songPlays"));
            String alPlays = safe(resp.get("albumPlays"));
            String arPlays = safe(resp.get("artistPlays"));

            h.append("<div class=\"date-grid\">");
            h.append("<div class=\"date-tile\"><div class=\"num\">").append(sPlays).append("</div><div class=\"lbl\">Song Plays</div></div>");
            h.append("<div class=\"date-tile\"><div class=\"num\">").append(alPlays).append("</div><div class=\"lbl\">Album Plays</div></div>");
            h.append("<div class=\"date-tile\"><div class=\"num\">").append(arPlays).append("</div><div class=\"lbl\">Artist Plays</div></div>");
            h.append("<div class=\"date-tile\"><div class=\"num\">").append(release).append("</div><div class=\"lbl\">Release</div></div>");
            h.append("<div class=\"date-tile\"><div class=\"num\">").append(first).append("</div><div class=\"lbl\">First Listened</div></div>");
            h.append("<div class=\"date-tile\"><div class=\"num\">").append(lastWithDaysSince).append("</div><div class=\"lbl\">Last Listened</div></div>");
            h.append("</div>");

            // Combined Ranks Section (current + projected together)
            h.append("<div class=\"chips\">");

            appendCombinedChipHTML(h, overall, overallIf, "Overall");
            appendCombinedChipHTML(h, rankArtist, rankArtistIf, trueArtist);
            appendCombinedChipHTML(h, rankAlbum, rankAlbumIf, trueAlbum);
            appendCombinedChipHTML(h, rankGender, rankGenderIf, genderName);
            appendCombinedChipHTML(h, rankGenre, rankGenreIf, genreName);
            appendCombinedChipHTML(h, rankSubgenre, rankSubgenreIf, subgenreName);
            appendCombinedChipHTML(h, rankLanguage, rankLanguageIf, languageName);
            appendCombinedChipHTML(h, rankCountry, rankCountryIf, countryName);
            appendCombinedChipHTML(h, rankSpanishRap, rankSpanishRapIf, "Spanish Rap");
            if (!rankRelYear.isEmpty() || !rankRelYearIf.isEmpty()) {
                if (!relYearStr.isEmpty()) appendCombinedChipHTML(h, rankRelYear, rankRelYearIf, "released in " + relYearStr);
            }
            if (!rankCurYear.isEmpty() || !rankCurYearIf.isEmpty()) {
                if (!curYearStr.isEmpty()) appendCombinedChipHTML(h, rankCurYear, rankCurYearIf, "most listened in " + curYearStr);
            }
            h.append("</div>");

            // Chart Filter Chips Group (separated with spacing)
            h.append("<div class=\"chips\" style=\"margin-top:16px;padding-top:16px;border-top:1px solid rgba(200,200,200,0.15)\">");

            Object rawChart = resp.get("yearlyChartHistory");
            if (rawChart instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> chartList = (List<Map<String, Object>>) rawChart;
                for (Map<String, Object> map : chartList) {
                    if (map != null && map.containsKey("displayName")) {
                        String disp = safe(map.get("displayName"));
                        String formattedDisp = escapeHtml(disp).replaceAll("^(#[0-9]+)", "<span class=\"val\">$1</span>");
                        h.append("<div class=\"chip-yearly\"><span>")
                         .append("\u2b50 ").append(formattedDisp)
                         .append("</span></div>");
                    }
                }
            }

            Object rawWeeklyStats = resp.get("weeklyChartStats");
            if (rawWeeklyStats instanceof WeeklyChartStatsDTO weeklyStats) {
                int totalWeeks = weeklyStats.getTotalWeeks() != null ? weeklyStats.getTotalWeeks() : 0;
                Integer peakPos = weeklyStats.getPeakPosition();
                Integer weeksAtPeakW = weeklyStats.getWeeksAtPeak();
                if (totalWeeks > 0 && peakPos != null) {
                    String weeklyPeakOne = peakPos == 1 ? " chip-peak-one" : "";
                    h.append("<a class=\"chip chip-link").append(weeklyPeakOne).append("\" href=\"").append(base).append("/charts/weekly\" target=\"_blank\"><span>");
                    if (peakPos == 1) h.append("\u2b50 ");
                    h.append("<span class=\"val\">").append(totalWeeks).append("</span> weeks on the Weekly Chart");
                    h.append(" (Peak <span class=\"val\">#").append(peakPos).append("</span>");
                    if (weeksAtPeakW != null) {
                        h.append(" for <span class=\"val\">").append(weeksAtPeakW).append("</span> ").append(weeksAtPeakW == 1 ? "week)" : "weeks)");
                    } else {
                        h.append(")");
                    }
                    h.append("</span></a>");
                }
            }

            Object rawBillboardStats = resp.get("billboardHot100Stats");
            if (rawBillboardStats instanceof Map<?, ?> billboardStats) {
                String weeksOnChart = safe(billboardStats.get("weeksOnChart"));
                String peakPosition = safe(billboardStats.get("peakPosition"));
                String weeksAtPeak = safe(billboardStats.get("weeksAtPeak"));
                if (!weeksOnChart.isEmpty()) {
                    String billboardPeakOne = "1".equals(peakPosition) ? " chip-peak-one" : "";
                    h.append("<a class=\"chip chip-link").append(billboardPeakOne).append("\" href=\"").append(base).append("/misc/billboard-hot-100\" target=\"_blank\"><span>");
                    if ("1".equals(peakPosition)) h.append("\u2b50 ");
                    h.append("<span class=\"val\">").append(escapeHtml(weeksOnChart)).append("</span> weeks on the Hot 100");
                    if (!peakPosition.isEmpty()) {
                        h.append(" (Peak <span class=\"val\">#").append(escapeHtml(peakPosition)).append("</span>");
                        if (!weeksAtPeak.isEmpty() && !"0".equals(weeksAtPeak)) {
                            h.append(" for <span class=\"val\">").append(escapeHtml(weeksAtPeak)).append("</span> ").append("1".equals(weeksAtPeak) ? "week)" : "weeks)");
                        } else {
                            h.append(")");
                        }
                    }
                    h.append("</span></a>");
                }
            }

            Object rawPcStats = resp.get("pcStats");
            if (rawPcStats instanceof Map<?, ?> pcStats) {
                Object daysObj = pcStats.get("daysOnCountdown");
                Object peakObj = pcStats.get("peakPosition");
                Object daysAtPeakObj = pcStats.get("daysAtPeak");
                if (daysObj != null) {
                    String daysStr = daysObj.toString();
                    String peakStr = peakObj != null ? peakObj.toString() : "";
                    String daysAtPeakStr = daysAtPeakObj != null ? daysAtPeakObj.toString() : "";
                    String pcPeakOne = "1".equals(peakStr) ? " chip-peak-one" : "";
                    h.append("<a class=\"chip chip-link").append(pcPeakOne).append("\" href=\"").append(base).append("/misc/vatos-cuntdown\" target=\"_blank\"><span>");
                    if ("1".equals(peakStr)) h.append("\u2b50 ");
                    h.append("<span class=\"val\">").append(escapeHtml(daysStr)).append("</span> Days on Vato's Cuntdown");
                    if (!peakStr.isEmpty()) {
                        h.append(" (Peak <span class=\"val\">#").append(escapeHtml(peakStr)).append("</span>");
                        if (!daysAtPeakStr.isEmpty()) {
                             h.append(" for <span class=\"val\">").append(escapeHtml(daysAtPeakStr)).append("</span> ").append("1".equals(daysAtPeakStr) ? "day)" : "days)");
                        } else {
                            h.append(")");
                        }
                    }
                    h.append("</span></a>");
                }
            }

            Object rawTrlStats = resp.get("trlStats");
            if (rawTrlStats instanceof Map<?, ?> trlStats) {
                Object daysObj = trlStats.get("daysOnCountdown");
                Object peakObj = trlStats.get("peakPosition");
                Object daysAtPeakObj = trlStats.get("daysAtPeak");
                if (daysObj != null) {
                    String daysStr = daysObj.toString();
                    String peakStr = peakObj != null ? peakObj.toString() : "";
                    String daysAtPeakStr = daysAtPeakObj != null ? daysAtPeakObj.toString() : "";
                    String trlPeakOne = "1".equals(peakStr) ? " chip-peak-one" : "";
                    h.append("<a class=\"chip chip-link").append(trlPeakOne).append("\" href=\"").append(base).append("/misc/trl\" target=\"_blank\"><span>");
                    if ("1".equals(peakStr)) h.append("\u2b50 ");
                    h.append("<span class=\"val\">").append(escapeHtml(daysStr)).append("</span> Days on TRL");
                    if (!peakStr.isEmpty()) {
                        h.append(" (Peak <span class=\"val\">#").append(escapeHtml(peakStr)).append("</span>");
                        if (!daysAtPeakStr.isEmpty()) {
                            h.append(" for <span class=\"val\">").append(escapeHtml(daysAtPeakStr)).append("</span> ").append("1".equals(daysAtPeakStr) ? "day)" : "days)");
                        } else {
                            h.append(")");
                        }
                    }
                    h.append("</span></a>");
                }
            }

            h.append("</div>");

            h.append("</div></body></html>");
            resp.put("html", h.toString());
        } catch (Exception ignored) {
        }

        return resp;
    }

    /**
     * Resolves the song: strict artist/album/song match first (same rules as the iTunes badges),
     * then a DB-wide strict key lookup, then a fuzzy artist + song match that ignores parentheses
     * and featuring tokens (e.g. "Top Top (feat. C-Kan, Gera MX...)" stored without the paren text).
     * Without a song title there is nothing to resolve, so this returns null instead of guessing.
     */
    private Integer findSongId(String artist, String album, String song, Map<String, Object> resp) {
        if (song == null || song.isBlank()) {
            resp.put("matches", List.of());
            return null;
        }
        StringBuilder sql = new StringBuilder(
            "SELECT s.id, s.name, a.name as artist_name, al.name as album_name, " +
            "CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id) THEN 1 ELSE 0 END as has_image " +
            "FROM Song s JOIN Artist a ON s.artist_id = a.id LEFT JOIN Album al ON s.album_id = al.id WHERE 1=1 "
        );
        List<Object> params = new ArrayList<>();
        if (artist != null && !artist.isBlank()) {
            sql.append(" AND " + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ?");
            params.add("%" + library.util.StringNormalizer.normalizeForSearch(artist) + "%");
        }
        sql.append(" AND " + library.util.StringNormalizer.sqlNormalizeColumn("s.name") + " LIKE ?");
        params.add("%" + library.util.StringNormalizer.normalizeForSearch(song) + "%");
        sql.append(" ORDER BY a.name, s.name LIMIT 20");
        List<Map<String, Object>> matches = jdbcTemplate.queryForList(sql.toString(), params.toArray());
        resp.put("matches", matches);

        String inputStrictArtist = artist != null ? normalizeForStrictMatch(artist) : null;
        String inputStrictSong = normalizeForStrictMatch(song);
        String inputStrictAlbum = album != null ? normalizeForStrictMatch(album) : null;
        for (Map<String, Object> m : matches) {
            String dbStrictName = normalizeForStrictMatch(safe(m.get("name")));
            String dbStrictArtist = m.get("artist_name") != null ? normalizeForStrictMatch(m.get("artist_name").toString()) : null;
            String dbStrictAlbum = m.get("album_name") != null ? normalizeForStrictMatch(m.get("album_name").toString()) : null;

            boolean artistMatches = inputStrictArtist == null || inputStrictArtist.equals(dbStrictArtist);
            boolean songMatches = inputStrictSong.equals(dbStrictName);
            boolean albumMatches = inputStrictAlbum == null || inputStrictAlbum.equals(dbStrictAlbum);
            if (artistMatches && songMatches && albumMatches && m.get("id") instanceof Number id) {
                return id.intValue();
            }
        }

        // DB-wide strict lookup that mirrors ItunesService.buildDatabaseLookupStrict()
        try {
            String sqlAll = "SELECT s.id as id, ar.name as artist_name, al.name as album_name, s.name as song_name " +
                    "FROM Song s INNER JOIN Artist ar ON s.artist_id = ar.id LEFT JOIN Album al ON s.album_id = al.id";
            Map<String, Integer> strictKeyToId = new HashMap<>();
            jdbcTemplate.query(sqlAll, rs -> {
                String key = normalizeForStrictMatch(rs.getString("artist_name")) + "||" + normalizeForStrictMatch(rs.getString("album_name")) + "||" + normalizeForStrictMatch(rs.getString("song_name"));
                strictKeyToId.put(key, rs.getInt("id"));
            });
            Integer strictId = strictKeyToId.get(normalizeForStrictMatch(artist) + "||" + normalizeForStrictMatch(album) + "||" + inputStrictSong);
            if (strictId != null) return strictId;
        } catch (Exception ignored) {
        }

        String fuzzyInputSong = library.util.StringNormalizer.normalizeForSearch(song);
        String fuzzyInputArtist = artist != null ? library.util.StringNormalizer.normalizeForSearch(artist) : null;
        for (Map<String, Object> m : matches) {
            String dbFuzzyName = library.util.StringNormalizer.normalizeForSearch(safe(m.get("name")));
            String dbFuzzyArtist = m.get("artist_name") != null ? library.util.StringNormalizer.normalizeForSearch(m.get("artist_name").toString()) : null;
            boolean artistMatchesFuzzy = fuzzyInputArtist == null || fuzzyInputArtist.equals(dbFuzzyArtist);
            if (artistMatchesFuzzy && fuzzyInputSong.equals(dbFuzzyName) && m.get("id") instanceof Number id) {
                return id.intValue();
            }
        }
        return null;
    }

    private Integer existingSongId(int songId) {
        List<Integer> ids = jdbcTemplate.queryForList("SELECT id FROM Song WHERE id = ?", Integer.class, songId);
        return ids.isEmpty() ? null : ids.get(0);
    }

    /**
     * Finds the album whose name equals, or failing that starts with, the received name. Only a prefix
     * arrives when a GET carries an unencoded '#' ("B.O.A.T.S. II #METIME" arrives as "B.O.A.T.S. II ").
     */
    private Map<String, Object> findAlbumByPrefix(Integer artistId, String album) {
        String prefix = normalizeForStrictMatch(album);
        if (prefix.isEmpty()) return null;
        List<Map<String, Object>> albums = artistId != null
                ? jdbcTemplate.queryForList("SELECT id, artist_id, name FROM Album WHERE artist_id = ? ORDER BY id", artistId)
                : jdbcTemplate.queryForList("SELECT id, artist_id, name FROM Album ORDER BY id");
        Map<String, Object> prefixMatch = null;
        for (Map<String, Object> row : albums) {
            String name = normalizeForStrictMatch(safe(row.get("name")));
            if (name.equals(prefix)) return row;
            if (prefixMatch == null && name.startsWith(prefix)) prefixMatch = row;
        }
        return prefixMatch;
    }

    /**
     * Tracks to offer when the song could not be resolved: the album's tracklist when the album is known,
     * otherwise the artist's most played songs. A received (possibly truncated) title narrows the list
     * to titles starting with it.
     */
    private List<Map<String, Object>> findTrackCandidates(Integer artistId, Integer albumId, String song) {
        if (albumId == null && artistId == null) return List.of();
        String sql = "SELECT s.id, s.name, s.track_number, (SELECT COUNT(*) FROM Play p WHERE p.song_id = s.id) AS plays FROM Song s "
                + (albumId != null
                    ? "WHERE s.album_id = ? ORDER BY s.track_number IS NULL, s.track_number, s.name"
                    : "WHERE s.artist_id = ? ORDER BY plays DESC, s.name");
        String songPrefix = normalizeForStrictMatch(song);
        List<Map<String, Object>> candidates = new ArrayList<>();
        for (Map<String, Object> row : jdbcTemplate.queryForList(sql, albumId != null ? albumId : artistId)) {
            if (!songPrefix.isEmpty() && !normalizeForStrictMatch(safe(row.get("name"))).startsWith(songPrefix)) continue;
            candidates.add(row);
            if (candidates.size() == MAX_TRACK_CANDIDATES) break;
        }
        return candidates;
    }

    private static String bodyText(Map<String, Object> body, String key) {
        Object value = body.get(key);
        return value != null ? value.toString() : null;
    }

    private void appendCombinedChipHTML(StringBuilder sb, String currentRank, String projectedRank, String name) {
        if (name == null || name.isEmpty() || "null".equalsIgnoreCase(name)) return;
        boolean hasCurrent = currentRank != null && !currentRank.isEmpty();
        boolean hasProjected = projectedRank != null && !projectedRank.isEmpty();
        if (!hasCurrent && !hasProjected) return;

        if (hasCurrent) {
            String projHtml = "";
            if (hasProjected && !projectedRank.equals(currentRank)) {
                try {
                    int curr = Integer.parseInt(currentRank);
                    int proj = Integer.parseInt(projectedRank);
                    int diff = curr - proj;
                    if (diff > 0) {
                        projHtml = " <span class=\"rank-arrow\">\u2192</span>"
                                 + " <span class=\"proj-val\">#" + projectedRank + "</span>"
                                 + " <span class=\"rank-delta up\">\u25b2" + diff + "</span>";
                    } else if (diff < 0) {
                        projHtml = " <span class=\"rank-arrow\">\u2192</span>"
                                 + " <span class=\"proj-val\">#" + projectedRank + "</span>"
                                 + " <span class=\"rank-delta down\">\u25bc" + Math.abs(diff) + "</span>";
                    }
                } catch (Exception ignored) { }
            }
            sb.append("<div class=\"chip\"><span class=\"val\">#").append(currentRank).append("</span> ")
              .append(name).append(projHtml).append("</div>");
        } else {
            // New entry: would rank for the first time if this play is recorded
            sb.append("<div class=\"chip chip-new\"><span class=\"proj-val\">#").append(projectedRank).append("</span> ")
              .append(name).append(" <span class=\"new-badge\">new</span></div>");
        }
    }

    // Replicate ItunesService.normalizeForStrictMatch behavior here so matching
    // in this controller uses the same rules as the iTunes badges.
    private String normalizeForStrictMatch(String input) {
        if (input == null || input.isBlank()) return "";
        // strip accents (uses StringNormalizer which is available in the project)
        String result = library.util.StringNormalizer.stripAccents(input.toLowerCase().trim());
        // Remove punctuation similar to ItunesService (keep parentheses/brackets intact)
        String toRemove = "\\.,'!?\"-_:;/&%";
        for (int i = 0; i < toRemove.length(); i++) {
            result = result.replace(String.valueOf(toRemove.charAt(i)), "");
        }
        result = result.replaceAll("\\s+", " ").trim();
        return result;
    }

    private String safe(Object o) {
        if (o == null) return "";
        return o.toString();
    }

    private String formatDaysSinceDate(String isoDate) {
        if (isoDate == null || isoDate.isBlank()) return "";
        try {
            LocalDate date = LocalDate.parse(isoDate, DateTimeFormatter.ISO_LOCAL_DATE);
            long days = ChronoUnit.DAYS.between(date, LocalDate.now(ZoneId.of("America/Mexico_City")));
            return " (" + days + " " + (days == 1 ? "day" : "days") + ")";
        } catch (Exception ignored) {
            return "";
        }
    }

    private String escapeHtml(String value) {
        if (value == null) return "";
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
