package library.controller;

import library.repository.SongRepository;
import library.service.BillboardHot100Service;
import library.service.SongService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import library.service.ChartService;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class NowPlayingLookupController {

    private final SongRepository songRepository;
    private final JdbcTemplate jdbcTemplate;
    private final SongService songService;
    private final ChartService chartService;
    private final BillboardHot100Service billboardHot100Service;

    public NowPlayingLookupController(SongRepository songRepository, JdbcTemplate jdbcTemplate, SongService songService, ChartService chartService, BillboardHot100Service billboardHot100Service) {
        this.songRepository = songRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.songService = songService;
        this.chartService = chartService;
        this.billboardHot100Service = billboardHot100Service;
    }

    @GetMapping("/lookup")
    public Map<String, Object> lookup(@RequestParam(required = false) String artist,
                                      @RequestParam(required = false) String album,
                                      @RequestParam(required = false) String song,
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

        resp.put("artistQuery", artist);
        resp.put("albumQuery", album);
        resp.put("songQuery", song);

        // Search songs by artist+song (best-effort) using JdbcTemplate to avoid
        // depending on a specific repository method signature.
        StringBuilder sql = new StringBuilder(
            "SELECT s.id, s.name, a.name as artist_name, al.name as album_name, " +
            "CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id) THEN 1 ELSE 0 END as has_image " +
            "FROM Song s JOIN Artist a ON s.artist_id = a.id LEFT JOIN Album al ON s.album_id = al.id WHERE 1=1 "
        );
        List<Object> params = new java.util.ArrayList<>();
        String normArtist = null;
        String normSong = null;
        String normAlbum = null;
        if (artist != null && !artist.trim().isEmpty()) {
            normArtist = library.util.StringNormalizer.normalizeForSearch(artist);
            sql.append(" AND " + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ?");
            params.add("%" + normArtist + "%");
        }
        if (song != null && !song.trim().isEmpty()) {
            normSong = library.util.StringNormalizer.normalizeForSearch(song);
            sql.append(" AND " + library.util.StringNormalizer.sqlNormalizeColumn("s.name") + " LIKE ?");
            params.add("%" + normSong + "%");
        }
        sql.append(" ORDER BY a.name, s.name LIMIT 20");
        List<Map<String, Object>> matches = jdbcTemplate.queryForList(sql.toString(), params.toArray());
        resp.put("matches", matches);

        // If no matches and the incoming song param may have been truncated by an un-encoded '&' (common from Shortcuts),
        // try to reconstruct the full song value from the raw query string and re-run the query.
        try {
            if ((matches == null || matches.isEmpty()) && song != null && request.getQueryString() != null && request.getQueryString().contains("song=")) {
                String raw = request.getQueryString();
                int i = raw.indexOf("song=");
                if (i >= 0) {
                    String rest = raw.substring(i + "song=".length());
                    String decoded = java.net.URLDecoder.decode(rest, java.nio.charset.StandardCharsets.UTF_8.name());
                    // Only update if decoded is longer than original song (indicates truncation)
                    if (decoded != null && decoded.length() > song.length()) {
                        song = decoded;
                        resp.put("songQuery", song);
                        // Rebuild the limited SQL search with the corrected song
                        StringBuilder sql2 = new StringBuilder(
                            "SELECT s.id, s.name, a.name as artist_name, al.name as album_name, " +
                            "CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id) THEN 1 ELSE 0 END as has_image " +
                            "FROM Song s JOIN Artist a ON s.artist_id = a.id LEFT JOIN Album al ON s.album_id = al.id WHERE 1=1 "
                        );
                        List<Object> params2 = new java.util.ArrayList<>();
                        if (artist != null && !artist.trim().isEmpty()) {
                            String nArtist = library.util.StringNormalizer.normalizeForSearch(artist);
                            sql2.append(" AND " + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ?");
                            params2.add("%" + nArtist + "%");
                        }
                        if (song != null && !song.trim().isEmpty()) {
                            String nSong = library.util.StringNormalizer.normalizeForSearch(song);
                            sql2.append(" AND " + library.util.StringNormalizer.sqlNormalizeColumn("s.name") + " LIKE ?");
                            params2.add("%" + nSong + "%");
                        }
                        sql2.append(" ORDER BY a.name, s.name LIMIT 20");
                        matches = jdbcTemplate.queryForList(sql2.toString(), params2.toArray());
                        resp.put("matches", matches);
                    }
                }
            }
        } catch (Exception ignored) {
        }

        Integer songId = null;
        Integer albumId = null;
        Integer artistId = null;

        // Try to find a best match by normalizing strings (use util normalizer if available)

        // Use strict normalization matching like the iTunes badges: lowercase + strip accents + remove punctuation
        String inputStrictArtist = artist != null ? normalizeForStrictMatch(artist) : null;
        String inputStrictSong = song != null ? normalizeForStrictMatch(song) : null;
        String inputStrictAlbum = album != null ? normalizeForStrictMatch(album) : null;

        for (Map<String, Object> m : matches) {
            String mName = m.get("name") != null ? m.get("name").toString() : null;
            String mArtist = m.get("artist_name") != null ? m.get("artist_name").toString() : null;
            String mAlbum = m.get("album_name") != null ? m.get("album_name").toString() : null;

            String dbStrictName = mName != null ? normalizeForStrictMatch(mName) : null;
            String dbStrictArtist = mArtist != null ? normalizeForStrictMatch(mArtist) : null;
            String dbStrictAlbum = mAlbum != null ? normalizeForStrictMatch(mAlbum) : null;

            boolean artistMatches = inputStrictArtist == null || (dbStrictArtist != null && dbStrictArtist.equals(inputStrictArtist));
            boolean songMatches = inputStrictSong == null || (dbStrictName != null && dbStrictName.equals(inputStrictSong));
            boolean albumMatches = inputStrictAlbum == null || (dbStrictAlbum != null && dbStrictAlbum.equals(inputStrictAlbum));

            if (artistMatches && songMatches && albumMatches) {
                Object idObj = m.get("id");
                if (idObj instanceof Number) songId = ((Number) idObj).intValue();
                break;
            }
        }

        // If strict matching didn't find a song, try a fuzzy search fallback that
        // removes parentheses and common featuring tokens (better for titles like
        // "Top Top (feat. C-Kan, Gera MX...)" which may be stored without the paren text).
        if (songId == null && song != null && !song.isBlank()) {
            // First, try a DB-wide strict lookup that mirrors ItunesService.buildDatabaseLookupStrict()
            try {
                String sqlAll = "SELECT s.id as id, ar.name as artist_name, al.name as album_name, s.name as song_name " +
                        "FROM Song s INNER JOIN Artist ar ON s.artist_id = ar.id LEFT JOIN Album al ON s.album_id = al.id";
                java.util.Map<String, Integer> strictKeyToId = new java.util.HashMap<>();
                jdbcTemplate.query(sqlAll, rs -> {
                    String dbArtist = rs.getString("artist_name");
                    String dbAlbum = rs.getString("album_name");
                    String dbSong = rs.getString("song_name");
                    String key = normalizeForStrictMatch(dbArtist) + "||" + normalizeForStrictMatch(dbAlbum) + "||" + normalizeForStrictMatch(dbSong);
                    strictKeyToId.put(key, rs.getInt("id"));
                });

                String inputKey = normalizeForStrictMatch(artist) + "||" + normalizeForStrictMatch(album) + "||" + normalizeForStrictMatch(song);
                if (strictKeyToId.containsKey(inputKey)) {
                    songId = strictKeyToId.get(inputKey);
                }
            } catch (Exception ignored) {
            }

            // If strict DB-wide lookup didn't find it, try a fuzzy search fallback that
            // removes parentheses and common featuring tokens (better for titles like
            // "Top Top (feat. C-Kan, Gera MX...)" which may be stored without the paren text).
            if (songId == null) {
                String fuzzyInputSong = library.util.StringNormalizer.normalizeForSearch(song);
                String fuzzyInputArtist = artist != null ? library.util.StringNormalizer.normalizeForSearch(artist) : null;
                for (Map<String, Object> m : matches) {
                    String mName = m.get("name") != null ? m.get("name").toString() : null;
                    String mArtist = m.get("artist_name") != null ? m.get("artist_name").toString() : null;

                    String dbFuzzyName = mName != null ? library.util.StringNormalizer.normalizeForSearch(mName) : null;
                    String dbFuzzyArtist = mArtist != null ? library.util.StringNormalizer.normalizeForSearch(mArtist) : null;

                    boolean artistMatchesFuzzy = fuzzyInputArtist == null || (dbFuzzyArtist != null && dbFuzzyArtist.equals(fuzzyInputArtist));
                    boolean songMatchesFuzzy = fuzzyInputSong == null || (dbFuzzyName != null && dbFuzzyName.equals(fuzzyInputSong));

                    if (artistMatchesFuzzy && songMatchesFuzzy) {
                        Object idObj = m.get("id");
                        if (idObj instanceof Number) {
                            songId = ((Number) idObj).intValue();
                            break;
                        }
                    }
                }
            }
        }

        // If we found a song id, fetch its artist_id and album_id
        if (songId != null) {
            Map<String, Object> songRow = jdbcTemplate.queryForMap("SELECT id, artist_id, album_id FROM Song WHERE id = ?", songId);
            artistId = songRow.get("artist_id") != null ? ((Number) songRow.get("artist_id")).intValue() : null;
            albumId = songRow.get("album_id") != null ? ((Number) songRow.get("album_id")).intValue() : null;
        } else {
            // If no song match, try to find artist by name and album by name heuristically
            if (artist != null) {
                List<Map<String,Object>> artistMatches = jdbcTemplate.queryForList("SELECT id FROM Artist WHERE " + library.util.StringNormalizer.sqlNormalizeColumn("name") + " LIKE ? LIMIT 1", "%" + normArtist + "%");
                if (!artistMatches.isEmpty()) {
                    artistId = ((Number) artistMatches.get(0).get("id")).intValue();
                }
            }
            if (albumId == null && album != null) {
                List<Map<String,Object>> albumMatches = jdbcTemplate.queryForList("SELECT id, artist_id FROM Album WHERE " + library.util.StringNormalizer.sqlNormalizeColumn("name") + " LIKE ? LIMIT 1", "%" + normAlbum + "%");
                if (!albumMatches.isEmpty()) {
                    albumId = ((Number) albumMatches.get(0).get("id")).intValue();
                    if (artistId == null && albumMatches.get(0).get("artist_id") != null) {
                        artistId = ((Number) albumMatches.get(0).get("artist_id")).intValue();
                    }
                }
            }
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
            }

            if (trueSong.isEmpty()) trueSong = safe(resp.get("songQuery"));
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
            h.append(".chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px}");
            h.append(".chip{background:var(--chip-bg);border:1px solid var(--chip-border);border-radius:6px;padding:6px 10px;font-size:13px;color:#fff;display:flex;align-items:center;gap:6px}");
            h.append(".chip .val{font-weight:700;color:var(--accent)}");
            h.append(".chip-link{text-decoration:none}");
            h.append(".chip-billboard{background:rgba(126,208,255,0.08);border:1px solid rgba(126,208,255,0.45)}");
            h.append(".chip-billboard .val{color:#7ed0ff}");
            h.append(".chip-proj{background:var(--chip-bg);border:1px dashed var(--accent);border-radius:6px;padding:6px 10px;font-size:13px;color:#fff;display:flex;align-items:center;gap:6px}");
            h.append(".chip-proj .val{font-weight:600;color:var(--accent)}");
            h.append(".chip-yearly{background:linear-gradient(135deg, var(--chip-bg), var(--accent));border:1px solid var(--accent);border-radius:6px;padding:6px 10px;font-size:13px;color:#fff;display:flex;align-items:center;gap:6px;font-weight:700;box-shadow:0 0 12px var(--chip-border)}");
            h.append(".chip-yearly .val{color:#fff;text-shadow:0 1px 3px rgba(0,0,0,0.6)}");
            h.append(".date-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:16px}");
            h.append(".date-tile{background:var(--chip-bg);padding:12px 8px;border-radius:8px;text-align:center;border:1px solid var(--chip-border)}");
            h.append(".date-tile .num{font-size:13px;font-weight:600;color:var(--accent);margin-bottom:4px}");
            h.append(".date-tile .lbl{font-size:11px;color:var(--muted);text-transform:uppercase}");
            h.append("a.header-link{color:var(--accent);text-decoration:none;transition:opacity 0.2s}");
            h.append("a.header-link:hover{opacity:0.8}");
            h.append(".small-muted{color:var(--muted);font-size:12px;margin:16px 0 8px}</style>");
            
            h.append("</head><body><div class=\"card\">\n");
            String songHtml = !songUrlS.isEmpty() ? "<a class=\"header-link\" href=\"" + songUrlS + "\">" + trueSong + "</a>" : trueSong;
            h.append("<p class=\"title\">").append(songHtml).append("</p>");
            String artistHtml = !artistUrlS.isEmpty() ? "<a class=\"header-link\" href=\"" + artistUrlS + "\">" + trueArtist + "</a>" : trueArtist;
            String albumHtml = !trueAlbum.isEmpty() ? (!albumUrlS.isEmpty() ? "<a class=\"header-link\" href=\"" + albumUrlS + "\">" + trueAlbum + "</a>" : trueAlbum) : "";
            String subLine = artistHtml;
            if (!albumHtml.isEmpty()) subLine += " • " + albumHtml;
            h.append("<p class=\"sub\">").append(subLine).append("</p>");

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
            h.append("<div class=\"date-tile\"><div class=\"num\">").append(last).append("</div><div class=\"lbl\">Last Listened</div></div>");
            h.append("</div>");

            // Current Ranks Section
            h.append("<div class=\"chips\">");
            
            Object rawChart = resp.get("yearlyChartHistory");
            if (rawChart instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> chartList = (List<Map<String, Object>>) rawChart;
                for (Map<String, Object> map : chartList) {
                    if (map != null && map.containsKey("displayName")) {
                        h.append("<div class=\"chip-yearly\">")
                         .append("<span class=\"val\">").append("⭐ ").append(safe(map.get("displayName")))
                         .append("</span></div>");
                    }
                }
            }

            Object rawBillboardStats = resp.get("billboardHot100Stats");
            if (rawBillboardStats instanceof Map<?, ?> billboardStats) {
                String weeksOnChart = safe(billboardStats.get("weeksOnChart"));
                String peakPosition = safe(billboardStats.get("peakPosition"));
                String weeksAtPeak = safe(billboardStats.get("weeksAtPeak"));
                if (!weeksOnChart.isEmpty()) {
                    String billboardText = weeksOnChart + " weeks on the Hot 100";
                    if (!peakPosition.isEmpty()) {
                        billboardText += " (Peak #" + peakPosition;
                        if (!weeksAtPeak.isEmpty() && !"0".equals(weeksAtPeak)) {
                            billboardText += " for " + weeksAtPeak + ("1".equals(weeksAtPeak) ? " week)" : " weeks)");
                        } else {
                            billboardText += ")";
                        }
                    }
                    h.append("<a class=\"chip chip-link chip-billboard\" href=\"").append(base).append("/misc/billboard-hot-100\" target=\"_blank\"><span class=\"val\">")
                     .append(escapeHtml(billboardText))
                     .append("</span></a>");
                }
            }

            appendChipHTML(h, overall, "Overall", false, null);
            appendChipHTML(h, rankArtist, trueArtist, false, null);
            appendChipHTML(h, rankAlbum, trueAlbum, false, null);
            appendChipHTML(h, rankGender, genderName, false, null);
            appendChipHTML(h, rankGenre, genreName, false, null);
            appendChipHTML(h, rankSubgenre, subgenreName, false, null);
            appendChipHTML(h, rankLanguage, languageName, false, null);
            appendChipHTML(h, rankCountry, countryName, false, null);
            appendChipHTML(h, rankSpanishRap, "Spanish Rap", false, null);
            if (!rankRelYear.isEmpty() && !relYearStr.isEmpty()) appendChipHTML(h, rankRelYear, "released in " + relYearStr, false, null);
            if (!rankCurYear.isEmpty() && !curYearStr.isEmpty()) appendChipHTML(h, rankCurYear, "most listened in " + curYearStr, false, null);
            h.append("</div>");

            // Projected section
            h.append("<div class=\"small-muted\">If this play is recorded:</div>");
            h.append("<div class=\"chips\">");
            appendChipHTML(h, overallIf, "Overall", true, overall);
            appendChipHTML(h, rankArtistIf, trueArtist, true, rankArtist);
            appendChipHTML(h, rankAlbumIf, trueAlbum, true, rankAlbum);
            appendChipHTML(h, rankGenderIf, genderName, true, rankGender);
            appendChipHTML(h, rankGenreIf, genreName, true, rankGenre);
            appendChipHTML(h, rankSubgenreIf, subgenreName, true, rankSubgenre);
            appendChipHTML(h, rankLanguageIf, languageName, true, rankLanguage);
            appendChipHTML(h, rankCountryIf, countryName, true, rankCountry);
            appendChipHTML(h, rankSpanishRapIf, "Spanish Rap", true, rankSpanishRap);
            if (!rankRelYearIf.isEmpty() && !relYearStr.isEmpty()) appendChipHTML(h, rankRelYearIf, "released in " + relYearStr, true, rankRelYear);
            if (!rankCurYearIf.isEmpty() && !curYearStr.isEmpty()) appendChipHTML(h, rankCurYearIf, "most listened in " + curYearStr, true, rankCurYear);
            h.append("</div>");

            h.append("</div></body></html>");
            resp.put("html", h.toString());
        } catch (Exception ignored) {
        }

        return resp;
    }

    private void appendChipHTML(StringBuilder sb, String rank, String name, boolean isProjected, String currentRank) {
        if (rank == null || rank.isEmpty()) return;
        if (name == null || name.isEmpty() || "null".equalsIgnoreCase(name)) return;

        String diffHtml = "";
        if (isProjected && currentRank != null && !currentRank.isEmpty()) {
            try {
                int curr = Integer.parseInt(currentRank);
                int proj = Integer.parseInt(rank);
                int diff = curr - proj;
                if (diff > 0) {
                    diffHtml = "<span style=\"color:#4ade80;font-size:12px;margin-left:4px;font-weight:700;\">▲" + diff + "</span>";
                }
            } catch (Exception ignored) { }
        }

        if (isProjected) {
            sb.append("<div class=\"chip-proj\"><span class=\"val\">#").append(rank).append("</span> ").append(name).append(diffHtml).append("</div>");
        } else {
            sb.append("<div class=\"chip\"><span class=\"val\">#").append(rank).append("</span> ").append(name).append("</div>");
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

    private String escapeHtml(String value) {
        if (value == null) return "";
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
