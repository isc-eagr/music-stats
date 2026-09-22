package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

@Service
public class PlaygroundService {
    private static final DateTimeFormatter DISPLAY_DATE = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private final JdbcTemplate jdbcTemplate;

    public PlaygroundService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Integer> getYears() {
        TreeSet<Integer> years = new TreeSet<>();
        years.add(LocalDate.now(ZoneId.of("America/Mexico_City")).getYear());
        jdbcTemplate.query("""
                SELECT DISTINCT SUBSTR(play_date, 1, 4) AS play_year
                FROM Play
                WHERE play_date IS NOT NULL
                  AND SUBSTR(play_date, 1, 4) BETWEEN '0001' AND '9998'
                  AND strftime('%Y', play_date) BETWEEN '0001' AND '9998'
                """, rs -> {
            years.add(Integer.parseInt(rs.getString("play_year")));
        });
        return new ArrayList<>(years.descendingSet());
    }

    public YearHeatmap getHeatmap(int year) {
        if (year < 1 || year > 9998) {
            throw new IllegalArgumentException("Year must be between 1 and 9998");
        }
        LocalDate start = LocalDate.of(year, 1, 1);
        Map<LocalDate, long[]> counts = new HashMap<>();
        // Stored dates are already Mexico City local time. Include unmatched plays in totals.
        jdbcTemplate.query("""
                SELECT date(p.play_date) AS day, COUNT(*) AS total,
                       SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN 1 ELSE 0 END) AS male,
                       SUM(CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN 1 ELSE 0 END) AS female
                FROM Play p
                LEFT JOIN Song s ON s.id = p.song_id
                LEFT JOIN Artist ar ON ar.id = s.artist_id
                WHERE p.play_date >= ? AND p.play_date < ? AND date(p.play_date) IS NOT NULL
                GROUP BY date(p.play_date)
                """, rs -> {
            counts.put(LocalDate.parse(rs.getString("day")),
                    new long[]{rs.getLong("total"), rs.getLong("male"), rs.getLong("female")});
        }, start.toString(), start.plusYears(1).toString());

        long max = counts.values().stream().mapToLong(c -> c[0]).max().orElse(0);
        List<HeatmapDay> days = new ArrayList<>();
        int offset = start.getDayOfWeek().getValue() - 1;
        for (int i = 0; i < start.lengthOfYear(); i++) {
            LocalDate date = start.plusDays(i);
            long[] count = counts.getOrDefault(date, new long[3]);
            long other = count[0] - count[1] - count[2];
            String gender = count[0] == 0 ? "no-plays"
                    : count[1] > count[2] && count[1] > other ? "male"
                    : count[2] > count[1] && count[2] > other ? "female" : "neutral";
            String purity = count[0] > 0 && count[1] == count[0] ? "all-male"
                    : count[0] > 0 && count[2] == count[0] ? "all-female" : "mixed";
            int intensity = count[0] == 0 ? 0 : (int) Math.ceil(4.0 * count[0] / max);
            String label = date.format(DISPLAY_DATE) + ": " + count[0] + " plays; "
                    + count[1] + " male, " + count[2] + " female, " + other + " other/unknown";
            days.add(new HeatmapDay(date, count[0], count[1], count[2], other, gender, purity, intensity,
                    (offset + i) / 7 + 2, date.getDayOfWeek().getValue() + 1, label));
        }
        return new YearHeatmap(year, days, days.stream().mapToLong(HeatmapDay::total).sum(),
                counts.size(), max, (offset + start.lengthOfYear() + 6) / 7);
    }

    public FlashbackReview getFlashback(String reviewType, int year, String season, int month) {
        if (year < 1 || year > 9998) {
            throw new IllegalArgumentException("Year must be between 1 and 9998");
        }
        ReviewPeriod period = reviewPeriod(reviewType, year, season, month);
        LocalDate today = LocalDate.now(ZoneId.of("America/Mexico_City"));
        LocalDate elapsedEnd = period.end().isBefore(today.plusDays(1)) ? period.end() : today.plusDays(1);
        long elapsedDays = period.start().isAfter(today) ? 0 : ChronoUnit.DAYS.between(period.start(), elapsedEnd);

        List<DailyPlayCount> dailyCounts = jdbcTemplate.query("""
                SELECT date(p.play_date) AS day, COUNT(*) AS total
                FROM Play p
                WHERE p.play_date >= ? AND p.play_date < ? AND date(p.play_date) IS NOT NULL
                GROUP BY date(p.play_date)
                ORDER BY day
                """, (rs, rowNum) -> new DailyPlayCount(LocalDate.parse(rs.getString("day")), rs.getLong("total")),
                period.start().toString(), period.end().toString());

        long totalPlays = 0;
        long busiestDayPlays = 0;
        LocalDate busiestDay = null;
        int longestStreak = 0;
        int currentStreak = 0;
        LocalDate previousDay = null;
        for (DailyPlayCount day : dailyCounts) {
            totalPlays += day.plays();
            if (day.plays() > busiestDayPlays) {
                busiestDay = day.date();
                busiestDayPlays = day.plays();
            }
            currentStreak = previousDay != null && previousDay.plusDays(1).equals(day.date())
                    ? currentStreak + 1 : 1;
            longestStreak = Math.max(longestStreak, currentStreak);
            previousDay = day.date();
        }

        long listeningSeconds = jdbcTemplate.queryForObject("""
                SELECT COALESCE(SUM(CASE WHEN s.length_seconds > 0 THEN s.length_seconds ELSE 0 END), 0)
                FROM Play p
                LEFT JOIN Song s ON s.id = p.song_id
                WHERE p.play_date >= ? AND p.play_date < ?
                  AND date(p.play_date) IS NOT NULL
                """, Long.class, period.start().toString(), period.end().toString());
        long newArtists = countFirstListens("s.artist_id", "s.artist_id", "artist_id", period);
        long newSongs = countFirstListens("s.id", "s.id", "id", period);
        NewArtistHighlight mostPlayedNewArtist = getMostPlayedNewArtist(period);
        NewSongHighlight mostPlayedNewSong = getMostPlayedNewSong(period);
        double averagePlaysPerDay = elapsedDays == 0 ? 0 : Math.round((double) totalPlays / elapsedDays * 10) / 10.0;
        QuickFacts facts = new QuickFacts(totalPlays, listeningSeconds, averagePlaysPerDay,
                longestStreak, busiestDay, busiestDayPlays, newArtists, newSongs,
                mostPlayedNewArtist, mostPlayedNewSong);

        List<FlashbackTrack> overall = getTopTracks(period, null, 30);
        List<GenreRanking> genreRankings = getMajorGenreRankings(period);
        return new FlashbackReview(period.title(), period.start(), period.end().minusDays(1), facts,
                overall, genreRankings);
    }

    private long countFirstListens(String entity, String groupBy, String priorEntity, ReviewPeriod period) {
        String sql = """
                WITH period_entities AS (
                    SELECT %s AS entity_id
                    FROM Play p
                    JOIN Song s ON s.id = p.song_id
                    WHERE p.play_date >= ? AND p.play_date < ?
                      AND date(p.play_date) IS NOT NULL
                    GROUP BY %s
                )
                SELECT COUNT(*)
                FROM period_entities candidate
                WHERE candidate.entity_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM Song prior_song
                      JOIN Play prior_play ON prior_play.song_id = prior_song.id
                      WHERE prior_song.%s = candidate.entity_id
                        AND prior_play.play_date < ?
                        AND date(prior_play.play_date) IS NOT NULL
                  )
                """.formatted(entity, groupBy, priorEntity);
        return jdbcTemplate.queryForObject(sql, Long.class, period.start().toString(),
                period.end().toString(), period.start().toString());
    }

    private NewArtistHighlight getMostPlayedNewArtist(ReviewPeriod period) {
        List<NewArtistHighlight> results = jdbcTemplate.query("""
                WITH new_artists AS (
                    SELECT s.artist_id
                    FROM Play p
                    JOIN Song s ON s.id = p.song_id
                    WHERE p.play_date >= ? AND p.play_date < ?
                      AND date(p.play_date) IS NOT NULL
                      AND s.artist_id IS NOT NULL
                    GROUP BY s.artist_id
                    HAVING NOT EXISTS (
                        SELECT 1
                        FROM Song prior_song
                        JOIN Play prior_play ON prior_play.song_id = prior_song.id
                        WHERE prior_song.artist_id = s.artist_id
                          AND prior_play.play_date < ?
                          AND date(prior_play.play_date) IS NOT NULL
                    )
                )
                SELECT ar.id, ar.name, COUNT(p.id) AS plays
                FROM new_artists na
                JOIN Artist ar ON ar.id = na.artist_id
                JOIN Song s ON s.artist_id = na.artist_id
                JOIN Play p ON p.song_id = s.id
                WHERE p.play_date >= ? AND p.play_date < ?
                  AND date(p.play_date) IS NOT NULL
                GROUP BY ar.id, ar.name
                ORDER BY plays DESC, ar.name COLLATE NOCASE, ar.id
                LIMIT 1
                """, (rs, rowNum) -> new NewArtistHighlight(rs.getInt("id"), rs.getString("name"),
                        rs.getLong("plays")), period.start().toString(), period.end().toString(),
                period.start().toString(), period.start().toString(), period.end().toString());
        return results.isEmpty() ? null : results.getFirst();
    }

    private NewSongHighlight getMostPlayedNewSong(ReviewPeriod period) {
        List<NewSongHighlight> results = jdbcTemplate.query("""
                SELECT s.id, s.name, ar.name AS artist_name, COUNT(p.id) AS plays
                FROM Play p
                JOIN Song s ON s.id = p.song_id
                LEFT JOIN Artist ar ON ar.id = s.artist_id
                WHERE p.play_date >= ? AND p.play_date < ?
                  AND date(p.play_date) IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM Play prior_play
                      WHERE prior_play.song_id = s.id
                        AND prior_play.play_date < ?
                        AND date(prior_play.play_date) IS NOT NULL
                  )
                GROUP BY s.id, s.name, ar.name
                ORDER BY plays DESC, s.name COLLATE NOCASE, ar.name COLLATE NOCASE, s.id
                LIMIT 1
                """, (rs, rowNum) -> new NewSongHighlight(rs.getInt("id"), rs.getString("name"),
                        rs.getString("artist_name"), rs.getLong("plays")), period.start().toString(),
                period.end().toString(), period.start().toString());
        return results.isEmpty() ? null : results.getFirst();
    }

    private List<FlashbackTrack> getTopTracks(ReviewPeriod period, Integer genreId, int limit) {
        String effectiveGenre = "COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id)";
        String genreCondition = genreId == null ? "" : " AND " + effectiveGenre + " = ?";
        List<Object> params = new ArrayList<>(List.of(period.start().toString(), period.end().toString()));
        if (genreId != null) params.add(genreId);
        params.add(limit);
        return jdbcTemplate.query("""
                WITH top_tracks AS MATERIALIZED (
                    SELECT s.id AS song_id, COUNT(p.id) AS plays
                    FROM Play p
                    JOIN Song s ON s.id = p.song_id
                    LEFT JOIN Album al ON al.id = s.album_id
                    LEFT JOIN Artist ar ON ar.id = s.artist_id
                    WHERE p.play_date >= ? AND p.play_date < ?
                    """ + genreCondition + """
                    GROUP BY s.id
                    ORDER BY plays DESC, s.name COLLATE NOCASE, ar.name COLLATE NOCASE, s.id
                    LIMIT ?
                )
                SELECT s.id AS song_id, al.id AS album_id, s.name AS song_name, ar.name AS artist_name,
                       al.name AS album_name, top_tracks.plays,
                       COALESCE(s.override_gender_id, ar.gender_id) AS gender_id,
                       CASE WHEN al.id IS NOT NULL AND (al.image IS NOT NULL OR EXISTS (
                           SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id AND ai.image IS NOT NULL
                       )) THEN 1 ELSE 0 END AS album_has_image,
                       CASE WHEN s.single_cover IS NOT NULL OR EXISTS (
                           SELECT 1 FROM SongImage si WHERE si.song_id = s.id AND si.image IS NOT NULL
                       ) THEN 1 ELSE 0 END AS song_has_image
                FROM top_tracks
                JOIN Song s ON s.id = top_tracks.song_id
                LEFT JOIN Album al ON al.id = s.album_id
                LEFT JOIN Artist ar ON ar.id = s.artist_id
                ORDER BY top_tracks.plays DESC, s.name COLLATE NOCASE, ar.name COLLATE NOCASE, s.id
                """, (rs, rowNum) -> mapFlashbackTrack(rs, rowNum + 1),
                params.toArray());
    }

    private List<GenreRanking> getMajorGenreRankings(ReviewPeriod period) {
        String effectiveGenre = "COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id)";
        String effectiveGender = "COALESCE(s.override_gender_id, ar.gender_id)";
        List<GenreSummary> genres = jdbcTemplate.query("""
                SELECT g.id, g.name, COUNT(DISTINCT s.id) AS library_song_count,
                       COUNT(DISTINCT CASE WHEN p.id IS NOT NULL THEN s.id END) AS played_song_count
                FROM Genre g
                JOIN Song s ON %s = g.id
                LEFT JOIN Album al ON al.id = s.album_id
                LEFT JOIN Artist ar ON ar.id = s.artist_id
                LEFT JOIN Play p ON p.song_id = s.id
                    AND p.play_date >= ? AND p.play_date < ?
                    AND date(p.play_date) IS NOT NULL
                GROUP BY g.id, g.name
                HAVING COUNT(DISTINCT s.id) >= 200
                ORDER BY g.name COLLATE NOCASE
                """.formatted(effectiveGenre), (rs, rowNum) -> new GenreSummary(
                rs.getInt("id"), rs.getString("name"), rs.getLong("library_song_count"),
                rs.getLong("played_song_count")), period.start().toString(), period.end().toString());
        if (genres.isEmpty()) return List.of();

        String placeholders = String.join(",", java.util.Collections.nCopies(genres.size(), "?"));
        List<Object> params = new ArrayList<>(List.of(period.start().toString(), period.end().toString()));
        genres.forEach(genre -> params.add(genre.id()));
        String sql = """
                WITH genre_tracks AS MATERIALIZED (
                    SELECT %s AS genre_id, s.id AS song_id, al.id AS album_id, s.name AS song_name,
                           ar.name AS artist_name, al.name AS album_name, COUNT(p.id) AS plays,
                           %s AS gender_id
                    FROM Play p
                    JOIN Song s ON s.id = p.song_id
                    LEFT JOIN Album al ON al.id = s.album_id
                    LEFT JOIN Artist ar ON ar.id = s.artist_id
                    WHERE p.play_date >= ? AND p.play_date < ?
                      AND %s IN (%s)
                    GROUP BY %s, s.id, s.name, ar.name, al.name, %s
                ), ranked AS MATERIALIZED (
                    SELECT genre_tracks.*,
                           ROW_NUMBER() OVER (PARTITION BY genre_id
                               ORDER BY plays DESC, song_name COLLATE NOCASE,
                                        artist_name COLLATE NOCASE, song_id) AS rank
                    FROM genre_tracks
                ), top_genre_tracks AS MATERIALIZED (
                    SELECT * FROM ranked WHERE rank <= 5
                )
                SELECT top_genre_tracks.*,
                       CASE WHEN top_genre_tracks.album_id IS NOT NULL AND (al.image IS NOT NULL OR EXISTS (
                           SELECT 1 FROM AlbumImage ai
                           WHERE ai.album_id = top_genre_tracks.album_id AND ai.image IS NOT NULL
                       )) THEN 1 ELSE 0 END AS album_has_image,
                       CASE WHEN s.single_cover IS NOT NULL OR EXISTS (
                           SELECT 1 FROM SongImage si
                           WHERE si.song_id = top_genre_tracks.song_id AND si.image IS NOT NULL
                       ) THEN 1 ELSE 0 END AS song_has_image
                FROM top_genre_tracks
                JOIN Song s ON s.id = top_genre_tracks.song_id
                LEFT JOIN Album al ON al.id = top_genre_tracks.album_id
                ORDER BY top_genre_tracks.genre_id, top_genre_tracks.rank
                """.formatted(effectiveGenre, effectiveGender, effectiveGenre, placeholders,
                effectiveGenre, effectiveGender);
        Map<Integer, List<FlashbackTrack>> tracksByGenre = new HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            int genreId = rs.getInt("genre_id");
            tracksByGenre.computeIfAbsent(genreId, ignored -> new ArrayList<>())
                    .add(mapFlashbackTrack(rs, rs.getInt("rank")));
        }, params.toArray());

        return genres.stream().map(genre -> new GenreRanking(genre.id(), genre.name(), genre.playedSongCount(),
                tracksByGenre.getOrDefault(genre.id(), List.of()))).toList();
    }

    private FlashbackTrack mapFlashbackTrack(ResultSet rs, int rank) throws SQLException {
        int genderId = rs.getInt("gender_id");
        String genderClass = genderId == 2 ? "gender-male" : genderId == 1 ? "gender-female" : "";
        int albumIdValue = rs.getInt("album_id");
        Integer albumId = rs.wasNull() ? null : albumIdValue;
        return new FlashbackTrack(rs.getInt("song_id"), albumId, rs.getString("song_name"),
                rs.getString("artist_name"), rs.getString("album_name"), rs.getLong("plays"), rank,
                genderClass, rs.getInt("album_has_image") == 1, rs.getInt("song_has_image") == 1);
    }

    private ReviewPeriod reviewPeriod(String reviewType, int year, String season, int month) {
        if (reviewType == null) throw new IllegalArgumentException("Review type is required");
        return switch (reviewType.toLowerCase(Locale.ROOT)) {
            case "year" -> {
                LocalDate start = LocalDate.of(year, 1, 1);
                yield new ReviewPeriod(start, start.plusYears(1), "Year in Review · " + year);
            }
            case "season" -> {
                if (season == null) throw new IllegalArgumentException("Season is required");
                String normalizedSeason = season.toLowerCase(Locale.ROOT);
                LocalDate start = switch (normalizedSeason) {
                    case "winter" -> LocalDate.of(year, 12, 1);
                    case "spring" -> LocalDate.of(year, 3, 1);
                    case "summer" -> LocalDate.of(year, 6, 1);
                    case "fall" -> LocalDate.of(year, 9, 1);
                    default -> throw new IllegalArgumentException("Season must be winter, spring, summer, or fall");
                };
                LocalDate end = start.plusMonths(3);
                String title = normalizedSeason.substring(0, 1).toUpperCase(Locale.ROOT)
                        + normalizedSeason.substring(1) + " " + year
                        + (normalizedSeason.equals("winter") ? "/" + (year + 1) : "");
                yield new ReviewPeriod(start, end, "Season in Review · " + title);
            }
            case "month" -> {
                if (month < 1 || month > 12) throw new IllegalArgumentException("Month must be between 1 and 12");
                LocalDate start = LocalDate.of(year, month, 1);
                String title = "Month in Review · " + start.format(DateTimeFormatter.ofPattern("MMMM yyyy", Locale.ENGLISH));
                yield new ReviewPeriod(start, start.plusMonths(1), title);
            }
            default -> throw new IllegalArgumentException("Review type must be year, season, or month");
        };
    }

    public record HeatmapDay(LocalDate date, long total, long male, long female, long other,
                             String gender, String purity, int intensity, int column, int row, String label) {}

    public record YearHeatmap(int year, List<HeatmapDay> days, long totalPlays,
                              int activeDays, long maxPlays, int weeks) {}

    private record ReviewPeriod(LocalDate start, LocalDate end, String title) {}
    private record DailyPlayCount(LocalDate date, long plays) {}
    private record GenreSummary(int id, String name, long librarySongCount, long playedSongCount) {}
    public record FlashbackTrack(int songId, Integer albumId, String songName, String artistName,
                                 String albumName, long plays, int rank, String genderClass,
                                 boolean albumHasImage, boolean songHasImage) {}
    public record GenreRanking(int id, String name, long songsPlayed, List<FlashbackTrack> tracks) {}
    public record NewArtistHighlight(int artistId, String name, long plays) {}
    public record NewSongHighlight(int songId, String name, String artistName, long plays) {}
    public record QuickFacts(long totalPlays, long listeningSeconds, double averagePlaysPerDay,
                             int longestStreakDays, LocalDate busiestDay, long busiestDayPlays,
                             long newArtists, long newSongs, NewArtistHighlight mostPlayedNewArtist,
                             NewSongHighlight mostPlayedNewSong) {
        public long listeningDays() { return listeningSeconds / 86400; }
        public long remainingListeningHours() { return (listeningSeconds % 86400) / 3600; }
        public long remainingListeningMinutes() { return (listeningSeconds % 3600) / 60; }
    }
    public record FlashbackReview(String title, LocalDate startDate, LocalDate endDate,
                                  QuickFacts quickFacts, List<FlashbackTrack> topOverall,
                                  List<GenreRanking> genreRankings) {}
}
