package library.service;

import library.dto.AlbumFullListenStats;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Service
public class AlbumFullListenCalculator {

    private final JdbcTemplate jdbcTemplate;
    private final AppConfigService appConfigService;

    public AlbumFullListenCalculator(JdbcTemplate jdbcTemplate, AppConfigService appConfigService) {
        this.jdbcTemplate = jdbcTemplate;
        this.appConfigService = appConfigService;
    }

    public Map<Integer, AlbumFullListenStats> calculateAll() {
        return calculate(null);
    }

    public String calculateAllAsJson() {
        StringBuilder json = new StringBuilder("[");
        boolean first = true;
        for (Map.Entry<Integer, AlbumFullListenStats> entry : calculateAll().entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            AlbumFullListenStats stats = entry.getValue();
            json.append("{\"albumId\":").append(entry.getKey())
                    .append(",\"firstFullListenDate\":");
            if (stats.firstFullListenDate() == null) {
                json.append("null");
            } else {
                json.append('\"').append(stats.firstFullListenDate()).append('\"');
            }
            json.append(",\"lastFullListenDate\":");
            if (stats.lastFullListenDate() == null) {
                json.append("null");
            } else {
                json.append('\"').append(stats.lastFullListenDate()).append('\"');
            }
            json.append(",\"fullAlbumPlays\":").append(stats.fullAlbumPlays()).append('}');
        }
        return json.append(']').toString();
    }

    public AlbumFullListenStats calculateForAlbum(int albumId) {
        return calculate(Set.of(albumId)).getOrDefault(albumId, new AlbumFullListenStats(null, null, 0));
    }

    public Map<Integer, AlbumFullListenStats> calculateForAlbums(Collection<Integer> albumIds) {
        if (albumIds == null || albumIds.isEmpty()) return Collections.emptyMap();
        return calculate(new HashSet<>(albumIds));
    }

    private Map<Integer, AlbumFullListenStats> calculate(Set<Integer> targetAlbumIds) {
        AppConfigService.AlbumFullListenConfig config = appConfigService.getAlbumFullListenConfig();
        Map<Integer, Integer> requiredSongsByAlbum = loadRequiredSongs(config, targetAlbumIds);
        if (requiredSongsByAlbum.isEmpty()) {
            return Collections.emptyMap();
        }

        // One chronological pass keeps this O(number of plays). Each album state
        // retains only its current candidate window and slides past excess interruptions.
        Map<Integer, RunState> states = new HashMap<>();
        if (targetAlbumIds == null) {
            long[] globalPosition = {0L};
            jdbcTemplate.query("""
                    SELECT p.play_date, p.song_id, s.album_id
                    FROM Play p
                    LEFT JOIN Song s ON s.id = p.song_id
                    ORDER BY p.play_date, p.id
                    """, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
                int albumId = rs.getInt("album_id");
                boolean albumWasNull = rs.wasNull();
                int songId = rs.getInt("song_id");
                boolean songWasNull = rs.wasNull();
                acceptPlay(0L, ++globalPosition[0], albumId, albumWasNull, songId, songWasNull,
                        rs.getString("play_date"), requiredSongsByAlbum, states, config);
            });
        } else {
            String placeholders = String.join(",", Collections.nCopies(targetAlbumIds.size(), "?"));
            String sql = """
                    WITH ranked_plays AS (
                        SELECT p.play_date, p.song_id,
                               ROW_NUMBER() OVER (ORDER BY p.play_date, p.id) AS global_position
                        FROM Play p
                    )
                    SELECT rp.play_date, rp.song_id, rp.global_position, s.album_id
                    FROM ranked_plays rp
                    JOIN Song s ON s.id = rp.song_id
                    WHERE s.album_id IN (%s)
                    ORDER BY rp.global_position
                    """.formatted(placeholders);
            jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
                int albumId = rs.getInt("album_id");
                boolean albumWasNull = rs.wasNull();
                int songId = rs.getInt("song_id");
                boolean songWasNull = rs.wasNull();
                acceptPlay(rs.getLong("global_position"), 0L, albumId, albumWasNull, songId, songWasNull,
                        rs.getString("play_date"), requiredSongsByAlbum, states, config);
            }, targetAlbumIds.toArray());
        }

        Map<Integer, AlbumFullListenStats> result = new HashMap<>();
        states.forEach((albumId, state) -> {
            if (state.fullAlbumPlays > 0) {
                result.put(albumId, new AlbumFullListenStats(
                        state.firstFullListenDate, state.lastFullListenDate, state.fullAlbumPlays));
            }
        });
        return Collections.unmodifiableMap(result);
    }

    private void acceptPlay(long rankedPosition, long fallbackPosition, int albumId, boolean albumWasNull,
                            int songId, boolean songWasNull, String playDate,
                            Map<Integer, Integer> requiredSongsByAlbum, Map<Integer, RunState> states,
                            AppConfigService.AlbumFullListenConfig config) {
            long position = rankedPosition > 0 ? rankedPosition : fallbackPosition;
            if (albumWasNull || !requiredSongsByAlbum.containsKey(albumId)) {
                return;
            }
            if (songWasNull) {
                return;
            }
            states.computeIfAbsent(albumId, ignored -> new RunState())
                    .accept(position, songId, playDate,
                            requiredSongsByAlbum.get(albumId), config.allowedInterruptingSongs());
    }

    private Map<Integer, Integer> loadRequiredSongs(AppConfigService.AlbumFullListenConfig config,
                                                     Set<Integer> targetAlbumIds) {
        StringBuilder sql = new StringBuilder("""
                SELECT album_id, COUNT(*) AS song_count
                FROM Song
                WHERE album_id IS NOT NULL
                """);
        Object[] params = new Object[0];
        if (targetAlbumIds != null) {
            sql.append(" AND album_id IN (")
                    .append(String.join(",", Collections.nCopies(targetAlbumIds.size(), "?")))
                    .append(")");
            params = targetAlbumIds.toArray();
        }
        sql.append(" GROUP BY album_id");

        Map<Integer, Integer> result = new HashMap<>();
        jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            result.put(
                    rs.getInt("album_id"),
                    config.requiredSongsFor(rs.getInt("song_count"))
            );
            return null;
        }, params);
        return result;
    }

    private static String datePart(String playDate) {
        if (playDate == null || playDate.isBlank()) {
            return null;
        }
        String trimmed = playDate.trim();
        return trimmed.length() >= 10 ? trimmed.substring(0, 10) : trimmed;
    }

    private static final class RunState {
        private final ArrayDeque<InterruptionGroup> interruptionGroups = new ArrayDeque<>();
        private final Map<Integer, Long> latestOccurrenceBySong = new HashMap<>();
        private final TreeMap<Long, Integer> songsByLatestOccurrence = new TreeMap<>();
        private long albumOccurrence;
        private int fullAlbumPlays;
        private String firstFullListenDate;
        private String lastFullListenDate;

        private void accept(long globalPosition, int songId, String playDate,
                            int requiredSongs, int allowedInterruptingSongs) {
            long currentAlbumOccurrence = ++albumOccurrence;
            long interruptionPrefix = globalPosition - currentAlbumOccurrence;
            if (interruptionGroups.isEmpty()
                    || interruptionGroups.getLast().interruptionPrefix() != interruptionPrefix) {
                interruptionGroups.addLast(new InterruptionGroup(interruptionPrefix, currentAlbumOccurrence));
            }

            long earliestAllowedPrefix = interruptionPrefix - allowedInterruptingSongs;
            while (interruptionGroups.getFirst().interruptionPrefix() < earliestAllowedPrefix) {
                interruptionGroups.removeFirst();
            }

            Long previousOccurrence = latestOccurrenceBySong.put(songId, currentAlbumOccurrence);
            if (previousOccurrence != null) {
                songsByLatestOccurrence.remove(previousOccurrence);
            }
            songsByLatestOccurrence.put(currentAlbumOccurrence, songId);

            long earliestAlbumOccurrence = interruptionGroups.getFirst().firstAlbumOccurrence();
            while (!songsByLatestOccurrence.isEmpty()
                    && songsByLatestOccurrence.firstKey() < earliestAlbumOccurrence) {
                Map.Entry<Long, Integer> expired = songsByLatestOccurrence.pollFirstEntry();
                latestOccurrenceBySong.remove(expired.getValue(), expired.getKey());
            }

            if (latestOccurrenceBySong.size() >= requiredSongs) {
                fullAlbumPlays++;
                lastFullListenDate = datePart(playDate);
                if (firstFullListenDate == null) {
                    firstFullListenDate = lastFullListenDate;
                }
                interruptionGroups.clear();
                latestOccurrenceBySong.clear();
                songsByLatestOccurrence.clear();
            }
        }
    }

    private record InterruptionGroup(long interruptionPrefix, long firstAlbumOccurrence) {
    }
}
