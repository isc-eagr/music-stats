package library.service;

import library.dto.AlbumFullListenStats;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

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

    private static final int MIN_ALBUM_TRACKS_FOR_FULL_LISTENS = 5;
    private static final String REQUEST_MEMO_ATTRIBUTE = AlbumFullListenCalculator.class.getName() + ".allJson:";

    private final JdbcTemplate jdbcTemplate;
    private final AppConfigService appConfigService;

    public AlbumFullListenCalculator(JdbcTemplate jdbcTemplate, AppConfigService appConfigService) {
        this.jdbcTemplate = jdbcTemplate;
        this.appConfigService = appConfigService;
    }

    /**
     * All-album stats as JSON for the list queries. A catalog request can run the list, count
     * and gender-count queries with the same full-listen filters, so the result is shared for
     * the rest of the current HTTP request (never across requests).
     */
    public String calculateAllAsJson() {
        AppConfigService.AlbumFullListenConfig config = appConfigService.getAlbumFullListenConfig();
        RequestAttributes request = RequestContextHolder.getRequestAttributes();
        String memoKey = REQUEST_MEMO_ATTRIBUTE + config;
        if (request != null && request.getAttribute(memoKey, RequestAttributes.SCOPE_REQUEST) instanceof String json) {
            return json;
        }
        String json = toJson(calculate(null, config));
        if (request != null) {
            request.setAttribute(memoKey, json, RequestAttributes.SCOPE_REQUEST);
        }
        return json;
    }

    private static String toJson(Map<Integer, AlbumFullListenStats> statsByAlbum) {
        StringBuilder json = new StringBuilder("[");
        boolean first = true;
        for (Map.Entry<Integer, AlbumFullListenStats> entry : statsByAlbum.entrySet()) {
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
        return calculate(targetAlbumIds, appConfigService.getAlbumFullListenConfig());
    }

    private Map<Integer, AlbumFullListenStats> calculate(Set<Integer> targetAlbumIds,
                                                         AppConfigService.AlbumFullListenConfig config) {
        Map<Integer, Integer> requiredSongsByAlbum = loadRequiredSongs(config, targetAlbumIds);
        if (requiredSongsByAlbum.isEmpty()) {
            return Collections.emptyMap();
        }

        // One chronological pass keeps this O(number of plays). Each album state
        // retains only its current candidate window and slides past excess interruptions.
        Map<Integer, RunState> states = new HashMap<>();
        Map<Integer, Integer> albumBySong = new HashMap<>();
        if (targetAlbumIds == null) {
            jdbcTemplate.query("SELECT id, album_id FROM Song WHERE album_id IS NOT NULL",
                    (org.springframework.jdbc.core.RowCallbackHandler) rs -> albumBySong.put(rs.getInt("id"), rs.getInt("album_id")));
        } else {
            String placeholders = String.join(",", Collections.nCopies(targetAlbumIds.size(), "?"));
            jdbcTemplate.query("SELECT id, album_id FROM Song WHERE album_id IN (" + placeholders + ")",
                    (org.springframework.jdbc.core.RowCallbackHandler) rs -> albumBySong.put(rs.getInt("id"), rs.getInt("album_id")),
                    targetAlbumIds.toArray());
        }
        // NULL names, like remixes, are excluded by the original SQL predicate.
        Set<Integer> excludedSongs = new HashSet<>(jdbcTemplate.queryForList(
                "SELECT id FROM Song WHERE name IS NULL OR LOWER(name) LIKE '%remix%'", Integer.class));
        long[] globalPosition = {0L};
        // Stream the Play index once instead of joining Song for every play. Plays of excluded
        // songs are skipped without taking a position; every other play (including unmatched
        // ones) counts as an interruption for the albums it does not belong to.
        jdbcTemplate.query("SELECT play_date, song_id FROM Play ORDER BY play_date, id",
                (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            int songId = rs.getInt("song_id");
            boolean songWasNull = rs.wasNull();
            if (!songWasNull && excludedSongs.contains(songId)) return;
            long position = ++globalPosition[0];
            Integer albumId = songWasNull ? null : albumBySong.get(songId);
            if (albumId != null && requiredSongsByAlbum.containsKey(albumId)) {
                states.computeIfAbsent(albumId, ignored -> new RunState())
                        .accept(position, songId, rs.getString("play_date"),
                                requiredSongsByAlbum.get(albumId), config.allowedInterruptingSongs());
            }
        });

        Map<Integer, AlbumFullListenStats> result = new HashMap<>();
        states.forEach((albumId, state) -> {
            if (state.fullAlbumPlays > 0) {
                result.put(albumId, new AlbumFullListenStats(
                        state.firstFullListenDate, state.lastFullListenDate, state.fullAlbumPlays));
            }
        });
        return Collections.unmodifiableMap(result);
    }

    private Map<Integer, Integer> loadRequiredSongs(AppConfigService.AlbumFullListenConfig config,
                                                     Set<Integer> targetAlbumIds) {
        StringBuilder sql = new StringBuilder("""
                SELECT album_id, COUNT(*) AS song_count
                FROM Song
                WHERE album_id IS NOT NULL
                  AND LOWER(name) NOT LIKE '%remix%'
                """);
        Object[] params = new Object[0];
        if (targetAlbumIds != null) {
            sql.append(" AND album_id IN (")
                    .append(String.join(",", Collections.nCopies(targetAlbumIds.size(), "?")))
                    .append(")");
            params = targetAlbumIds.toArray();
        }
        sql.append(" GROUP BY album_id HAVING COUNT(*) >= ")
                .append(MIN_ALBUM_TRACKS_FOR_FULL_LISTENS);

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
