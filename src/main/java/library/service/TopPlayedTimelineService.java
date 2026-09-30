package library.service;

import library.dto.TopPlayedSnapshotDTO;
import library.dto.TopPlayedSnapshotItemDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Service for building "Top Played Reigns" data.
 * Computes the history of top-3 snapshots (artists, songs, or genres) over time
 * by replaying all plays in chronological order and tracking every time the top-3 changes.
 */
@Service
public class TopPlayedTimelineService {

    private final JdbcTemplate jdbcTemplate;

    private static final DateTimeFormatter DB_DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DISPLAY_DATE_FMT = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    // Only display snapshots that were active on or after this date
    private static final LocalDate DISPLAY_CUTOFF = LocalDate.of(2005, 2, 14);

    public TopPlayedTimelineService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<TopPlayedSnapshotDTO> getArtistTimeline() {
        String sql = """
            SELECT s.id as song_id, s.artist_id as item_id, ar.name as item_name,
                   NULL as secondary_name,
                   COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
                   gn.name as gender_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
            """;
        return buildSnapshots(sql);
    }

    public List<TopPlayedSnapshotDTO> getSongTimeline() {
        String sql = """
            SELECT s.id as song_id, s.id as item_id, s.name as item_name,
                   ar.name as secondary_name,
                   COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
                   gn.name as gender_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
            """;
        return buildSnapshots(sql);
    }

    public List<TopPlayedSnapshotDTO> getGenreTimeline() {
        String sql = """
            SELECT s.id as song_id,
                   COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id) as item_id,
                   g.name as item_name,
                   NULL as secondary_name,
                   NULL as gender_id,
                   NULL as gender_name
            FROM song s
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN album al ON s.album_id = al.id
            LEFT JOIN genre g ON COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id) = g.id
            """;
        return buildSnapshots(sql);
    }

    /** Timeline item a song's plays count toward, with its display fields. */
    private record SongItem(Integer itemId, String itemName, String secondaryName, Integer genderId, String genderName) {
    }

    /**
     * Core algorithm: replays all plays in order, tracks cumulative counts per item,
     * builds a new snapshot whenever the top-3 ranking changes.
     * Song-to-item metadata is loaded once; the replay then only streams (play_date, song_id)
     * from the covering index, ordered by (play_date, song_id) to keep tie order deterministic.
     */
    private List<TopPlayedSnapshotDTO> buildSnapshots(String songItemSql) {
        Map<Integer, SongItem> songItems = new HashMap<>();
        jdbcTemplate.query(songItemSql, rs -> {
            songItems.put(rs.getInt("song_id"), new SongItem(
                    getNullableInt(rs, "item_id"),
                    rs.getString("item_name"),
                    rs.getString("secondary_name"),
                    getNullableInt(rs, "gender_id"),
                    rs.getString("gender_name")));
        });

        String playSql = """
            SELECT p.play_date, p.song_id
            FROM play p
            WHERE p.song_id IS NOT NULL AND p.play_date IS NOT NULL
            ORDER BY p.play_date ASC, p.song_id ASC
            """;
        return jdbcTemplate.query(playSql, rs -> {
            List<TopPlayedSnapshotDTO> snapshots = new ArrayList<>();

            Map<Integer, Integer> playCounts = new HashMap<>();
            Map<Integer, String> nameCache = new HashMap<>();
            Map<Integer, String> secondaryNameCache = new HashMap<>();
            Map<Integer, Integer> genderIdCache = new HashMap<>();
            Map<Integer, String> genderNameCache = new HashMap<>();
            Map<Integer, int[]> positionDays = new HashMap<>();
            Map<Integer, int[]> positionEntryPlays = new HashMap<>();
            Map<Integer, Integer> snapshotStartPlayCounts = new HashMap<>();

            List<Integer> currentTop3 = Collections.emptyList();

            Map<Integer, String> currentSnapshotMovement = Collections.emptyMap();
            Set<Integer> everCompletedTop3 = new HashSet<>();
            String snapshotStartDate = null;

            while (rs.next()) {
                String playDateStr = rs.getString("play_date");
                SongItem songItem = songItems.get(rs.getInt("song_id"));
                if (songItem == null || songItem.itemId() == null || playDateStr == null) {
                    continue;
                }

                int itemId = songItem.itemId();
                String playDate = extractDate(playDateStr);

                int previousCount = playCounts.getOrDefault(itemId, 0);
                int newCount = previousCount + 1;
                playCounts.put(itemId, newCount);

                if (!nameCache.containsKey(itemId)) {
                    nameCache.put(itemId, songItem.itemName() != null ? songItem.itemName() : "Unknown");
                    secondaryNameCache.put(itemId, songItem.secondaryName());
                    genderIdCache.put(itemId, songItem.genderId());
                    genderNameCache.put(itemId, songItem.genderName());
                    positionDays.put(itemId, new int[]{0, 0, 0});
                    positionEntryPlays.put(itemId, new int[]{0, 0, 0});
                }

                boolean inTop3 = currentTop3.contains(itemId);
                if (!inTop3) {
                    int threshold = currentTop3.size() == 3 ? playCounts.get(currentTop3.get(2)) : 0;
                    if (newCount < threshold) {
                        continue;
                    }
                }

                List<Integer> newTop3 = computeTop3(playCounts, currentTop3, itemId);

                if (!top3Equal(currentTop3, newTop3)) {
                    if (snapshotStartDate != null && !currentTop3.isEmpty()) {
                        // Only the outgoing top-3 counts are read when building the snapshot.
                        Map<Integer, Integer> playCountsBeforeChange = new HashMap<>();
                        for (int topId : currentTop3) {
                            playCountsBeforeChange.put(topId, topId == itemId ? previousCount : playCounts.get(topId));
                        }

                        int days = daysBetween(snapshotStartDate, playDate);
                        for (int i = 0; i < currentTop3.size(); i++) {
                            positionDays.get(currentTop3.get(i))[i] += days;
                        }
                        snapshots.add(buildSnapshotDTO(
                                snapshots.size() + 1,
                                formatDate(snapshotStartDate), formatDate(playDate), days,
                                currentTop3, currentSnapshotMovement,
                                playCountsBeforeChange, snapshotStartPlayCounts, positionDays, positionEntryPlays,
                                nameCache, secondaryNameCache, genderIdCache, genderNameCache,
                                false
                        ));
                        everCompletedTop3.addAll(currentTop3);
                    }

                    Map<Integer, String> newMovement = new HashMap<>();
                    for (int i = 0; i < newTop3.size(); i++) {
                        int id = newTop3.get(i);
                        int oldPos = currentTop3.indexOf(id);
                        if (oldPos == -1) {
                            newMovement.put(id, everCompletedTop3.contains(id) ? "CLIMBED" : "NEW");
                        } else if (oldPos > i) {
                            newMovement.put(id, "CLIMBED");
                        } else if (oldPos < i) {
                            newMovement.put(id, "DROPPED");
                        }
                    }

                    for (int i = 0; i < newTop3.size(); i++) {
                        int id = newTop3.get(i);
                        int oldPos = currentTop3.indexOf(id);
                        if (oldPos != i) {
                            positionEntryPlays.get(id)[i] = playCounts.get(id);
                        }
                    }

                    snapshotStartDate = playDate;
                    currentTop3 = newTop3;
                    currentSnapshotMovement = newMovement;

                    snapshotStartPlayCounts = new HashMap<>();
                    for (int ssId : currentTop3) {
                        snapshotStartPlayCounts.put(ssId, playCounts.getOrDefault(ssId, 0));
                    }
                }
            }

            if (snapshotStartDate != null && !currentTop3.isEmpty()) {
                String today = LocalDate.now().format(DB_DATE_FMT);
                int days = daysBetween(snapshotStartDate, today);
                for (int i = 0; i < currentTop3.size(); i++) {
                    positionDays.get(currentTop3.get(i))[i] += days;
                }
                snapshots.add(buildSnapshotDTO(
                        snapshots.size() + 1,
                        formatDate(snapshotStartDate), "Present", days,
                        currentTop3, currentSnapshotMovement,
                        playCounts, snapshotStartPlayCounts, positionDays, positionEntryPlays,
                        nameCache, secondaryNameCache, genderIdCache, genderNameCache,
                        true
                ));
            }

            return filterByCutoff(snapshots);
        });
    }

    private TopPlayedSnapshotDTO buildSnapshotDTO(
            int rank, String startDate, String endDate, int configDays,
            List<Integer> top3Ids, Map<Integer, String> movementMap,
            Map<Integer, Integer> playCounts, Map<Integer, Integer> snapshotStartPlayCounts,
            Map<Integer, int[]> positionDays, Map<Integer, int[]> positionEntryPlays,
            Map<Integer, String> nameCache, Map<Integer, String> secondaryNameCache,
            Map<Integer, Integer> genderIdCache, Map<Integer, String> genderNameCache,
            boolean current) {

        List<TopPlayedSnapshotItemDTO> items = new ArrayList<>();
        for (int i = 0; i < top3Ids.size(); i++) {
            int id = top3Ids.get(i);
            int[] pd = positionDays.get(id);
            int[] pe = positionEntryPlays.get(id);

            TopPlayedSnapshotItemDTO item = new TopPlayedSnapshotItemDTO();
            item.setItemId(id);
            item.setItemName(nameCache.get(id));
            item.setSecondaryName(secondaryNameCache.get(id));
            item.setGenderId(genderIdCache.get(id));
            item.setGenderName(genderNameCache.get(id));
            item.setPosition(i + 1);
            item.setPlaysCount(playCounts.get(id));
            item.setPlaysWhenEntered(pe[i]);
            item.setPlaysAtSnapshotStart(snapshotStartPlayCounts.getOrDefault(id, 0));
            item.setDaysAtPos1(pd[0]);
            item.setDaysAtPos2(pd[1]);
            item.setDaysAtPos3(pd[2]);
            item.setMovement(movementMap.getOrDefault(id, null));
            items.add(item);
        }

        TopPlayedSnapshotDTO snapshot = new TopPlayedSnapshotDTO();
        snapshot.setRank(rank);
        snapshot.setStartDate(startDate);
        snapshot.setEndDate(endDate);
        snapshot.setDaysInConfig(configDays);
        snapshot.setItems(items);
        snapshot.setCurrent(current);
        return snapshot;
    }

    /**
     * Compute the top-3 item IDs sorted by play count descending, then previous top-3
     * position (incumbents keep ties), then item ID.
     * Counts only ever grow by one play, so the only item that can enter or move is the one
     * just played: ranking the previous top 3 plus that item gives the same result as ranking
     * every item.
     */
    static List<Integer> computeTop3(Map<Integer, Integer> playCounts, List<Integer> previousTop3, int playedItemId) {
        List<Integer> candidates = new ArrayList<>(previousTop3);
        if (!candidates.contains(playedItemId)) {
            candidates.add(playedItemId);
        }
        candidates.sort(Comparator.<Integer>comparingInt(id -> -playCounts.get(id))
                .thenComparingInt(id -> {
                    int position = previousTop3.indexOf(id);
                    return position >= 0 ? position : Integer.MAX_VALUE;
                })
                .thenComparingInt(id -> id));
        return candidates.size() > 3 ? new ArrayList<>(candidates.subList(0, 3)) : candidates;
    }

    /**
     * Check if two top-3 lists are identical (same items in same positions).
     */
    private boolean top3Equal(List<Integer> a, List<Integer> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).equals(b.get(i))) return false;
        }
        return true;
    }

    /**
     * Filter snapshots to those active (end date) on or after cutoff.
     * A snapshot that started before the cutoff but ended after is still included.
     */
    private List<TopPlayedSnapshotDTO> filterByCutoff(List<TopPlayedSnapshotDTO> snapshots) {
        List<TopPlayedSnapshotDTO> result = new ArrayList<>();
        int newRank = 1;
        for (TopPlayedSnapshotDTO snapshot : snapshots) {
            try {
                LocalDate endDate = "Present".equals(snapshot.getEndDate())
                        ? LocalDate.now()
                        : LocalDate.parse(snapshot.getEndDate(), DISPLAY_DATE_FMT);
                if (!endDate.isBefore(DISPLAY_CUTOFF)) {
                    snapshot.setRank(newRank++);
                    result.add(snapshot);
                }
            } catch (Exception e) {
                // skip unparseable
            }
        }

        normalizeVisibleMovements(result);

        return result;
    }

    /**
     * Movement for UI is based on the visible (post-cutoff) timeline.
     * This ensures an item's first visible appearance is always NEW.
     */
    private void normalizeVisibleMovements(List<TopPlayedSnapshotDTO> visibleSnapshots) {
        Set<Integer> seenInVisibleTimeline = new HashSet<>();
        Map<Integer, Integer> previousPositions = Collections.emptyMap();

        for (TopPlayedSnapshotDTO snapshot : visibleSnapshots) {
            if (snapshot.getItems() == null) {
                previousPositions = Collections.emptyMap();
                continue;
            }

            Map<Integer, Integer> currentPositions = new HashMap<>();

            for (TopPlayedSnapshotItemDTO item : snapshot.getItems()) {
                if (item == null || item.getItemId() == null) {
                    continue;
                }

                Integer itemId = item.getItemId();
                int currentPos = item.getPosition();
                String movement = null;

                if (!seenInVisibleTimeline.contains(itemId)) {
                    movement = "NEW";
                } else {
                    Integer oldPos = previousPositions.get(itemId);
                    if (oldPos == null) {
                        movement = "CLIMBED";
                    } else if (oldPos > currentPos) {
                        movement = "CLIMBED";
                    } else if (oldPos < currentPos) {
                        movement = "DROPPED";
                    }
                }

                item.setMovement(movement);
                currentPositions.put(itemId, currentPos);
            }

            seenInVisibleTimeline.addAll(currentPositions.keySet());
            previousPositions = currentPositions;
        }
    }

    private Integer getNullableInt(java.sql.ResultSet rs, String columnLabel) throws SQLException {
        int value = rs.getInt(columnLabel);
        return rs.wasNull() ? null : value;
    }

    private String extractDate(String playDate) {
        if (playDate == null) return null;
        String trimmed = playDate.trim();
        return trimmed.length() >= 10 ? trimmed.substring(0, 10) : trimmed;
    }

    private String formatDate(String isoDate) {
        try {
            return LocalDate.parse(isoDate, DB_DATE_FMT).format(DISPLAY_DATE_FMT);
        } catch (Exception e) {
            return isoDate;
        }
    }

    private int daysBetween(String isoStart, String isoEnd) {
        try {
            LocalDate s = LocalDate.parse(isoStart, DB_DATE_FMT);
            LocalDate e = LocalDate.parse(isoEnd, DB_DATE_FMT);
            return (int) ChronoUnit.DAYS.between(s, e);
        } catch (Exception e) {
            return 0;
        }
    }
}
