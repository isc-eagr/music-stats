package library.service;

import library.dto.TopPlayedTimelineEntryDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Service for building "Top Played Timeline" data.
 * Computes the history of #1 most-played artists, songs, and genres over time
 * by replaying all plays in chronological order and tracking when the leader changes.
 */
@Service
public class TopPlayedTimelineService {

    private final JdbcTemplate jdbcTemplate;

    private static final DateTimeFormatter DB_DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DISPLAY_DATE_FMT = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public TopPlayedTimelineService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Build the artist #1 timeline: replays all plays in chronological order,
     * tracking cumulative plays per artist, and recording every time the #1 artist changes.
     */
    public List<TopPlayedTimelineEntryDTO> getArtistTimeline() {
        // Get all plays with artist info, ordered by date (no filtering yet)
        String sql = """
            SELECT p.play_date, s.artist_id, ar.name as artist_name,
                   COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
                   gn.name as gender_name
            FROM play p
            JOIN song s ON p.song_id = s.id
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
            WHERE p.song_id IS NOT NULL
            ORDER BY p.play_date ASC
            """;

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        return buildTimeline(rows, "artist_id", "artist_name", null, "gender_id", "gender_name");
    }

    /**
     * Build the song #1 timeline: replays all plays chronologically,
     * tracking cumulative plays per song.
     */
    public List<TopPlayedTimelineEntryDTO> getSongTimeline() {
        String sql = """
            SELECT p.play_date, p.song_id as song_id, s.name as song_name, ar.name as artist_name,
                   COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
                   gn.name as gender_name
            FROM play p
            JOIN song s ON p.song_id = s.id
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id
            WHERE p.song_id IS NOT NULL
            ORDER BY p.play_date ASC
            """;

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        return buildTimeline(rows, "song_id", "song_name", "artist_name", "gender_id", "gender_name");
    }

    /**
     * Build the genre #1 timeline: replays all plays chronologically,
     * tracking cumulative plays per effective genre.
     */
    public List<TopPlayedTimelineEntryDTO> getGenreTimeline() {
        String sql = """
            SELECT p.play_date,
                   COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id) as genre_id,
                   g.name as genre_name
            FROM play p
            JOIN song s ON p.song_id = s.id
            JOIN artist ar ON s.artist_id = ar.id
            LEFT JOIN album al ON s.album_id = al.id
            LEFT JOIN genre g ON COALESCE(s.override_genre_id, al.override_genre_id, ar.genre_id) = g.id
            WHERE p.song_id IS NOT NULL
            ORDER BY p.play_date ASC
            """;

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        return buildTimeline(rows, "genre_id", "genre_name", null, null, null);
    }

    /**
     * Generic timeline builder. Replays plays in order, tracks cumulative counts per item,
     * and records every time the #1 leader changes.
     */
    private List<TopPlayedTimelineEntryDTO> buildTimeline(
            List<Map<String, Object>> rows,
            String idCol, String nameCol, String secondaryNameCol,
            String genderIdCol, String genderNameCol) {

        List<TopPlayedTimelineEntryDTO> timeline = new ArrayList<>();
        if (rows.isEmpty()) return timeline;

        // Cumulative play counts per item
        Map<Integer, Integer> playCounts = new HashMap<>();
        // Metadata cache
        Map<Integer, String> nameCache = new HashMap<>();
        Map<Integer, String> secondaryNameCache = new HashMap<>();
        Map<Integer, Integer> genderIdCache = new HashMap<>();
        Map<Integer, String> genderNameCache = new HashMap<>();

        Integer currentLeaderId = null;
        int currentLeaderPlays = 0;

        for (Map<String, Object> row : rows) {
            String playDateStr = (String) row.get("play_date");
            Number itemIdNum = (Number) row.get(idCol);
            if (itemIdNum == null || playDateStr == null) continue;

            int itemId = itemIdNum.intValue();
            String playDate = extractDate(playDateStr); // extract just date part

            // Increment cumulative count
            int newCount = playCounts.merge(itemId, 1, Integer::sum);

            // Cache metadata
            if (!nameCache.containsKey(itemId)) {
                nameCache.put(itemId, row.get(nameCol) != null ? row.get(nameCol).toString() : "Unknown");
                if (secondaryNameCol != null) {
                    secondaryNameCache.put(itemId, row.get(secondaryNameCol) != null ? row.get(secondaryNameCol).toString() : "");
                }
                if (genderIdCol != null) {
                    Number gid = (Number) row.get(genderIdCol);
                    genderIdCache.put(itemId, gid != null ? gid.intValue() : null);
                    genderNameCache.put(itemId, row.get(genderNameCol) != null ? row.get(genderNameCol).toString() : null);
                }
            }

            // Check if leader changed: the played item just overtook the current leader
            boolean leaderChanged = false;
            if (currentLeaderId == null) {
                leaderChanged = true;
            } else if (itemId != currentLeaderId && newCount > currentLeaderPlays) {
                leaderChanged = true;
            } else if (itemId == currentLeaderId) {
                currentLeaderPlays = newCount; // update current leader's count
            }

            if (leaderChanged) {
                // Close previous reign
                if (currentLeaderId != null) {
                    TopPlayedTimelineEntryDTO prev = timeline.get(timeline.size() - 1);
                    prev.setEndDate(formatDate(playDate));
                    prev.setPlaysAtEnd(playCounts.get(currentLeaderId));
                    prev.setDaysAtNumberOne(daysBetween(prev.getStartDate(), prev.getEndDate()));
                    prev.setCurrent(false);
                }

                // Start new reign
                Integer leaderId = itemId;
                TopPlayedTimelineEntryDTO entry = new TopPlayedTimelineEntryDTO();
                entry.setRank(timeline.size() + 1);
                entry.setItemId(leaderId);
                entry.setItemName(nameCache.get(leaderId));
                if (secondaryNameCol != null) {
                    entry.setSecondaryName(secondaryNameCache.get(leaderId));
                }
                if (genderIdCol != null) {
                    entry.setGenderId(genderIdCache.get(leaderId));
                    entry.setGenderName(genderNameCache.get(leaderId));
                }
                entry.setStartDate(formatDate(playDate));
                entry.setPlaysAtStart(newCount);
                entry.setCurrent(true);

                timeline.add(entry);
                currentLeaderId = leaderId;
                currentLeaderPlays = newCount;
            }
        }

        // Close the last (current) reign
        if (!timeline.isEmpty()) {
            TopPlayedTimelineEntryDTO last = timeline.get(timeline.size() - 1);
            String today = LocalDate.now().format(DB_DATE_FMT);
            last.setEndDate("Present");
            last.setPlaysAtEnd(playCounts.get(currentLeaderId));
            last.setDaysAtNumberOne(daysBetween(last.getStartDate(), formatDate(today)));
            last.setCurrent(true);
        }

        // Post-process: filter by cutoff date and add multi-reign tracking
        return postProcessTimeline(timeline);
    }

    /**
     * Post-process timeline to:
     * 1. Filter to only display reigns that were active on or after Feb 14, 2005
     * 2. Calculate total days across all displayed reigns for each item
     * 3. Assign reign sequence numbers (1st, 2nd, 3rd reign, etc.)
     * 4. Re-rank the filtered entries
     */
    private List<TopPlayedTimelineEntryDTO> postProcessTimeline(List<TopPlayedTimelineEntryDTO> rawTimeline) {
        // Filter by cutoff date - show reigns that:
        // - Started on or after Feb 14, 2005, OR
        // - Started before but ended on or after Feb 14, 2005 (i.e., were active during that date)
        LocalDate cutoffDate = LocalDate.of(2005, 2, 14);
        List<TopPlayedTimelineEntryDTO> filtered = new ArrayList<>();
        for (TopPlayedTimelineEntryDTO entry : rawTimeline) {
            try {
                LocalDate startDate = LocalDate.parse(entry.getStartDate(), DISPLAY_DATE_FMT);
                LocalDate endDate;
                
                // Parse end date (might be "Present")
                if ("Present".equals(entry.getEndDate())) {
                    endDate = LocalDate.now();
                } else {
                    endDate = LocalDate.parse(entry.getEndDate(), DISPLAY_DATE_FMT);
                }
                
                // Include if: started on/after cutoff, OR ended on/after cutoff
                if (startDate.isAfter(cutoffDate) || startDate.isEqual(cutoffDate) ||
                    endDate.isAfter(cutoffDate) || endDate.isEqual(cutoffDate)) {
                    filtered.add(entry);
                }
            } catch (Exception e) {
                // If can't parse, skip it
            }
        }

        // Group by itemId to count total reigns and accumulate days
        Map<Integer, List<TopPlayedTimelineEntryDTO>> byItem = new HashMap<>();
        Map<Integer, Integer> totalDaysByItem = new HashMap<>();
        for (TopPlayedTimelineEntryDTO entry : filtered) {
            byItem.computeIfAbsent(entry.getItemId(), k -> new ArrayList<>()).add(entry);
            totalDaysByItem.merge(entry.getItemId(), entry.getDaysAtNumberOne(), Integer::sum);
        }

        // Assign sequence numbers and totals, then re-rank
        List<TopPlayedTimelineEntryDTO> result = new ArrayList<>();
        for (TopPlayedTimelineEntryDTO entry : filtered) {
            List<TopPlayedTimelineEntryDTO> reignsForItem = byItem.get(entry.getItemId());
            int totalReigns = reignsForItem.size();
            int sequence = reignsForItem.indexOf(entry) + 1; // 1-based
            int totalDays = totalDaysByItem.get(entry.getItemId());

            entry.setTotalReignCount(totalReigns);
            entry.setReignSequence(sequence);
            entry.setTotalDaysAllReigns(totalDays);
            result.add(entry);
        }

        // Re-rank the filtered timeline
        for (int i = 0; i < result.size(); i++) {
            result.get(i).setRank(i + 1);
        }

        return result;
    }

    /**
     * Extract date portion from play_date string (format: "yyyy-MM-dd HH:mm" -> "yyyy-MM-dd")
     */
    private String extractDate(String playDate) {
        if (playDate == null) return null;
        String trimmed = playDate.trim();
        if (trimmed.length() >= 10) {
            return trimmed.substring(0, 10);
        }
        return trimmed;
    }

    /**
     * Format a yyyy-MM-dd date string to dd/MM/yyyy display format
     */
    private String formatDate(String isoDate) {
        try {
            LocalDate d = LocalDate.parse(isoDate, DB_DATE_FMT);
            return d.format(DISPLAY_DATE_FMT);
        } catch (Exception e) {
            return isoDate;
        }
    }

    /**
     * Calculate days between two dd/MM/yyyy formatted date strings
     */
    private int daysBetween(String startDisplay, String endDisplay) {
        try {
            LocalDate start = LocalDate.parse(startDisplay, DISPLAY_DATE_FMT);
            LocalDate end;
            if ("Present".equals(endDisplay)) {
                end = LocalDate.now();
            } else {
                end = LocalDate.parse(endDisplay, DISPLAY_DATE_FMT);
            }
            return (int) ChronoUnit.DAYS.between(start, end);
        } catch (Exception e) {
            return 0;
        }
    }
}
