package library.service;

import jakarta.annotation.PostConstruct;
import library.dto.LinkedSongDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

@Service
public class SongLinkService {
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern VARIANT_PATTERN = Pattern.compile(
            "(?i)\\b(remix|demo|alternate|alternative|version|edit|radio edit|single edit|club mix|mix|live|acoustic|remaster(?:ed)?|re-record(?:ed)?|instrumental|karaoke|sped up|slowed|deluxe|bonus|feat\\.?|ft\\.?)\\b");

    private final JdbcTemplate jdbcTemplate;

    public SongLinkService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void initialize() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS song_link_group (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS song_link_group_member (
                    group_id INTEGER NOT NULL,
                    song_id INTEGER NOT NULL PRIMARY KEY,
                    created_at TEXT NOT NULL
                )
                """);
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_song_link_group_member_group ON song_link_group_member(group_id)");
    }

    public List<Integer> getLinkedSongIds(Integer songId) {
        Integer groupId = getGroupId(songId);
        if (groupId == null) {
            return List.of(songId);
        }
        return jdbcTemplate.queryForList(
                "SELECT song_id FROM song_link_group_member WHERE group_id = ? ORDER BY song_id",
                Integer.class,
                groupId
        );
    }

    public Map<Integer, Integer> getGroupIdsForSongs(List<Integer> songIds) {
        if (songIds == null || songIds.isEmpty()) {
            return Map.of();
        }
        if (songIds.size() > 900) {
            Set<Integer> requestedSongIds = new HashSet<>(songIds);
            return jdbcTemplate.query(
                    "SELECT song_id, group_id FROM song_link_group_member",
                    rs -> {
                        java.util.Map<Integer, Integer> result = new java.util.HashMap<>();
                        while (rs.next()) {
                            int songId = rs.getInt("song_id");
                            if (requestedSongIds.contains(songId)) {
                                result.put(songId, rs.getInt("group_id"));
                            }
                        }
                        return result;
                    }
            );
        }
        String placeholders = String.join(",", songIds.stream().map(id -> "?").toList());
        return jdbcTemplate.query(
                "SELECT song_id, group_id FROM song_link_group_member WHERE song_id IN (" + placeholders + ")",
                rs -> {
                    java.util.Map<Integer, Integer> result = new java.util.HashMap<>();
                    while (rs.next()) {
                        result.put(rs.getInt("song_id"), rs.getInt("group_id"));
                    }
                    return result;
                },
                songIds.toArray()
        );
    }

    public List<LinkedSongDTO> getLinkedSongs(Integer songId) {
        List<Integer> ids = getLinkedSongIds(songId);
        if (ids.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        List<LinkedSongDTO> songs = jdbcTemplate.query("""
                SELECT s.id, s.name, s.artist_id, ar.name as artist_name, s.album_id, al.name as album_name,
                       s.length_seconds, COUNT(p.id) as play_count
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE s.id IN (%s)
                GROUP BY s.id, s.name, s.artist_id, ar.name, s.album_id, al.name, s.length_seconds
                """.formatted(placeholders), (rs, rowNum) -> {
            LinkedSongDTO dto = new LinkedSongDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            int albumId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : albumId);
            dto.setAlbumName(rs.getString("album_name"));
            int lengthSeconds = rs.getInt("length_seconds");
            dto.setLengthSeconds(rs.wasNull() ? null : lengthSeconds);
            dto.setLengthFormatted(formatLength(dto.getLengthSeconds()));
            dto.setPlayCount(rs.getInt("play_count"));
            return dto;
        }, ids.toArray());

        Integer cleanestId = chooseCleanestSongId(songs);
        songs.forEach(song -> song.setCleanest(song.getId().equals(cleanestId)));
        songs.sort(Comparator.comparing((LinkedSongDTO song) -> !Boolean.TRUE.equals(song.getCleanest()))
                .thenComparing(LinkedSongDTO::getName, String.CASE_INSENSITIVE_ORDER));
        return songs;
    }

    public Integer chooseCleanestSongId(List<LinkedSongDTO> songs) {
        LinkedSongDTO representative = chooseRepresentativeSong(songs, LinkedSongDTO::getId, LinkedSongDTO::getName);
        return representative != null ? representative.getId() : null;
        }

        public <T> T chooseRepresentativeSong(List<T> songs, Function<T, Integer> idExtractor, Function<T, String> nameExtractor) {
        if (songs == null || songs.isEmpty()) {
            return null;
        }

        List<Integer> songIds = songs.stream()
            .map(idExtractor)
            .filter(id -> id != null)
            .distinct()
            .toList();
        Map<Integer, RepresentativeSongMetadata> metadataBySongId = getRepresentativeSongMetadata(songIds);

        return songs.stream()
            .min(Comparator.comparingInt((T song) -> cleanTitleScore(nameExtractor.apply(song)))
                .thenComparing(song -> getReleaseDate(metadataBySongId, idExtractor.apply(song)), Comparator.nullsLast(LocalDate::compareTo))
                .thenComparing(song -> getFirstPlayDate(metadataBySongId, idExtractor.apply(song)), Comparator.nullsLast(LocalDate::compareTo))
                .thenComparing(song -> {
                    String name = nameExtractor.apply(song);
                    return name == null ? "" : name;
                }, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(idExtractor, Comparator.nullsLast(Integer::compareTo)))
            .orElse(null);
    }

    public int cleanTitleScore(String name) {
        if (name == null) {
            return 1000;
        }
        int score = name.length();
        java.util.regex.Matcher matcher = VARIANT_PATTERN.matcher(name);
        while (matcher.find()) {
            score += 200;
        }
        score += count(name, '(') * 20;
        score += count(name, '[') * 20;
        score += count(name, '-') * 5;
        return score;
    }

    @Transactional
    public Set<Integer> saveLinkedSongs(Integer songId, List<Integer> requestedSongIds) {
        Set<Integer> impactedSongIds = new LinkedHashSet<>(getLinkedSongIds(songId));
        Set<Integer> desired = new LinkedHashSet<>();
        desired.add(songId);
        if (requestedSongIds != null) {
            for (Integer requestedId : requestedSongIds) {
                if (requestedId != null) {
                    desired.add(requestedId);
                    impactedSongIds.addAll(getLinkedSongIds(requestedId));
                }
            }
        }

        Integer groupId = null;
        for (Integer desiredId : desired) {
            groupId = getGroupId(desiredId);
            if (groupId != null) {
                break;
            }
        }
        if (groupId == null && desired.size() > 1) {
            groupId = createGroup();
        }

        Set<Integer> groupsToClean = new HashSet<>();
        for (Integer impactedId : impactedSongIds) {
            Integer existingGroupId = getGroupId(impactedId);
            if (existingGroupId != null) {
                groupsToClean.add(existingGroupId);
            }
        }
        for (Integer desiredId : desired) {
            Integer existingGroupId = getGroupId(desiredId);
            if (existingGroupId != null) {
                groupsToClean.add(existingGroupId);
            }
        }

        if (!groupsToClean.isEmpty()) {
            String placeholders = String.join(",", groupsToClean.stream().map(id -> "?").toList());
            jdbcTemplate.update("DELETE FROM song_link_group_member WHERE group_id IN (" + placeholders + ")", groupsToClean.toArray());
        }

        if (desired.size() > 1) {
            String now = now();
            for (Integer desiredId : desired) {
                jdbcTemplate.update(
                        "INSERT OR REPLACE INTO song_link_group_member (group_id, song_id, created_at) VALUES (?, ?, ?)",
                        groupId,
                        desiredId,
                        now
                );
            }
            jdbcTemplate.update("UPDATE song_link_group SET updated_at = ? WHERE id = ?", now, groupId);
        }

        cleanupEmptyGroups(groupsToClean);
        impactedSongIds.addAll(desired);
        return impactedSongIds;
    }

    public List<String> getAffectedWeeklyPeriodKeysForLinkedSongs() {
        return jdbcTemplate.queryForList("""
                SELECT DISTINCT strftime('%Y-W%W', p.play_date) as period_key
                FROM Play p
                INNER JOIN song_link_group_member slgm ON slgm.song_id = p.song_id
                WHERE p.play_date IS NOT NULL
                  AND p.song_id IS NOT NULL
                  AND slgm.group_id IN (
                      SELECT group_id
                      FROM song_link_group_member
                      GROUP BY group_id
                      HAVING COUNT(*) > 1
                  )
                ORDER BY period_key
                """, String.class);
    }

    public List<String> getAffectedWeeklyPeriodKeysForSongIds(Set<Integer> songIds) {
        if (songIds == null || songIds.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", songIds.stream().map(id -> "?").toList());
        return jdbcTemplate.queryForList("""
                SELECT DISTINCT strftime('%%Y-W%%W', play_date) as period_key
                FROM Play
                WHERE song_id IN (%s)
                  AND play_date IS NOT NULL
                ORDER BY period_key
                """.formatted(placeholders), String.class, songIds.toArray());
    }

    private Integer getGroupId(Integer songId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT group_id FROM song_link_group_member WHERE song_id = ?",
                    Integer.class,
                    songId
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private Integer createGroup() {
        String now = now();
        jdbcTemplate.update("INSERT INTO song_link_group (created_at, updated_at) VALUES (?, ?)", now, now);
        return jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Integer.class);
    }

    private void cleanupEmptyGroups(Set<Integer> groupIds) {
        if (groupIds == null || groupIds.isEmpty()) {
            return;
        }
        List<Integer> emptyGroups = new ArrayList<>();
        for (Integer groupId : groupIds) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM song_link_group_member WHERE group_id = ?",
                    Integer.class,
                    groupId
            );
            if (count == null || count == 0) {
                emptyGroups.add(groupId);
            }
        }
        if (!emptyGroups.isEmpty()) {
            String placeholders = String.join(",", emptyGroups.stream().map(id -> "?").toList());
            jdbcTemplate.update("DELETE FROM song_link_group WHERE id IN (" + placeholders + ")", emptyGroups.toArray());
        }
    }

    private int count(String value, char character) {
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == character) {
                count++;
            }
        }
        return count;
    }

    private String formatLength(Integer lengthSeconds) {
        if (lengthSeconds == null || lengthSeconds <= 0) {
            return null;
        }
        return String.format("%d:%02d", lengthSeconds / 60, lengthSeconds % 60);
    }

    private String now() {
        return LocalDateTime.now().format(TIMESTAMP_FORMAT);
    }

    private Map<Integer, RepresentativeSongMetadata> getRepresentativeSongMetadata(List<Integer> songIds) {
        if (songIds == null || songIds.isEmpty()) {
            return Map.of();
        }

        String placeholders = String.join(",", songIds.stream().map(id -> "?").toList());
        return jdbcTemplate.query("""
                SELECT s.id,
                       DATE(s.release_date) as release_date,
                       MIN(DATE(p.play_date)) as first_play_date
                FROM Song s
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE s.id IN (%s)
                GROUP BY s.id, s.release_date
                """.formatted(placeholders), rs -> {
            java.util.Map<Integer, RepresentativeSongMetadata> result = new java.util.HashMap<>();
            while (rs.next()) {
                Integer songId = rs.getInt("id");
                result.put(songId, new RepresentativeSongMetadata(
                        parseIsoDate(rs.getString("release_date")),
                        parseIsoDate(rs.getString("first_play_date"))
                ));
            }
            return result;
        }, songIds.toArray());
    }

    private LocalDate getReleaseDate(Map<Integer, RepresentativeSongMetadata> metadataBySongId, Integer songId) {
        RepresentativeSongMetadata metadata = metadataBySongId.get(songId);
        return metadata != null ? metadata.releaseDate() : null;
    }

    private LocalDate getFirstPlayDate(Map<Integer, RepresentativeSongMetadata> metadataBySongId, Integer songId) {
        RepresentativeSongMetadata metadata = metadataBySongId.get(songId);
        return metadata != null ? metadata.firstPlayDate() : null;
    }

    private LocalDate parseIsoDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return LocalDate.parse(value);
    }

    private record RepresentativeSongMetadata(LocalDate releaseDate, LocalDate firstPlayDate) {
    }
}
