package library.service;

import library.dto.TagCardDTO;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class TagService {
    private final JdbcTemplate jdbcTemplate;

    public TagService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<Integer, String> getAllTags() {
        Map<Integer, String> tags = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT id, name FROM Tag ORDER BY LOWER(name)", (rs, rowNum) -> {
            tags.put(rs.getInt("id"), rs.getString("name"));
            return null;
        });
        return tags;
    }

    public List<Map<String, Object>> getAllTagOptions() {
        return jdbcTemplate.query("SELECT id, name FROM Tag ORDER BY LOWER(name)", (rs, rowNum) -> {
            Map<String, Object> tag = new LinkedHashMap<>();
            tag.put("id", rs.getInt("id"));
            tag.put("name", rs.getString("name"));
            return tag;
        });
    }

    public List<TagCardDTO> getTagCards() {
        String sql = """
                SELECT
                    t.id,
                    t.name,
                    (SELECT COUNT(*) FROM ArtistTag artist_tag WHERE artist_tag.tag_id = t.id) as artist_count,
                    (SELECT COUNT(*) FROM AlbumTag album_tag WHERE album_tag.tag_id = t.id) as album_count,
                    (SELECT COUNT(*) FROM SongTag song_tag WHERE song_tag.tag_id = t.id) as song_count
                FROM Tag t
                ORDER BY LOWER(t.name)
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> new TagCardDTO(
                rs.getInt("id"),
                rs.getString("name"),
                rs.getInt("artist_count"),
                rs.getInt("album_count"),
                rs.getInt("song_count")
        ));
    }

    @Transactional
    public Integer createTag(String rawName) {
        String name = rawName != null ? rawName.trim() : "";
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Tag name is required");
        }

        Integer existingId = findTagIdByName(name);
        if (existingId != null) {
            return existingId;
        }

        try {
            jdbcTemplate.update("INSERT INTO Tag (name) VALUES (?)", name);
        } catch (DataAccessException ignored) {
            Integer id = findTagIdByName(name);
            if (id != null) {
                return id;
            }
            throw ignored;
        }

        return findTagIdByName(name);
    }

    public List<Map<String, Object>> getArtistTags(Integer artistId) {
        return getAssignedTags("ArtistTag", "artist_id", artistId);
    }

    public List<Map<String, Object>> getAlbumTags(Integer albumId) {
        return getAssignedTags("AlbumTag", "album_id", albumId);
    }

    public List<Map<String, Object>> getSongTags(Integer songId) {
        return getAssignedTags("SongTag", "song_id", songId);
    }

    @Transactional
    public void saveArtistTags(Integer artistId, List<Integer> tagIds) {
        saveEntityTags("ArtistTag", "artist_id", artistId, tagIds);
    }

    @Transactional
    public void saveAlbumTags(Integer albumId, List<Integer> tagIds) {
        saveEntityTags("AlbumTag", "album_id", albumId, tagIds);
    }

    @Transactional
    public void saveSongTags(Integer songId, List<Integer> tagIds) {
        saveEntityTags("SongTag", "song_id", songId, tagIds);
    }

    private Integer findTagIdByName(String name) {
        List<Integer> ids = jdbcTemplate.query(
                "SELECT id FROM Tag WHERE LOWER(name) = LOWER(?) LIMIT 1",
                (rs, rowNum) -> rs.getInt("id"),
                name
        );
        return ids.isEmpty() ? null : ids.get(0);
    }

    private List<Map<String, Object>> getAssignedTags(String tableName, String entityColumn, Integer entityId) {
        if (entityId == null) {
            return List.of();
        }

        String sql = """
                SELECT t.id, t.name
                FROM Tag t
                INNER JOIN %s et ON et.tag_id = t.id
                WHERE et.%s = ?
                ORDER BY LOWER(t.name)
                """.formatted(tableName, entityColumn);
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> tag = new LinkedHashMap<>();
            tag.put("id", rs.getInt("id"));
            tag.put("name", rs.getString("name"));
            return tag;
        }, entityId);
    }

    private void saveEntityTags(String tableName, String entityColumn, Integer entityId, List<Integer> tagIds) {
        if (entityId == null) {
            return;
        }

        jdbcTemplate.update("DELETE FROM " + tableName + " WHERE " + entityColumn + " = ?", entityId);
        Set<Integer> distinctTagIds = new LinkedHashSet<>();
        if (tagIds != null) {
            for (Integer tagId : tagIds) {
                if (tagId != null) {
                    distinctTagIds.add(tagId);
                }
            }
        }
        if (distinctTagIds.isEmpty()) {
            return;
        }

        List<Object[]> batchArgs = new ArrayList<>();
        for (Integer tagId : distinctTagIds) {
            batchArgs.add(new Object[]{entityId, tagId});
        }
        jdbcTemplate.batchUpdate(
                "INSERT OR IGNORE INTO " + tableName + " (" + entityColumn + ", tag_id) VALUES (?, ?)",
                batchArgs
        );
    }
}
