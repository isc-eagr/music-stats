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
                WITH
                artist_stats AS (
                    SELECT
                        at.tag_id,
                        COUNT(DISTINCT a.id) as artist_count,
                        COUNT(DISTINCT CASE WHEN a.gender_id = 2 THEN a.id END) as male_artist_count,
                        COUNT(DISTINCT CASE WHEN a.gender_id = 1 THEN a.id END) as female_artist_count,
                        COUNT(DISTINCT CASE WHEN a.gender_id NOT IN (1, 2) AND a.gender_id IS NOT NULL THEN a.id END) as other_artist_count
                    FROM ArtistTag at
                    INNER JOIN Artist a ON a.id = at.artist_id
                    GROUP BY at.tag_id
                ),
                album_stats AS (
                    SELECT
                        alt.tag_id,
                        COUNT(DISTINCT al.id) as album_count,
                        COUNT(DISTINCT CASE WHEN ar.gender_id = 2 THEN al.id END) as male_album_count,
                        COUNT(DISTINCT CASE WHEN ar.gender_id = 1 THEN al.id END) as female_album_count,
                        COUNT(DISTINCT CASE WHEN ar.gender_id NOT IN (1, 2) AND ar.gender_id IS NOT NULL THEN al.id END) as other_album_count
                    FROM AlbumTag alt
                    INNER JOIN Album al ON al.id = alt.album_id
                    INNER JOIN Artist ar ON ar.id = al.artist_id
                    GROUP BY alt.tag_id
                ),
                song_stats AS (
                    SELECT
                        st.tag_id,
                        COUNT(DISTINCT s.id) as song_count,
                        COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 2 THEN s.id END) as male_song_count,
                        COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) = 1 THEN s.id END) as female_song_count,
                        COUNT(DISTINCT CASE WHEN COALESCE(s.override_gender_id, ar.gender_id) NOT IN (1, 2) AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL THEN s.id END) as other_song_count
                    FROM SongTag st
                    INNER JOIN Song s ON s.id = st.song_id
                    INNER JOIN Artist ar ON ar.id = s.artist_id
                    GROUP BY st.tag_id
                ),
                top_artists AS (
                    SELECT
                        at.tag_id,
                        a.id,
                        a.name,
                        a.gender_id,
                        ROW_NUMBER() OVER (PARTITION BY at.tag_id ORDER BY COUNT(p.id) DESC, LOWER(a.name)) as rn
                    FROM ArtistTag at
                    INNER JOIN Artist a ON a.id = at.artist_id
                    LEFT JOIN Song s ON s.artist_id = a.id
                    LEFT JOIN Play p ON p.song_id = s.id
                    GROUP BY at.tag_id, a.id, a.name, a.gender_id
                ),
                top_albums AS (
                    SELECT
                        alt.tag_id,
                        al.id,
                        al.name,
                        ar.name as artist_name,
                        ar.gender_id,
                        ROW_NUMBER() OVER (PARTITION BY alt.tag_id ORDER BY COUNT(p.id) DESC, LOWER(ar.name), LOWER(al.name)) as rn
                    FROM AlbumTag alt
                    INNER JOIN Album al ON al.id = alt.album_id
                    INNER JOIN Artist ar ON ar.id = al.artist_id
                    LEFT JOIN Song s ON s.album_id = al.id
                    LEFT JOIN Play p ON p.song_id = s.id
                    GROUP BY alt.tag_id, al.id, al.name, ar.name, ar.gender_id
                ),
                top_songs AS (
                    SELECT
                        st.tag_id,
                        s.id,
                        s.name,
                        ar.name as artist_name,
                        COALESCE(s.override_gender_id, ar.gender_id) as gender_id,
                        ROW_NUMBER() OVER (PARTITION BY st.tag_id ORDER BY COUNT(p.id) DESC, LOWER(ar.name), LOWER(s.name)) as rn
                    FROM SongTag st
                    INNER JOIN Song s ON s.id = st.song_id
                    INNER JOIN Artist ar ON ar.id = s.artist_id
                    LEFT JOIN Play p ON p.song_id = s.id
                    GROUP BY st.tag_id, s.id, s.name, ar.name, COALESCE(s.override_gender_id, ar.gender_id)
                )
                SELECT
                    t.id,
                    t.name,
                    COALESCE(ast.artist_count, 0) as artist_count,
                    COALESCE(alst.album_count, 0) as album_count,
                    COALESCE(sst.song_count, 0) as song_count,
                    COALESCE(ast.male_artist_count, 0) as male_artist_count,
                    COALESCE(ast.female_artist_count, 0) as female_artist_count,
                    COALESCE(ast.other_artist_count, 0) as other_artist_count,
                    COALESCE(alst.male_album_count, 0) as male_album_count,
                    COALESCE(alst.female_album_count, 0) as female_album_count,
                    COALESCE(alst.other_album_count, 0) as other_album_count,
                    COALESCE(sst.male_song_count, 0) as male_song_count,
                    COALESCE(sst.female_song_count, 0) as female_song_count,
                    COALESCE(sst.other_song_count, 0) as other_song_count,
                    ta.id as top_artist_id,
                    ta.name as top_artist_name,
                    ta.gender_id as top_artist_gender_id,
                    tal.id as top_album_id,
                    tal.name as top_album_name,
                    tal.artist_name as top_album_artist_name,
                    tal.gender_id as top_album_gender_id,
                    ts.id as top_song_id,
                    ts.name as top_song_name,
                    ts.artist_name as top_song_artist_name,
                    ts.gender_id as top_song_gender_id
                FROM Tag t
                LEFT JOIN artist_stats ast ON ast.tag_id = t.id
                LEFT JOIN album_stats alst ON alst.tag_id = t.id
                LEFT JOIN song_stats sst ON sst.tag_id = t.id
                LEFT JOIN top_artists ta ON ta.tag_id = t.id AND ta.rn = 1
                LEFT JOIN top_albums tal ON tal.tag_id = t.id AND tal.rn = 1
                LEFT JOIN top_songs ts ON ts.tag_id = t.id AND ts.rn = 1
                ORDER BY LOWER(t.name)
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            TagCardDTO tag = new TagCardDTO(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("artist_count"),
                    rs.getInt("album_count"),
                    rs.getInt("song_count")
            );
            tag.setMaleArtistCount(rs.getInt("male_artist_count"));
            tag.setFemaleArtistCount(rs.getInt("female_artist_count"));
            tag.setOtherArtistCount(rs.getInt("other_artist_count"));
            tag.setMaleAlbumCount(rs.getInt("male_album_count"));
            tag.setFemaleAlbumCount(rs.getInt("female_album_count"));
            tag.setOtherAlbumCount(rs.getInt("other_album_count"));
            tag.setMaleSongCount(rs.getInt("male_song_count"));
            tag.setFemaleSongCount(rs.getInt("female_song_count"));
            tag.setOtherSongCount(rs.getInt("other_song_count"));
            tag.setTopArtistId(getNullableInt(rs, "top_artist_id"));
            tag.setTopArtistName(rs.getString("top_artist_name"));
            tag.setTopArtistGenderId(getNullableInt(rs, "top_artist_gender_id"));
            tag.setTopAlbumId(getNullableInt(rs, "top_album_id"));
            tag.setTopAlbumName(rs.getString("top_album_name"));
            tag.setTopAlbumArtistName(rs.getString("top_album_artist_name"));
            tag.setTopAlbumGenderId(getNullableInt(rs, "top_album_gender_id"));
            tag.setTopSongId(getNullableInt(rs, "top_song_id"));
            tag.setTopSongName(rs.getString("top_song_name"));
            tag.setTopSongArtistName(rs.getString("top_song_artist_name"));
            tag.setTopSongGenderId(getNullableInt(rs, "top_song_gender_id"));
            return tag;
        });
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
    public void deleteTag(Integer tagId) {
        if (tagId == null) {
            return;
        }

        jdbcTemplate.update("DELETE FROM ArtistTag WHERE tag_id = ?", tagId);
        jdbcTemplate.update("DELETE FROM AlbumTag WHERE tag_id = ?", tagId);
        jdbcTemplate.update("DELETE FROM SongTag WHERE tag_id = ?", tagId);
        jdbcTemplate.update("DELETE FROM Tag WHERE id = ?", tagId);
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

    private Integer getNullableInt(java.sql.ResultSet rs, String columnName) throws java.sql.SQLException {
        int value = rs.getInt(columnName);
        return rs.wasNull() ? null : value;
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
