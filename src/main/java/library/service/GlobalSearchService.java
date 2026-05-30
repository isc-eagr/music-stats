package library.service;

import library.dto.GlobalSearchResultDTO;
import library.util.StringNormalizer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class GlobalSearchService {

    private static final int DEFAULT_LIMIT_PER_TYPE = 6;
    private static final int MAX_LIMIT_PER_TYPE = 10;

    private final JdbcTemplate jdbcTemplate;

    public GlobalSearchService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<GlobalSearchResultDTO> search(String query, int limitPerType) {
        String normalizedQuery = StringNormalizer.normalizeForSearch(query);
        if (normalizedQuery == null || normalizedQuery.isBlank()) {
            return List.of();
        }

        int limit = normalizeLimit(limitPerType);
        String wildcardQuery = "%" + normalizedQuery + "%";
        String startsWithQuery = normalizedQuery + "%";

        List<GlobalSearchResultDTO> results = new ArrayList<>();
        results.addAll(searchArtists(wildcardQuery, normalizedQuery, startsWithQuery, limit));
        results.addAll(searchAlbums(wildcardQuery, normalizedQuery, startsWithQuery, limit));
        results.addAll(searchSongs(wildcardQuery, normalizedQuery, startsWithQuery, limit));
        return results;
    }

    private int normalizeLimit(int limitPerType) {
        if (limitPerType <= 0) {
            return DEFAULT_LIMIT_PER_TYPE;
        }
        return Math.min(limitPerType, MAX_LIMIT_PER_TYPE);
    }

    private List<GlobalSearchResultDTO> searchArtists(String wildcardQuery, String exactQuery, String startsWithQuery, int limit) {
        String artistName = StringNormalizer.sqlNormalizeColumn("a.name");
        String sql = """
            SELECT a.id, a.name,
                   CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image
            FROM Artist a
            WHERE %s LIKE ?
            ORDER BY CASE
                         WHEN %s = ? THEN 0
                         WHEN %s LIKE ? THEN 1
                         ELSE 2
                     END,
                     a.name
            LIMIT ?
            """.formatted(artistName, artistName, artistName);

        return jdbcTemplate.query(sql, (rs, rowNum) -> new GlobalSearchResultDTO(
            "artist",
            rs.getInt("id"),
            rs.getString("name"),
            "/artists/" + rs.getInt("id"),
            "/artists/" + rs.getInt("id") + "/image?thumbnail=true",
            rs.getInt("has_image") == 1
        ), wildcardQuery, exactQuery, startsWithQuery, limit);
    }

    private List<GlobalSearchResultDTO> searchAlbums(String wildcardQuery, String exactQuery, String startsWithQuery, int limit) {
        String albumName = StringNormalizer.sqlNormalizeColumn("al.name");
        String artistName = StringNormalizer.sqlNormalizeColumn("ar.name");
        String sql = """
            SELECT al.id, al.name,
                   CASE
                       WHEN al.image IS NOT NULL
                            OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id)
                       THEN 1 ELSE 0
                   END as has_image
            FROM Album al
            JOIN Artist ar ON al.artist_id = ar.id
            WHERE %s LIKE ? OR %s LIKE ?
            ORDER BY CASE
                         WHEN %s = ? THEN 0
                         WHEN %s LIKE ? THEN 1
                         WHEN %s = ? THEN 2
                         WHEN %s LIKE ? THEN 3
                         ELSE 4
                     END,
                     al.name
            LIMIT ?
            """.formatted(albumName, artistName, albumName, albumName, artistName, artistName);

        return jdbcTemplate.query(sql, (rs, rowNum) -> new GlobalSearchResultDTO(
            "album",
            rs.getInt("id"),
            rs.getString("name"),
            "/albums/" + rs.getInt("id"),
            "/albums/" + rs.getInt("id") + "/image?thumbnail=true",
            rs.getInt("has_image") == 1
        ), wildcardQuery, wildcardQuery, exactQuery, startsWithQuery, exactQuery, startsWithQuery, limit);
    }

    private List<GlobalSearchResultDTO> searchSongs(String wildcardQuery, String exactQuery, String startsWithQuery, int limit) {
        String songName = StringNormalizer.sqlNormalizeColumn("s.name");
        String artistName = StringNormalizer.sqlNormalizeColumn("ar.name");
        String albumName = StringNormalizer.sqlNormalizeColumn("al.name");
        String sql = """
            SELECT s.id, s.name,
                   CASE
                       WHEN s.single_cover IS NOT NULL
                            OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id)
                            OR al.image IS NOT NULL
                       THEN 1 ELSE 0
                   END as has_image
            FROM Song s
            JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE %s LIKE ? OR %s LIKE ? OR %s LIKE ?
            ORDER BY CASE
                         WHEN %s = ? THEN 0
                         WHEN %s LIKE ? THEN 1
                         WHEN %s = ? THEN 2
                         WHEN %s LIKE ? THEN 3
                         WHEN %s = ? THEN 4
                         WHEN %s LIKE ? THEN 5
                         ELSE 6
                     END,
                     s.name
            LIMIT ?
            """.formatted(songName, artistName, albumName, songName, songName, artistName, artistName, albumName, albumName);

        return jdbcTemplate.query(sql, (rs, rowNum) -> new GlobalSearchResultDTO(
            "song",
            rs.getInt("id"),
            rs.getString("name"),
            "/songs/" + rs.getInt("id"),
            "/songs/" + rs.getInt("id") + "/image?thumbnail=true",
            rs.getInt("has_image") == 1
        ), wildcardQuery, wildcardQuery, wildcardQuery, exactQuery, startsWithQuery, exactQuery, startsWithQuery, exactQuery, startsWithQuery, limit);
    }
}
