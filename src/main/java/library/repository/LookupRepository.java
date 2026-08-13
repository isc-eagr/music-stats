package library.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.util.LinkedHashMap;
import java.util.Map;

@Repository
public class LookupRepository {
    
    private final JdbcTemplate jdbcTemplate;
    
    public LookupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public Map<Integer, String> getAllGenders() {
        return getAllLookupValues("Gender", "name");
    }
    
    public Map<Integer, String> getAllEthnicities() {
        return getAllLookupValues("Ethnicity", "name");
    }
    
    public Map<Integer, String> getAllGenres() {
        return getAllLookupValues("Genre", "name");
    }
    
    public Map<Integer, String> getAllSubGenres() {
        return getAllLookupValues("SubGenre", "name");
    }
    
    public Map<Integer, String> getAllLanguages() {
        return getAllLookupValues("Language", "name");
    }

    public Map<Integer, String> getAllTags() {
        return getAllLookupValues("Tag", "LOWER(name)");
    }

    private Map<Integer, String> getAllLookupValues(String tableName, String orderByExpression) {
        Map<Integer, String> values = new LinkedHashMap<>();
        String sql = "SELECT id, name FROM " + tableName + " ORDER BY " + orderByExpression;
        jdbcTemplate.query(sql, (RowCallbackHandler) rs ->
                values.put(rs.getInt("id"), rs.getString("name")));
        return values;
    }

    /**
     * Get genre ID by name (e.g., "Rap" -> 5)
     */
    public Integer getGenreIdByName(String name) {
        String sql = "SELECT id FROM Genre WHERE name = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, name);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get language ID by name (e.g., "Spanish" -> 2)
     */
    public Integer getLanguageIdByName(String name) {
        String sql = "SELECT id FROM Language WHERE name = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, name);
        } catch (Exception e) {
            return null;
        }
    }
}
