package library.repository;

import org.springframework.jdbc.core.JdbcTemplate;
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
        Map<Integer, String> genders = new LinkedHashMap<>();
        String sql = "SELECT id, name FROM Gender ORDER BY name";
        jdbcTemplate.query(sql, (rs, rowNum) -> {
            genders.put(rs.getInt("id"), rs.getString("name"));
            return null;
        });
        return genders;
    }
    
    public Map<Integer, String> getAllEthnicities() {
        Map<Integer, String> ethnicities = new LinkedHashMap<>();
        String sql = "SELECT id, name FROM Ethnicity ORDER BY name";
        jdbcTemplate.query(sql, (rs, rowNum) -> {
            ethnicities.put(rs.getInt("id"), rs.getString("name"));
            return null;
        });
        return ethnicities;
    }
    
    public Map<Integer, String> getAllGenres() {
        Map<Integer, String> genres = new LinkedHashMap<>();
        String sql = "SELECT id, name FROM Genre ORDER BY name";
        jdbcTemplate.query(sql, (rs, rowNum) -> {
            genres.put(rs.getInt("id"), rs.getString("name"));
            return null;
        });
        return genres;
    }
    
    public Map<Integer, String> getAllSubGenres() {
        Map<Integer, String> subgenres = new LinkedHashMap<>();
        String sql = "SELECT id, name FROM SubGenre ORDER BY name";
        jdbcTemplate.query(sql, (rs, rowNum) -> {
            subgenres.put(rs.getInt("id"), rs.getString("name"));
            return null;
        });
        return subgenres;
    }
    
    public Map<Integer, String> getAllLanguages() {
        Map<Integer, String> languages = new LinkedHashMap<>();
        String sql = "SELECT id, name FROM Language ORDER BY name";
        jdbcTemplate.query(sql, (rs, rowNum) -> {
            languages.put(rs.getInt("id"), rs.getString("name"));
            return null;
        });
        return languages;
    }
}
