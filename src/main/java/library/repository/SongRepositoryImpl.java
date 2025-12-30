package library.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SongRepositoryImpl {

	private JdbcTemplate template;

	public SongRepositoryImpl(JdbcTemplate template) {
		this.template = template;
	}

	// Get total number of unique artists
	public long getTotalArtistsCount() {
		String sql = "SELECT COUNT(DISTINCT id) FROM Artist";
		Long count = template.queryForObject(sql, Long.class);
		return count != null ? count : 0;
	}
	
	// Get total number of unique albums
	public long getTotalAlbumsCount() {
		String sql = "SELECT COUNT(DISTINCT id) FROM Album";
		Long count = template.queryForObject(sql, Long.class);
		return count != null ? count : 0;
	}

	// Get total number of songs
	public long getTotalSongsCount() {
		String sql = "SELECT COUNT(*) FROM Song";
		Long count = template.queryForObject(sql, Long.class);
		return count != null ? count : 0;
	}

	// Get total number of scrobbles (plays)
	public long getTotalScrobblesCount() {
		String sql = "SELECT COUNT(*) FROM Scrobble";
		Long count = template.queryForObject(sql, Long.class);
		return count != null ? count : 0;
	}
	
	// Get play counts by account (vatito = primary, robertlover = legacy)
	public java.util.Map<String, Long> getPlayCountsByAccount() {
		String sql = "SELECT COALESCE(account, '') as account, COUNT(*) as cnt FROM Scrobble GROUP BY account";
		java.util.Map<String, Long> result = new java.util.HashMap<>();
		result.put("primary", 0L);   // vatito
		result.put("legacy", 0L);    // robertlover
		result.put("total", 0L);
		
		template.query(sql, rs -> {
			String account = rs.getString("account");
			long count = rs.getLong("cnt");
			if ("vatito".equalsIgnoreCase(account)) {
				result.put("primary", count);
			} else if ("robertlover".equalsIgnoreCase(account)) {
				result.put("legacy", count);
			}
			result.put("total", result.get("total") + count);
		});
		
		return result;
	}
	
	// Get total listening time across all scrobbles
	public String getTotalListeningTime() {
		String sql = """
			SELECT SUM(s.length_seconds) as total_seconds
			FROM Scrobble scr
			INNER JOIN Song s ON scr.song_id = s.id
			WHERE s.length_seconds IS NOT NULL
			""";
		
		try {
			Integer totalSeconds = template.queryForObject(sql, Integer.class);
			if (totalSeconds == null || totalSeconds == 0) {
				return "0:00";
			}
			return formatDuration(totalSeconds);
		} catch (Exception e) {
			return "0:00";
		}
	}
	
	// Helper method to format duration from seconds
	private String formatDuration(int totalSeconds) {
		if (totalSeconds <= 0) return "0:00";
		
		long days = totalSeconds / 86400;
		long hours = (totalSeconds % 86400) / 3600;
		long minutes = (totalSeconds % 3600) / 60;
		
		if (days > 0) {
			return String.format("%dd %dh %dm", days, hours, minutes);
		} else if (hours > 0) {
			return String.format("%dh %dm", hours, minutes);
		} else {
			return String.format("%dm", minutes);
		}
	}

	// Get play counts by gender (returns map with male, female counts)
	public java.util.Map<String, Long> getPlayCountsByGender() {
		String sql = """
			SELECT 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END as gender,
				COUNT(*) as play_count
			FROM Scrobble scr
			INNER JOIN Song s ON scr.song_id = s.id
			INNER JOIN Artist a ON s.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			GROUP BY 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END
			""";
		
		java.util.Map<String, Long> result = new java.util.HashMap<>();
		result.put("male", 0L);
		result.put("female", 0L);
		result.put("other", 0L);

		template.query(sql, rs -> {
			String gender = rs.getString("gender");
			long count = rs.getLong("play_count");
			result.put(gender, count);
		});
		
		return result;
	}

	// Get listening time by gender (returns map with male, female, other times in seconds)
	public java.util.Map<String, Long> getListeningTimeByGender() {
		String sql = """
			SELECT 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END as gender,
				SUM(COALESCE(s.length_seconds, 0)) as total_seconds
			FROM Scrobble scr
			INNER JOIN Song s ON scr.song_id = s.id
			INNER JOIN Artist a ON s.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			GROUP BY 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END
			""";
		
		java.util.Map<String, Long> result = new java.util.HashMap<>();
		result.put("male", 0L);
		result.put("female", 0L);
		result.put("other", 0L);

		template.query(sql, rs -> {
			String gender = rs.getString("gender");
			long seconds = rs.getLong("total_seconds");
			result.put(gender, seconds);
		});
		
		return result;
	}

	// Get artist counts by gender
	public java.util.Map<String, Long> getArtistCountsByGender() {
		String sql = """
			SELECT 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END as gender,
				COUNT(DISTINCT a.id) as artist_count
			FROM Artist a
			LEFT JOIN Gender g ON a.gender_id = g.id
			GROUP BY 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END
			""";
		
		java.util.Map<String, Long> result = new java.util.HashMap<>();
		result.put("male", 0L);
		result.put("female", 0L);
		result.put("other", 0L);

		template.query(sql, rs -> {
			String gender = rs.getString("gender");
			long count = rs.getLong("artist_count");
			result.put(gender, count);
		});
		
		return result;
	}

	// Get song counts by gender
	public java.util.Map<String, Long> getSongCountsByGender() {
		String sql = """
			SELECT 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END as gender,
				COUNT(DISTINCT s.id) as song_count
			FROM Song s
			INNER JOIN Artist a ON s.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			GROUP BY 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END
			""";
		
		java.util.Map<String, Long> result = new java.util.HashMap<>();
		result.put("male", 0L);
		result.put("female", 0L);
		result.put("other", 0L);

		template.query(sql, rs -> {
			String gender = rs.getString("gender");
			long count = rs.getLong("song_count");
			result.put(gender, count);
		});
		
		return result;
	}

	// Get album counts by gender
	public java.util.Map<String, Long> getAlbumCountsByGender() {
		String sql = """
			SELECT 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END as gender,
				COUNT(DISTINCT al.id) as album_count
			FROM Album al
			INNER JOIN Artist a ON al.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			GROUP BY 
				CASE 
					WHEN g.name LIKE '%Female%' THEN 'female'
					WHEN g.name LIKE '%Male%' THEN 'male'
					ELSE 'other'
				END
			""";
		
		java.util.Map<String, Long> result = new java.util.HashMap<>();
		result.put("male", 0L);
		result.put("female", 0L);
		result.put("other", 0L);

		template.query(sql, rs -> {
			String gender = rs.getString("gender");
			long count = rs.getLong("album_count");
			result.put(gender, count);
		});
		
		return result;
	}

	// Get play counts by genre with gender breakdown
	public java.util.List<java.util.Map<String, Object>> getPlayCountsByGenreAndGender() {
		String sql = """
			SELECT 
				COALESCE(gr.name, 'Unknown') as genre_name,
				SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
				SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count
			FROM Scrobble scr
			INNER JOIN Song s ON scr.song_id = s.id
			INNER JOIN Artist a ON s.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			LEFT JOIN Genre gr ON COALESCE(s.genre_id_override, a.genre_id) = gr.id
			GROUP BY COALESCE(gr.name, 'Unknown')
			HAVING (male_count + female_count) > 0
			ORDER BY (male_count + female_count) DESC
			LIMIT 10
			""";
		
		return template.query(sql, (rs, rowNum) -> {
			java.util.Map<String, Object> row = new java.util.HashMap<>();
			row.put("name", rs.getString("genre_name"));
			row.put("male", rs.getLong("male_count"));
			row.put("female", rs.getLong("female_count"));
			return row;
		});
	}

	// Get play counts by ethnicity with gender breakdown
	public java.util.List<java.util.Map<String, Object>> getPlayCountsByEthnicityAndGender() {
		String sql = """
			SELECT 
				COALESCE(e.name, 'Unknown') as ethnicity_name,
				SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
				SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count
			FROM Scrobble scr
			INNER JOIN Song s ON scr.song_id = s.id
			INNER JOIN Artist a ON s.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
			GROUP BY COALESCE(e.name, 'Unknown')
			HAVING (male_count + female_count) > 0
			ORDER BY (male_count + female_count) DESC
			LIMIT 10
			""";
		
		return template.query(sql, (rs, rowNum) -> {
			java.util.Map<String, Object> row = new java.util.HashMap<>();
			row.put("name", rs.getString("ethnicity_name"));
			row.put("male", rs.getLong("male_count"));
			row.put("female", rs.getLong("female_count"));
			return row;
		});
	}

	// Get play counts by language with gender breakdown
	public java.util.List<java.util.Map<String, Object>> getPlayCountsByLanguageAndGender() {
		String sql = """
			SELECT 
				COALESCE(l.name, 'Unknown') as language_name,
				SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
				SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count
			FROM Scrobble scr
			INNER JOIN Song s ON scr.song_id = s.id
			INNER JOIN Artist a ON s.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			LEFT JOIN Language l ON COALESCE(s.language_id_override, a.language_id) = l.id
			GROUP BY COALESCE(l.name, 'Unknown')
			HAVING (male_count + female_count) > 0
			ORDER BY (male_count + female_count) DESC
			LIMIT 10
			""";
		
		return template.query(sql, (rs, rowNum) -> {
			java.util.Map<String, Object> row = new java.util.HashMap<>();
			row.put("name", rs.getString("language_name"));
			row.put("male", rs.getLong("male_count"));
			row.put("female", rs.getLong("female_count"));
			return row;
		});
	}

	// Get play counts by year listened with gender breakdown
	public java.util.List<java.util.Map<String, Object>> getPlayCountsByYearAndGender() {
		String sql = """
			SELECT 
				strftime('%Y', scr.scrobble_date) as year,
				SUM(CASE WHEN g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%' THEN 1 ELSE 0 END) as male_count,
				SUM(CASE WHEN g.name LIKE '%Female%' THEN 1 ELSE 0 END) as female_count
			FROM Scrobble scr
			INNER JOIN Song s ON scr.song_id = s.id
			INNER JOIN Artist a ON s.artist_id = a.id
			LEFT JOIN Gender g ON a.gender_id = g.id
			WHERE scr.scrobble_date IS NOT NULL
			GROUP BY strftime('%Y', scr.scrobble_date)
			HAVING (male_count + female_count) > 0
			ORDER BY year DESC
			LIMIT 10
			""";
		
		return template.query(sql, (rs, rowNum) -> {
			java.util.Map<String, Object> row = new java.util.HashMap<>();
			row.put("name", rs.getString("year"));
			row.put("male", rs.getLong("male_count"));
			row.put("female", rs.getLong("female_count"));
			return row;
		});
	}
	
}
