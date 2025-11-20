package library.repository;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import library.dto.PlayDTO;
import library.dto.CategoryTotalsDTO;
import library.dto.ArtistAlbumsQueryDTO;
import library.dto.AlbumSongsQueryDTO;
import library.dto.AccountCountDTO;

@Repository
public class ArtistRepository{

	private JdbcTemplate template;

	public ArtistRepository(JdbcTemplate template) {
		this.template = template;
	}

    private static final String ARTIST_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, 
    		CASE WHEN LOWER(sc.artist)=LOWER(?) THEN 'Main' ELSE 'Feature' END as main_or_feature, so.id, sc.account, 
    		sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, so.cloud_status, 
    		strftime('%%Y%%W', sc.scrobble_date) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where (LOWER(sc.artist)=LOWER(?)
            <includeFeatures>)
            order by play_date asc
			""";
    
    private static final String ALBUM_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, 
    		sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, so.cloud_status, so.id, sc.account, 
    		strftime('%%Y%%W', sc.scrobble_date) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
            and LOWER(IFNULL(so.album,'(single)'))=LOWER(?) 
            order by play_date asc
			""";
    
    private static final String SONG_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.account, 
    				sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, strftime('%%Y%%W', sc.scrobble_date) week
            from scrobble sc inner join song so on sc.song_id = so.id
            where LOWER(sc.artist)=LOWER(?)
            and LOWER(IFNULL(so.album,'(single)'))=LOWER(?)
            and LOWER(so.song) = LOWER(?)
            order by play_date asc
			""";
    
    private static final String CATEGORY_PLAYS_QUERY = """
    		select so.artist, so.song, IFNULL(so.album,'(single)') album, so.duration track_length, sc.account, 
    				sc.scrobble_date play_date, so.genre, so.year, so.language, so.sex, so.race, so.cloud_status, 
    				strftime('%%Y%%W', sc.scrobble_date) week 
            from scrobble sc inner join song so on sc.song_id = so.id
            where 1=1
			""";
    
	public List<PlayDTO> artistPlays(String artist, boolean includeFeatures) {
		
		String artistPlaysQuery = ARTIST_PLAYS_QUERY;
		
		if(includeFeatures) {
			artistPlaysQuery = artistPlaysQuery.replace("<includeFeatures>", 
					"or (LOWER(sc.song) like LOWER(?) and (LOWER(sc.song) like '%feat%' or LOWER(sc.song) like '%with%'))");
			
			return template.query(artistPlaysQuery, new BeanPropertyRowMapper<>(PlayDTO.class), artist, artist, "%"+artist+"%");
		}
		else {
			artistPlaysQuery = artistPlaysQuery.replace("<includeFeatures>","");
			
			return template.query(artistPlaysQuery, new BeanPropertyRowMapper<>(PlayDTO.class), artist, artist);
		}
		
		
	}
	
	public List<PlayDTO> albumPlays(String artist, String album) {
		return template.query(ALBUM_PLAYS_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), artist, album);
	}
	
	public List<PlayDTO> songPlays(String artist, String album, String song) {
		return template.query(SONG_PLAYS_QUERY, new BeanPropertyRowMapper<>(PlayDTO.class), artist, album, song);
	}
	
	public List<PlayDTO> categoryPlays(String[] categories, String[] values, int startYear, int endYear) {
		String query = CATEGORY_PLAYS_QUERY;
		query += String.format(" and so.year between %s and %s ",startYear, endYear);
		for(int i=0 ; i < categories.length ; i++) {
			if(categories[i].equals("PlayYear")) {
				query += "and strftime('%%Y', sc.%s)=? ";
				categories[i] = "scrobble_date";
			} else if(categories[i].equalsIgnoreCase("Account")) {
				boolean wildcard = values[i] != null && (values[i].contains("%") || values[i].contains("_"));
				query += (wildcard ? "and sc.%s like ? " : "and sc.%s = ? ");
				categories[i] = "account";				
			}else{
				boolean wildcard = values[i] != null && (values[i].contains("%") || values[i].contains("_"));
				query += (wildcard ? "and so.%s like ? " : "and so.%s = ? ");
			}
		}
	
		// Use sargable date filtering (no function on column)
		query += " and sc.scrobble_date >= ? and sc.scrobble_date < date(?, '+1 day')";
		
		return template.query(String.format(query, (Object[])categories), new BeanPropertyRowMapper<>(PlayDTO.class), (Object[])values);
	}
	
	private String buildFilterWhere(String[] categories, String[] values, int startYear, int endYear, List<Object> params) {
		StringBuilder sb = new StringBuilder();
		sb.append(" and so.year between ").append(startYear).append(" and ").append(endYear).append(" ");
		String[] cols = categories.clone();
		for (int i = 0; i < cols.length; i++) {
			if (cols[i] == null) continue;
			if ("PlayYear".equals(cols[i])) {
				sb.append("and strftime('%%Y', sc.%s)=? ");
				cols[i] = "scrobble_date";
				params.add(values[i]);
			} else if ("Account".equalsIgnoreCase(cols[i])) {
				boolean wildcard = values[i] != null && (values[i].contains("%") || values[i].contains("_"));
				sb.append(wildcard ? "and sc.%s like ? " : "and sc.%s = ? ");
				cols[i] = "account";
				params.add(values[i]);
			} else {
				boolean wildcard = values[i] != null && (values[i].contains("%") || values[i].contains("_"));
				sb.append(wildcard ? "and so.%s like ? " : "and so.%s = ? ");
				params.add(values[i]);
			}
		}
		// date range, sargable
		sb.append(" and sc.scrobble_date >= ? and sc.scrobble_date < date(?, '+1 day')");
		// push the corresponding start/end parameters (last two entries in values)
		if (values != null && values.length >= 2) {
			params.add(values[values.length - 2]);
			params.add(values[values.length - 1]);
		}
		return String.format(sb.toString(), (Object[]) cols);
	}

	public CategoryTotalsDTO categoryTotals(String[] categories, String[] values, int startYear, int endYear) {
		String base = """
			select 
			  COUNT(*) as totalPlays,
			  SUM(so.duration) as totalPlaytime,
			  COUNT(DISTINCT so.artist) as numberOfArtists,
			  COUNT(DISTINCT so.artist || '::' || IFNULL(so.album,'(single)')) as numberOfAlbums,
			  COUNT(DISTINCT so.id) as numberOfSongs,
			  COUNT(DISTINCT DATE(sc.scrobble_date)) as daysCategoryWasPlayed,
			  COUNT(DISTINCT strftime('%%Y%%W', sc.scrobble_date)) as weeksCategoryWasPlayed,
			  COUNT(DISTINCT strftime('%%Y-%%m', sc.scrobble_date)) as monthsCategoryWasPlayed,
			  (select AVG(t.dur) from (
			     select so2.id, MAX(so2.duration) as dur
			     from scrobble sc2 join song so2 on sc2.song_id = so2.id
			     where 1=1 %s
			     group by so2.id
			  ) t) as averageSongLength
			from scrobble sc join song so on sc.song_id = so.id
			where 1=1 %s
		""";
		new Object();
		// Build filters twice: for main select and inner average subquery
		java.util.ArrayList<Object> paramsMain = new java.util.ArrayList<>();
		java.util.ArrayList<Object> paramsSub = new java.util.ArrayList<>();
		String whereMain = buildFilterWhere(categories, values, startYear, endYear, paramsMain);
		String whereSub = buildFilterWhere(categories, values, startYear, endYear, paramsSub)
				.replace(" so.", " so2.")
				.replace(" sc.", " sc2.");
		String sql = String.format(base, whereSub, whereMain);
		// Merge params: first subquery params then main params
		paramsSub.addAll(paramsMain);
		return template.query(sql, rs -> {
			CategoryTotalsDTO dto = new CategoryTotalsDTO();
			if (rs.next()) {
				dto.setTotalPlays(rs.getInt("totalPlays"));
				dto.setTotalPlaytime(rs.getInt("totalPlaytime"));
				dto.setNumberOfArtists(rs.getInt("numberOfArtists"));
				dto.setNumberOfAlbums(rs.getInt("numberOfAlbums"));
				dto.setNumberOfSongs(rs.getInt("numberOfSongs"));
				dto.setDaysCategoryWasPlayed(rs.getInt("daysCategoryWasPlayed"));
				dto.setWeeksCategoryWasPlayed(rs.getInt("weeksCategoryWasPlayed"));
				dto.setMonthsCategoryWasPlayed(rs.getInt("monthsCategoryWasPlayed"));
				// averageSongLength available if needed later
			}
			return dto;
		}, paramsSub.toArray());
	}

	public List<ArtistAlbumsQueryDTO> categoryTopAlbums(int limit, String[] categories, String[] values, int startYear, int endYear) {
		String base = """
			select so.artist as artist,
		       IFNULL(so.album,'(single)') as album,
		       MAX(so.genre) as genre,
		       MAX(so.race) as race,
		       MAX(so.sex) as sex,
		       MAX(so.language) as language,
		       MAX(so.year) as releaseYear,
		       COUNT(*) as totalPlays
			from scrobble sc join song so on sc.song_id = so.id
			where 1=1 %s
			group by so.artist, IFNULL(so.album,'(single)')
			order by totalPlays desc
			limit ?
		""";
		java.util.ArrayList<Object> params = new java.util.ArrayList<>();
		String where = buildFilterWhere(categories, values, startYear, endYear, params);
		params.add(limit);
		String sql = String.format(base, where);
		return template.query(sql, new BeanPropertyRowMapper<>(ArtistAlbumsQueryDTO.class), params.toArray());
	}

	public List<AlbumSongsQueryDTO> categoryTopSongs(int limit, String[] categories, String[] values, int startYear, int endYear) {
		String base = """
			select so.artist as artist,
		       IFNULL(so.album,'(single)') as album,
		       so.song as song,
		       MAX(so.genre) as genre,
		       MAX(so.race) as race,
		       MAX(so.sex) as sex,
		       MAX(so.language) as language,
		       MAX(so.year) as year,
		       MAX(so.cloud_status) as cloudStatus,
		       MAX(so.id) as id,
		       MAX(so.duration) as trackLength,
		       COUNT(*) as totalPlays
			from scrobble sc join song so on sc.song_id = so.id
			where 1=1 %s
			group by so.artist, IFNULL(so.album,'(single)'), so.song
			order by totalPlays desc
			limit ?
		""";
		java.util.ArrayList<Object> params = new java.util.ArrayList<>();
		String where = buildFilterWhere(categories, values, startYear, endYear, params);
		params.add(limit);
		String sql = String.format(base, where);
		return template.query(sql, new BeanPropertyRowMapper<>(AlbumSongsQueryDTO.class), params.toArray());
	}

	public List<AccountCountDTO> categoryPlaysByAccount(String[] categories, String[] values, int startYear, int endYear) {
		String base = """
			select sc.account as account, COUNT(*) as count
			from scrobble sc join song so on sc.song_id = so.id
			where 1=1 %s
			group by sc.account
			order by sc.account
		""";
		java.util.ArrayList<Object> params = new java.util.ArrayList<>();
		String where = buildFilterWhere(categories, values, startYear, endYear, params);
		String sql = String.format(base, where);
		return template.query(sql, new BeanPropertyRowMapper<>(AccountCountDTO.class), params.toArray());
	}

	public String categoryTopArtist(String[] categories, String[] values, int startYear, int endYear) {
		String base = """
			select so.artist as artist, COUNT(*) as totalPlays
			from scrobble sc join song so on sc.song_id = so.id
			where 1=1 %s
			group by so.artist
			order by totalPlays desc
			limit 1
		""";
		java.util.ArrayList<Object> params = new java.util.ArrayList<>();
		String where = buildFilterWhere(categories, values, startYear, endYear, params);
		String sql = String.format(base, where);
		return template.query(sql, rs -> rs.next() ? rs.getString("artist") + " - " + rs.getInt("totalPlays") : "", params.toArray());
	}
}