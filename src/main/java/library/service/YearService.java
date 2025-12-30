package library.service;

import library.dto.YearCardDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class YearService {

    private final JdbcTemplate jdbcTemplate;

    public YearService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Get listen year statistics - organized by the year when scrobbles occurred.
     * Includes empty cards for years with no listens within the range.
     */
    public List<YearCardDTO> getListenYears(String sortBy, String sortDir, int page, int perPage) {
        // First, get the min and max years from scrobble data
        String minMaxSql = "SELECT MIN(CAST(strftime('%Y', scrobble_date) AS INTEGER)) as min_year, " +
                          "MAX(CAST(strftime('%Y', scrobble_date) AS INTEGER)) as max_year " +
                          "FROM Scrobble WHERE scrobble_date IS NOT NULL";

        Integer minYear = null;
        Integer maxYear = null;
        try {
            var result = jdbcTemplate.queryForMap(minMaxSql);
            minYear = result.get("min_year") != null ? ((Number) result.get("min_year")).intValue() : null;
            maxYear = result.get("max_year") != null ? ((Number) result.get("max_year")).intValue() : null;
        } catch (Exception e) {
            // No data
        }

        if (minYear == null || maxYear == null) {
            return List.of();
        }

        // Generate all years in range
        java.util.List<Integer> allYears = new java.util.ArrayList<>();
        for (int y = minYear; y <= maxYear; y++) {
            allYears.add(y);
        }

        String sql =
            "SELECT " +
            "    CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) as year, " +
            "    COUNT(DISTINCT scr.id) as play_count, " +
            "    COUNT(DISTINCT CASE WHEN scr.account = 'vatito' THEN scr.id END) as vatito_play_count, " +
            "    COUNT(DISTINCT CASE WHEN scr.account = 'robertlover' THEN scr.id END) as robertlover_play_count, " +
            "    SUM(s.length_seconds) as time_listened, " +
            "    COUNT(DISTINCT ar.id) as artist_count, " +
            "    COUNT(DISTINCT al.id) as album_count, " +
            "    COUNT(DISTINCT s.id) as song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.id END) as male_song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN s.id END) as female_song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.id END) as other_song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) as male_artist_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN ar.id END) as female_artist_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) as other_artist_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) as male_album_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN al.id END) as female_album_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) as other_album_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) as male_play_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN scr.id END) as female_play_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) as other_play_count, " +
            "    SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as male_time_listened, " +
            "    SUM(CASE WHEN gn.name LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as female_time_listened, " +
            "    SUM(CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as other_time_listened " +
            "FROM Scrobble scr " +
            "JOIN Song s ON scr.song_id = s.id " +
            "JOIN Artist ar ON s.artist_id = ar.id " +
            "LEFT JOIN Album al ON s.album_id = al.id " +
            "LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id " +
            "WHERE scr.scrobble_date IS NOT NULL " +
            "GROUP BY year";

        // Query all years with data
        java.util.Map<Integer, YearCardDTO> yearDataMap = new java.util.HashMap<>();
        jdbcTemplate.query(sql, (rs, rowNum) -> {
            YearCardDTO dto = new YearCardDTO();
            dto.setYear(rs.getInt("year"));
            dto.setYearType("listen");
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setVatitoPlayCount(rs.getInt("vatito_play_count"));
            dto.setRobertloverPlayCount(rs.getInt("robertlover_play_count"));
            dto.setTimeListened(rs.getLong("time_listened"));
            dto.setTimeListenedFormatted(formatTime(rs.getLong("time_listened")));
            dto.setArtistCount(rs.getInt("artist_count"));
            dto.setAlbumCount(rs.getInt("album_count"));
            dto.setSongCount(rs.getInt("song_count"));
            dto.setMaleCount(rs.getInt("male_song_count"));
            dto.setFemaleCount(rs.getInt("female_song_count"));
            dto.setOtherCount(rs.getInt("other_song_count"));
            dto.setMaleArtistCount(rs.getInt("male_artist_count"));
            dto.setFemaleArtistCount(rs.getInt("female_artist_count"));
            dto.setOtherArtistCount(rs.getInt("other_artist_count"));
            dto.setMaleAlbumCount(rs.getInt("male_album_count"));
            dto.setFemaleAlbumCount(rs.getInt("female_album_count"));
            dto.setOtherAlbumCount(rs.getInt("other_album_count"));
            dto.setMalePlayCount(rs.getInt("male_play_count"));
            dto.setFemalePlayCount(rs.getInt("female_play_count"));
            dto.setOtherPlayCount(rs.getInt("other_play_count"));
            dto.setMaleTimeListened(rs.getLong("male_time_listened"));
            dto.setFemaleTimeListened(rs.getLong("female_time_listened"));
            dto.setOtherTimeListened(rs.getLong("other_time_listened"));
            yearDataMap.put(dto.getYear(), dto);
            return dto;
        });

        // Build complete list including empty years
        java.util.List<YearCardDTO> allYearDtos = new java.util.ArrayList<>();
        for (Integer year : allYears) {
            if (yearDataMap.containsKey(year)) {
                allYearDtos.add(yearDataMap.get(year));
            } else {
                // Create empty card for this year
                YearCardDTO emptyDto = new YearCardDTO();
                emptyDto.setYear(year);
                emptyDto.setYearType("listen");
                emptyDto.setPlayCount(0);
                emptyDto.setVatitoPlayCount(0);
                emptyDto.setRobertloverPlayCount(0);
                emptyDto.setTimeListened(0L);
                emptyDto.setTimeListenedFormatted("0m");
                emptyDto.setArtistCount(0);
                emptyDto.setAlbumCount(0);
                emptyDto.setSongCount(0);
                emptyDto.setMaleCount(0);
                emptyDto.setFemaleCount(0);
                emptyDto.setOtherCount(0);
                emptyDto.setMaleArtistCount(0);
                emptyDto.setFemaleArtistCount(0);
                emptyDto.setOtherArtistCount(0);
                emptyDto.setMaleAlbumCount(0);
                emptyDto.setFemaleAlbumCount(0);
                emptyDto.setOtherAlbumCount(0);
                emptyDto.setMalePlayCount(0);
                emptyDto.setFemalePlayCount(0);
                emptyDto.setOtherPlayCount(0);
                emptyDto.setMaleTimeListened(0L);
                emptyDto.setFemaleTimeListened(0L);
                emptyDto.setOtherTimeListened(0L);
                allYearDtos.add(emptyDto);
            }
        }

        // Sort based on the requested sort
        String sortColumn = "year";
        boolean descending = "desc".equalsIgnoreCase(sortDir);

        if (sortBy != null) {
            switch (sortBy.toLowerCase()) {
                case "plays": sortColumn = "plays"; break;
                case "time": sortColumn = "time"; break;
                case "artists": sortColumn = "artists"; break;
                case "albums": sortColumn = "albums"; break;
                case "songs": sortColumn = "songs"; break;
                case "maleartistpct": sortColumn = "maleartistpct"; break;
                case "malealbumpct": sortColumn = "malealbumpct"; break;
                case "malesongpct": sortColumn = "malesongpct"; break;
                case "maleplaypct": sortColumn = "maleplaypct"; break;
                case "maletimepct": sortColumn = "maletimepct"; break;
                default: sortColumn = "year";
            }
        }

        final String finalSortColumn = sortColumn;
        java.util.Comparator<YearCardDTO> comparator = switch (finalSortColumn) {
            case "plays" -> {
                java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getPlayCount);
                yield descending ? c.reversed() : c;
            }
            case "time" -> {
                java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getTimeListened);
                yield descending ? c.reversed() : c;
            }
            case "artists" -> {
                java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getArtistCount);
                yield descending ? c.reversed() : c;
            }
            case "albums" -> {
                java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getAlbumCount);
                yield descending ? c.reversed() : c;
            }
            case "songs" -> {
                java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getSongCount);
                yield descending ? c.reversed() : c;
            }
            case "maleartistpct" -> {
                java.util.Comparator<Double> valueComparator = descending ? java.util.Comparator.reverseOrder() : java.util.Comparator.naturalOrder();
                yield java.util.Comparator.comparing(YearCardDTO::getMaleArtistPercentage, java.util.Comparator.nullsLast(valueComparator));
            }
            case "malealbumpct" -> {
                java.util.Comparator<Double> valueComparator = descending ? java.util.Comparator.reverseOrder() : java.util.Comparator.naturalOrder();
                yield java.util.Comparator.comparing(YearCardDTO::getMaleAlbumPercentage, java.util.Comparator.nullsLast(valueComparator));
            }
            case "malesongpct" -> {
                java.util.Comparator<Double> valueComparator = descending ? java.util.Comparator.reverseOrder() : java.util.Comparator.naturalOrder();
                yield java.util.Comparator.comparing(YearCardDTO::getMaleSongPercentage, java.util.Comparator.nullsLast(valueComparator));
            }
            case "maleplaypct" -> {
                java.util.Comparator<Double> valueComparator = descending ? java.util.Comparator.reverseOrder() : java.util.Comparator.naturalOrder();
                yield java.util.Comparator.comparing(YearCardDTO::getMalePlayPercentage, java.util.Comparator.nullsLast(valueComparator));
            }
            case "maletimepct" -> {
                java.util.Comparator<Double> valueComparator = descending ? java.util.Comparator.reverseOrder() : java.util.Comparator.naturalOrder();
                yield java.util.Comparator.comparing(YearCardDTO::getMaleTimePercentage, java.util.Comparator.nullsLast(valueComparator));
            }
            default -> {
                java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getYear);
                yield descending ? c.reversed() : c;
            }
        };

        allYearDtos.sort(comparator);

        // Apply pagination
        int offset = page * perPage;
        int toIndex = Math.min(offset + perPage, allYearDtos.size());
        if (offset >= allYearDtos.size()) {
            return List.of();
        }

        List<YearCardDTO> years = allYearDtos.subList(offset, toIndex);

        // Populate top items only for years with data
        List<YearCardDTO> yearsWithData = years.stream()
            .filter(y -> y.getPlayCount() != null && y.getPlayCount() > 0)
            .toList();
        if (!yearsWithData.isEmpty()) {
            populateTopItemsForListenYears(new java.util.ArrayList<>(yearsWithData));
        }

        return years;
    }

    /**
     * Get release year statistics - organized by the release year of songs/albums.
     * Uses song's release_date if available, otherwise falls back to album's release_date.
     */
    public List<YearCardDTO> getReleaseYears(String sortBy, String sortDir, int page, int perPage) {
        int offset = page * perPage;

        String sortColumn = "year";
        String sortDirection = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        String nullsHandling = " NULLS LAST";

        if (sortBy != null) {
            switch (sortBy.toLowerCase()) {
                case "plays": sortColumn = "play_count"; nullsHandling = ""; break;
                case "time": sortColumn = "time_listened"; nullsHandling = ""; break;
                case "artists": sortColumn = "artist_count"; nullsHandling = ""; break;
                case "albums": sortColumn = "album_count"; nullsHandling = ""; break;
                case "songs": sortColumn = "song_count"; nullsHandling = ""; break;
                case "maleartistpct": sortColumn = "male_artist_pct"; break;
                case "malealbumpct": sortColumn = "male_album_pct"; break;
                case "malesongpct": sortColumn = "male_song_pct"; break;
                case "maleplaypct": sortColumn = "male_play_pct"; break;
                case "maletimepct": sortColumn = "male_time_pct"; break;
                default: sortColumn = "year"; nullsHandling = "";
            }
        }

        // Uses effective release year: song's release_date > album's release_date
        String sql =
            "SELECT " +
            "    CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) as year, " +
            "    COUNT(DISTINCT scr.id) as play_count, " +
            "    COUNT(DISTINCT CASE WHEN scr.account = 'vatito' THEN scr.id END) as vatito_play_count, " +
            "    COUNT(DISTINCT CASE WHEN scr.account = 'robertlover' THEN scr.id END) as robertlover_play_count, " +
            "    SUM(s.length_seconds) as time_listened, " +
            "    COUNT(DISTINCT ar.id) as artist_count, " +
            "    COUNT(DISTINCT al.id) as album_count, " +
            "    COUNT(DISTINCT s.id) as song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.id END) as male_song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN s.id END) as female_song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.id END) as other_song_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) as male_artist_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN ar.id END) as female_artist_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) as other_artist_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) as male_album_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN al.id END) as female_album_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) as other_album_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) as male_play_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN scr.id END) as female_play_count, " +
            "    COUNT(DISTINCT CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) as other_play_count, " +
            "    SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as male_time_listened, " +
            "    SUM(CASE WHEN gn.name LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as female_time_listened, " +
            "    SUM(CASE WHEN gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) as other_time_listened, " +
            "    CASE WHEN COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN ar.id END) > 0 " +
            "         THEN CAST(COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) AS REAL) / (COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN ar.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN ar.id END)) " +
            "         ELSE NULL END as male_artist_pct, " +
            "    CASE WHEN COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN al.id END) > 0 " +
            "         THEN CAST(COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) AS REAL) / (COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN al.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN al.id END)) " +
            "         ELSE NULL END as male_album_pct, " +
            "    CASE WHEN COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN s.id END) > 0 " +
            "         THEN CAST(COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.id END) AS REAL) / (COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN s.id END)) " +
            "         ELSE NULL END as male_song_pct, " +
            "    CASE WHEN COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN scr.id END) > 0 " +
            "         THEN CAST(COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) AS REAL) / (COUNT(DISTINCT CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN scr.id END) + COUNT(DISTINCT CASE WHEN gn.name LIKE '%Female%' THEN scr.id END)) " +
            "         ELSE NULL END as male_play_pct, " +
            "    CASE WHEN SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) + SUM(CASE WHEN gn.name LIKE '%Female%' THEN s.length_seconds ELSE 0 END) > 0 " +
            "         THEN CAST(SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) AS REAL) / (SUM(CASE WHEN gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%' THEN s.length_seconds ELSE 0 END) + SUM(CASE WHEN gn.name LIKE '%Female%' THEN s.length_seconds ELSE 0 END)) " +
            "         ELSE NULL END as male_time_pct " +
            "FROM Scrobble scr " +
            "JOIN Song s ON scr.song_id = s.id " +
            "JOIN Artist ar ON s.artist_id = ar.id " +
            "LEFT JOIN Album al ON s.album_id = al.id " +
            "LEFT JOIN Gender gn ON COALESCE(s.override_gender_id, ar.gender_id) = gn.id " +
            "WHERE COALESCE(s.release_date, al.release_date) IS NOT NULL " +
            "GROUP BY year " +
            "ORDER BY " + sortColumn + " " + sortDirection + nullsHandling + " " +
            "LIMIT ? OFFSET ?";

        List<YearCardDTO> years = jdbcTemplate.query(sql, (rs, rowNum) -> {
            YearCardDTO dto = new YearCardDTO();
            dto.setYear(rs.getInt("year"));
            dto.setYearType("release");
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setVatitoPlayCount(rs.getInt("vatito_play_count"));
            dto.setRobertloverPlayCount(rs.getInt("robertlover_play_count"));
            dto.setTimeListened(rs.getLong("time_listened"));
            dto.setTimeListenedFormatted(formatTime(rs.getLong("time_listened")));
            dto.setArtistCount(rs.getInt("artist_count"));
            dto.setAlbumCount(rs.getInt("album_count"));
            dto.setSongCount(rs.getInt("song_count"));
            dto.setMaleCount(rs.getInt("male_song_count"));
            dto.setFemaleCount(rs.getInt("female_song_count"));
            dto.setOtherCount(rs.getInt("other_song_count"));
            dto.setMaleArtistCount(rs.getInt("male_artist_count"));
            dto.setFemaleArtistCount(rs.getInt("female_artist_count"));
            dto.setOtherArtistCount(rs.getInt("other_artist_count"));
            dto.setMaleAlbumCount(rs.getInt("male_album_count"));
            dto.setFemaleAlbumCount(rs.getInt("female_album_count"));
            dto.setOtherAlbumCount(rs.getInt("other_album_count"));
            dto.setMalePlayCount(rs.getInt("male_play_count"));
            dto.setFemalePlayCount(rs.getInt("female_play_count"));
            dto.setOtherPlayCount(rs.getInt("other_play_count"));
            dto.setMaleTimeListened(rs.getLong("male_time_listened"));
            dto.setFemaleTimeListened(rs.getLong("female_time_listened"));
            dto.setOtherTimeListened(rs.getLong("other_time_listened"));
            return dto;
        }, perPage, offset);

        if (!years.isEmpty()) {
            populateTopItemsForReleaseYears(years);
        }

        return years;
    }

    public long countListenYears() {
        // Count total years in the range (including empty years)
        String minMaxSql = "SELECT MIN(CAST(strftime('%Y', scrobble_date) AS INTEGER)) as min_year, " +
                          "MAX(CAST(strftime('%Y', scrobble_date) AS INTEGER)) as max_year " +
                          "FROM Scrobble WHERE scrobble_date IS NOT NULL";
        try {
            var result = jdbcTemplate.queryForMap(minMaxSql);
            Integer minYear = result.get("min_year") != null ? ((Number) result.get("min_year")).intValue() : null;
            Integer maxYear = result.get("max_year") != null ? ((Number) result.get("max_year")).intValue() : null;
            if (minYear != null && maxYear != null) {
                return maxYear - minYear + 1;
            }
        } catch (Exception e) {
            // No data
        }
        return 0;
    }

    public long countReleaseYears() {
        String sql = "SELECT COUNT(DISTINCT strftime('%Y', COALESCE(s.release_date, al.release_date))) " +
                     "FROM Song s LEFT JOIN Album al ON s.album_id = al.id " +
                     "WHERE COALESCE(s.release_date, al.release_date) IS NOT NULL";
        Long count = jdbcTemplate.queryForObject(sql, Long.class);
        return count != null ? count : 0;
    }

    private void populateTopItemsForListenYears(List<YearCardDTO> years) {
        List<Integer> yearValues = years.stream().map(YearCardDTO::getYear).toList();
        if (yearValues.isEmpty()) return;

        String placeholders = String.join(",", yearValues.stream().map(y -> "?").toList());

        // Top artist per listen year
        String topArtistSql =
            "WITH artist_plays AS ( " +
            "    SELECT " +
            "        CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) as year, " +
            "        ar.id as artist_id, " +
            "        ar.name as artist_name, " +
            "        ar.gender_id as gender_id, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    WHERE CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) IN (" + placeholders + ") " +
            "    GROUP BY year, ar.id, ar.name, ar.gender_id " +
            ") " +
            "SELECT year, artist_id, artist_name, gender_id FROM artist_plays WHERE rn = 1";

        List<Object[]> artistResults = jdbcTemplate.query(topArtistSql, (rs, rowNum) ->
            new Object[]{rs.getInt("year"), rs.getInt("artist_id"), rs.getString("artist_name"),
                        rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null},
            yearValues.toArray()
        );

        // Top album per listen year
        String topAlbumSql =
            "WITH album_plays AS ( " +
            "    SELECT " +
            "        CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) as year, " +
            "        al.id as album_id, " +
            "        al.name as album_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE al.id IS NOT NULL AND CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) IN (" + placeholders + ") " +
            "    GROUP BY year, al.id, al.name, ar.name " +
            ") " +
            "SELECT year, album_id, album_name, artist_name FROM album_plays WHERE rn = 1";

        List<Object[]> albumResults = jdbcTemplate.query(topAlbumSql, (rs, rowNum) ->
            new Object[]{rs.getInt("year"), rs.getInt("album_id"), rs.getString("album_name"), rs.getString("artist_name")},
            yearValues.toArray()
        );

        // Top song per listen year
        String topSongSql =
            "WITH song_plays AS ( " +
            "    SELECT " +
            "        CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) as year, " +
            "        s.id as song_id, " +
            "        s.name as song_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    WHERE CAST(strftime('%Y', scr.scrobble_date) AS INTEGER) IN (" + placeholders + ") " +
            "    GROUP BY year, s.id, s.name, ar.name " +
            ") " +
            "SELECT year, song_id, song_name, artist_name FROM song_plays WHERE rn = 1";

        List<Object[]> songResults = jdbcTemplate.query(topSongSql, (rs, rowNum) ->
            new Object[]{rs.getInt("year"), rs.getInt("song_id"), rs.getString("song_name"), rs.getString("artist_name")},
            yearValues.toArray()
        );

        mapTopItemResults(years, artistResults, albumResults, songResults);
    }

    private void populateTopItemsForReleaseYears(List<YearCardDTO> years) {
        List<Integer> yearValues = years.stream().map(YearCardDTO::getYear).toList();
        if (yearValues.isEmpty()) return;

        String placeholders = String.join(",", yearValues.stream().map(y -> "?").toList());

        // Top artist per release year
        String topArtistSql =
            "WITH artist_plays AS ( " +
            "    SELECT " +
            "        CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) as year, " +
            "        ar.id as artist_id, " +
            "        ar.name as artist_name, " +
            "        ar.gender_id as gender_id, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) IN (" + placeholders + ") " +
            "    GROUP BY year, ar.id, ar.name, ar.gender_id " +
            ") " +
            "SELECT year, artist_id, artist_name, gender_id FROM artist_plays WHERE rn = 1";

        List<Object[]> artistResults = jdbcTemplate.query(topArtistSql, (rs, rowNum) ->
            new Object[]{rs.getInt("year"), rs.getInt("artist_id"), rs.getString("artist_name"),
                        rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null},
            yearValues.toArray()
        );

        // Top album per release year
        String topAlbumSql =
            "WITH album_plays AS ( " +
            "    SELECT " +
            "        CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) as year, " +
            "        al.id as album_id, " +
            "        al.name as album_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE al.id IS NOT NULL AND CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) IN (" + placeholders + ") " +
            "    GROUP BY year, al.id, al.name, ar.name " +
            ") " +
            "SELECT year, album_id, album_name, artist_name FROM album_plays WHERE rn = 1";

        List<Object[]> albumResults = jdbcTemplate.query(topAlbumSql, (rs, rowNum) ->
            new Object[]{rs.getInt("year"), rs.getInt("album_id"), rs.getString("album_name"), rs.getString("artist_name")},
            yearValues.toArray()
        );

        // Top song per release year
        String topSongSql =
            "WITH song_plays AS ( " +
            "    SELECT " +
            "        CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) as year, " +
            "        s.id as song_id, " +
            "        s.name as song_name, " +
            "        ar.name as artist_name, " +
            "        COUNT(*) as play_count, " +
            "        ROW_NUMBER() OVER (PARTITION BY CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) ORDER BY COUNT(*) DESC) as rn " +
            "    FROM Scrobble scr " +
            "    JOIN Song s ON scr.song_id = s.id " +
            "    JOIN Artist ar ON s.artist_id = ar.id " +
            "    LEFT JOIN Album al ON s.album_id = al.id " +
            "    WHERE CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER) IN (" + placeholders + ") " +
            "    GROUP BY year, s.id, s.name, ar.name " +
            ") " +
            "SELECT year, song_id, song_name, artist_name FROM song_plays WHERE rn = 1";

        List<Object[]> songResults = jdbcTemplate.query(topSongSql, (rs, rowNum) ->
            new Object[]{rs.getInt("year"), rs.getInt("song_id"), rs.getString("song_name"), rs.getString("artist_name")},
            yearValues.toArray()
        );

        mapTopItemResults(years, artistResults, albumResults, songResults);
    }

    private void mapTopItemResults(List<YearCardDTO> years, List<Object[]> artistResults,
                                   List<Object[]> albumResults, List<Object[]> songResults) {
        for (YearCardDTO yr : years) {
            for (Object[] row : artistResults) {
                if (yr.getYear().equals(row[0])) {
                    yr.setTopArtistId((Integer) row[1]);
                    yr.setTopArtistName((String) row[2]);
                    yr.setTopArtistGenderId((Integer) row[3]);
                    break;
                }
            }
            for (Object[] row : albumResults) {
                if (yr.getYear().equals(row[0])) {
                    yr.setTopAlbumId((Integer) row[1]);
                    yr.setTopAlbumName((String) row[2]);
                    yr.setTopAlbumArtistName((String) row[3]);
                    break;
                }
            }
            for (Object[] row : songResults) {
                if (yr.getYear().equals(row[0])) {
                    yr.setTopSongId((Integer) row[1]);
                    yr.setTopSongName((String) row[2]);
                    yr.setTopSongArtistName((String) row[3]);
                    break;
                }
            }
        }
    }

    private String formatTime(long totalSeconds) {
        if (totalSeconds == 0) {
            return "0m";
        }

        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;

        if (days > 0) {
            return String.format("%dd %dh", days, hours);
        } else if (hours > 0) {
            return String.format("%dh %dm", hours, minutes);
        } else {
            return String.format("%dm", minutes);
        }
    }
}

