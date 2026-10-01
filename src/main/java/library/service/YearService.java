package library.service;

import library.dto.YearCardDTO;
import library.util.RandomSortUtils;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class YearService {

    private final JdbcTemplate jdbcTemplate;

    public YearService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Reduce the play history before joining metadata or computing distinct gender totals.
    private String yearSongCounts(boolean listen) {
        return listen ? """
                SELECT strftime('%Y', play_date) AS year, song_id,
                       COUNT(*) AS plays,
                       SUM(account = 'vatito') AS primary_plays,
                       SUM(account = 'robertlover') AS legacy_plays
                FROM Play WHERE play_date IS NOT NULL GROUP BY year, song_id ORDER BY song_id
                """ : """
                SELECT song_id, COUNT(*) AS plays,
                       SUM(account = 'vatito') AS primary_plays,
                       SUM(account = 'robertlover') AS legacy_plays
                FROM Play GROUP BY song_id
                """;
    }

    private String yearStatisticsSql(boolean listen) {
        String year = listen ? "CAST(p.year AS INTEGER)" : "CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER)";
        StringBuilder sql = new StringBuilder("WITH play_counts AS MATERIALIZED (")
                .append(yearSongCounts(listen)).append("), year_stats AS (SELECT ")
                .append(year).append(" AS year, SUM(p.plays) AS play_count, ")
                .append("SUM(p.primary_plays) AS vatito_play_count, SUM(p.legacy_plays) AS robertlover_play_count, ")
                .append("SUM(s.length_seconds * p.plays) AS time_listened, ")
                .append("COUNT(DISTINCT ar.id) AS artist_count, COUNT(DISTINCT al.id) AS album_count, COUNT(DISTINCT s.id) AS song_count");
        String[] genders = {"male", "female", "other"};
        String[] conditions = {"gn.name LIKE '%Male%' AND gn.name NOT LIKE '%Female%'", "gn.name LIKE '%Female%'",
                "gn.name IS NOT NULL AND gn.name NOT LIKE '%Male%' AND gn.name NOT LIKE '%Female%'"};
        for (int i = 0; i < genders.length; i++) {
            String condition = conditions[i];
            String gender = genders[i];
            sql.append(", COUNT(DISTINCT CASE WHEN ").append(condition).append(" THEN s.id END) AS ").append(gender).append("_song_count")
                    .append(", COUNT(DISTINCT CASE WHEN ").append(condition).append(" THEN ar.id END) AS ").append(gender).append("_artist_count")
                    .append(", COUNT(DISTINCT CASE WHEN ").append(condition).append(" THEN al.id END) AS ").append(gender).append("_album_count")
                    .append(", SUM(CASE WHEN ").append(condition).append(" THEN p.plays ELSE 0 END) AS ").append(gender).append("_play_count")
                    .append(", SUM(CASE WHEN ").append(condition).append(" THEN s.length_seconds * p.plays ELSE 0 END) AS ").append(gender).append("_time_listened");
        }
        sql.append(" FROM play_counts p JOIN Song s ON s.id=p.song_id JOIN Artist ar ON ar.id=s.artist_id ")
                .append("LEFT JOIN Album al ON al.id=s.album_id LEFT JOIN Gender gn ON gn.id=COALESCE(s.override_gender_id, ar.gender_id) ");
        if (!listen) sql.append("WHERE COALESCE(s.release_date, al.release_date) IS NOT NULL ");
        sql.append("GROUP BY year) SELECT year_stats.*");
        for (String metric : List.of("artist", "album", "song", "play", "time")) {
            String suffix = metric.equals("time") ? "time_listened" : metric + "_count";
            sql.append(", CASE WHEN male_").append(suffix).append(" + female_").append(suffix)
                    .append(" > 0 THEN CAST(male_").append(suffix).append(" AS REAL) / (male_").append(suffix)
                    .append(" + female_").append(suffix).append(") END AS male_").append(metric).append("_pct");
        }
        return sql.append(" FROM year_stats").toString();
    }

    private void populateTopItems(List<YearCardDTO> years, boolean listen) {
        String year = listen ? "CAST(p.year AS INTEGER)" : "CAST(strftime('%Y', COALESCE(s.release_date, al.release_date)) AS INTEGER)";
        String sql = "WITH play_counts AS MATERIALIZED (" + yearSongCounts(listen) + "), items AS MATERIALIZED ("
                + "SELECT " + year + " AS year, s.id AS song_id, s.name AS song_name, ar.id AS artist_id, ar.name AS artist_name, "
                + "ar.gender_id, al.id AS album_id, al.name AS album_name, p.plays FROM play_counts p "
                + "JOIN Song s ON s.id=p.song_id JOIN Artist ar ON ar.id=s.artist_id LEFT JOIN Album al ON al.id=s.album_id), "
                + "artist_plays AS (SELECT year, artist_id AS id, artist_name AS name, artist_name, gender_id, "
                + "ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(plays) DESC, artist_id, artist_name, gender_id) rn "
                + "FROM items GROUP BY year, artist_id, artist_name, gender_id), "
                + "album_plays AS (SELECT year, album_id AS id, album_name AS name, artist_name, gender_id, "
                + "ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(plays) DESC, album_id, album_name, artist_name, gender_id) rn "
                + "FROM items WHERE album_id IS NOT NULL GROUP BY year, album_id, album_name, artist_name, gender_id), "
                + "song_plays AS (SELECT year, song_id AS id, song_name AS name, artist_name, gender_id, "
                + "ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(plays) DESC, song_id, song_name, artist_name, gender_id) rn "
                + "FROM items GROUP BY year, song_id, song_name, artist_name, gender_id) "
                + "SELECT 'artist' AS kind, * FROM artist_plays WHERE rn=1 UNION ALL "
                + "SELECT 'album', * FROM album_plays WHERE rn=1 UNION ALL SELECT 'song', * FROM song_plays WHERE rn=1";
        var byYear = new java.util.HashMap<Integer, YearCardDTO>();
        years.forEach(dto -> byYear.put(dto.getYear(), dto));
        jdbcTemplate.query(sql, (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
            Integer yearValue = (Integer) rs.getObject("year");
            YearCardDTO dto = byYear.get(yearValue);
            if (dto == null) return;
            int id = rs.getInt("id");
            String name = rs.getString("name");
            String artist = rs.getString("artist_name");
            Integer gender = (Integer) rs.getObject("gender_id");
            switch (rs.getString("kind")) {
                case "artist" -> { dto.setTopArtistId(id); dto.setTopArtistName(name); dto.setTopArtistGenderId(gender); }
                case "album" -> { dto.setTopAlbumId(id); dto.setTopAlbumName(name); dto.setTopAlbumArtistName(artist); dto.setTopAlbumGenderId(gender); }
                case "song" -> { dto.setTopSongId(id); dto.setTopSongName(name); dto.setTopSongArtistName(artist); dto.setTopSongGenderId(gender); }
            }
        });
    }

    /**
     * Get listen year statistics - organized by the year when plays occurred.
     * Includes empty cards for years with no listens within the range.
     */
    public List<YearCardDTO> getListenYears(String sortBy, String sortDir) {
        return getListenYears(sortBy, sortDir, null);
    }

    public List<YearCardDTO> getListenYears(String sortBy, String sortDir, Integer randomSeed) {
        // First, get the min and max years from play data
        String minMaxSql = "SELECT MIN(CAST(year AS INTEGER)) AS min_year, MAX(CAST(year AS INTEGER)) AS max_year " +
                          "FROM (SELECT strftime('%Y', play_date) AS year FROM Play WHERE play_date IS NOT NULL GROUP BY year)";

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

        String sql = yearStatisticsSql(true);

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
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(rs.getLong("time_listened")));
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
                case "primary_plays": sortColumn = "primary_plays"; break;
                case "legacy_plays": sortColumn = "legacy_plays"; break;
                case "time": sortColumn = "time"; break;
                case "artists": sortColumn = "artists"; break;
                case "albums": sortColumn = "albums"; break;
                case "songs": sortColumn = "songs"; break;
                case "maleartistpct": sortColumn = "maleartistpct"; break;
                case "malealbumpct": sortColumn = "malealbumpct"; break;
                case "malesongpct": sortColumn = "malesongpct"; break;
                case "maleplaypct": sortColumn = "maleplaypct"; break;
                case "maletimepct": sortColumn = "maletimepct"; break;
                case "random": sortColumn = "random"; break;
                default: sortColumn = "year";
            }
        }

        if ("random".equals(sortColumn)) {
            RandomSortUtils.shuffle(allYearDtos, randomSeed);
        } else {
            final String finalSortColumn = sortColumn;
            java.util.Comparator<YearCardDTO> comparator = switch (finalSortColumn) {
                case "plays" -> {
                    java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getPlayCount);
                    yield descending ? c.reversed() : c;
                }
                case "primary_plays" -> {
                    java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getVatitoPlayCount);
                    yield descending ? c.reversed() : c;
                }
                case "legacy_plays" -> {
                    java.util.Comparator<YearCardDTO> c = java.util.Comparator.comparing(YearCardDTO::getRobertloverPlayCount);
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
        }

        List<YearCardDTO> years = allYearDtos;

        // Populate top items only for years with data
        List<YearCardDTO> yearsWithData = years.stream()
            .filter(y -> y.getPlayCount() != null && y.getPlayCount() > 0)
            .toList();
        if (!yearsWithData.isEmpty()) {
            populateTopItems(yearsWithData, true);
        }

        return years;
    }

    /**
     * Get release year statistics - organized by the release year of songs/albums.
     * Uses song's release_date if available, otherwise falls back to album's release_date.
     */
    public List<YearCardDTO> getReleaseYears(String sortBy, String sortDir) {
        return getReleaseYears(sortBy, sortDir, null);
    }

    public List<YearCardDTO> getReleaseYears(String sortBy, String sortDir, Integer randomSeed) {

        String sortColumn = "year";
        String sortDirection = "desc".equalsIgnoreCase(sortDir) ? "DESC" : "ASC";
        String nullsHandling = " NULLS LAST";

        if (sortBy != null) {
            switch (sortBy.toLowerCase()) {
                case "plays": sortColumn = "play_count"; nullsHandling = ""; break;
                case "primary_plays": sortColumn = "vatito_play_count"; nullsHandling = ""; break;
                case "legacy_plays": sortColumn = "robertlover_play_count"; nullsHandling = ""; break;
                case "time": sortColumn = "time_listened"; nullsHandling = ""; break;
                case "artists": sortColumn = "artist_count"; nullsHandling = ""; break;
                case "albums": sortColumn = "album_count"; nullsHandling = ""; break;
                case "songs": sortColumn = "song_count"; nullsHandling = ""; break;
                case "maleartistpct": sortColumn = "male_artist_pct"; break;
                case "malealbumpct": sortColumn = "male_album_pct"; break;
                case "malesongpct": sortColumn = "male_song_pct"; break;
                case "maleplaypct": sortColumn = "male_play_pct"; break;
                case "maletimepct": sortColumn = "male_time_pct"; break;
                case "random":
                    sortColumn = RandomSortUtils.sqliteNumericExpression("year", randomSeed);
                    sortDirection = "";
                    nullsHandling = "";
                    break;
                default: sortColumn = "year"; nullsHandling = "";
            }
        }

        // Uses effective release year: song's release_date > album's release_date
        String sql = yearStatisticsSql(false) + " ORDER BY " + sortColumn + " " + sortDirection + nullsHandling
                + ", year " + (sortDirection.isEmpty() ? "ASC" : sortDirection);

        List<YearCardDTO> years = jdbcTemplate.query(sql, (rs, rowNum) -> {
            YearCardDTO dto = new YearCardDTO();
            dto.setYear(rs.getInt("year"));
            dto.setYearType("release");
            dto.setPlayCount(rs.getInt("play_count"));
            dto.setVatitoPlayCount(rs.getInt("vatito_play_count"));
            dto.setRobertloverPlayCount(rs.getInt("robertlover_play_count"));
            dto.setTimeListened(rs.getLong("time_listened"));
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(rs.getLong("time_listened")));
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
        });

        if (!years.isEmpty()) {
            populateTopItems(years, false);
        }

        return years;
    }

    public long countReleaseYears() {
        String sql = "SELECT COUNT(DISTINCT strftime('%Y', COALESCE(s.release_date, al.release_date))) " +
                     "FROM Song s LEFT JOIN Album al ON s.album_id = al.id " +
                     "WHERE COALESCE(s.release_date, al.release_date) IS NOT NULL";
        Long count = jdbcTemplate.queryForObject(sql, Long.class);
        return count != null ? count : 0;
    }

}
