package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Consistency ranks: an item's position among all items of its catalog by days / weeks / months / years
 * with plays. Ties go to the higher share of periods since the first play (the later first play),
 * then to more plays. Gender ranks use the same order within the gender (a song's override wins over its artist's).
 * Counts every account and every date (~1 s over the full Play table), so pages load it in the background.
 */
@Service
public class ConsistencyRankService {

    public enum Catalog {
        ARTISTS("s.artist_id", "JOIN Artist ar ON ar.id = st.id", "ar.gender_id"),
        ALBUMS("s.album_id", "JOIN Album item ON item.id = st.id JOIN Artist ar ON ar.id = item.artist_id", "ar.gender_id"),
        SONGS("s.id", "JOIN Song item ON item.id = st.id JOIN Artist ar ON ar.id = item.artist_id",
                "COALESCE(item.override_gender_id, ar.gender_id)");

        private final String key;
        private final String artistJoin;
        private final String genderId;

        Catalog(String key, String artistJoin, String genderId) {
            this.key = key;
            this.artistJoin = artistJoin;
            this.genderId = genderId;
        }

        public static Optional<Catalog> fromPath(String path) {
            for (Catalog catalog : values()) {
                if (catalog.name().equalsIgnoreCase(path)) {
                    return Optional.of(catalog);
                }
            }
            return Optional.empty();
        }
    }

    public record PeriodRank(int rank, Integer genderRank) {}

    public record ConsistencyRanks(String genderName, Map<String, PeriodRank> periods) {}

    /** Period key, its distinct-count column, and the first-play bucket that orders ties by share. */
    private static final List<String[]> PERIODS = List.of(
            new String[] {"days", "d", "DATE(st.first_play)"},
            new String[] {"weeks", "w", "strftime('%Y-%W', st.first_play)"},
            new String[] {"months", "m", "strftime('%Y-%m', st.first_play)"},
            new String[] {"years", "y", "strftime('%Y', st.first_play)"});

    private final JdbcTemplate jdbcTemplate;

    public ConsistencyRankService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<ConsistencyRanks> findRanks(Catalog catalog, int id) {
        List<ConsistencyRanks> rows = jdbcTemplate.query(buildSql(catalog), (rs, rowNum) -> {
            Map<String, PeriodRank> periods = new LinkedHashMap<>();
            for (String[] period : PERIODS) {
                int genderRank = rs.getInt(period[1] + "_grank");
                periods.put(period[0], new PeriodRank(rs.getInt(period[1] + "_rank"), rs.wasNull() ? null : genderRank));
            }
            return new ConsistencyRanks(rs.getString("gender_name"), periods);
        }, id);
        return rows.stream().findFirst();
    }

    static String buildSql(Catalog catalog) {
        StringBuilder ranks = new StringBuilder();
        for (String[] period : PERIODS) {
            String order = "st." + period[1] + " DESC, " + period[2] + " DESC, st.plays DESC, st.id";
            ranks.append(",\n           ROW_NUMBER() OVER (ORDER BY ").append(order).append(") AS ").append(period[1]).append("_rank")
                 .append(",\n           CASE WHEN ").append(catalog.genderId).append(" IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY ")
                 .append(catalog.genderId).append(" ORDER BY ").append(order).append(") END AS ").append(period[1]).append("_grank");
        }
        return """
            WITH stats AS MATERIALIZED (
                SELECT %1$s AS id,
                       COUNT(DISTINCT DATE(p.play_date)) AS d,
                       COUNT(DISTINCT strftime('%%Y-%%W', p.play_date)) AS w,
                       COUNT(DISTINCT strftime('%%Y-%%m', p.play_date)) AS m,
                       COUNT(DISTINCT strftime('%%Y', p.play_date)) AS y,
                       MIN(p.play_date) AS first_play,
                       COUNT(*) AS plays
                FROM Play p
                JOIN Song s ON s.id = p.song_id
                WHERE %1$s IS NOT NULL AND p.play_date IS NOT NULL
                GROUP BY %1$s
            ), ranked AS (
                SELECT st.id, %3$s AS gender_id%4$s
                FROM stats st
                %2$s
            )
            SELECT ranked.*, g.name AS gender_name
            FROM ranked
            LEFT JOIN Gender g ON g.id = ranked.gender_id
            WHERE ranked.id = ?
            """.formatted(catalog.key, catalog.artistJoin, catalog.genderId, ranks);
    }
}
