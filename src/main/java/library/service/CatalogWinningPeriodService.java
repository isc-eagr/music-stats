package library.service;

import library.dto.CatalogWinningPeriodStatsDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

@Service
public class CatalogWinningPeriodService {

    public enum CatalogAttribute {
        COUNTRY("ar.country"),
        ETHNICITY("COALESCE(s.override_ethnicity_id, ar.ethnicity_id)"),
        GENDER("COALESCE(s.override_gender_id, ar.gender_id)"),
        GENRE("COALESCE(s.override_genre_id, COALESCE(al.override_genre_id, ar.genre_id))"),
        LANGUAGE("COALESCE(s.override_language_id, COALESCE(al.override_language_id, ar.language_id))"),
        SUBGENRE("COALESCE(s.override_subgenre_id, COALESCE(al.override_subgenre_id, ar.subgenre_id))");

        private final String expression;

        CatalogAttribute(String expression) {
            this.expression = expression;
        }
    }

    private final JdbcTemplate jdbcTemplate;

    public CatalogWinningPeriodService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public static boolean isWinningPeriodSort(String sortBy) {
        if (sortBy == null) {
            return false;
        }

        return switch (sortBy.toLowerCase(Locale.ROOT)) {
            case "winningdays", "winningweeks", "winningmonths",
                 "winningseasons", "winningyears", "winningdecades" -> true;
            default -> false;
        };
    }

    public static int getWinningPeriodCount(CatalogWinningPeriodStatsDTO item, String sortBy) {
        if (item == null || sortBy == null) {
            return 0;
        }

        return switch (sortBy.toLowerCase(Locale.ROOT)) {
            case "winningdays" -> item.getWinningDaysCount();
            case "winningweeks" -> item.getWinningWeeksCount();
            case "winningmonths" -> item.getWinningMonthsCount();
            case "winningseasons" -> item.getWinningSeasonsCount();
            case "winningyears" -> item.getWinningYearsCount();
            case "winningdecades" -> item.getWinningDecadesCount();
            default -> 0;
        };
    }

    public static <T extends CatalogWinningPeriodStatsDTO> void sortByWinningPeriod(
            List<T> items, String sortBy, String sortDir, Function<T, String> nameExtractor) {
        Comparator<T> countComparator = Comparator.comparingInt(item -> getWinningPeriodCount(item, sortBy));
        if ("desc".equalsIgnoreCase(sortDir)) {
            countComparator = countComparator.reversed();
        }

        items.sort(countComparator.thenComparing(
            item -> {
                String name = nameExtractor.apply(item);
                return name != null ? name : "";
            },
            String.CASE_INSENSITIVE_ORDER
        ));
    }

    public <T extends CatalogWinningPeriodStatsDTO> void populateWinningCounts(
            List<T> items, Function<T, ?> keyExtractor, CatalogAttribute attribute) {
        if (items == null || items.isEmpty()) {
            return;
        }

        Map<String, T> itemsByKey = new HashMap<>();
        for (T item : items) {
            item.resetWinningPeriodCounts();

            Object rawKey = keyExtractor.apply(item);
            if (rawKey == null) {
                continue;
            }

            String key = String.valueOf(rawKey);
            if (!key.isBlank()) {
                itemsByKey.putIfAbsent(key, item);
            }
        }

        if (itemsByKey.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<>(itemsByKey.keySet());
        String placeholders = String.join(",", keys.stream().map(key -> "?").toList());
        String attrExpression = attribute.expression;

        String sql = """
            WITH raw_plays AS (
                SELECT
                    p.play_date,
                    CAST(%s AS TEXT) as attr_key
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album al ON s.album_id = al.id
                WHERE p.play_date IS NOT NULL
            ),
            base_plays AS (
                SELECT play_date, attr_key
                FROM raw_plays
                WHERE attr_key IS NOT NULL
            ),
            period_attr_counts AS (
                SELECT 'days' as period_type, SUBSTR(play_date, 1, 10) as period_key, attr_key, COUNT(*) as cnt
                FROM base_plays
                GROUP BY period_key, attr_key
                UNION ALL
                SELECT 'weeks' as period_type, strftime('%%Y-W%%W', play_date) as period_key, attr_key, COUNT(*) as cnt
                FROM base_plays
                GROUP BY period_key, attr_key
                UNION ALL
                SELECT 'months' as period_type, SUBSTR(play_date, 1, 7) as period_key, attr_key, COUNT(*) as cnt
                FROM base_plays
                GROUP BY period_key, attr_key
                UNION ALL
                SELECT 'seasons' as period_type,
                    CASE
                        WHEN SUBSTR(play_date, 6, 2) = '12'
                            THEN (SUBSTR(play_date, 1, 4) + 1) || '-Winter'
                        WHEN SUBSTR(play_date, 6, 2) <= '02'
                            THEN SUBSTR(play_date, 1, 4) || '-Winter'
                        WHEN SUBSTR(play_date, 6, 2) <= '05'
                            THEN SUBSTR(play_date, 1, 4) || '-Spring'
                        WHEN SUBSTR(play_date, 6, 2) <= '08'
                            THEN SUBSTR(play_date, 1, 4) || '-Summer'
                        ELSE SUBSTR(play_date, 1, 4) || '-Fall'
                    END as period_key,
                    attr_key,
                    COUNT(*) as cnt
                FROM base_plays
                GROUP BY period_key, attr_key
                UNION ALL
                SELECT 'years' as period_type, SUBSTR(play_date, 1, 4) as period_key, attr_key, COUNT(*) as cnt
                FROM base_plays
                GROUP BY period_key, attr_key
                UNION ALL
                SELECT 'decades' as period_type, (SUBSTR(play_date, 1, 4) / 10 * 10) || 's' as period_key, attr_key, COUNT(*) as cnt
                FROM base_plays
                GROUP BY period_key, attr_key
            ),
            winning_periods AS (
                SELECT
                    period_type,
                    period_key,
                    attr_key,
                    ROW_NUMBER() OVER (PARTITION BY period_type, period_key ORDER BY cnt DESC) as rn
                FROM period_attr_counts
                WHERE period_key IS NOT NULL
            )
            SELECT
                attr_key,
                SUM(CASE WHEN period_type = 'days' THEN 1 ELSE 0 END) as winning_days_count,
                SUM(CASE WHEN period_type = 'weeks' THEN 1 ELSE 0 END) as winning_weeks_count,
                SUM(CASE WHEN period_type = 'months' THEN 1 ELSE 0 END) as winning_months_count,
                SUM(CASE WHEN period_type = 'seasons' THEN 1 ELSE 0 END) as winning_seasons_count,
                SUM(CASE WHEN period_type = 'years' THEN 1 ELSE 0 END) as winning_years_count,
                SUM(CASE WHEN period_type = 'decades' THEN 1 ELSE 0 END) as winning_decades_count
            FROM winning_periods
            WHERE rn = 1
              AND attr_key IN (%s)
            GROUP BY attr_key
            """.formatted(attrExpression, placeholders);

        jdbcTemplate.query(sql, rs -> {
            T item = itemsByKey.get(rs.getString("attr_key"));
            if (item == null) {
                return;
            }

            item.setWinningDaysCount(rs.getInt("winning_days_count"));
            item.setWinningWeeksCount(rs.getInt("winning_weeks_count"));
            item.setWinningMonthsCount(rs.getInt("winning_months_count"));
            item.setWinningSeasonsCount(rs.getInt("winning_seasons_count"));
            item.setWinningYearsCount(rs.getInt("winning_years_count"));
            item.setWinningDecadesCount(rs.getInt("winning_decades_count"));
        }, keys.toArray());
    }
}
