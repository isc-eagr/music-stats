package library.service;

import library.dto.TodayMalePlayStatsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;

@Service
public class PlayStatsService {

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    @Autowired
    public PlayStatsService(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, Clock.systemUTC());
    }

    PlayStatsService(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock.withZone(ZoneId.of("America/Mexico_City"));
    }

    /** Returns the percentage among known genders and a count summary including all plays. */
    public TodayMalePlayStatsDTO getTodayMalePlayStats() {
        LocalDate today = LocalDate.now(clock);
        // Imported play dates are already stored in Mexico City local time.
        String sql = """
                SELECT COUNT(*) AS total_plays,
                       SUM(CASE WHEN s.id IS NOT NULL AND ar.id IS NOT NULL
                                     AND COALESCE(s.override_gender_id, ar.gender_id) IS NOT NULL
                                THEN 1 ELSE 0 END) AS known_gender_plays,
                       SUM(CASE WHEN ar.id IS NOT NULL
                                     AND g.name LIKE '%Male%' AND g.name NOT LIKE '%Female%'
                                THEN 1 ELSE 0 END) AS male_plays
                FROM Play p
                LEFT JOIN Song s ON s.id = p.song_id
                LEFT JOIN Artist ar ON ar.id = s.artist_id
                LEFT JOIN Gender g ON g.id = COALESCE(s.override_gender_id, ar.gender_id)
                WHERE p.play_date >= ? AND p.play_date < ?
                """;
        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
            long malePlays = rs.getLong("male_plays");
            long knownGenderPlays = rs.getLong("known_gender_plays");
            BigDecimal percentage = knownGenderPlays == 0
                    ? BigDecimal.ZERO.setScale(2)
                    : BigDecimal.valueOf(malePlays).multiply(BigDecimal.valueOf(100))
                            .divide(BigDecimal.valueOf(knownGenderPlays), 2, RoundingMode.HALF_UP);
            return new TodayMalePlayStatsDTO(percentage,
                    malePlays + " male out of " + rs.getLong("total_plays"));
        },
                today + " 00:00", today.plusDays(1) + " 00:00");
    }
}
