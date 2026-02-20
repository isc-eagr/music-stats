package library.service;

import library.entity.ArtistImageTheme;
import library.entity.ArtistTheme;
import library.repository.ArtistImageThemeRepository;
import library.repository.ArtistThemeRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class ThemeService {

    private final ArtistThemeRepository themeRepository;
    private final ArtistImageThemeRepository imageThemeRepository;
    private final JdbcTemplate jdbcTemplate;

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public ThemeService(ArtistThemeRepository themeRepository,
                        ArtistImageThemeRepository imageThemeRepository,
                        JdbcTemplate jdbcTemplate) {
        this.themeRepository = themeRepository;
        this.imageThemeRepository = imageThemeRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    // -------------------------------------------------------------------------
    // Theme CRUD
    // -------------------------------------------------------------------------

    public List<ArtistTheme> getAllThemes() {
        return themeRepository.findAllByOrderByNameAsc();
    }

    public Optional<ArtistTheme> getActiveTheme() {
        return themeRepository.findByIsActiveTrue();
    }

    @Transactional
    public ArtistTheme createTheme(String name) {
        ArtistTheme theme = new ArtistTheme();
        theme.setName(name.trim());
        theme.setIsActive(false);
        theme.setCreationDate(LocalDateTime.now().format(DATE_FMT));
        return themeRepository.save(theme);
    }

    @Transactional
    public void deleteTheme(Integer themeId) {
        // Cascade in DB handles ArtistImageTheme rows; just delete the theme.
        themeRepository.deleteById(themeId);
    }

    @Transactional
    public void activateTheme(Integer themeId) {
        themeRepository.deactivateAll();
        ArtistTheme theme = themeRepository.findById(themeId)
                .orElseThrow(() -> new IllegalArgumentException("Theme not found: " + themeId));
        theme.setIsActive(true);
        themeRepository.save(theme);
    }

    @Transactional
    public void deactivateAll() {
        themeRepository.deactivateAll();
    }

    // -------------------------------------------------------------------------
    // Artist image → theme assignment
    // -------------------------------------------------------------------------

    /**
     * Returns the existing assignment for (themeId, artistId) or empty.
     */
    public Optional<ArtistImageTheme> getAssignment(Integer themeId, Integer artistId) {
        return imageThemeRepository.findByThemeIdAndArtistId(themeId, artistId);
    }

    /**
     * Assign an artist image (secondary or null = default) to a theme for a given artist.
     * Replaces any existing assignment.
     * Returns a Map with keys: wasReplaced (boolean), previousImageId (Integer|null)
     */
    @Transactional
    public Map<String, Object> assignImageToTheme(Integer themeId, Integer artistId, Integer artistImageId) {
        Optional<ArtistImageTheme> existing = imageThemeRepository.findByThemeIdAndArtistId(themeId, artistId);
        boolean wasReplaced = existing.isPresent();
        Integer previousImageId = wasReplaced ? existing.get().getArtistImageId() : null;

        // If a specific secondary image is being assigned, delete any other theme row
        // that currently holds that same image for this artist — before saving the new assignment.
        if (artistImageId != null) {
            imageThemeRepository.findByArtistIdAndArtistImageId(artistId, artistImageId)
                    .filter(conflict -> existing.isEmpty() || !conflict.getId().equals(existing.get().getId()))
                    .ifPresent(conflict -> imageThemeRepository.deleteById(conflict.getId()));
        }

        ArtistImageTheme assignment = existing.orElseGet(ArtistImageTheme::new);
        assignment.setThemeId(themeId);
        assignment.setArtistId(artistId);
        assignment.setArtistImageId(artistImageId);
        if (assignment.getCreationDate() == null) {
            assignment.setCreationDate(LocalDateTime.now().format(DATE_FMT));
        }
        imageThemeRepository.save(assignment);

        Map<String, Object> result = new java.util.HashMap<>();
        result.put("wasReplaced", wasReplaced);
        result.put("previousImageId", previousImageId != null ? previousImageId : -1);
        return result;
    }

    /**
     * Remove a theme assignment for an artist.
     */
    @Transactional
    public void removeAssignment(Integer themeId, Integer artistId) {
        imageThemeRepository.findByThemeIdAndArtistId(themeId, artistId)
                .ifPresent(a -> imageThemeRepository.deleteById(a.getId()));
    }

    /**
     * Return the bytes for the active-theme image for this artist, or null if none.
     * Called by ArtistService.getArtistImage() to override the default image.
     * Uses raw JDBC only — never touches Artist or ArtistImage JPA entities.
     */
    public byte[] getActiveThemeImageForArtist(Integer artistId) {
        // Single query: find the artist_image_id assigned to the active theme for this artist.
        String sql = """
                SELECT ait.artist_image_id
                FROM ArtistImageTheme ait
                JOIN ArtistTheme t ON t.id = ait.theme_id
                WHERE t.is_active = 1 AND ait.artist_id = ?
                LIMIT 1
                """;
        List<Integer> rows = jdbcTemplate.query(sql, (rs, i) -> {
            int val = rs.getInt("artist_image_id");
            return rs.wasNull() ? null : val;
        }, artistId);

        if (rows.isEmpty()) return null;

        Integer artistImageId = rows.get(0);
        if (artistImageId == null) {
            // NULL means "use the artist's default image" — no override needed.
            return null;
        }

        // Fetch the secondary image bytes via JDBC, not JPA.
        String imgSql = "SELECT image FROM ArtistImage WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(imgSql, (rs, i) -> rs.getBytes("image"), artistImageId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Returns all theme assignments for a given artist as lightweight maps.
     * Used by the detail page to populate the assign-to-theme modal.
     */
    public List<Map<String, Object>> getAssignmentsForArtist(Integer artistId) {
        return imageThemeRepository.findByArtistId(artistId).stream()
                .map(a -> {
                    Map<String, Object> m = new java.util.HashMap<>();
                    m.put("themeId", a.getThemeId());
                    m.put("artistImageId", a.getArtistImageId() != null ? a.getArtistImageId() : -1);
                    return m;
                })
                .collect(java.util.stream.Collectors.toList());
    }
}
