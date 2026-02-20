package library.controller;

import library.entity.ArtistTheme;
import library.service.ThemeService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/themes")
public class ThemeController {

    private final ThemeService themeService;

    public ThemeController(ThemeService themeService) {
        this.themeService = themeService;
    }

    // ------------------------------------------------------------------
    // Page
    // ------------------------------------------------------------------

    @GetMapping
    public String listThemes(Model model) {
        List<ArtistTheme> themes = themeService.getAllThemes();
        model.addAttribute("themes", themes);
        model.addAttribute("activeTheme", themeService.getActiveTheme().orElse(null));
        model.addAttribute("currentSection", "themes");
        return "themes/list";
    }

    // ------------------------------------------------------------------
    // CRUD API
    // ------------------------------------------------------------------

    @PostMapping
    @ResponseBody
    public Map<String, Object> createTheme(@RequestBody Map<String, String> body) {
        String name = body.get("name");
        if (name == null || name.isBlank()) {
            return Map.of("success", false, "message", "Name is required");
        }
        ArtistTheme created = themeService.createTheme(name);
        return Map.of("success", true, "id", created.getId(), "name", created.getName());
    }

    @DeleteMapping("/{id}")
    @ResponseBody
    public Map<String, Object> deleteTheme(@PathVariable Integer id) {
        try {
            themeService.deleteTheme(id);
            return Map.of("success", true);
        } catch (Exception e) {
            return Map.of("success", false, "message", e.getMessage());
        }
    }

    @PostMapping("/{id}/activate")
    @ResponseBody
    public Map<String, Object> activateTheme(@PathVariable Integer id) {
        try {
            themeService.activateTheme(id);
            return Map.of("success", true);
        } catch (Exception e) {
            return Map.of("success", false, "message", e.getMessage());
        }
    }

    @PostMapping("/deactivate")
    @ResponseBody
    public Map<String, Object> deactivateAll() {
        try {
            themeService.deactivateAll();
            return Map.of("success", true);
        } catch (Exception e) {
            return Map.of("success", false, "message", e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Read-only API used by artist detail page
    // ------------------------------------------------------------------

    @GetMapping("/api/all")
    @ResponseBody
    public List<Map<String, Object>> getAllThemesApi() {
        return themeService.getAllThemes().stream()
                .map(t -> {
                    Map<String, Object> m = new java.util.HashMap<>();
                    m.put("id", t.getId());
                    m.put("name", t.getName());
                    m.put("isActive", t.getIsActive() != null && t.getIsActive());
                    return m;
                })
                .collect(java.util.stream.Collectors.toList());
    }
}
