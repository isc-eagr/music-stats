package library.controller;

import library.dto.GenreCardDTO;
import library.entity.Genre;
import library.service.GenreService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/genres")
public class GenreController {
    
    private final GenreService genreService;
    
    public GenreController(GenreService genreService) {
        this.genreService = genreService;
    }
    
    @GetMapping
    public String listGenres(
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            @RequestParam(required = false) Integer randomSeed,
            Model model) {
        
        // Get filtered and sorted genres
        List<GenreCardDTO> genres = genreService.getGenres(q, sortby, sortdir, randomSeed);

        long totalCount = genreService.countGenres(q);
        
        // Add data to model
        model.addAttribute("currentSection", "genres");
        model.addAttribute("genres", genres);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", totalCount > 0 ? 1 : 0);
        model.addAttribute("endIndex", totalCount);
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "name");
        
        return "genres/list";
    }

    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getGenreImage(@PathVariable Integer id) {
        byte[] image = genreService.getGenreImage(id);
        return image != null ? image : new byte[0];
    }

    /**
     * API endpoint to get all genres for dropdowns.
     */
    @GetMapping("/api/list")
    @ResponseBody
    public java.util.List<java.util.Map<String, Object>> listGenresApi() {
        return genreService.getAllGenresSimple();
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createGenre(@RequestBody Map<String, String> payload) {
        try {
            Genre genre = new Genre();
            genre.setName(payload.get("name"));
            Genre created = genreService.createGenre(genre);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create genre");
        }
    }
}
