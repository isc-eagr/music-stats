package library.controller;

import library.dto.SubGenreCardDTO;
import library.entity.SubGenre;
import library.service.SubGenreService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/subgenres")
public class SubGenreController {
    
    private final SubGenreService subGenreService;
    
    public SubGenreController(SubGenreService subGenreService) {
        this.subGenreService = subGenreService;
    }
    
    @GetMapping
    public String listSubGenres(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) Integer parentGenre,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            @RequestParam(required = false) Integer randomSeed,
            Model model) {
        
        // Get filtered and sorted subgenres
        List<SubGenreCardDTO> subgenres = subGenreService.getSubGenres(q, parentGenre, sortby, sortdir, randomSeed);

        long totalCount = subGenreService.countSubGenres(q, parentGenre);
        
        // Add data to model
        model.addAttribute("currentSection", "subgenres");
        model.addAttribute("subgenres", subgenres);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", totalCount > 0 ? 1 : 0);
        model.addAttribute("endIndex", totalCount);
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedParentGenre", parentGenre);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "name");
        
        // Add filter options
        model.addAttribute("genres", subGenreService.getGenres());
        
        return "subgenres/list";
    }

    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getSubGenreImage(@PathVariable Integer id) {
        byte[] image = subGenreService.getSubGenreImage(id);
        return image != null ? image : new byte[0];
    }

    /**
     * API endpoint to get all subgenres for dropdowns.
     */
    @GetMapping("/api/list")
    @ResponseBody
    public java.util.List<java.util.Map<String, Object>> listSubGenresApi() {
        return subGenreService.getAllSubGenresSimple();
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createSubGenre(@RequestBody Map<String, Object> payload) {
        try {
            SubGenre subGenre = new SubGenre();
            subGenre.setName((String) payload.get("name"));
            subGenre.setParentGenreId((Integer) payload.get("parentGenreId"));
            SubGenre created = subGenreService.createSubGenre(subGenre);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create subgenre");
        }
    }
}
