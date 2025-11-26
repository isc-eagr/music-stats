package library.controller;

import library.dto.SubGenreCardDTO;
import library.entity.SubGenre;
import library.service.SubGenreService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Get filtered and sorted subgenres
        List<SubGenreCardDTO> subgenres = subGenreService.getSubGenres(q, parentGenre, sortby, sortdir, page, perpage);
        
        // Get total count for pagination
        long totalCount = subGenreService.countSubGenres(q, parentGenre);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "subgenres");
        model.addAttribute("subgenres", subgenres);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedParentGenre", parentGenre);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        
        // Add filter options
        model.addAttribute("genres", subGenreService.getGenres());
        
        return "subgenres/list";
    }
    
    @GetMapping("/{id}")
    public String viewSubGenre(@PathVariable Integer id, Model model) {
        Optional<SubGenre> subGenre = subGenreService.getSubGenreById(id);
        
        if (subGenre.isEmpty()) {
            return "redirect:/subgenres";
        }
        
        // Check if subgenre has an image
        byte[] image = subGenreService.getSubGenreImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
        model.addAttribute("currentSection", "subgenres");
        model.addAttribute("subgenre", subGenre.get());
        model.addAttribute("hasImage", hasImage);
        
        // Add genres for the parent genre dropdown
        model.addAttribute("genres", subGenreService.getGenres());
        
        // Add statistics for the subgenre
        Map<String, Object> stats = subGenreService.getSubGenreStats(id);
        model.addAttribute("stats", stats);
        
        // Add top 50 artists, albums, and songs
        model.addAttribute("topArtists", subGenreService.getTopArtistsForSubGenre(id));
        model.addAttribute("topAlbums", subGenreService.getTopAlbumsForSubGenre(id));
        model.addAttribute("topSongs", subGenreService.getTopSongsForSubGenre(id));
        
        return "subgenres/detail";
    }
    
    @PostMapping("/{id}")
    public String updateSubGenre(@PathVariable Integer id, @RequestParam Integer parentGenreId) {
        subGenreService.updateParentGenre(id, parentGenreId);
        return "redirect:/subgenres/" + id;
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getSubGenreImage(@PathVariable Integer id) {
        byte[] image = subGenreService.getSubGenreImage(id);
        return image != null ? image : new byte[0];
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadSubGenreImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            subGenreService.updateSubGenreImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteSubGenreImage(@PathVariable Integer id) {
        try {
            subGenreService.updateSubGenreImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
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
