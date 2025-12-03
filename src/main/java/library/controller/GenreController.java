package library.controller;

import library.dto.GenreCardDTO;
import library.entity.Genre;
import library.service.GenreService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Get filtered and sorted genres
        List<GenreCardDTO> genres = genreService.getGenres(q, sortby, sortdir, page, perpage);
        
        // Get total count for pagination
        long totalCount = genreService.countGenres(q);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "genres");
        model.addAttribute("genres", genres);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        
        return "genres/list";
    }
    
    @GetMapping("/{id}")
    public String viewGenre(@PathVariable Integer id, Model model) {
        Optional<Genre> genre = genreService.getGenreById(id);
        
        if (genre.isEmpty()) {
            return "redirect:/genres";
        }
        
        // Check if genre has an image
        byte[] image = genreService.getGenreImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
        model.addAttribute("currentSection", "genres");
        model.addAttribute("genre", genre.get());
        model.addAttribute("hasImage", hasImage);
        
        // Add statistics for the genre
        Map<String, Object> stats = genreService.getGenreStats(id);
        model.addAttribute("stats", stats);
        
        // Add top 50 artists, albums, and songs
        model.addAttribute("topArtists", genreService.getTopArtistsForGenre(id));
        model.addAttribute("topAlbums", genreService.getTopAlbumsForGenre(id));
        model.addAttribute("topSongs", genreService.getTopSongsForGenre(id));
        
        return "genres/detail";
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getGenreImage(@PathVariable Integer id) {
        byte[] image = genreService.getGenreImage(id);
        return image != null ? image : new byte[0];
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadGenreImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            genreService.updateGenreImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteGenreImage(@PathVariable Integer id) {
        try {
            genreService.updateGenreImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
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
