package library.controller;

import library.dto.EthnicityCardDTO;
import library.entity.Ethnicity;
import library.service.EthnicityService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller
@RequestMapping("/ethnicities")
public class EthnicityController {
    
    private final EthnicityService ethnicityService;
    
    public EthnicityController(EthnicityService ethnicityService) {
        this.ethnicityService = ethnicityService;
    }
    
    @GetMapping
    public String listEthnicities(
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Get filtered and sorted ethnicities
        List<EthnicityCardDTO> ethnicities = ethnicityService.getEthnicities(q, sortby, sortdir, page, perpage);
        
        // Get total count for pagination
        long totalCount = ethnicityService.countEthnicities(q);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "ethnicities");
        model.addAttribute("ethnicities", ethnicities);
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
        
        return "ethnicities/list";
    }
    
    @GetMapping("/{id}")
    public String viewEthnicity(@PathVariable Integer id, Model model) {
        Optional<Ethnicity> ethnicity = ethnicityService.getEthnicityById(id);
        
        if (ethnicity.isEmpty()) {
            return "redirect:/ethnicities";
        }
        
        // Check if ethnicity has an image
        byte[] image = ethnicityService.getEthnicityImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
        model.addAttribute("currentSection", "ethnicities");
        model.addAttribute("ethnicity", ethnicity.get());
        model.addAttribute("hasImage", hasImage);
        
        // Add statistics for the ethnicity
        Map<String, Object> stats = ethnicityService.getEthnicityStats(id);
        model.addAttribute("stats", stats);
        
        // Add top 50 artists, albums, and songs
        model.addAttribute("topArtists", ethnicityService.getTopArtistsForEthnicity(id));
        model.addAttribute("topAlbums", ethnicityService.getTopAlbumsForEthnicity(id));
        model.addAttribute("topSongs", ethnicityService.getTopSongsForEthnicity(id));
        
        return "ethnicities/detail";
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getEthnicityImage(@PathVariable Integer id) {
        byte[] image = ethnicityService.getEthnicityImage(id);
        return image != null ? image : new byte[0];
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadEthnicityImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            ethnicityService.updateEthnicityImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteEthnicityImage(@PathVariable Integer id) {
        try {
            ethnicityService.updateEthnicityImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    /**
     * API endpoint to get all ethnicities for dropdowns.
     */
    @GetMapping("/api/list")
    @ResponseBody
    public java.util.List<java.util.Map<String, Object>> listEthnicitiesApi() {
        return ethnicityService.getAllEthnicitiesSimple();
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createEthnicity(@RequestBody Map<String, String> payload) {
        try {
            Ethnicity ethnicity = new Ethnicity();
            ethnicity.setName(payload.get("name"));
            Ethnicity created = ethnicityService.createEthnicity(ethnicity);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create ethnicity");
        }
    }
}
