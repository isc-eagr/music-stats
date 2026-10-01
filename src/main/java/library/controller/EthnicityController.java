package library.controller;

import library.dto.EthnicityCardDTO;
import library.entity.Ethnicity;
import library.service.EthnicityService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

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
            @RequestParam(required = false) Integer randomSeed,
            Model model) {
        
        // Get filtered and sorted ethnicities
        List<EthnicityCardDTO> ethnicities = ethnicityService.getEthnicities(q, sortby, sortdir, randomSeed);

        long totalCount = ethnicityService.countEthnicities(q);
        
        // Add data to model
        model.addAttribute("currentSection", "ethnicities");
        model.addAttribute("ethnicities", ethnicities);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", totalCount > 0 ? 1 : 0);
        model.addAttribute("endIndex", totalCount);
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "name");
        
        return "ethnicities/list";
    }

    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getEthnicityImage(@PathVariable Integer id) {
        byte[] image = ethnicityService.getEthnicityImage(id);
        return image != null ? image : new byte[0];
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
