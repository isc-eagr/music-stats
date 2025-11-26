package library.controller;

import library.dto.ArtistCardDTO;
import library.entity.Artist;
import library.service.ArtistService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller
@RequestMapping("/artists")
public class ArtistController {
    
    private final ArtistService artistService;
    
    public ArtistController(ArtistService artistService) {
        this.artistService = artistService;
    }
    
    @GetMapping
    public String listArtists(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> gender,
            @RequestParam(required = false) String genderMode,
            @RequestParam(required = false) List<Integer> ethnicity,
            @RequestParam(required = false) String ethnicityMode,
            @RequestParam(required = false) List<Integer> genre,
            @RequestParam(required = false) String genreMode,
            @RequestParam(required = false) List<Integer> subgenre,
            @RequestParam(required = false) String subgenreMode,
            @RequestParam(required = false) List<Integer> language,
            @RequestParam(required = false) String languageMode,
            @RequestParam(required = false) List<String> country,
            @RequestParam(required = false) String countryMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Convert date formats from dd/mm/yyyy to yyyy-MM-dd for database queries
        String firstListenedDateConverted = convertDateFormat(firstListenedDate);
        String firstListenedDateFromConverted = convertDateFormat(firstListenedDateFrom);
        String firstListenedDateToConverted = convertDateFormat(firstListenedDateTo);
        String lastListenedDateConverted = convertDateFormat(lastListenedDate);
        String lastListenedDateFromConverted = convertDateFormat(lastListenedDateFrom);
        String lastListenedDateToConverted = convertDateFormat(lastListenedDateTo);
        
        // Get filtered and sorted artists
        List<ArtistCardDTO> artists = artistService.getArtists(
                q, gender, genderMode, ethnicity, ethnicityMode, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, country, countryMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                sortby, sortdir, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = artistService.countArtists(q, gender, genderMode, ethnicity, 
                ethnicityMode, genre, genreMode, subgenre, subgenreMode, language, 
                languageMode, country, countryMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "artists");
        model.addAttribute("artists", artists);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedGenders", gender);
        model.addAttribute("genderMode", genderMode != null ? genderMode : "includes");
        model.addAttribute("selectedEthnicities", ethnicity);
        model.addAttribute("ethnicityMode", ethnicityMode != null ? ethnicityMode : "includes");
        model.addAttribute("selectedGenres", genre);
        model.addAttribute("genreMode", genreMode != null ? genreMode : "includes");
        model.addAttribute("selectedSubgenres", subgenre);
        model.addAttribute("subgenreMode", subgenreMode != null ? subgenreMode : "includes");
        model.addAttribute("selectedLanguages", language);
        model.addAttribute("languageMode", languageMode != null ? languageMode : "includes");
        model.addAttribute("selectedCountries", country);
        model.addAttribute("countryMode", countryMode != null ? countryMode : "includes");
        
        // First listened date filter attributes
        model.addAttribute("firstListenedDate", firstListenedDate);
        model.addAttribute("firstListenedDateFrom", firstListenedDateFrom);
        model.addAttribute("firstListenedDateTo", firstListenedDateTo);
        model.addAttribute("firstListenedDateMode", firstListenedDateMode != null ? firstListenedDateMode : "exact");
        model.addAttribute("firstListenedDateFormatted", formatDateForDisplay(firstListenedDate));
        model.addAttribute("firstListenedDateFromFormatted", formatDateForDisplay(firstListenedDateFrom));
        model.addAttribute("firstListenedDateToFormatted", formatDateForDisplay(firstListenedDateTo));
        
        // Last listened date filter attributes
        model.addAttribute("lastListenedDate", lastListenedDate);
        model.addAttribute("lastListenedDateFrom", lastListenedDateFrom);
        model.addAttribute("lastListenedDateTo", lastListenedDateTo);
        model.addAttribute("lastListenedDateMode", lastListenedDateMode != null ? lastListenedDateMode : "exact");
        model.addAttribute("lastListenedDateFormatted", formatDateForDisplay(lastListenedDate));
        model.addAttribute("lastListenedDateFromFormatted", formatDateForDisplay(lastListenedDateFrom));
        model.addAttribute("lastListenedDateToFormatted", formatDateForDisplay(lastListenedDateTo));
        
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        
        // Add filter options
        model.addAttribute("genders", artistService.getGenders());
        model.addAttribute("ethnicities", artistService.getEthnicities());
        model.addAttribute("genres", artistService.getGenres());
        model.addAttribute("subgenres", artistService.getSubGenres());
        model.addAttribute("languages", artistService.getLanguages());
        model.addAttribute("countries", artistService.getCountries());
        
        return "artists/list";
    }
    
    @GetMapping("/{id}")
    public String viewArtist(@PathVariable Integer id, 
                            @RequestParam(defaultValue = "general") String tab,
                            @RequestParam(defaultValue = "0") int playsPage,
                            Model model) {
        Optional<Artist> artist = artistService.getArtistById(id);
        
        if (artist.isEmpty()) {
            return "redirect:/artists";
        }
        
        // Check if artist has an image
        byte[] image = artistService.getArtistImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
        model.addAttribute("currentSection", "artists");
        model.addAttribute("artist", artist.get());
        model.addAttribute("hasImage", hasImage);
        model.addAttribute("genders", artistService.getGenders());
        model.addAttribute("ethnicities", artistService.getEthnicities());
        model.addAttribute("genres", artistService.getGenres());
        model.addAttribute("subgenres", artistService.getSubGenres());
        model.addAttribute("languages", artistService.getLanguages());
        // Add dynamic countries list
        model.addAttribute("countries", artistService.getCountries());
        // Add album and song counts for quick stats
        int[] counts = artistService.getAlbumAndSongCounts(id);
        model.addAttribute("albumCount", counts[0]);
        model.addAttribute("songCount", counts[1]);
        // Add play count for artist
        model.addAttribute("artistPlayCount", artistService.getPlayCountForArtist(id));
        // Add per-account breakdown string for tooltip
        model.addAttribute("artistPlaysByAccount", artistService.getPlaysByAccountForArtist(id));
        
        // Add statistics for the artist
        model.addAttribute("totalListeningTime", artistService.getTotalListeningTimeForArtist(id));
        model.addAttribute("firstListenedDate", artistService.getFirstListenedDateForArtist(id));
        model.addAttribute("lastListenedDate", artistService.getLastListenedDateForArtist(id));
        
        // Add albums list for the artist
        model.addAttribute("albums", artistService.getAlbumsForArtist(id));
        
        // Add songs list for the artist
        model.addAttribute("songs", artistService.getSongsForArtist(id));
        
        // Tab and plays data
        model.addAttribute("activeTab", tab);
        
        if ("plays".equals(tab)) {
            int pageSize = 100;
            model.addAttribute("scrobbles", artistService.getScrobblesForArtist(id, playsPage, pageSize));
            model.addAttribute("scrobblesTotalCount", artistService.countScrobblesForArtist(id));
            model.addAttribute("scrobblesPage", playsPage);
            model.addAttribute("scrobblesPageSize", pageSize);
            model.addAttribute("scrobblesTotalPages", (int) Math.ceil((double) artistService.countScrobblesForArtist(id) / pageSize));
            model.addAttribute("playsByYear", artistService.getPlaysByYearForArtist(id));
        }
        
        // Get gender name for bar color
        model.addAttribute("artistGenderName", artistService.getArtistGenderName(id));
        
        return "artists/detail";
    }
    
    @PostMapping("/{id}")
    public String updateArtist(@PathVariable Integer id, @ModelAttribute Artist artist) {
        artist.setId(id);
        artistService.saveArtist(artist);
        return "redirect:/artists/" + id;
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getArtistImage(@PathVariable Integer id) {
        return artistService.getArtistImage(id);
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadArtistImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            artistService.updateArtistImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteArtistImage(@PathVariable Integer id) {
        try {
            artistService.updateArtistImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}")
    @ResponseBody
    public String deleteArtist(@PathVariable Integer id) {
        try {
            artistService.deleteArtist(id);
            return "success";
        } catch (IllegalStateException e) {
            return "error:" + e.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createArtist(@RequestBody Artist artist) {
        try {
            Artist created = artistService.createArtist(artist);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create artist");
        }
    }
    
    @GetMapping("/api/artists")
    @ResponseBody
    public List<Map<String, Object>> getAllArtistsForApi() {
        return artistService.getAllArtistsForApi();
    }
    
    // Helper method to format date strings for display (yyyy-MM-dd -> dd MMM yyyy)
    private String formatDateForDisplay(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            String[] parts = dateStr.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                String[] monthNames = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
                return day + " " + monthNames[month - 1] + " " + year;
            }
        } catch (Exception e) {
            // If parsing fails, return original
        }
        return dateStr;
    }
    
    // Helper method to convert date format from dd/mm/yyyy to yyyy-MM-dd for database queries
    private String convertDateFormat(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            // Check if it's already in yyyy-MM-dd format
            if (dateStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return dateStr;
            }
            // Try to parse dd/mm/yyyy format
            if (dateStr.matches("\\d{2}/\\d{2}/\\d{4}")) {
                String[] parts = dateStr.split("/");
                if (parts.length == 3) {
                    return parts[2] + "-" + parts[1] + "-" + parts[0];
                }
            }
        } catch (Exception e) {
            // If parsing fails, return original
        }
        return dateStr;
    }
}