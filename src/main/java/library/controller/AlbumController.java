package library.controller;

import library.dto.AlbumCardDTO;
import library.entity.Album;
import library.service.AlbumService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller
@RequestMapping("/albums")
public class AlbumController {
    
    private final AlbumService albumService;
    
    public AlbumController(AlbumService albumService) {
        this.albumService = albumService;
    }
    
    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(Date.class, new java.beans.PropertyEditorSupport() {
            private final java.text.SimpleDateFormat displayFormat = new java.text.SimpleDateFormat("dd MMM yyyy");
            private final java.text.SimpleDateFormat inputFormat = new java.text.SimpleDateFormat("dd/MM/yyyy");
            
            @Override
            public void setAsText(String text) {
                if (text == null || text.trim().isEmpty()) {
                    setValue(null);
                } else {
                    try {
                        // Try input format first (dd/MM/yyyy)
                        java.util.Date parsed = inputFormat.parse(text);
                        setValue(new Date(parsed.getTime()));
                    } catch (Exception e) {
                        try {
                            // Fallback to display format (dd MMM yyyy)
                            java.util.Date parsed = displayFormat.parse(text);
                            setValue(new Date(parsed.getTime()));
                        } catch (Exception e2) {
                            // Last resort: try yyyy-MM-dd format
                            try {
                                setValue(Date.valueOf(text));
                            } catch (Exception ex) {
                                System.err.println("Failed to parse date: " + text);
                                setValue(null);
                            }
                        }
                    }
                }
            }
            
            @Override
            public String getAsText() {
                Date value = (Date) getValue();
                return value != null ? displayFormat.format(value) : "";
            }
        });
    }
    
    @GetMapping
    public String listAlbums(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) List<Integer> genre,
            @RequestParam(required = false) String genreMode,
            @RequestParam(required = false) List<Integer> subgenre,
            @RequestParam(required = false) String subgenreMode,
            @RequestParam(required = false) List<Integer> language,
            @RequestParam(required = false) String languageMode,
            @RequestParam(required = false) List<Integer> gender,
            @RequestParam(required = false) String genderMode,
            @RequestParam(required = false) List<Integer> ethnicity,
            @RequestParam(required = false) String ethnicityMode,
            @RequestParam(required = false) List<String> country,
            @RequestParam(required = false) String countryMode,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Get filtered and sorted albums
        List<AlbumCardDTO> albums = albumService.getAlbums(
                q, artist, genre, genreMode, subgenre, subgenreMode,
                language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
                country, countryMode, sortby, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = albumService.countAlbums(q, artist, genre, 
                genreMode, subgenre, subgenreMode, language, languageMode, gender, 
                genderMode, ethnicity, ethnicityMode, country, countryMode);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "albums");
        model.addAttribute("albums", albums);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedArtist", artist);
        model.addAttribute("selectedGenres", genre);
        model.addAttribute("genreMode", genreMode != null ? genreMode : "includes");
        model.addAttribute("selectedSubgenres", subgenre);
        model.addAttribute("subgenreMode", subgenreMode != null ? subgenreMode : "includes");
        model.addAttribute("selectedLanguages", language);
        model.addAttribute("languageMode", languageMode != null ? languageMode : "includes");
        model.addAttribute("selectedGenders", gender);
        model.addAttribute("genderMode", genderMode != null ? genderMode : "includes");
        model.addAttribute("selectedEthnicities", ethnicity);
        model.addAttribute("ethnicityMode", ethnicityMode != null ? ethnicityMode : "includes");
        model.addAttribute("selectedCountries", country);
        model.addAttribute("countryMode", countryMode != null ? countryMode : "includes");
        model.addAttribute("sortBy", sortby);
        
        // Add filter options
        model.addAttribute("genres", albumService.getGenres());
        model.addAttribute("subgenres", albumService.getSubGenres());
        model.addAttribute("languages", albumService.getLanguages());
        model.addAttribute("genders", albumService.getGenders());
        model.addAttribute("ethnicities", albumService.getEthnicities());
        model.addAttribute("countries", albumService.getCountries());
        
        return "albums/list";
    }
    
    @GetMapping("/{id}")
    public String viewAlbum(@PathVariable Integer id, Model model) {
        Optional<Album> album = albumService.getAlbumById(id);
        
        if (album.isEmpty()) {
            return "redirect:/albums";
        }
        
        // Check if album has an image
        byte[] image = albumService.getAlbumImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
        // Get song count
        int songCount = albumService.getSongCount(id);
        
        // Get artist name and gender
        String artistName = albumService.getArtistName(album.get().getArtistId());
        String artistGender = albumService.getArtistGender(album.get().getArtistId());
        
        model.addAttribute("currentSection", "albums");
        model.addAttribute("album", album.get());
        model.addAttribute("hasImage", hasImage);
        model.addAttribute("artistName", artistName);
        model.addAttribute("artistGender", artistGender);
        
        // Add lookup maps
        Map<Integer, String> genres = albumService.getGenres();
        Map<Integer, String> subgenres = albumService.getSubGenres();
        Map<Integer, String> languages = albumService.getLanguages();
        
        model.addAttribute("genres", genres);
        model.addAttribute("subgenres", subgenres);
        model.addAttribute("languages", languages);
        
        // Add effective (resolved) value names for display
        Album a = album.get();
        model.addAttribute("effectiveGenreName", a.getEffectiveGenreId() != null ? genres.get(a.getEffectiveGenreId()) : null);
        model.addAttribute("effectiveSubgenreName", a.getEffectiveSubgenreId() != null ? subgenres.get(a.getEffectiveSubgenreId()) : null);
        model.addAttribute("effectiveLanguageName", a.getEffectiveLanguageId() != null ? languages.get(a.getEffectiveLanguageId()) : null);
        
        // NEW: add album play count
        model.addAttribute("albumPlayCount", albumService.getPlayCountForAlbum(id));
        // Add per-account breakdown string for tooltip
        model.addAttribute("albumPlaysByAccount", albumService.getPlaysByAccountForAlbum(id));
        
        // Add songs list
        model.addAttribute("songs", albumService.getSongsForAlbum(id));
        model.addAttribute("songCount", songCount);
        
        // Add total listening time for the album
        model.addAttribute("totalListeningTime", albumService.getTotalListeningTimeForAlbum(id));
        
        // Add first and last listened dates for the album
        model.addAttribute("firstListenedDate", albumService.getFirstListenedDateForAlbum(id));
        model.addAttribute("lastListenedDate", albumService.getLastListenedDateForAlbum(id));
        
        return "albums/detail";
    }
    
    @PostMapping("/{id}")
    public String updateAlbum(@PathVariable Integer id, @ModelAttribute Album album) {
        album.setId(id);
        albumService.saveAlbum(album);
        return "redirect:/albums/" + id;
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getAlbumImage(@PathVariable Integer id) {
        return albumService.getAlbumImage(id);
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadAlbumImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            albumService.updateAlbumImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteAlbumImage(@PathVariable Integer id) {
        try {
            albumService.updateAlbumImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}")
    @ResponseBody
    public String deleteAlbum(@PathVariable Integer id) {
        try {
            albumService.deleteAlbum(id);
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
    public Map<String, Object> createAlbum(@RequestBody Album album) {
        try {
            Album created = albumService.createAlbum(album);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create album");
        }
    }
    
    @GetMapping("/api/albums")
    @ResponseBody
    public List<Map<String, Object>> getAlbumsByArtist(@RequestParam Integer artistId) {
        return albumService.getAlbumsByArtistForApi(artistId);
    }
}