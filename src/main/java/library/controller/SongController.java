package library.controller;

import library.dto.SongCardDTO;
import library.entity.SongNew;
import library.service.SongService;
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
@RequestMapping("/songs")
public class SongController {
    
    private final SongService songService;
    
    public SongController(SongService songService) {
        this.songService = songService;
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
    public String listSongs(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String album,
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
        
        // Get filtered and sorted songs
        List<SongCardDTO> songs = songService.getSongs(
                q, artist, album, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, gender, genderMode,
                ethnicity, ethnicityMode, country, countryMode, sortby, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = songService.countSongs(q, artist, album, 
                genre, genreMode, subgenre, subgenreMode, language, languageMode,
                gender, genderMode, ethnicity, ethnicityMode, country, countryMode);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "songs");
        model.addAttribute("songs", songs);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedArtist", artist);
        model.addAttribute("selectedAlbum", album);
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
        model.addAttribute("genres", songService.getGenres());
        model.addAttribute("subgenres", songService.getSubGenres());
        model.addAttribute("languages", songService.getLanguages());
        model.addAttribute("genders", songService.getGenders());
        model.addAttribute("ethnicities", songService.getEthnicities());
        model.addAttribute("countries", songService.getCountries());
        
        return "songs/list";
    }
    
    @GetMapping("/{id}")
    public String viewSong(@PathVariable Integer id, Model model) {
        Optional<SongNew> song = songService.getSongById(id);
        
        if (song.isEmpty()) {
            return "redirect:/songs";
        }
        
        // Check if song has an image
        byte[] image = songService.getSongImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
        // Get artist and album names
        String artistName = songService.getArtistName(song.get().getArtistId());
        String artistGender = songService.getArtistGender(song.get().getArtistId());
        String albumName = song.get().getAlbumId() != null ? 
                          songService.getAlbumName(song.get().getAlbumId()) : null;
        
        model.addAttribute("currentSection", "songs");
        model.addAttribute("song", song.get());
        model.addAttribute("hasImage", hasImage);
        model.addAttribute("artistName", artistName);
        model.addAttribute("artistGender", artistGender);
        model.addAttribute("albumName", albumName);
        
        // NEW: add song play count
        model.addAttribute("songPlayCount", songService.getPlayCountForSong(id));
        // Add per-account breakdown string for tooltip
        model.addAttribute("songPlaysByAccount", songService.getPlaysByAccountForSong(id));
        
        // Add statistics for the song
        model.addAttribute("totalListeningTime", songService.getTotalListeningTimeForSong(id));
        model.addAttribute("firstListenedDate", songService.getFirstListenedDateForSong(id));
        model.addAttribute("lastListenedDate", songService.getLastListenedDateForSong(id));
        
        // Add lookup maps
        Map<Integer, String> genres = songService.getGenres();
        Map<Integer, String> subgenres = songService.getSubGenres();
        Map<Integer, String> languages = songService.getLanguages();
        Map<Integer, String> genders = songService.getGenders();
        Map<Integer, String> ethnicities = songService.getEthnicities();
        
        model.addAttribute("genres", genres);
        model.addAttribute("subgenres", subgenres);
        model.addAttribute("languages", languages);
        model.addAttribute("genders", genders);
        model.addAttribute("ethnicities", ethnicities);
        
        // Add effective (resolved) value names for display
        SongNew s = song.get();
        model.addAttribute("effectiveGenreName", s.getEffectiveGenreId() != null ? genres.get(s.getEffectiveGenreId()) : null);
        model.addAttribute("effectiveSubgenreName", s.getEffectiveSubgenreId() != null ? subgenres.get(s.getEffectiveSubgenreId()) : null);
        model.addAttribute("effectiveLanguageName", s.getEffectiveLanguageId() != null ? languages.get(s.getEffectiveLanguageId()) : null);
        model.addAttribute("effectiveGenderName", s.getEffectiveGenderId() != null ? genders.get(s.getEffectiveGenderId()) : null);
        model.addAttribute("effectiveEthnicityName", s.getEffectiveEthnicityId() != null ? ethnicities.get(s.getEffectiveEthnicityId()) : null);
        
        return "songs/detail";
    }
    
    @PostMapping("/{id}")
    public String updateSong(@PathVariable Integer id, @ModelAttribute SongNew song) {
        song.setId(id);
        songService.saveSong(song);
        return "redirect:/songs/" + id;
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getSongImage(@PathVariable Integer id) {
        return songService.getSongImage(id);
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadSongImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            songService.updateSongImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteSongImage(@PathVariable Integer id) {
        try {
            songService.updateSongImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}")
    @ResponseBody
    public String deleteSong(@PathVariable Integer id) {
        try {
            songService.deleteSong(id);
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
    public Map<String, Object> createSong(@RequestBody SongNew song) {
        try {
            SongNew created = songService.createSong(song);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create song");
        }
    }
}