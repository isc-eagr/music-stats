package library.controller;

import library.dto.FeaturedArtistDTO;
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String organized,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Convert date formats from dd/mm/yyyy to yyyy-MM-dd for database queries
        String releaseDateConverted = convertDateFormat(releaseDate);
        String releaseDateFromConverted = convertDateFormat(releaseDateFrom);
        String releaseDateToConverted = convertDateFormat(releaseDateTo);
        String firstListenedDateConverted = convertDateFormat(firstListenedDate);
        String firstListenedDateFromConverted = convertDateFormat(firstListenedDateFrom);
        String firstListenedDateToConverted = convertDateFormat(firstListenedDateTo);
        String lastListenedDateConverted = convertDateFormat(lastListenedDate);
        String lastListenedDateFromConverted = convertDateFormat(lastListenedDateFrom);
        String lastListenedDateToConverted = convertDateFormat(lastListenedDateTo);
        
        // Get filtered and sorted songs
        List<SongCardDTO> songs = songService.getSongs(
                q, artist, album, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, gender, genderMode,
                ethnicity, ethnicityMode, country, countryMode, organized,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                sortby, sortdir, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = songService.countSongs(q, artist, album, 
                genre, genreMode, subgenre, subgenreMode, language, languageMode,
                gender, genderMode, ethnicity, ethnicityMode, country, countryMode, organized,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode);
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
        model.addAttribute("selectedOrganized", organized);
        model.addAttribute("releaseDate", releaseDate);
        model.addAttribute("releaseDateFrom", releaseDateFrom);
        model.addAttribute("releaseDateTo", releaseDateTo);
        model.addAttribute("releaseDateMode", releaseDateMode != null ? releaseDateMode : "exact");
        // Add formatted dates for display
        model.addAttribute("releaseDateFormatted", formatDateForDisplay(releaseDate));
        model.addAttribute("releaseDateFromFormatted", formatDateForDisplay(releaseDateFrom));
        model.addAttribute("releaseDateToFormatted", formatDateForDisplay(releaseDateTo));
        
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
        model.addAttribute("genres", songService.getGenres());
        model.addAttribute("subgenres", songService.getSubGenres());
        model.addAttribute("languages", songService.getLanguages());
        model.addAttribute("genders", songService.getGenders());
        model.addAttribute("ethnicities", songService.getEthnicities());
        model.addAttribute("countries", songService.getCountries());
        
        return "songs/list";
    }
    
    @GetMapping("/{id}")
    public String viewSong(@PathVariable Integer id, 
                          @RequestParam(defaultValue = "general") String tab,
                          @RequestParam(defaultValue = "0") int playsPage,
                          Model model) {
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
        String artistCountry = songService.getArtistCountry(song.get().getArtistId());
        String albumName = song.get().getAlbumId() != null ? 
                          songService.getAlbumName(song.get().getAlbumId()) : null;
        
        model.addAttribute("currentSection", "songs");
        model.addAttribute("song", song.get());
        model.addAttribute("hasImage", hasImage);
        model.addAttribute("artistName", artistName);
        model.addAttribute("artistGender", artistGender);
        model.addAttribute("artistCountry", artistCountry);
        model.addAttribute("albumName", albumName);
        
        // NEW: add song play count
        model.addAttribute("songPlayCount", songService.getPlayCountForSong(id));
        model.addAttribute("songVatitoPlayCount", songService.getVatitoPlayCountForSong(id));
        model.addAttribute("songRobertloverPlayCount", songService.getRobertloverPlayCountForSong(id));
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
        
        // Tab and plays data
        model.addAttribute("activeTab", tab);
        
        // Add featured artists for this song (for editing)
        model.addAttribute("featuredArtists", songService.getFeaturedArtistsForSong(id));
        
        // Add featured artist cards (for the Featured Artists tab)
        if ("featured".equals(tab)) {
            model.addAttribute("featuredArtistCards", songService.getFeaturedArtistCardsForSong(id));
        }
        
        if ("plays".equals(tab)) {
            int pageSize = 100;
            model.addAttribute("scrobbles", songService.getScrobblesForSong(id, playsPage, pageSize));
            model.addAttribute("scrobblesTotalCount", songService.countScrobblesForSong(id));
            model.addAttribute("scrobblesPage", playsPage);
            model.addAttribute("scrobblesPageSize", pageSize);
            model.addAttribute("scrobblesTotalPages", (int) Math.ceil((double) songService.countScrobblesForSong(id) / pageSize));
            model.addAttribute("playsByYear", songService.getPlaysByYearForSong(id));
        }
        
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
    
    // API endpoint for General tab chart data (pie charts)
    @GetMapping("/api/charts/general")
    @ResponseBody
    public Map<String, Object> getGeneralChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getGeneralChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Genre tab chart data (bar charts)
    @GetMapping("/api/charts/genre")
    @ResponseBody
    public Map<String, Object> getGenreChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getGenreChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Subgenre tab chart data (bar charts)
    @GetMapping("/api/charts/subgenre")
    @ResponseBody
    public Map<String, Object> getSubgenreChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getSubgenreChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Ethnicity tab chart data (bar charts)
    @GetMapping("/api/charts/ethnicity")
    @ResponseBody
    public Map<String, Object> getEthnicityChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getEthnicityChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Language tab chart data (bar charts)
    @GetMapping("/api/charts/language")
    @ResponseBody
    public Map<String, Object> getLanguageChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getLanguageChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Country tab chart data (bar charts)
    @GetMapping("/api/charts/country")
    @ResponseBody
    public Map<String, Object> getCountryChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getCountryChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Release Year tab chart data (bar charts)
    @GetMapping("/api/charts/releaseYear")
    @ResponseBody
    public Map<String, Object> getReleaseYearChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getReleaseYearChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Listen Year tab chart data (bar charts)
    @GetMapping("/api/charts/listenYear")
    @ResponseBody
    public Map<String, Object> getListenYearChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getListenYearChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
    }
    
    // API endpoint for Top tab data (artists, albums, songs tables)
    @GetMapping("/api/charts/top")
    @ResponseBody
    public Map<String, Object> getTopChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(defaultValue = "10") int limit) {
        
        return songService.getTopChartData(
            q, artist, album,
            genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode,
            ethnicity, ethnicityMode, country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo,
            limit
        );
    }
    
    // Legacy API endpoint for filtered gender breakdown chart data (kept for backwards compatibility)
    @GetMapping("/api/charts/gender")
    @ResponseBody
    public Map<String, Object> getFilteredGenderChartData(
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo) {
        
        return songService.getFilteredChartData(
            q, artist, album,
            genre, genreMode,
            subgenre, subgenreMode,
            language, languageMode,
            gender, genderMode,
            ethnicity, ethnicityMode,
            country, countryMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            listenedDateFrom, listenedDateTo
        );
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
    
    // ============================================
    // Featured Artists API Endpoints
    // ============================================
    
    /**
     * Search artists by name for the featured artists autocomplete
     */
    @GetMapping("/api/artists/search")
    @ResponseBody
    public List<FeaturedArtistDTO> searchArtists(@RequestParam String q) {
        return songService.searchArtists(q, 10);
    }
    
    /**
     * Get featured artists for a song
     */
    @GetMapping("/{id}/featured-artists")
    @ResponseBody
    public List<FeaturedArtistDTO> getFeaturedArtists(@PathVariable Integer id) {
        return songService.getFeaturedArtistsForSong(id);
    }
    
    /**
     * Save featured artists for a song
     */
    @PostMapping("/{id}/featured-artists")
    @ResponseBody
    public String saveFeaturedArtists(@PathVariable Integer id, @RequestBody List<Integer> artistIds) {
        try {
            songService.saveFeaturedArtists(id, artistIds);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    /**
     * Search songs by name for the unmatched scrobbles assignment
     */
    @GetMapping("/api/search")
    @ResponseBody
    public List<Map<String, Object>> searchSongsForApi(
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String song,
            @RequestParam(required = false, defaultValue = "0") int limit) {
        return songService.searchSongs(artist, song, limit);
    }
}