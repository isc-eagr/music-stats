package library.controller;

import library.dto.ChartFilterDTO;
import library.dto.FeaturedArtistDTO;
import library.dto.SongCardDTO;
import library.entity.Album;
import library.entity.Artist;
import library.entity.Song;
import library.repository.LookupRepository;
import library.service.AlbumService;
import library.service.ArtistService;
import library.service.ChartService;
import library.service.SongService;
import library.service.iTunesLibraryService;
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
    private final ChartService chartService;
    private final ArtistService artistService;
    private final AlbumService albumService;
    private final iTunesLibraryService iTunesLibraryService;
    private final LookupRepository lookupRepository;

    public SongController(SongService songService, ChartService chartService, ArtistService artistService, 
                         AlbumService albumService, iTunesLibraryService iTunesLibraryService, LookupRepository lookupRepository) {
        this.songService = songService;
        this.chartService = chartService;
        this.artistService = artistService;
        this.albumService = albumService;
        this.iTunesLibraryService = iTunesLibraryService;
        this.lookupRepository = lookupRepository;
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
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
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) String hasImage,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) Integer lengthMin,
            @RequestParam(required = false) Integer lengthMax,
            @RequestParam(required = false) String lengthMode,
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
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
        String listenedDateFromConverted = convertDateFormat(listenedDateFrom);
        String listenedDateToConverted = convertDateFormat(listenedDateTo);
        
        // Get filtered and sorted songs
        List<SongCardDTO> songs = songService.getSongs(
                q, artist, album, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, gender, genderMode,
                ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, hasImage, hasFeaturedArtists, isBand, isSingle,
                playCountMin, playCountMax,
                lengthMin, lengthMax, lengthMode,
                sortby, sortdir, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = songService.countSongs(q, artist, album, 
                genre, genreMode, subgenre, subgenreMode, language, languageMode,
                gender, genderMode, ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, hasImage, hasFeaturedArtists, isBand, isSingle,
                playCountMin, playCountMax,
                lengthMin, lengthMax, lengthMode);
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
        model.addAttribute("selectedAccounts", account);
        model.addAttribute("accountMode", accountMode != null ? accountMode : "includes");
        model.addAttribute("selectedOrganized", organized);
        model.addAttribute("selectedHasImage", hasImage);
        model.addAttribute("selectedHasFeaturedArtists", hasFeaturedArtists);
        model.addAttribute("selectedIsBand", isBand);
        model.addAttribute("selectedIsSingle", isSingle);
        model.addAttribute("playCountMin", playCountMin);
        model.addAttribute("playCountMax", playCountMax);
        model.addAttribute("lengthMin", lengthMin);
        model.addAttribute("lengthMax", lengthMax);
        model.addAttribute("lengthMode", lengthMode != null ? lengthMode : "range");
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
        
        // Listened date filter attributes (filters by actual scrobble date)
        model.addAttribute("listenedDateFrom", listenedDateFrom);
        model.addAttribute("listenedDateTo", listenedDateTo);
        model.addAttribute("listenedDateFromFormatted", formatDateForDisplay(listenedDateFrom));
        model.addAttribute("listenedDateToFormatted", formatDateForDisplay(listenedDateTo));
        
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
        Optional<Song> song = songService.getSongById(id);
        
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
        
        // Add artist and album entities for ranking chips
        Artist artist = artistService.findById(song.get().getArtistId());
        model.addAttribute("artist", artist);
        Album album = song.get().getAlbumId() != null ? 
                      albumService.getAlbumById(song.get().getAlbumId()).orElse(null) : null;
        model.addAttribute("album", album);
        
        // Add album release date for inheritance display
        String albumReleaseDate = (album != null && album.getReleaseDateFormatted() != null) ? 
                                  album.getReleaseDateFormatted() : null;
        model.addAttribute("albumReleaseDate", albumReleaseDate);
        
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
        
        // Add lookup maps for ranking chips
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
        Song s = song.get();
        model.addAttribute("effectiveGenreName", s.getEffectiveGenreId() != null ? genres.get(s.getEffectiveGenreId()) : null);
        model.addAttribute("effectiveSubgenreName", s.getEffectiveSubgenreId() != null ? subgenres.get(s.getEffectiveSubgenreId()) : null);
        model.addAttribute("effectiveLanguageName", s.getEffectiveLanguageId() != null ? languages.get(s.getEffectiveLanguageId()) : null);
        model.addAttribute("effectiveGenderName", s.getEffectiveGenderId() != null ? genders.get(s.getEffectiveGenderId()) : null);
        model.addAttribute("effectiveEthnicityName", s.getEffectiveEthnicityId() != null ? ethnicities.get(s.getEffectiveEthnicityId()) : null);
        
        // Add inherited value names (what would be used if no song override) for dropdown "Inherit" options
        Integer inheritedGenreId = s.getAlbumGenreId() != null ? s.getAlbumGenreId() : s.getArtistGenreId();
        Integer inheritedSubgenreId = s.getAlbumSubgenreId() != null ? s.getAlbumSubgenreId() : s.getArtistSubgenreId();
        Integer inheritedLanguageId = s.getAlbumLanguageId() != null ? s.getAlbumLanguageId() : s.getArtistLanguageId();
        Integer inheritedGenderId = s.getArtistGenderId();
        Integer inheritedEthnicityId = s.getArtistEthnicityId();
        
        model.addAttribute("inheritedGenreName", inheritedGenreId != null ? genres.get(inheritedGenreId) : null);
        model.addAttribute("inheritedSubgenreName", inheritedSubgenreId != null ? subgenres.get(inheritedSubgenreId) : null);
        model.addAttribute("inheritedLanguageName", inheritedLanguageId != null ? languages.get(inheritedLanguageId) : null);
        model.addAttribute("inheritedGenderName", inheritedGenderId != null ? genders.get(inheritedGenderId) : null);
        model.addAttribute("inheritedEthnicityName", inheritedEthnicityId != null ? ethnicities.get(inheritedEthnicityId) : null);
        
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
        
        // Always load seasonal/yearly chart history for sidebar chips
        model.addAttribute("seasonalChartHistory", chartService.getChartHistoryForItem(id, "song", "seasonal"));
        model.addAttribute("yearlyChartHistory", chartService.getChartHistoryForItem(id, "song", "yearly"));
        
        // Add weekly chart history for the Chart History tab
        if ("chart-history".equals(tab)) {
            model.addAttribute("chartHistory", chartService.getSongChartHistory(id));
        }
        
        // Add ranking chips data - optimized single query
        java.util.Map<String, Integer> rankings = songService.getAllSongRankings(id);
        model.addAttribute("rankByGender", rankings.get("gender"));
        model.addAttribute("rankByGenre", rankings.get("genre"));
        model.addAttribute("rankBySubgenre", rankings.get("subgenre"));
        model.addAttribute("rankByEthnicity", rankings.get("ethnicity"));
        model.addAttribute("rankByLanguage", rankings.get("language"));
        model.addAttribute("rankByCountry", rankings.get("country"));
        model.addAttribute("ranksByYear", songService.getSongRanksByYear(id));
        
        // Add Spanish Rap rank (special combination)
        if (songService.isSongSpanishRap(id)) {
            model.addAttribute("rankBySpanishRap", songService.getSongSpanishRapRank(id));
            model.addAttribute("rapGenreId", lookupRepository.getGenreIdByName("Rap"));
            model.addAttribute("spanishLanguageId", lookupRepository.getLanguageIdByName("Spanish"));
        }

        // Add new ranking chips
        model.addAttribute("weeklyChartStats", chartService.getSongWeeklyChartStats(id));
        model.addAttribute("overallPosition", songService.getSongOverallPosition(id));
        model.addAttribute("rankByReleaseYear", songService.getSongRankByReleaseYear(id));
        model.addAttribute("releaseYear", songService.getSongReleaseYear(id));
        model.addAttribute("rankByArtist", songService.getSongRankByArtist(id));
        model.addAttribute("rankByAlbum", songService.getSongRankByAlbum(id));

        return "songs/detail";
    }
    
    @PostMapping("/{id}")
    public String updateSong(@PathVariable Integer id, @ModelAttribute Song song) {
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
    
    // Gallery endpoints for secondary images
    @GetMapping("/{id}/images")
    @ResponseBody
    public List<Map<String, Object>> getSongImages(@PathVariable Integer id) {
        var images = songService.getSecondaryImages(id);
        return images.stream()
                .map(img -> Map.<String, Object>of(
                        "id", img.getId(),
                        "displayOrder", img.getDisplayOrder() != null ? img.getDisplayOrder() : 0
                ))
                .toList();
    }

    @GetMapping("/{id}/images/{imageId}")
    @ResponseBody
    public byte[] getSecondaryImage(@PathVariable Integer id, @PathVariable Integer imageId) {
        return songService.getSecondaryImage(imageId);
    }

    @PostMapping("/{id}/images")
    @ResponseBody
    public String addSecondaryImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            songService.addSecondaryImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }

    @DeleteMapping("/{id}/images/{imageId}")
    @ResponseBody
    public String deleteSecondaryImage(@PathVariable Integer id, @PathVariable Integer imageId) {
        try {
            songService.deleteSecondaryImage(imageId);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }

    @PostMapping("/{id}/images/{imageId}/set-default")
    @ResponseBody
    public String setImageAsDefault(@PathVariable Integer id, @PathVariable Integer imageId) {
        try {
            songService.swapToDefault(id, imageId);
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
    
    /**
     * Fetch release date and song length from iTunes XML for a specific song.
     * Returns JSON with releaseDate (YYYY-MM-DD) and lengthSeconds.
     */
    @PostMapping("/{id}/fetch-itunes-data")
    @ResponseBody
    public Map<String, Object> fetchItunesData(@PathVariable Integer id) {
        Map<String, Object> response = new java.util.HashMap<>();
        
        try {
            // Get the song details
            Optional<Song> songOpt = songService.getSongById(id);
            if (!songOpt.isPresent()) {
                response.put("success", false);
                response.put("message", "Song not found");
                return response;
            }
            
            Song song = songOpt.get();
            
            // Get artist and album names
            String artistName = null;
            String albumName = null;
            
            if (song.getArtistId() != null) {
                Optional<Artist> artistOpt = artistService.getArtistById(song.getArtistId());
                if (artistOpt.isPresent()) {
                    artistName = artistOpt.get().getName();
                }
            }
            
            if (song.getAlbumId() != null) {
                Optional<Album> albumOpt = albumService.getAlbumById(song.getAlbumId());
                if (albumOpt.isPresent()) {
                    albumName = albumOpt.get().getName();
                }
            }
            
            // Look up in iTunes XML
            iTunesLibraryService.iTunesTrackData trackData = 
                iTunesLibraryService.findTrackData(song.getName(), artistName, albumName);
            
            if (trackData != null) {
                response.put("success", true);
                response.put("releaseDate", trackData.releaseDate);
                response.put("lengthSeconds", trackData.lengthSeconds);
                
                // Format length as mm:ss for display
                if (trackData.lengthSeconds != null) {
                    int minutes = trackData.lengthSeconds / 60;
                    int seconds = trackData.lengthSeconds % 60;
                    response.put("lengthFormatted", String.format("%d:%02d", minutes, seconds));
                }
            } else {
                response.put("success", false);
                response.put("message", "Song not found in iTunes library");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
        }
        
        return response;
    }

    /**
     * Fetch release date and song length from iTunes XML by artist/album/song names.
     * Accepts JSON body: { "songName": "...", "artistName": "...", "albumName": "..." }
     */
    @PostMapping("/fetch-itunes-by-fields")
    @ResponseBody
    public Map<String, Object> fetchItunesByFields(@RequestBody Map<String, String> payload) {
        Map<String, Object> response = new java.util.HashMap<>();

        try {
            String songName = payload.get("songName");
            String artistName = payload.get("artistName");
            String albumName = payload.get("albumName");

            if ((songName == null || songName.trim().isEmpty()) && (artistName == null || artistName.trim().isEmpty())) {
                response.put("success", false);
                response.put("message", "Missing song or artist name");
                return response;
            }

            iTunesLibraryService.iTunesTrackData trackData =
                    iTunesLibraryService.findTrackData(songName, artistName, albumName);

            if (trackData != null) {
                // Determine whether we should populate the song's release date
                String albumReleaseDate = null;
                if (albumName != null && artistName != null && !albumName.trim().isEmpty() && !artistName.trim().isEmpty()) {
                    albumReleaseDate = albumService.findAlbumReleaseDateByNameAndArtist(albumName, artistName);
                }

                boolean populateReleaseDate = false;
                if (albumReleaseDate == null || albumReleaseDate.trim().isEmpty()) {
                    // No album release date -> safe to populate
                    populateReleaseDate = true;
                } else if (trackData.releaseDate != null && !trackData.releaseDate.equals(albumReleaseDate)) {
                    // Only populate if different from album release date
                    populateReleaseDate = true;
                }

                response.put("success", true);
                response.put("releaseDate", trackData.releaseDate);
                response.put("lengthSeconds", trackData.lengthSeconds);
                response.put("populateReleaseDate", populateReleaseDate);
                if (trackData.lengthSeconds != null) {
                    int minutes = trackData.lengthSeconds / 60;
                    int seconds = trackData.lengthSeconds % 60;
                    response.put("lengthFormatted", String.format("%d:%02d", minutes, seconds));
                }
            } else {
                response.put("success", false);
                response.put("message", "Song not found in iTunes library");
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
        }

        return response;
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createSong(@RequestBody Song song) {
        try {
            Song created = songService.createSong(song);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create song");
        }
    }
    
    /**
     * Helper method to build ChartFilterDTO from request parameters.
     * Centralizes all filter parameter handling for chart endpoints.
     */
    private ChartFilterDTO buildChartFilter(
            String q, String artist, String album,
            List<Integer> genre, String genreMode,
            List<Integer> subgenre, String subgenreMode,
            List<Integer> language, String languageMode,
            List<Integer> gender, String genderMode,
            List<Integer> ethnicity, String ethnicityMode,
            List<String> country, String countryMode,
            List<String> account, String accountMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode, String releaseDateEntity,
            String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode, String firstListenedDateEntity,
            String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode, String lastListenedDateEntity,
            String listenedDateFrom, String listenedDateTo,
            Integer playCountMin, Integer playCountMax, String playCountEntity,
            String hasFeaturedArtists, String isBand, String isSingle,
            Integer limit) {
        
        return ChartFilterDTO.builder()
            .setName(q)
            .setArtistName(artist)
            .setAlbumName(album)
            .setGenreIds(genre)
            .setGenreMode(genreMode)
            .setSubgenreIds(subgenre)
            .setSubgenreMode(subgenreMode)
            .setLanguageIds(language)
            .setLanguageMode(languageMode)
            .setGenderIds(gender)
            .setGenderMode(genderMode)
            .setEthnicityIds(ethnicity)
            .setEthnicityMode(ethnicityMode)
            .setCountries(country)
            .setCountryMode(countryMode)
            .setAccounts(account)
            .setAccountMode(accountMode)
            .setReleaseDate(convertDateFormat(releaseDate))
            .setReleaseDateFrom(convertDateFormat(releaseDateFrom))
            .setReleaseDateTo(convertDateFormat(releaseDateTo))
            .setReleaseDateMode(releaseDateMode)
            .setReleaseDateEntity(releaseDateEntity)
            .setFirstListenedDate(convertDateFormat(firstListenedDate))
            .setFirstListenedDateFrom(convertDateFormat(firstListenedDateFrom))
            .setFirstListenedDateTo(convertDateFormat(firstListenedDateTo))
            .setFirstListenedDateMode(firstListenedDateMode)
            .setFirstListenedDateEntity(firstListenedDateEntity)
            .setLastListenedDate(convertDateFormat(lastListenedDate))
            .setLastListenedDateFrom(convertDateFormat(lastListenedDateFrom))
            .setLastListenedDateTo(convertDateFormat(lastListenedDateTo))
            .setLastListenedDateMode(lastListenedDateMode)
            .setLastListenedDateEntity(lastListenedDateEntity)
            .setListenedDateFrom(convertDateFormat(listenedDateFrom))
            .setListenedDateTo(convertDateFormat(listenedDateTo))
            .setPlayCountMin(playCountMin)
            .setPlayCountMax(playCountMax)
            .setPlayCountEntity(playCountEntity)
            .setHasFeaturedArtists(hasFeaturedArtists)
            .setIsBand(isBand)
            .setIsSingle(isSingle)
            .setTopLimit(limit);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit,
            @RequestParam(required = false) String limitEntity) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        // For General tab, the limit should apply to the selected entity for scrobble-derived metrics
        filter.setLimitEntity(limitEntity);

        return songService.getGeneralChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        return songService.getGenreChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        return songService.getSubgenreChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        return songService.getEthnicityChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        return songService.getLanguageChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        return songService.getCountryChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        return songService.getReleaseYearChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);

        return songService.getListenYearChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "10") int limit) {
        
        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, limit);
        
        // Set include toggles for top artists
        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        
        return songService.getTopChartData(filter);
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle) {
        
        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode, releaseDateEntity,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, null);
        
        return songService.getFilteredChartData(filter);
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
    
    /**
     * Get all songs matching the current filters for playlist export (no pagination)
     * Returns minimal data needed for iTunes validation: id, name, artist, album
     */
    @GetMapping("/api/export")
    @ResponseBody
    public List<Map<String, Object>> getFilteredSongsForExport(
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
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
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) String hasImage,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(defaultValue = "10000") int limit) {
        
        // Convert date formats
        String releaseDateConverted = convertDateFormat(releaseDate);
        String releaseDateFromConverted = convertDateFormat(releaseDateFrom);
        String releaseDateToConverted = convertDateFormat(releaseDateTo);
        String firstListenedDateConverted = convertDateFormat(firstListenedDate);
        String firstListenedDateFromConverted = convertDateFormat(firstListenedDateFrom);
        String firstListenedDateToConverted = convertDateFormat(firstListenedDateTo);
        String lastListenedDateConverted = convertDateFormat(lastListenedDate);
        String lastListenedDateFromConverted = convertDateFormat(lastListenedDateFrom);
        String lastListenedDateToConverted = convertDateFormat(lastListenedDateTo);
        String listenedDateFromConverted = convertDateFormat(listenedDateFrom);
        String listenedDateToConverted = convertDateFormat(listenedDateTo);
        
        // Get all songs matching filters (using a large limit instead of pagination)
        List<SongCardDTO> songs = songService.getSongs(
                q, artist, album, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, gender, genderMode,
                ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, hasImage, hasFeaturedArtists, isBand, isSingle,
                playCountMin, playCountMax,
                null, null, null,           // lengthMin, lengthMax, lengthMode (not used in export)
                sortby, sortdir, 0, limit
        );
        
        // Convert to minimal export format
        return songs.stream().map(song -> {
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("id", song.getId());
            map.put("name", song.getName());
            map.put("artist", song.getArtistName());
            map.put("album", song.getAlbumName());
            return map;
        }).toList();
    }
}

