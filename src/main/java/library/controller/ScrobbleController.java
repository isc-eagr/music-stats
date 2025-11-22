package library.controller;

import library.service.ScrobbleService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.opencsv.bean.CsvToBeanBuilder;

import library.entity.Scrobble;
import library.entity.Song;
import library.repository.SongRepository;
import library.repository.ScrobbleRepository;

@Controller
@RequestMapping("/scrobbles")
public class ScrobbleController {
    
    private final ScrobbleService scrobbleService;
    private final SongRepository songRepository;
    private final ScrobbleRepository scrobbleRepository;
    
    public ScrobbleController(ScrobbleService scrobbleService, SongRepository songRepository, ScrobbleRepository scrobbleRepository) {
        this.scrobbleService = scrobbleService;
        this.songRepository = songRepository;
        this.scrobbleRepository = scrobbleRepository;
    }
    
    /**
     * Endpoint to populate song_id for all scrobbles by matching with songs in the database.
     * This processes records in batches of 2000 for efficiency with large datasets.
     * 
     * POST /scrobbles/populate-song-ids
     * 
     * @return JSON response with statistics about the matching process
     */
    @PostMapping("/populate-song-ids")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> populateSongIds() {
        try {
            System.out.println("Starting scrobble song_id population process...");
            
            long startTime = System.currentTimeMillis();
            Map<String, Integer> stats = scrobbleService.populateSongIds();
            long endTime = System.currentTimeMillis();
            
            long durationSeconds = (endTime - startTime) / 1000;
            
            System.out.println(String.format(
                "Completed! Processed: %d, Matched: %d, Unmatched: %d, Duration: %d seconds",
                stats.get("totalProcessed"),
                stats.get("totalMatched"),
                stats.get("totalUnmatched"),
                durationSeconds
            ));
            
            Map<String, Object> response = Map.of(
                "success", true,
                "totalProcessed", stats.get("totalProcessed"),
                "totalMatched", stats.get("totalMatched"),
                "totalUnmatched", stats.get("totalUnmatched"),
                "durationSeconds", durationSeconds
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            System.err.println("Error populating song IDs: " + e.getMessage());
            e.printStackTrace();
            
            Map<String, Object> errorResponse = Map.of(
                "success", false,
                "error", e.getMessage()
            );
            
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Legacy insertScrobbles behavior ported with batching (commits every batchSize records).
     * Saves in chunks to avoid large single transactions and memory spikes.
     */
    @RequestMapping("/insertScrobbles")
    @ResponseBody
    public String insertScrobbles() throws FileNotFoundException, IOException {
        List<String> accounts = List.of("vatito", "robertlover");
        String result = "";
        final int batchSize = 2000; // commit every 2000 records

        for (String account : accounts) {
            String fileName = String.format("C:\\scrobbles-%s.csv", account);
            FileReader file = new FileReader(fileName);

            List<Scrobble> scrobbles = new CsvToBeanBuilder<Scrobble>(file).withType(Scrobble.class).withSkipLines(1)
                    .build().parse();

            List<Song> everySong = songRepository.findAll();

            Song emptySong = new Song();
            List<Song> duplicateSongs = Collections.synchronizedList(new ArrayList<>()); // thread-safe collection

            // Process scrobbles in batches to commit every batchSize
            int total = scrobbles.size();
            for (int i = 0; i < total; i += batchSize) {
                int end = Math.min(total, i + batchSize);
                List<Scrobble> chunk = scrobbles.subList(i, end);

                // perform matching for the chunk (parallel for speed)
                chunk.stream().parallel().forEach(sc -> {
                    List<Song> foundSongs = everySong.stream().parallel()
                            .filter(song -> song.getArtist().equalsIgnoreCase(sc.getArtist())
                                    && song.getSong().replace("????", "").equalsIgnoreCase(sc.getSong().replace("????", ""))
                                    && String.valueOf(song.getAlbum()).equalsIgnoreCase(String.valueOf(sc.getAlbum())))
                            .toList();

                    if (foundSongs.size() > 1) {
                        duplicateSongs.add(foundSongs.get(0));
                    }

                    sc.setSongId(foundSongs.stream().findFirst().orElse(emptySong).getId());
                    sc.setAlbum(sc.getAlbum() == null || sc.getAlbum().isBlank() ? null : sc.getAlbum());
                    sc.setAccount(account);
                });

                // save this chunk so we commit every batchSize
                scrobbleRepository.saveAll(new ArrayList<>(chunk));
                System.out.println(String.format("Saved chunk %d - %d for account %s", i, end - 1, account));
            }

            // Print duplicates (distinct)
            duplicateSongs.stream().map(s -> s.getArtist() + " - " + s.getAlbum() + " - " + s.getSong()).distinct()
                    .forEach(System.out::println);

            file.close();

            result += "Success!" + scrobbles.size() + " scrobbles inserted from " + account + " account!<br/>";
        }

        return result;
    }

    // New file upload UI
    @GetMapping("/upload")
    public String showUploadForm(Model model) {
        model.addAttribute("accounts", List.of("vatito", "robertlover"));
        return "insertScrobblesForm";
    }

    // Handle the uploaded file and stream-import
    @PostMapping("/upload")
    public String handleUpload(@RequestParam("file") MultipartFile file,
                               @RequestParam(value = "account", required = false) String account,
                               @RequestParam(value = "batchSize", required = false, defaultValue = "2000") int batchSize,
                               @RequestParam(value = "dryRun", required = false, defaultValue = "false") boolean dryRun,
                               Model model) {
        if (account == null || account.isBlank()) account = "uploaded";
        try {
            Map<String, Integer> stats = scrobbleService.importScrobblesStream(file, account, batchSize, dryRun);
            model.addAttribute("stats", stats);
            model.addAttribute("dryRun", dryRun);
            model.addAttribute("account", account);
            model.addAttribute("batchSize", batchSize);
            return "insertScrobblesResult";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            return "insertScrobblesResult";
        }
    }

    /**
     * Confirmation page for running the populate process from the UI.
     */
    @GetMapping("/populate")
    public String showPopulateConfirm(Model model) {
        // Provide a small form with an option for dryRun
        model.addAttribute("defaultBatchSize", 2000);
        return "populateConfirm";
    }

    /**
     * Run the populate process and show results in a page.
     */
    @PostMapping("/populate")
    public String runPopulate(@RequestParam(value = "batchSize", required = false, defaultValue = "2000") int batchSize,
                              @RequestParam(value = "dryRun", required = false, defaultValue = "false") boolean dryRun,
                              Model model) {
        // Note: current service ignores supplied batchSize; keeping parameter for future extension
        try {
            long start = System.currentTimeMillis();
            Map<String, Integer> stats = scrobbleService.populateSongIds();
            long duration = (System.currentTimeMillis() - start) / 1000;
            model.addAttribute("stats", stats);
            model.addAttribute("durationSeconds", duration);
            model.addAttribute("batchSize", batchSize);
            model.addAttribute("dryRun", dryRun);
            return "populateResult";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            return "populateResult";
        }
    }

    /**
     * Page that shows unmatched scrobbles grouped by artist/album/song with counts.
     * Supports optional account filter via ?account=<accountName>
     */
    @GetMapping("/unmatched")
    public String showUnmatchedScrobbles(@RequestParam(value = "account", required = false) String account,
                                         Model model) {
        // Fetch list of distinct accounts for the UI
        java.util.List<String> accounts = scrobbleService.getDistinctAccounts();

        // Interpret empty string as "no filter" (show all)
        String serviceAccount;
        if (account != null && account.equals("")) {
            serviceAccount = null; // no filter
        } else {
            serviceAccount = account; // can be null, '__BLANK__', or actual account name
        }

        // Fetch unmatched rows filtered by account (if provided)
        java.util.List<java.util.Map<String, Object>> rows = scrobbleService.getUnmatchedScrobbles(serviceAccount);

        model.addAttribute("unmatched", rows);
        model.addAttribute("accounts", accounts);
        model.addAttribute("selectedAccount", account == null ? "" : account);
        return "unmatchedScrobbles";
    }
}