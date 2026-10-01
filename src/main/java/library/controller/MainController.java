package library.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RequestParam;

import library.dto.GlobalSearchResultDTO;
import library.repository.SongRepositoryImpl;
import library.service.GlobalSearchService;

import java.util.List;

@Controller
public class MainController {

	@Autowired
	private SongRepositoryImpl songRepositoryImpl;

	@Autowired
	private GlobalSearchService globalSearchService;

	
	@RequestMapping("/")
	public String index(Model model) {
		// Add overall statistics
		model.addAttribute("totalArtists", songRepositoryImpl.getTotalArtistsCount());
		model.addAttribute("totalAlbums", songRepositoryImpl.getTotalAlbumsCount());
		model.addAttribute("totalSongs", songRepositoryImpl.getTotalSongsCount());
		
		// Add play counts breakdown
		java.util.Map<String, Long> playCounts = songRepositoryImpl.getPlayCountsByAccount();
		model.addAttribute("primaryPlays", playCounts.get("primary"));
		model.addAttribute("legacyPlays", playCounts.get("legacy"));
		model.addAttribute("totalPlays", playCounts.get("total"));
		
		model.addAttribute("totalListeningTime", songRepositoryImpl.getTotalListeningTime());
		
		// Add gender breakdown stats
		model.addAttribute("playsByGender", songRepositoryImpl.getPlayCountsByGender());
		model.addAttribute("artistsByGender", songRepositoryImpl.getArtistCountsByGender());
		model.addAttribute("songsByGender", songRepositoryImpl.getSongCountsByGender());
		model.addAttribute("albumsByGender", songRepositoryImpl.getAlbumCountsByGender());
		model.addAttribute("listeningTimeByGender", songRepositoryImpl.getListeningTimeByGender());
		
		return "index";
	}

	@GetMapping("/api/search/global")
	@ResponseBody
	public List<GlobalSearchResultDTO> searchAllCatalogs(
			@RequestParam(required = false) String q,
			@RequestParam(required = false, defaultValue = "20") int limit) {
		return globalSearchService.search(q, limit);
	}

}
