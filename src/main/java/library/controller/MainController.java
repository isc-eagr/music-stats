package library.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import library.repository.SongRepositoryImpl;

import java.util.HashMap;
import java.util.Map;

@Controller
public class MainController {

	@Autowired
	private SongRepositoryImpl songRepositoryImpl;

	
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

	// API endpoint for gender breakdown chart data
	@GetMapping("/api/charts/gender")
	@ResponseBody
	public Map<String, Object> getGenderChartData() {
		Map<String, Object> data = new HashMap<>();
		
		data.put("playsByGender", songRepositoryImpl.getPlayCountsByGender());
		data.put("artistsByGender", songRepositoryImpl.getArtistCountsByGender());
		data.put("songsByGender", songRepositoryImpl.getSongCountsByGender());
		data.put("albumsByGender", songRepositoryImpl.getAlbumCountsByGender());
		data.put("listeningTimeByGender", songRepositoryImpl.getListeningTimeByGender());
		data.put("playsByGenreAndGender", songRepositoryImpl.getPlayCountsByGenreAndGender());
		data.put("playsByEthnicityAndGender", songRepositoryImpl.getPlayCountsByEthnicityAndGender());
		data.put("playsByLanguageAndGender", songRepositoryImpl.getPlayCountsByLanguageAndGender());
		data.put("playsByYearAndGender", songRepositoryImpl.getPlayCountsByYearAndGender());
		
		return data;
	}

}
