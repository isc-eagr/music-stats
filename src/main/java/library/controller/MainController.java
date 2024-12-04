package library.controller;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.opencsv.bean.CsvToBeanBuilder;

import library.dto.TopCountDTO;
import library.dto.TopGenresDTO;
import library.dto.AlbumPageDTO;
import library.dto.AlbumSongsQueryDTO;
import library.dto.ArtistAlbumsQueryDTO;
import library.dto.ArtistPageDTO;
import library.dto.PlayDTO;
import library.dto.ArtistSongsQueryDTO;
import library.dto.Criterion;
import library.dto.DeletedSongsDTO;
import library.dto.Filter;
import library.dto.CategoryPageDTO;
import library.dto.SaveAlbumDTO;
import library.dto.SongPageDTO;
import library.dto.TimeUnitDetailDTO;
import library.dto.TopAlbumsDTO;
import library.dto.TopArtistsDTO;
import library.dto.TopGroupDTO;
import library.dto.TopSongsDTO;
import library.dto.TimeUnitStatsDTO;
import library.entity.Scrobble;
import library.entity.Song;
import library.repository.ArtistRepository;
import library.repository.ScrobbleRepository;
import library.repository.ScrobbleRepositoryImpl;
import library.repository.SongRepository;
import library.repository.SongRepositoryImpl;
import library.repository.TimeUnitRepository;
import library.util.Utils;

@Controller
public class MainController {

	@Autowired
	private SongRepository songRepository;

	@Autowired
	private SongRepositoryImpl songRepositoryImpl;

	@Autowired
	private ScrobbleRepository scrobbleRepository;

	@Autowired
	private ScrobbleRepositoryImpl scrobbleRepositoryImpl;

	@Autowired
	private ArtistRepository artistRepository;

	@Autowired
	private TimeUnitRepository timeUnitRepository;

	public static int daysElapsedSinceFirstPlay = 1;

	@RequestMapping("/insertScrobbles")
	@ResponseBody
	public String insertScrobbles(Model model) throws FileNotFoundException, IOException {
		
		List<String> accounts = List.of("vatito", "robertlover");
		String result = "";

		for(String account : accounts) {
			
			String fileName = String.format("C:\\scrobbles-%s.csv",account);
			FileReader file = new FileReader(fileName);
	
			List<Scrobble> scrobbles = new CsvToBeanBuilder<Scrobble>(file).withType(Scrobble.class).withSkipLines(1)
					.build().parse();
	
			List<Song> everySong = songRepository.findAll();
	
			Song emptySong = new Song();
			List<Song> duplicateSongs = new ArrayList<>();
			scrobbles.stream().parallel().forEach(sc -> {
				List<Song> foundSongs = everySong.stream().parallel()
						.filter(song -> song.getArtist().equalsIgnoreCase(sc.getArtist())
								&& song.getSong().replace("????", "").equalsIgnoreCase(sc.getSong().replace("????", "")) //specifically for Rauw's DIME QUIEN????
								&& String.valueOf(song.getAlbum()).equalsIgnoreCase(String.valueOf(sc.getAlbum())))
						.toList();
	
				// This determines duplicate entries in song
				if (foundSongs.size() > 1) {
					duplicateSongs.add(foundSongs.get(0));
				}
	
				sc.setSongId(foundSongs.stream().findFirst().orElse(emptySong).getId());
				sc.setAlbum(sc.getAlbum() == null || sc.getAlbum().isBlank() ? null : sc.getAlbum());
				sc.setAccount(account);
			});// forEach
	
			duplicateSongs.stream().map(s -> s.getArtist() + " - " + s.getAlbum() + " - " + s.getSong()).distinct()
					.forEach(System.out::println);
	
			scrobbleRepository.saveAll(scrobbles);
	
			file.close();
			
			result += "Success!" + scrobbles.size() + " scrobbles inserted from "+account+" account!<br/>";
		
		}

		return result;
	}

	@RequestMapping("/insertItunesInfo")
	@ResponseBody
	public String insertItunesInfo(Model model)
			throws FileNotFoundException, IOException, SAXException, ParserConfigurationException, ParseException {

		File file = new File("c:\\Library.xml");
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(file);
		doc.getDocumentElement().normalize();
		NodeList nodeLst = doc.getElementsByTagName("dict");
		Element dict1 = (Element) nodeLst.item(0);

		NodeList nodeLst2 = dict1.getElementsByTagName("dict");
		Element dict2 = (Element) nodeLst2.item(0);

		// Aqui ya estan todas las canciones
		NodeList nodeList3 = dict2.getElementsByTagName("dict");
		List<Song> everySong = new ArrayList<>(nodeList3.getLength());

		for (int i = 0; i < nodeList3.getLength(); i++) {
			Element song = (Element) nodeList3.item(i);
			NodeList details = song.getChildNodes();
			Song songObject = new Song();

			LocalDateTime now = LocalDateTime.now();

			songObject.setCreated(java.sql.Timestamp.valueOf(now));
			songObject.setUpdated(java.sql.Timestamp.valueOf(now));

			boolean ignore = false;
			for (int j = 0; j < details.getLength(); j++) {
				Node detail = details.item(j);
				if (detail.getNodeName().equals("key")) {
					Node valor = detail.getNextSibling();
					if (detail.getChildNodes().item(0) == null) {
						continue;
					}

					String property = detail.getChildNodes().item(0).getNodeValue();
					String value = "null";
					try {
						value = valor.getChildNodes().item(0).getNodeValue();
					} catch (DOMException | NullPointerException e) {}
					switch (property) {
					case "Name":
						songObject.setSong(value);
						break;
					case "Artist":
						songObject.setArtist(value);
						break;
					case "Album":
						songObject.setAlbum(value == null || value.isBlank() ? null : value);
						break;
					case "Genre":
						songObject.setGenre(value);
						break;
					case "Total Time":
						songObject.setDuration(Integer.parseInt(value) / 1000);
						break;
					case "Year":
						songObject.setYear(value == null ? 0 : Integer.parseInt(value));
						break;
					case "Comments":
						try {
							String values[] = value.split("---");
							songObject.setLanguage(values[0].strip());
							songObject.setSex(values[1].strip());
							songObject.setRace(values[2].strip());
						}catch(Exception e) {ignore = true;}
						break;
					case "Kind":
						switch (value) {
						case "Matched AAC audio file":
							songObject.setCloudStatus("Matched");
							break;
						case "AAC audio file", "MPEG audio file":
							songObject.setCloudStatus("Uploaded");
							break;
						case "Apple Music AAC audio file":
							songObject.setCloudStatus("No Longer Available");
							break;
						default:
							songObject.setCloudStatus("Unknown");
							break;
						}
						break;

					case "Location":
						if (value.toLowerCase().contains(".mp3"))
							songObject.setCloudStatus("Uploaded");
						break;

					case "Playlist Only":
						ignore = true;
						break;

					case "Matched":
						songObject.setCloudStatus("Matched");
						break;

					case "Apple Music":
						songObject.setCloudStatus("Apple Music");
						break;

					case "Purchased":
						if (!String.valueOf(songObject.getCloudStatus()).equals("Uploaded"))
							songObject.setCloudStatus("Purchased");
						break;

					case "Music Video":
						ignore = true;
						break;
					}
				}
			}
			if (!ignore) {
				songObject.setSource("iTunes");
				everySong.add(songObject);
			}
		}

		songRepository.saveAll(everySong);

		return "Success! " + everySong.size() + " itunes records inserted!";
	}

	@RequestMapping("/")
	public String index(Model model) {
		return "main";
	}

	@RequestMapping("/topArtists")
	public String topArtists(Model model, @Valid @ModelAttribute(value = "filter") Filter filter) {
		model.addAttribute("sexes", songRepositoryImpl.getAllSexes());
		model.addAttribute("genres", songRepositoryImpl.getAllGenres());
		model.addAttribute("races", songRepositoryImpl.getAllRaces());
		model.addAttribute("languages", songRepositoryImpl.getAllLanguages());
		model.addAttribute("accounts", songRepositoryImpl.getAllAccounts());

		model.addAttribute("sortField", filter.getSortField());
		model.addAttribute("sortDir", filter.getSortDir());
		
		Map<String, Comparator<TopArtistsDTO>> sorting = new HashMap<>(); 
				
		sorting.put("artist", (o1,o2)->o1.getArtist().compareToIgnoreCase(o2.getArtist()));
		sorting.put("count", (o1,o2)->o1.getCount()>o2.getCount()?1:(o1.getCount()==o2.getCount()?0:-1));
		sorting.put("playtime", (o1,o2)->o1.getPlaytime()>o2.getPlaytime()?1:(o1.getPlaytime()==o2.getPlaytime()?0:-1));
		sorting.put("average_length", (o1,o2)->o1.getAverageLength()>o2.getAverageLength()?1:(o1.getAverageLength()==o2.getAverageLength()?0:-1));
		sorting.put("average_plays", (o1,o2)->o1.getAveragePlays()>o2.getAveragePlays()?1:(o1.getAveragePlays()==o2.getAveragePlays()?0:-1));
		sorting.put("first_play", (o1,o2)->o1.getFirstPlay().compareToIgnoreCase(o2.getFirstPlay()));
		sorting.put("last_play", (o1,o2)->o1.getLastPlay().compareToIgnoreCase(o2.getLastPlay()));
		sorting.put("number_of_albums", (o1,o2)->o1.getNumberOfAlbums()>o2.getNumberOfAlbums()?1:(o1.getNumberOfAlbums()==o2.getNumberOfAlbums()?0:-1));
		sorting.put("number_of_songs", (o1,o2)->o1.getNumberOfSongs()>o2.getNumberOfSongs()?1:(o1.getNumberOfSongs()==o2.getNumberOfSongs()?0:-1));
		sorting.put("play_days", (o1,o2)->o1.getPlayDays()>o2.getPlayDays()?1:(o1.getPlayDays()==o2.getPlayDays()?0:-1));
		sorting.put("play_weeks", (o1,o2)->o1.getPlayWeeks()>o2.getPlayWeeks()?1:(o1.getPlayWeeks()==o2.getPlayWeeks()?0:-1));
		sorting.put("play_months", (o1,o2)->o1.getPlayMonths()>o2.getPlayMonths()?1:(o1.getPlayMonths()==o2.getPlayMonths()?0:-1));
		

		List<String> categories = new ArrayList<>();
		List<String> values = new ArrayList<>();
		
		if(filter.getArtist()!=null && !filter.getArtist().isBlank()) {categories.add("artist");values.add("%"+filter.getArtist()+"%");}
		if(filter.getSex()!=null && !filter.getSex().isBlank()) {categories.add("sex");values.add(filter.getSex());}
		if(filter.getGenre()!=null && !filter.getGenre().isBlank()) {categories.add("genre");values.add(filter.getGenre());}
		if(filter.getRace()!=null && !filter.getRace().isBlank()) {categories.add("race");values.add(filter.getRace());}
		if(filter.getYear()>0) {categories.add("year");values.add(String.valueOf(filter.getYear()));}
		if(filter.getLanguage()!=null && !filter.getLanguage().isBlank()) {categories.add("language");values.add(filter.getLanguage());}
		if(filter.getAccount()!=null && !filter.getAccount().isBlank()) {categories.add("account");values.add(filter.getAccount());}
		
		values.add("1930-01-01");
		values.add("2200-01-01");
		
		List<TopArtistsDTO> topArtists = new ArrayList<>();
		
		List<PlayDTO> plays = artistRepository.categoryPlays(categories.toArray(String[]::new),
				values.toArray(String[]::new), 0, 10000);
		
		List<String> accounts = plays.stream().map(p->p.getAccount()).distinct().toList();
		
		Map<String, List<PlayDTO>> mapArtists = plays.stream().collect(Collectors.groupingBy(play->play.getArtist()));
		
		/* TODO Too complex, find a way to include features in these calculations
		if(filter.isIncludeFeatures()) {
			mapArtists.forEach((k,v)->{
				v.addAll(plays.stream().filter(s->s.getSong().toLowerCase().contains(k.toLowerCase()) && s.getSong().toLowerCase().contains("feat") || s.getSong().toLowerCase().contains("with") ).toList());
				
			});
		}*/
		
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(mapArtists.entrySet());
		
		if(filter.getPlaysMoreThan()>0) {
			sortedList = sortedList.stream().filter(e->e.getValue().size()>=filter.getPlaysMoreThan()).toList();
		}
		
		for(Entry<String, List<PlayDTO>> entryArtist : sortedList) {
			
			List<PlayDTO> sorted = entryArtist.getValue().stream()
					.sorted((sc1, sc2) -> sc1.getPlayDate().compareTo(sc2.getPlayDate())).toList();
			
			TopArtistsDTO dto = new TopArtistsDTO();
			dto.setArtist(entryArtist.getKey());
			dto.setGenre(entryArtist.getValue().get(0).getGenre());
			dto.setRace(entryArtist.getValue().get(0).getRace());
			dto.setSex(entryArtist.getValue().get(0).getSex());
			dto.setLanguage(entryArtist.getValue().get(0).getLanguage());
			dto.setCount(sorted.size()); 
			dto.setFirstPlay(sorted.get(0).getPlayDate()); 
			dto.setLastPlay(sorted.get(sorted.size() - 1).getPlayDate());
			dto.setPlaytime(sorted.stream().mapToLong(s->s.getTrackLength()).sum());
			dto.setPlayDays(entryArtist.getValue().stream().map(e->e.getPlayDate().substring(0,10)).distinct().count());
			dto.setPlayWeeks(entryArtist.getValue().stream().map(e->e.getWeek()).distinct().count());
			dto.setPlayMonths(entryArtist.getValue().stream().map(e->e.getPlayDate().substring(0,8)).distinct().count());
			String playsByAccount="";
			for(String account : accounts) {
				playsByAccount += account+": "+sorted.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
			}
			dto.setPlaysByAccount(playsByAccount);
			
			Map<String, List<PlayDTO>> mapSongs = sorted.stream().collect(Collectors.groupingBy(s -> s.getArtist() + "::" + s.getAlbum() + "::" + s.getSong()));
			dto.setAverageLength(mapSongs.entrySet().stream().mapToInt(e->e.getValue().get(0).getTrackLength()).sum()/mapSongs.entrySet().size());
			dto.setAveragePlays(mapSongs.entrySet().stream().mapToInt(e->e.getValue().size()).sum()/(double)mapSongs.entrySet().size());
			dto.setNumberOfSongs(mapSongs.entrySet().size());
			dto.setNumberOfAlbums(sorted.stream().collect(Collectors.groupingBy(s -> s.getArtist() + "::" + s.getAlbum() + "::")).entrySet().size());
			
			topArtists.add(dto);
						
		}
		
		if(filter.getSortDir().equals("asc"))
			topArtists = topArtists.stream().sorted(sorting.get(filter.getSortField())).limit(filter.getPageSize()).toList();
		else
			topArtists = topArtists.stream().sorted(sorting.get(filter.getSortField()).reversed()).limit(filter.getPageSize()).toList();
		
		model.addAttribute("topArtists", topArtists);

		
		//Categories
		List<Criterion<TopArtistsDTO>> criteria = List.of(
					new Criterion<>("Sex", artist -> artist.getSex(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Genre", artist -> artist.getGenre(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Race", artist -> artist.getRace(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Language", artist -> artist.getLanguage(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1))
					);

			List<TopGroupDTO> topArtistsGroupList = new ArrayList<>();

			for (Criterion<TopArtistsDTO> criterion : criteria) {
				Map<String, List<TopArtistsDTO>> classifiedMap = topArtists.stream().parallel()
						.collect(Collectors.groupingBy(criterion.groupingBy));

				List<TopCountDTO> counts = new ArrayList<>();

				List<Entry<String, List<TopArtistsDTO>>> sortedListCategories = new ArrayList<>(classifiedMap.entrySet());
				Collections.sort(sortedListCategories, criterion.getSortBy());

				for (Entry<String, List<TopArtistsDTO>> e : sortedListCategories) {
					int playsCategories = e.getValue().stream().mapToInt(topSong -> topSong.getCount()).sum();
					long playtime = e.getValue().stream().mapToLong(topArtist -> topArtist.getPlaytime()).sum();
					counts.add(new TopCountDTO(e.getKey(), e.getValue().size(),
							(double) e.getValue().size() / (double) topArtists.size() * 100, playsCategories,
							(double) playsCategories * 100
									/ topArtists.stream().mapToDouble(a -> a.getCount()).sum(),
							playtime,
							(double) playtime * 100 / topArtists.stream().mapToDouble(a -> a.getPlaytime()).sum()));
				}

				topArtistsGroupList.add(new TopGroupDTO(criterion.getName(), counts));
			}

			model.addAttribute("topArtistsGroupList", topArtistsGroupList);

		return "topartists";

	}

	@RequestMapping("/topAlbums")
	public String topAlbums(Model model, @Valid @ModelAttribute(value = "filter") Filter filter) {

		model.addAttribute("sexes", songRepositoryImpl.getAllSexes());
		model.addAttribute("genres", songRepositoryImpl.getAllGenres());
		model.addAttribute("races", songRepositoryImpl.getAllRaces());
		model.addAttribute("languages", songRepositoryImpl.getAllLanguages());

		model.addAttribute("sortField", filter.getSortField());
		model.addAttribute("sortDir", filter.getSortDir());
		model.addAttribute("filterMode", filter.getFilterMode());

		Sort sort = filter.getSortDir().equalsIgnoreCase(Sort.Direction.ASC.name())
				? Sort.by(filter.getSortField()).ascending()
				: Sort.by(filter.getSortField()).descending();
		PageRequest pageable = PageRequest.of(filter.getPage() - 1, filter.getPageSize(), sort);

		Page<TopAlbumsDTO> topAlbums = songRepositoryImpl.getTopAlbums(pageable, filter);
		model.addAttribute("topAlbums", topAlbums);

		int totalPages = topAlbums.getTotalPages();
		if (totalPages > 0) {
			List<Integer> pageNumbers = IntStream.rangeClosed(1, totalPages).boxed().collect(Collectors.toList());
			model.addAttribute("pageNumbers", pageNumbers);
		}

		if (filter.getFilterMode().equals("2")) {
			List<Criterion<TopAlbumsDTO>> criteria = List.of(
					new Criterion<>("Sex", album -> album.getSex(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Genre", album -> album.getGenre(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Race", album -> album.getRace(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Language", album -> album.getLanguage(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Release Year", album -> album.getYear(),
							(o1, o2) -> o2.getKey().compareTo(o1.getKey())));

			List<TopGroupDTO> topAlbumsGroupList = new ArrayList<>();

			for (Criterion<TopAlbumsDTO> criterion : criteria) {
				Map<String, List<TopAlbumsDTO>> classifiedMap = topAlbums.stream().parallel()
						.collect(Collectors.groupingBy(criterion.groupingBy));

				List<TopCountDTO> counts = new ArrayList<>();

				List<Entry<String, List<TopAlbumsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
				Collections.sort(sortedList, criterion.getSortBy());

				for (Entry<String, List<TopAlbumsDTO>> e : sortedList) {
					int plays = e.getValue().stream().mapToInt(topSong -> Integer.parseInt(topSong.getCount())).sum();
					long playtime = e.getValue().stream().mapToLong(topAlbum -> topAlbum.getPlaytime()).sum();
					counts.add(new TopCountDTO(e.getKey(), e.getValue().size(),
							(double) e.getValue().size() / (double) topAlbums.getNumberOfElements() * 100, plays,
							(double) plays * 100
									/ topAlbums.stream().mapToDouble(a -> Double.parseDouble(a.getCount())).sum(),
							playtime,
							(double) playtime * 100 / topAlbums.stream().mapToDouble(a -> a.getPlaytime()).sum()));
				}

				topAlbumsGroupList.add(new TopGroupDTO(criterion.getName(), counts));
			}

			model.addAttribute("topAlbumsGroupList", topAlbumsGroupList);
		}

		return "topalbums";
	}

	@RequestMapping("/topSongs")
	public String topSongs(Model model, @Valid @ModelAttribute(value = "filter") Filter filter) {

		model.addAttribute("sexes", songRepositoryImpl.getAllSexes());
		model.addAttribute("genres", songRepositoryImpl.getAllGenres());
		model.addAttribute("races", songRepositoryImpl.getAllRaces());
		model.addAttribute("languages", songRepositoryImpl.getAllLanguages());

		model.addAttribute("sortField", filter.getSortField());
		model.addAttribute("sortDir", filter.getSortDir());
		model.addAttribute("filterMode", filter.getFilterMode());

		Sort sort = filter.getSortDir().equalsIgnoreCase(Sort.Direction.ASC.name())
				? Sort.by(filter.getSortField()).ascending()
				: Sort.by(filter.getSortField()).descending();
		PageRequest pageable = PageRequest.of(filter.getPage() - 1, filter.getPageSize(), sort);

		Page<TopSongsDTO> topSongs = songRepositoryImpl.getTopSongs(pageable, filter);
		model.addAttribute("topSongs", topSongs);

		int totalPages = topSongs.getTotalPages();
		if (totalPages > 0) {
			List<Integer> pageNumbers = IntStream.rangeClosed(1, totalPages).boxed().collect(Collectors.toList());
			model.addAttribute("pageNumbers", pageNumbers);
		}

		if (filter.getFilterMode().equals("2")) {
			List<Criterion<TopSongsDTO>> criteria = List.of(
					new Criterion<>("Sex", song -> song.getSex(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Genre", song -> song.getGenre(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Race", song -> song.getRace(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Language", song -> song.getLanguage(),
							(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
									: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
					new Criterion<>("Release Year", song -> song.getYear(),
							(o1, o2) -> o2.getKey().compareTo(o1.getKey())));

			List<TopGroupDTO> topSongsGroupList = new ArrayList<>();

			for (Criterion<TopSongsDTO> criterion : criteria) {
				Map<String, List<TopSongsDTO>> classifiedMap = topSongs.stream().parallel()
						.collect(Collectors.groupingBy(criterion.groupingBy));

				List<TopCountDTO> counts = new ArrayList<>();

				List<Entry<String, List<TopSongsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
				Collections.sort(sortedList, criterion.getSortBy());

				for (Entry<String, List<TopSongsDTO>> e : sortedList) {
					int plays = e.getValue().stream().mapToInt(topSong -> Integer.parseInt(topSong.getCount())).sum();
					long playtime = e.getValue().stream().mapToLong(topSong -> topSong.getPlaytime()).sum();
					counts.add(new TopCountDTO(e.getKey(), e.getValue().size(),
							(double) e.getValue().size() / (double) topSongs.getNumberOfElements() * 100, plays,
							(double) plays * 100
									/ topSongs.stream().mapToDouble(a -> Double.parseDouble(a.getCount())).sum(),
							playtime,
							(double) playtime * 100 / topSongs.stream().mapToDouble(a -> a.getPlaytime()).sum()));
				}

				topSongsGroupList.add(new TopGroupDTO(criterion.getName(), counts));
			}

			model.addAttribute("topSongsGroupList", topSongsGroupList);
		}

		return "topsongs";

	}

	@RequestMapping("/topGenres")
	public String topGenres(Model model, @RequestParam(defaultValue = "10000000") int limit) {
		List<TopGenresDTO> topGenres = songRepositoryImpl.getTopGenres(limit);
		model.addAttribute("topGenres", topGenres);

		return "topGenres";

	}

	@RequestMapping({"/songsLastFmButNotLocal","/songsLastFmButNotLocal/{account}"})
	public String songsLastFmButNotLocal(Model model, @PathVariable(required = false) String account) {
		model.addAttribute("accounts", songRepositoryImpl.getAllAccounts());
		model.addAttribute("listSongs", scrobbleRepositoryImpl.songsInLastFmButNotLocal(account));
		return "songsLastFmButNotLocal";

	}

	@RequestMapping("/songsLocalButNotLastfm")
	public String songsLocalButNotLastFm(Model model) {
		model.addAttribute("listSongs", songRepositoryImpl.songsLocalButNotLastfm());
		return "songsLocalButNotLastfm";

	}

	@RequestMapping("/timeUnit")
	public String timeUnit(Model model, @RequestParam String unit) {
		List<TimeUnitStatsDTO> timeUnits = songRepositoryImpl.timeUnitStats(unit);

		model.addAttribute("timeUnits", timeUnits);

		List<TopGroupDTO> timeUnitGroupList = new ArrayList<>();
		List<TopCountDTO> counts = new ArrayList<>();

		Map<String, List<TimeUnitStatsDTO>> mapGenre = timeUnits.stream()
				.collect(Collectors.groupingBy(ww -> ww.getGenre()));

		List<Entry<String, List<TimeUnitStatsDTO>>> sortedGenreList = new ArrayList<>(mapGenre.entrySet());
		Collections.sort(sortedGenreList, (o1, o2) -> ((o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)));

		for (Entry<String, List<TimeUnitStatsDTO>> e : sortedGenreList) {
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(),
					(double) e.getValue().size() / (double) timeUnits.size() * 100, 0, 0, 0, 0));
		}
		timeUnitGroupList.add(new TopGroupDTO("Genre", counts));

		counts = new ArrayList<>();
		Map<String, List<TimeUnitStatsDTO>> mapRace = timeUnits.stream()
				.collect(Collectors.groupingBy(ww -> ww.getRace()));

		List<Entry<String, List<TimeUnitStatsDTO>>> sortedRaceList = new ArrayList<>(mapRace.entrySet());
		Collections.sort(sortedRaceList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));

		for (Entry<String, List<TimeUnitStatsDTO>> e : sortedRaceList) {
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(),
					(double) e.getValue().size() / (double) timeUnits.size() * 100, 0, 0, 0, 0));
		}

		timeUnitGroupList.add(new TopGroupDTO("Race", counts));

		counts = new ArrayList<>();
		Map<String, List<TimeUnitStatsDTO>> mapSex = timeUnits.stream()
				.collect(Collectors.groupingBy(ww -> ww.getSex()));

		List<Entry<String, List<TimeUnitStatsDTO>>> sortedSexList = new ArrayList<>(mapSex.entrySet());
		Collections.sort(sortedSexList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));

		for (Entry<String, List<TimeUnitStatsDTO>> e : sortedSexList) {
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(),
					(double) e.getValue().size() / (double) timeUnits.size() * 100, 0, 0, 0, 0));
		}

		timeUnitGroupList.add(new TopGroupDTO("Sex", counts));

		model.addAttribute("timeUnitGroupList", timeUnitGroupList);
		model.addAttribute("unit", unit);
		model.addAttribute("top", switch (unit) {
		case "day" -> 5;
		case "week" -> 10;
		case "month" -> 50;
		case "season" -> 100;
		case "year" -> 250;
		case "decade" -> 500;
		default -> 5;
		});
		return "timeunit";

	}

	@RequestMapping("/timeUnitDetail/{unit}/{unitValue}/{top}")
	public String timeUnitDetail(Model model, @PathVariable String unit, @PathVariable String unitValue,
			@PathVariable Integer top) {

		List<PlayDTO> plays = switch (unit) {
			case "day" -> timeUnitRepository.dayPlays(unitValue);
			case "week" -> timeUnitRepository.weekPlays(unitValue);
			case "month" -> timeUnitRepository.monthPlays(unitValue);
			case "season" -> timeUnitRepository.seasonPlays(unitValue);
			case "year" -> timeUnitRepository.yearPlays(unitValue);
			case "decade" -> timeUnitRepository.decadePlays(unitValue);
			default -> new ArrayList<>();
		};

		TimeUnitDetailDTO timeUnitDetailDTO = new TimeUnitDetailDTO();
		timeUnitDetailDTO.setTotalPlays(plays.size());
		timeUnitDetailDTO.setTotalPlaytime(plays.stream().mapToInt(s -> s.getTrackLength()).sum());

		timeUnitDetailDTO.setPercentageofUnitWhereMusicWasPlayed(switch (unit) {
		case "day" -> (double) timeUnitDetailDTO.getTotalPlaytime() * 100 / (double) Utils.SECONDS_IN_A_DAY;
		case "week" -> (double) timeUnitDetailDTO.getTotalPlaytime() * 100 / (double) Utils.SECONDS_IN_A_WEEK;
		case "month" -> (double) timeUnitDetailDTO.getTotalPlaytime() * 100
				/ (double) Utils.secondsInAMonth(unitValue.split("-")[1], Integer.parseInt(unitValue.split("-")[0]));
		case "season" -> (double) timeUnitDetailDTO.getTotalPlaytime() * 100
				/ (double) Utils.secondsInASeason(unitValue.substring(4), Integer.parseInt(unitValue.substring(0, 4)));
		case "year" -> (double) timeUnitDetailDTO.getTotalPlaytime() * 100
				/ (double) Utils.secondsInAYear(Integer.parseInt(unitValue));
		case "decade" -> (double) timeUnitDetailDTO.getTotalPlaytime() * 100
				/ (double) Utils.secondsInADecade(Integer.parseInt(unitValue.substring(0, 4)));
		default -> 0.0;
		});

		Map<String, List<PlayDTO>> map = plays.stream().collect(Collectors.groupingBy(s -> s.getArtist()));
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));

		timeUnitDetailDTO.setMostPlayedArtist(sortedList.get(0).getKey() + " - " + sortedList.get(0).getValue().size());
		timeUnitDetailDTO.setUniqueArtistsPlayed(map.entrySet().size());

		// Group by album and sort by most played
		map = plays.stream().filter(s -> !s.getAlbum().equals("(single)") && !s.getAlbum().isBlank())
				.collect(Collectors.groupingBy(s -> s.getArtist() + "::" + s.getAlbum()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));
		timeUnitDetailDTO.setMostPlayedAlbum(sortedList.get(0).getKey() + " - " + sortedList.get(0).getValue().size());
		timeUnitDetailDTO.setUniqueAlbumsPlayed(map.entrySet().size());

		timeUnitDetailDTO.setMostPlayedAlbums(sortedList.stream().limit(top).map(e -> {
			ArtistAlbumsQueryDTO song = new ArtistAlbumsQueryDTO();
			song.setArtist(e.getValue().get(0).getArtist());
			song.setAlbum(e.getValue().get(0).getAlbum());
			song.setGenre(e.getValue().get(0).getGenre());
			song.setRace(e.getValue().get(0).getRace());
			song.setSex(e.getValue().get(0).getSex());
			song.setLanguage(e.getValue().get(0).getLanguage());
			song.setReleaseYear(e.getValue().get(0).getYear());
			song.setTotalPlays(e.getValue().size());
			return song;
		}).toList());

		// Group by song and sort by most played
		map = plays.stream()
				.collect(Collectors.groupingBy(s -> s.getArtist() + "::" + s.getAlbum() + "::" + s.getSong()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));
		timeUnitDetailDTO.setMostPlayedSong(sortedList.get(0).getKey() + " - " + sortedList.get(0).getValue().size());
		timeUnitDetailDTO.setUniqueSongsPlayed(map.entrySet().size());

		timeUnitDetailDTO.setMostPlayedSongs(sortedList.stream().limit(top).map(e -> {
			AlbumSongsQueryDTO song = new AlbumSongsQueryDTO();
			song.setArtist(e.getValue().get(0).getArtist());
			song.setAlbum(e.getValue().get(0).getAlbum());
			song.setSong(e.getValue().get(0).getSong());
			song.setGenre(e.getValue().get(0).getGenre());
			song.setRace(e.getValue().get(0).getRace());
			song.setSex(e.getValue().get(0).getSex());
			song.setLanguage(e.getValue().get(0).getLanguage());
			song.setYear(e.getValue().get(0).getYear());
			song.setTotalPlays(e.getValue().size());
			song.setCloudStatus(e.getValue().get(0).getCloudStatus());
			return song;
		}).toList());

		List<Criterion<PlayDTO>> criteria = List.of(
				new Criterion<>("Sex", play -> play.getSex(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Genre", play -> play.getGenre(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Race", play -> play.getRace(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Language", play -> play.getLanguage(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Release Year", play -> String.valueOf(play.getYear()),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey())),
				new Criterion<>("Cloud Status", play -> play.getCloudStatus(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1))		
						
						);

		model.addAttribute("timeUnitGroupList", Utils.generateChartData(criteria, plays,
				timeUnitDetailDTO.getUniqueSongsPlayed(), (int) timeUnitDetailDTO.getTotalPlaytime()));
		model.addAttribute("timeUnitDetail", timeUnitDetailDTO);
		model.addAttribute("unit", unit);
		model.addAttribute("unitValue", unitValue);

		return "timeunitdetail";

	}

	@RequestMapping(value = { "/insertSongForm", "/insertSongForm/{artist}/{song}",
			"/insertSongForm/{artist}/{song}/{album}" })
	public String insertSongForm(Model model, @PathVariable(required = false) String artist,
			@PathVariable(required = false) String song, @PathVariable(required = false) String album) {

		if (artist != null && song != null) {
			Song s = new Song();
			s.setArtist(artist.replace("*", "/"));
			s.setSong(song.replace("*", "/"));
			s.setAlbum(album == null || album.equals("null")? "" : album.replace("*", "/"));
			model.addAttribute("songForm", s);
		} else {
			model.addAttribute("songForm", new Song());
		}
		return "insertsongform";
	}

	@RequestMapping("/insertSong")
	public String insertSong(@Valid @ModelAttribute(value = "songForm") Song songForm, BindingResult result,
			Model model) {
		if (result.hasErrors()) {
			return "insertsongform";
		}

		songForm.setSource("Manual");
		songForm.setCloudStatus("Deleted");
		songForm.setAlbum(songForm.getAlbum().isBlank()?null:songForm.getAlbum());

		String duration[] = songForm.getDurationString().split(":");
		songForm.setDuration(Integer.parseInt(duration[0]) * 60 + Integer.parseInt(duration[1]));

		LocalDateTime now = LocalDateTime.now();

		songForm.setCreated(java.sql.Timestamp.valueOf(now));
		songForm.setUpdated(java.sql.Timestamp.valueOf(now));

		songRepository.save(songForm);
		scrobbleRepositoryImpl.updateSongIds(songForm.getId(), songForm.getArtist(), songForm.getSong(),
				songForm.getAlbum());
		songForm = new Song();
		model.addAttribute("success", true);
		return "insertsongform";
	}

	@RequestMapping({ "/insertAlbumForm/{artist}", "/insertAlbumForm/{artist}/{album}" })
	public String insertAlbumForm(Model model, @PathVariable(required = false) String artist,
			@PathVariable(required = false) String album) {

		List<Song> songsList;

		if (album == null) {
			songsList = scrobbleRepositoryImpl.unmatchedSongsFromArtist(artist);
		} else {
			songsList = scrobbleRepositoryImpl.unmatchedSongsFromAlbum(artist,
					album == null || album.equals("null") ? "" : album.replace("*", "/"));
			model.addAttribute("album", album.equals("null") ? "(single)" : album.replace("*", "/"));
		}

		SaveAlbumDTO form = new SaveAlbumDTO();
		form.setSongs(songsList);

		model.addAttribute("form", form);

		return "insertalbumform";
	}

	@RequestMapping("/insertAlbum")
	public String insertAlbum(@ModelAttribute(value = "form") SaveAlbumDTO form, Model model) {

		for (Song s : form.getSongs()) {
			s.setSource("Manual");
			s.setCloudStatus("Deleted");
			s.setAlbum(s.getAlbum().isBlank()?null:s.getAlbum());
			String duration[] = s.getDurationString().split(":");
			s.setDuration(Integer.parseInt(duration[0]) * 60 + Integer.parseInt(duration[1]));

			LocalDateTime now = LocalDateTime.now();

			s.setCreated(java.sql.Timestamp.valueOf(now));
			s.setUpdated(java.sql.Timestamp.valueOf(now));

			songRepository.save(s);
			scrobbleRepositoryImpl.updateSongIds(s.getId(), s.getArtist(), s.getSong(), s.getAlbum());
		}

		model.addAttribute("success", true);
		return "insertalbumform";
	}

	@RequestMapping("/artist")
	public String artist(Model model, @RequestParam(required = true) String artist, 
			@RequestParam(defaultValue="false") boolean includeFeatures) {

		List<PlayDTO> plays = artistRepository.artistPlays(artist, includeFeatures);
		List<ArtistSongsQueryDTO> artistSongsList = new ArrayList<>();
		List<ArtistAlbumsQueryDTO> artistAlbumsList = new ArrayList<>();

		Map<String, List<PlayDTO>> songGrouping = plays.stream().collect(Collectors.groupingBy(PlayDTO::getSong));
		List<String> accounts = plays.stream().map(p->p.getAccount()).distinct().toList();

		for (String song : songGrouping.keySet()) {
			List<PlayDTO> sorted = songGrouping.get(song).stream()
					.sorted((sc1, sc2) -> sc1.getPlayDate().compareTo(sc2.getPlayDate())).toList();

			ArtistSongsQueryDTO artistSong = new ArtistSongsQueryDTO();
			artistSong.setArtist(sorted.get(0).getArtist());
			artistSong.setAlbum(sorted.get(0).getAlbum());
			artistSong.setSong(song);
			artistSong.setReleaseYear(sorted.stream().mapToInt(s -> s.getYear()).max().getAsInt());
			artistSong.setFirstPlay(sorted.get(0).getPlayDate());
			artistSong.setLastPlay(sorted.get(sorted.size() - 1).getPlayDate());
			artistSong.setTotalPlays(songGrouping.get(song).size());
			artistSong.setTrackLength(sorted.get(sorted.size() - 1).getTrackLength());
			artistSong.setDaysSongWasPlayed(
					(int) sorted.stream().map(s -> s.getPlayDate().substring(0, 10)).distinct().count());
			artistSong.setWeeksSongWasPlayed((int) sorted.stream().map(s -> s.getWeek()).distinct().count());
			artistSong.setMonthsSongWasPlayed(
					(int) sorted.stream().map(s -> s.getPlayDate().substring(0, 7)).distinct().count());
			artistSong.setCloudStatus(sorted.get(0).getCloudStatus());
			artistSong.setMainOrFeature(sorted.get(0).getMainOrFeature());
			artistSong.setId(sorted.get(0).getId());
			String playsByAccount="";
			for(String account : accounts) {
				playsByAccount += account+": "+sorted.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
			}
			artistSong.setPlaysByAccount(playsByAccount);
			artistSongsList.add(artistSong);
		}
		artistSongsList.sort((s1, s2) -> s1.getTotalPlays() < s2.getTotalPlays() ? 1
				: s1.getTotalPlays() == s2.getTotalPlays() ? 0 : -1);

		// TODO find a way for the grouping to ignore case, right now Aaa and aaa are
		// considered separate albums
		Map<String, List<PlayDTO>> albumGrouping = plays.stream().collect(Collectors.groupingBy(PlayDTO::getAlbum));
		for (String album : albumGrouping.keySet()) {
			List<PlayDTO> sorted = albumGrouping.get(album).stream()
					.sorted((sc1, sc2) -> sc1.getPlayDate().compareTo(sc2.getPlayDate())).toList();

			List<PlayDTO> distinct = sorted.stream().distinct().toList();

			ArtistAlbumsQueryDTO artistAlbum = new ArtistAlbumsQueryDTO();
			artistAlbum.setArtist(sorted.get(0).getArtist());
			artistAlbum.setAlbum(album);
			// Gets the release year with most ocurrences, as some albums have songs with
			// different release years if a song was a single before being on the album
			artistAlbum.setReleaseYear(sorted.stream()
					.collect(Collectors.groupingBy(s -> s.getYear(), Collectors.counting())).entrySet().stream()
					.sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue())).findFirst().get().getKey());
			artistAlbum.setFirstPlay(sorted.get(0).getPlayDate());
			artistAlbum.setLastPlay(sorted.get(sorted.size() - 1).getPlayDate());
			artistAlbum.setTotalPlays(albumGrouping.get(album).size());
			artistAlbum.setTotalPlaytime(sorted.stream().mapToInt(s -> s.getTrackLength()).sum());
			artistAlbum.setAlbumLength(distinct.stream().mapToInt(s -> s.getTrackLength()).sum());
			artistAlbum.setNumberOfTracks(distinct.size());
			artistAlbum.setAverageSongLength(
					(int) distinct.stream().mapToInt(s -> s.getTrackLength()).average().orElse(0.0));
			artistAlbum.setAveragePlaysPerSong((double) artistAlbum.getTotalPlays() / artistAlbum.getNumberOfTracks());
			artistAlbum.setDaysAlbumWasPlayed(
					(int) sorted.stream().map(s -> s.getPlayDate().substring(0, 10)).distinct().count());
			artistAlbum.setWeeksAlbumWasPlayed((int) sorted.stream().map(s -> s.getWeek()).distinct().count());
			artistAlbum.setMonthsAlbumWasPlayed(
					(int) sorted.stream().map(s -> s.getPlayDate().substring(0, 7)).distinct().count());
			String playsByAccount="";
			for(String account : accounts) {
				playsByAccount += account+": "+sorted.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
			}
			artistAlbum.setPlaysByAccount(playsByAccount);
			artistAlbumsList.add(artistAlbum);
		}
		artistAlbumsList.sort((s1, s2) -> s1.getTotalPlays() < s2.getTotalPlays() ? 1
				: s1.getTotalPlays() == s2.getTotalPlays() ? 0 : -1);

		int numberOfSongs = artistSongsList.size();
		int totalPlays = plays.size();
		
		String playsByAccount="";
		for(String account : accounts) {
			playsByAccount += account+": "+plays.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
		}
		
		int sumOfTrackLengths = artistSongsList.stream().mapToInt(s -> s.getTrackLength()).sum();
		double averagePlaysPerSong = (double) totalPlays / numberOfSongs;
		int totalPlaytimeInt = artistSongsList.stream().mapToInt(s -> s.getTotalPlays() * s.getTrackLength()).sum();
		String totalPlaytime = Utils.secondsToString(totalPlaytimeInt);
		String averageSongLength = Utils.secondsToStringColon(sumOfTrackLengths / numberOfSongs);

		ArtistSongsQueryDTO firstSong = artistSongsList.stream()
				.min((s1, s2) -> s1.getFirstPlay().compareTo(s2.getFirstPlay())).orElse(new ArtistSongsQueryDTO());
		ArtistSongsQueryDTO lastSong = artistSongsList.stream()
				.max((s1, s2) -> s1.getLastPlay().compareTo(s2.getLastPlay())).orElse(new ArtistSongsQueryDTO());

		String firstSongPlayed = firstSong.getSong() + " - " + firstSong.getFirstPlay();
		String lastSongPlayed = lastSong.getSong() + " - " + lastSong.getLastPlay();

		int daysArtistWasPlayed = (int) plays.stream().map(s -> s.getPlayDate().substring(0, 10)).distinct().count();
		int weeksArtistWasPlayed = (int) plays.stream().map(s -> s.getWeek()).distinct().count();
		int monthsArtistWasPlayed = (int) plays.stream().map(s -> s.getPlayDate().substring(0, 7)).distinct().count();

		List<Criterion<PlayDTO>> criteria = List.of(new Criterion<>("Release Year",
				play -> String.valueOf(play.getYear()), (o1, o2) -> o1.getKey().compareTo(o2.getKey())));

		ArtistPageDTO artistInfo = new ArtistPageDTO(artistSongsList, artistAlbumsList, firstSongPlayed, lastSongPlayed,
				totalPlays, totalPlaytime, averageSongLength, averagePlaysPerSong, numberOfSongs, daysArtistWasPlayed,
				weeksArtistWasPlayed, monthsArtistWasPlayed, Utils.generateMilestones(plays, 100, 200, 300, 400, 500,
						1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000), playsByAccount);

		model.addAttribute("artist", artist);
		model.addAttribute("includeFeatures", includeFeatures);
		model.addAttribute("artistInfo", artistInfo);
		model.addAttribute("artistGroupList",
				Utils.generateChartData(criteria, plays, numberOfSongs, totalPlaytimeInt));

		Map<String, Integer> chartDataMap = Utils.generateChartDataCumulative(new Criterion<>("Play Year",
				play -> play.getPlayDate().substring(0, 7), (o1, o2) -> o1.getKey().compareTo(o2.getKey())), plays);

		model.addAttribute("chartLabels",
				"'" + chartDataMap.keySet().stream().collect(Collectors.joining("','")) + "'");
		model.addAttribute("chartData",
				chartDataMap.values().stream().map(String::valueOf).collect(Collectors.joining(",")));

		return "artist";
	}

	@GetMapping("/album")
	public String album(Model model, @RequestParam(required = true) String artist,
			@RequestParam(required = true) String album) {

		List<PlayDTO> plays = artistRepository.albumPlays(artist, album);
		List<AlbumSongsQueryDTO> albumSongsList = new ArrayList<>();
		
		List<String> accounts = plays.stream().map(p->p.getAccount()).distinct().toList();

		Map<String, List<PlayDTO>> songGrouping = plays.stream().collect(Collectors.groupingBy(PlayDTO::getSong));
		for (String song : songGrouping.keySet()) {
			List<PlayDTO> sorted = songGrouping.get(song).stream()
					.sorted((sc1, sc2) -> sc1.getPlayDate().compareTo(sc2.getPlayDate())).toList();

			AlbumSongsQueryDTO albumSong = new AlbumSongsQueryDTO();
			albumSong.setArtist(artist);
			albumSong.setAlbum(sorted.get(sorted.size() - 1).getAlbum());
			albumSong.setReleaseYear(sorted.stream().mapToInt(s -> s.getYear()).max().getAsInt());
			albumSong.setSong(song);
			albumSong.setFirstPlay(sorted.get(0).getPlayDate());
			albumSong.setLastPlay(sorted.get(sorted.size() - 1).getPlayDate());
			albumSong.setTotalPlays(songGrouping.get(song).size());
			albumSong.setTrackLength(sorted.get(sorted.size() - 1).getTrackLength());
			albumSong.setDaysSongWasPlayed(
					(int) sorted.stream().map(s -> s.getPlayDate().substring(0, 10)).distinct().count());
			albumSong.setWeeksSongWasPlayed((int) sorted.stream().map(s -> s.getWeek()).distinct().count());
			albumSong.setMonthsSongWasPlayed(
					(int) sorted.stream().map(s -> s.getPlayDate().substring(0, 7)).distinct().count());
			albumSong.setCloudStatus(sorted.get(0).getCloudStatus());
			albumSong.setId(sorted.get(0).getId());
			String playsByAccount="";
			for(String account : accounts) {
				playsByAccount += account+": "+sorted.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
			}
			albumSong.setPlaysByAccount(playsByAccount);
			albumSongsList.add(albumSong);
		}
		albumSongsList.sort((s1, s2) -> s1.getTotalPlays() < s2.getTotalPlays() ? 1
				: s1.getTotalPlays() == s2.getTotalPlays() ? 0 : -1);

		int numberOfSongs = albumSongsList.size();
		int totalPlays = plays.size();
		
		String playsByAccount="";
		for(String account : accounts) {
			playsByAccount += account+": "+plays.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
		}
		
		int sumOfTrackLengths = albumSongsList.stream().mapToInt(s -> s.getTrackLength()).sum();
		double averagePlaysPerSong = (double) totalPlays / numberOfSongs;
		int totalPlaytimeInt = albumSongsList.stream().mapToInt(s -> s.getTotalPlays() * s.getTrackLength()).sum();
		String totalPlaytime = Utils.secondsToString(totalPlaytimeInt);
		String averageSongLength = Utils.secondsToStringColon(sumOfTrackLengths / numberOfSongs);

		AlbumSongsQueryDTO firstSong = albumSongsList.stream()
				.min((s1, s2) -> s1.getFirstPlay().compareTo(s2.getFirstPlay())).orElse(new AlbumSongsQueryDTO());
		AlbumSongsQueryDTO lastSong = albumSongsList.stream()
				.max((s1, s2) -> s1.getLastPlay().compareTo(s2.getLastPlay())).orElse(new AlbumSongsQueryDTO());

		String firstSongPlayed = firstSong.getSong() + " - " + firstSong.getFirstPlay();
		String lastSongPlayed = lastSong.getSong() + " - " + lastSong.getLastPlay();

		int daysAlbumWasPlayed = (int) plays.stream().map(s -> s.getPlayDate().substring(0, 10)).distinct().count();
		int weeksArtistWasPlayed = (int) plays.stream().map(s -> s.getWeek()).distinct().count();
		int monthsAlbumWasPlayed = (int) plays.stream().map(s -> s.getPlayDate().substring(0, 7)).distinct().count();

		AlbumPageDTO albumInfo = new AlbumPageDTO(albumSongsList, firstSongPlayed, lastSongPlayed, totalPlays,
				totalPlaytime, averageSongLength, averagePlaysPerSong, numberOfSongs,
				Utils.secondsToStringColon(sumOfTrackLengths), daysAlbumWasPlayed, weeksArtistWasPlayed,
				monthsAlbumWasPlayed, Utils.generateMilestones(plays, 100, 200, 300, 400, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000),
				playsByAccount);

		model.addAttribute("artist", artist);
		model.addAttribute("album", album);
		model.addAttribute("albumInfo", albumInfo);

		Map<String, Integer> chartDataMap = Utils.generateChartDataCumulative(new Criterion<>("Play Year",
				play -> play.getPlayDate().substring(0, 7), (o1, o2) -> o1.getKey().compareTo(o2.getKey())), plays);

		model.addAttribute("chartLabels",
				"'" + chartDataMap.keySet().stream().collect(Collectors.joining("','")) + "'");
		model.addAttribute("chartData",
				chartDataMap.values().stream().map(String::valueOf).collect(Collectors.joining(",")));

		return "album";
	}

	@GetMapping("/song")
	public String song(Model model, @RequestParam(required = true) String artist,
			@RequestParam(required = true) String album, @RequestParam(required = true) String song) {

		List<PlayDTO> plays = artistRepository.songPlays(artist, album, song);
		List<String> accounts = plays.stream().map(p->p.getAccount()).distinct().toList();

		SongPageDTO songPage = new SongPageDTO();
		songPage.setArtist(artist);
		songPage.setAlbum(album);
		songPage.setSong(song);
		songPage.setFirstPlay(plays.stream().map(s -> s.getPlayDate()).min((s1, s2) -> s1.compareTo(s2)).orElse(""));
		songPage.setLastPlay(plays.stream().map(s -> s.getPlayDate()).max((s1, s2) -> s1.compareTo(s2)).orElse(""));
		songPage.setTotalPlays(plays.size());
		songPage.setTrackLength(plays.stream().findFirst().orElse(new PlayDTO()).getTrackLength());
		songPage.setDaysSongWasPlayed(
				(int) plays.stream().map(s -> s.getPlayDate().substring(0, 10)).distinct().count());
		songPage.setWeeksSongWasPlayed((int) plays.stream().map(s -> s.getWeek()).distinct().count());
		songPage.setMonthsSongWasPlayed(
				(int) plays.stream().map(s -> s.getPlayDate().substring(0, 7)).distinct().count());
		songPage.setMilestones(
				Utils.generateMilestones(plays, 10, 30, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000));
		String playsByAccount="";
		for(String account : accounts) {
			playsByAccount += account+": "+plays.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
		}
		songPage.setPlaysByAccount(playsByAccount);

		model.addAttribute("song", songPage);

		Map<String, Integer> chartDataMap = Utils.generateChartDataCumulative(new Criterion<>("Play Year",
				play -> play.getPlayDate().substring(0, 7), (o1, o2) -> o1.getKey().compareTo(o2.getKey())), plays);

		model.addAttribute("chartLabels",
				"'" + chartDataMap.keySet().stream().collect(Collectors.joining("','")) + "'");
		model.addAttribute("chartData",
				chartDataMap.values().stream().map(String::valueOf).collect(Collectors.joining(",")));

		return "song";
	}

	@GetMapping({ "/deleted" })
	public String deleted(Model model) {
		List<DeletedSongsDTO> deletedSongs = songRepositoryImpl.getDeletedSongs();
		
		model.addAttribute("deletedSongs", deletedSongs);
		
		List<Criterion<DeletedSongsDTO>> criteria = List.of(
				new Criterion<>("Sex", song -> song.getSex(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Genre", song -> song.getGenre(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Race", song -> song.getRace(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Language", song -> song.getLanguage(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Release Year", song -> song.getYear(),
						(o1, o2) -> o2.getKey().compareTo(o1.getKey())));

		List<TopGroupDTO> deletedSongsGroupList = new ArrayList<>();

		for (Criterion<DeletedSongsDTO> criterion : criteria) {
			Map<String, List<DeletedSongsDTO>> classifiedMap = deletedSongs.stream().parallel()
					.collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			List<Entry<String, List<DeletedSongsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for (Entry<String, List<DeletedSongsDTO>> e : sortedList) {
				//int plays = e.getValue().stream().mapToInt(topSong -> Integer.parseInt(topSong.getPlays())).sum();
				counts.add(new TopCountDTO(e.getKey(), e.getValue().size(),
						(double) e.getValue().size() / (double) deletedSongs.size() * 100, 0,0,0,0
						));
			}

			deletedSongsGroupList.add(new TopGroupDTO(criterion.getName(), counts));
		}

		model.addAttribute("deletedSongsGroupList", deletedSongsGroupList);
		

		return "deleted";

	}
	
	@GetMapping({ "/categorySearch" })
	public String categorySearch(Model model) {
		model.addAttribute("sexes", songRepositoryImpl.getAllSexes());
		model.addAttribute("genres", songRepositoryImpl.getAllGenres());
		model.addAttribute("races", songRepositoryImpl.getAllRaces());
		model.addAttribute("languages", songRepositoryImpl.getAllLanguages());
		model.addAttribute("accounts", songRepositoryImpl.getAllAccounts());

		return "categorysearchform";

	}

	@GetMapping({ "/category/{limit}", "/category/{limit}/{category1}/{value1}",
			"/category/{limit}/{category1}/{value1}/{category2}/{value2}",
			"/category/{limit}/{category1}/{value1}/{category2}/{value2}/{category3}/{value3}",
			"/category/{limit}/{category1}/{value1}/{category2}/{value2}/{category3}/{value3}/{category4}/{value4}",
			"/category/{limit}/{category1}/{value1}/{category2}/{value2}/{category3}/{value3}/{category4}/{value4}/{category5}/{value5}",
			"/category/{limit}/{category1}/{value1}/{category2}/{value2}/{category3}/{value3}/{category4}/{value4}/{category5}/{value5}/{category6}/{value6}",
			"/category/{limit}/{category1}/{value1}/{category2}/{value2}/{category3}/{value3}/{category4}/{value4}/{category5}/{value5}/{category6}/{value6}/{category7}/{value7}",
			"/category/{limit}/{category1}/{value1}/{category2}/{value2}/{category3}/{value3}/{category4}/{value4}/{category5}/{value5}/{category6}/{value6}/{category7}/{value7}/{category8}/{value8}"
			})
	public String category(Model model, HttpServletRequest request, @PathVariable(required = true) int limit,
			@PathVariable(required = false) String category1, @PathVariable(required = false) String value1,
			@PathVariable(required = false) String category2, @PathVariable(required = false) String value2,
			@PathVariable(required = false) String category3, @PathVariable(required = false) String value3,
			@PathVariable(required = false) String category4, @PathVariable(required = false) String value4,
			@PathVariable(required = false) String category5, @PathVariable(required = false) String value5,
			@PathVariable(required = false) String category6, @PathVariable(required = false) String value6,
			@PathVariable(required = false) String category7, @PathVariable(required = false) String value7,
			@PathVariable(required = false) String category8, @PathVariable(required = false) String value8,
			@RequestParam(defaultValue = "1970-01-01") String start,
			@RequestParam(defaultValue = "2400-12-31") String end) {

		List<String> categories = new ArrayList<>();
		List<String> values = new ArrayList<>();

		if (category1 != null) {
			categories.add(category1);
			categories.add(category2);
			categories.add(category3);
			categories.add(category4);
			categories.add(category5);
			categories.add(category6);
			categories.add(category7);

			values.add(value1);
			values.add(value2);
			values.add(value3);
			values.add(value4);
			values.add(value5);
			values.add(value6);
			values.add(value7);
		}
		
		int startYearIndex = categories.indexOf("YearStart");
		int endYearIndex = categories.indexOf("YearEnd");
		
		int startYear = startYearIndex < 0 ? 0 : Integer.parseInt(values.get(startYearIndex));
		int endYear = endYearIndex < 0 ? 10000 : Integer.parseInt(values.get(endYearIndex));
		
		if(endYearIndex >= 0) {
			categories.remove(endYearIndex);
			values.remove(endYearIndex);
		}
		if(startYearIndex >= 0) {
			categories.remove(startYearIndex);
			values.remove(startYearIndex);
		}
			
		values.add(start);
		values.add(end);
		categories = categories.stream().filter(c -> c != null).toList();
		values = values.stream().filter(v -> v != null).toList();

		List<PlayDTO> plays = artistRepository.categoryPlays(categories.toArray(String[]::new),
				values.toArray(String[]::new), startYear, endYear);
		
		List<String> accounts = plays.stream().map(p->p.getAccount()).distinct().toList();

		CategoryPageDTO categoryPage = new CategoryPageDTO();

		String categoryValueDisplay = "";
		for (int i = 0; i < categories.size(); i++) {
			if (categories.get(i).equals("Year")) {
				categoryValueDisplay += ("Released " + values.get(i) + " ");
			} else if (categories.get(i).equals("PlayYear")) {
				categoryValueDisplay += ("Played " + values.get(i) + " ");
			} else {
				categoryValueDisplay += (values.get(i) + " ");
			}
		}
		categoryPage.setCategoryValue(categoryValueDisplay);
		categoryPage.setTotalPlays(plays.size());
		
		String playsByAccount="";
		for(String account : accounts) {
			playsByAccount += account+": "+plays.stream().filter(s->s.getAccount().equals(account)).count()+"\n";
		}
		categoryPage.setPlaysByAccount(playsByAccount);
		
		categoryPage.setTotalPlaytime(plays.stream().mapToInt(s -> s.getTrackLength()).sum());
		categoryPage.setDaysCategoryWasPlayed(
				(int) plays.stream().map(s -> s.getPlayDate().substring(0, 10)).distinct().count());
		categoryPage.setWeeksCategoryWasPlayed((int) plays.stream().map(s -> s.getWeek()).distinct().count());
		categoryPage.setMonthsCategoryWasPlayed(
				(int) plays.stream().map(s -> s.getPlayDate().substring(0, 7)).distinct().count());

		// Breakdowns
		Map<String, List<PlayDTO>> map = plays.stream().collect(Collectors.groupingBy(s -> s.getArtist()));
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));

		categoryPage.setMostPlayedArtist(
				sortedList.size() > 0 ? (sortedList.get(0).getKey() + " - " + sortedList.get(0).getValue().size())
						: "");
		categoryPage.setNumberOfArtists(map.entrySet().size());

		map = plays.stream().filter(s -> !s.getAlbum().equals("(single)") && !s.getAlbum().isBlank())
				.collect(Collectors.groupingBy(s -> s.getArtist() + "::" + s.getAlbum()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));
		categoryPage.setMostPlayedAlbum(
				sortedList.size() > 0 ? (sortedList.get(0).getKey() + " - " + sortedList.get(0).getValue().size())
						: "");
		categoryPage.setNumberOfAlbums(map.entrySet().size());

		categoryPage.setMostPlayedAlbums(sortedList.stream().limit(limit).map(e -> {
			ArtistAlbumsQueryDTO album = new ArtistAlbumsQueryDTO();
			album.setArtist(e.getValue().get(0).getArtist());
			album.setAlbum(e.getValue().get(0).getAlbum());
			album.setGenre(e.getValue().get(0).getGenre());
			album.setRace(e.getValue().get(0).getRace());
			album.setSex(e.getValue().get(0).getSex());
			album.setLanguage(e.getValue().get(0).getLanguage());
			album.setReleaseYear(e.getValue().get(0).getYear());
			album.setTotalPlays(e.getValue().size());
			String temp="";
			for(String account : accounts) {
				temp += account+": "+e.getValue().stream().filter(s->s.getAccount().equals(account)).count()+"\n";
			}
			album.setPlaysByAccount(temp);
			return album;
		}).toList());

		map = plays.stream()
				.collect(Collectors.groupingBy(s -> s.getArtist() + "::" + s.getAlbum() + "::" + s.getSong()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
				: (o1.getValue().size() == o2.getValue().size() ? 0 : 1));
		categoryPage.setMostPlayedSong(
				sortedList.size() > 0 ? (sortedList.get(0).getKey() + " - " + sortedList.get(0).getValue().size())
						: "");
		categoryPage.setNumberOfSongs(map.entrySet().size());

		categoryPage.setMostPlayedSongs(sortedList.stream().limit(limit).map(e -> {
			AlbumSongsQueryDTO song = new AlbumSongsQueryDTO();
			song.setArtist(e.getValue().get(0).getArtist());
			song.setAlbum(e.getValue().get(0).getAlbum());
			song.setSong(e.getValue().get(0).getSong());
			song.setGenre(e.getValue().get(0).getGenre());
			song.setRace(e.getValue().get(0).getRace());
			song.setSex(e.getValue().get(0).getSex());
			song.setLanguage(e.getValue().get(0).getLanguage());
			song.setYear(e.getValue().get(0).getYear());
			song.setTotalPlays(e.getValue().size());
			song.setCloudStatus(e.getValue().get(0).getCloudStatus());
			String temp="";
			for(String account : accounts) {
				temp += account+": "+e.getValue().stream().filter(s->s.getAccount().equals(account)).count()+"\n";
			}
			song.setPlaysByAccount(temp);
			return song;
		}).toList());

		categoryPage.setAveragePlaysPerSong(categoryPage.getNumberOfSongs() > 0
				? (double) categoryPage.getTotalPlays() / categoryPage.getNumberOfSongs()
				: 0);
		categoryPage.setAverageSongLength(
				categoryPage.getNumberOfSongs() > 0
						? sortedList.stream().mapToInt(e -> e.getValue().get(0).getTrackLength()).sum()
								/ categoryPage.getNumberOfSongs()
						: 0);

		List<Criterion<PlayDTO>> criteria = List.of(
				new Criterion<>("Sex", play -> play.getSex(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Genre", play -> play.getGenre(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Race", play -> play.getRace(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Language", play -> play.getLanguage(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Release Year", play -> String.valueOf(play.getYear()),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey())),
				new Criterion<>("PlayYear", play -> play.getPlayDate().substring(0, 4),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey())),
				new Criterion<>("Cloud Status", play -> play.getCloudStatus(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1)),
				new Criterion<>("Account", play -> play.getAccount(),
						(o1, o2) -> (o1.getValue()).size() > (o2.getValue()).size() ? -1
								: (o1.getValue().size() == o2.getValue().size() ? 0 : 1))
				);

		model.addAttribute("categories", categories);
		model.addAttribute("category", categoryPage);
		model.addAttribute("categoryGroupList", Utils.generateChartData(criteria, plays,
				categoryPage.getNumberOfSongs(), categoryPage.getTotalPlaytime()));
		model.addAttribute("daysElapsedSinceFirstPlay", daysElapsedSinceFirstPlay);
		model.addAttribute("start", start == null || start.isBlank() || start.equals("1970-01-01") ? "" : start);
		model.addAttribute("end", end == null || end.isBlank() || end.equals("2400-12-31") ? "" : end);
		model.addAttribute("servletPath", request.getServletPath());

		return "category";
	}
	
	@GetMapping({ "/softDeleteSong/{songId}" })
	@ResponseBody
	public String softDeleteSong(Model model, @PathVariable Integer songId) {
		Optional<Song> songOptional = songRepository.findById(songId);
		if(songOptional.isPresent()) {
			Song s = songOptional.get();
			s.setCloudStatus("Deleted");
			
			LocalDateTime now = LocalDateTime.now();
			s.setUpdated(java.sql.Timestamp.valueOf(now));
			songRepository.save(s);
			return "Success!";
		}
		else {
			return "Song not found";
		}
	}
	
	@GetMapping({ "/softUndeleteSong/{songId}" })
	@ResponseBody
	public String softUndeleteSong(Model model, @PathVariable Integer songId) {
		Optional<Song> songOptional = songRepository.findById(songId);
		if(songOptional.isPresent()) {
			Song s = songOptional.get();
			s.setCloudStatus("Apple Music");
			
			LocalDateTime now = LocalDateTime.now();
			s.setUpdated(java.sql.Timestamp.valueOf(now));
			songRepository.save(s);
			return "Success!";
		}
		else {
			return "Song not found";
		}
	}

}