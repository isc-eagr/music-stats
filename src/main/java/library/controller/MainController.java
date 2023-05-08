package library.controller;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.validation.Valid;
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
import library.dto.DataForGraphs;
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
	public String insertScrobbles(Model model) throws FileNotFoundException, IOException{

		String fileName = "c:\\scrobbles.csv";
		FileReader file = new FileReader(fileName);


		List<Scrobble> scrobbles = new CsvToBeanBuilder<Scrobble>(file)
				.withType(Scrobble.class)
				.withSkipLines(1)
				.build()
				.parse();

		List<Song> everySong = songRepository.findAll();

		Song emptySong = new Song();
		List<Song> duplicateSongs = new ArrayList<>();
		scrobbles.stream().parallel().forEach(sc ->{

			List<Song> foundSongs = everySong.stream().parallel().filter(song -> 
			song.getArtist().equalsIgnoreCase(sc.getArtist()) &&
			song.getSong().replace("????","").equalsIgnoreCase(sc.getSong().replace("????", "")) &&
			String.valueOf(song.getAlbum()).equalsIgnoreCase(String.valueOf(sc.getAlbum()))
					).toList();

			//This determines duplicate entries in song
			if(foundSongs.size()>1) {
				duplicateSongs.add(foundSongs.get(0));
			}

			sc.setSongId(foundSongs.stream().findFirst().orElse(emptySong).getId());
		}
				);//forEach

		duplicateSongs.stream().map(s->s.getArtist()+" - "+s.getAlbum()+" - "+s.getSong()).distinct().forEach(System.out::println);

		scrobbleRepository.saveAll(scrobbles);

		file.close();

		return "Success!"+ scrobbles.size()+" scrobbles inserted!";
	}

	@RequestMapping("/insertItunesInfo")
	@ResponseBody
	public String insertItunesInfo(Model model) throws FileNotFoundException, IOException, SAXException, ParserConfigurationException, ParseException{

		File file = new File("c:\\Library.xml");
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(file);
		doc.getDocumentElement().normalize();
		NodeList nodeLst = doc.getElementsByTagName("dict");
		Element dict1 = (Element)nodeLst.item(0);

		NodeList nodeLst2 = dict1.getElementsByTagName("dict");
		Element dict2 = (Element)nodeLst2.item(0);

		//Aqui ya estan todas las canciones
		NodeList nodeList3 = dict2.getElementsByTagName("dict");
		List<Song> everySong = new ArrayList<>(nodeList3.getLength());

		for (int i=0;i<nodeList3.getLength();i++){
			Element song = (Element)nodeList3.item(i);
			NodeList details = song.getChildNodes();
			Song songObject = new Song();

			LocalDateTime now = LocalDateTime.now();

			songObject.setCreated(java.sql.Timestamp.valueOf(now));
			songObject.setUpdated(java.sql.Timestamp.valueOf(now));

			boolean ignore=false;
			for (int j=0;j<details.getLength();j++){
				Node detail = details.item(j);
				if(detail.getNodeName().equals("key")){
					Node valor = detail.getNextSibling();
					if(detail.getChildNodes().item(0)==null){
						continue;
					}

					String property = detail.getChildNodes().item(0).getNodeValue();
					String value = "null";
					try{
						value = valor.getChildNodes().item(0).getNodeValue();
					}
					catch(DOMException | NullPointerException e){}
					switch (property) {
					case "Name":
						songObject.setSong(value);
						break;
					case "Artist":
						songObject.setArtist(value);
						break;
					case "Album":
						songObject.setAlbum(value);
						break;
					case "Grouping":
						songObject.setLanguage(value);
						break;
					case "Genre":
						songObject.setGenre(value);
						break;
					case "Movement Name":
						songObject.setRace(value);
						break;
					case "Total Time":
						songObject.setDuration(Integer.parseInt(value)/1000);
						break;
					case "Year":
						songObject.setYear(value==null?0:Integer.parseInt(value));
						break;
					case "Comments":
						songObject.setSex(value);
						break;

					case "Kind":
						switch (value) {
						case "Matched AAC audio file":
							songObject.setCloudStatus("Matched");
							break;
						case "AAC audio file":
							songObject.setCloudStatus("Uploaded");
							break;
						case "Apple Music AAC audio file":
							songObject.setCloudStatus("No Longer Available");
							break;
						default:
							break;
						}
						break;

					case "Location":
						if(value.toLowerCase().contains(".mp3"))
							songObject.setCloudStatus("Uploaded");
						break;

					case "Playlist Only":
						ignore=true;
						break;

					case "Matched":
						songObject.setCloudStatus("Matched");
						break;

					case "Apple Music":
						songObject.setCloudStatus("Apple Music");
						break;

					case "Purchased":
						if(!String.valueOf(songObject.getCloudStatus()).equals("Uploaded"))
							songObject.setCloudStatus("Purchased");
						break;

					case "Music Video":
						ignore=true;
						break;
					}
				}
			}
			if(!ignore){
				songObject.setSource("iTunes");
				everySong.add(songObject);
			}
		}


		songRepository.saveAll(everySong);

		return "Success! "+everySong.size()+" itunes records inserted!";
	}

	@RequestMapping("/")
	public String index(Model model, @RequestParam(defaultValue="1970-01-01") String start, @RequestParam(defaultValue="2400-12-31") String end) {

		List<PlayDTO> allPlays = scrobbleRepositoryImpl.getPlaysByDateRange(start, end);
		Map<String, List<PlayDTO>> mapPlaysBySong = allPlays.stream().collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong()));


		String firstPlayOn = allPlays.stream().min((s1,s2)->s1.getPlayDate().substring(0,10).compareTo(s2.getPlayDate().substring(0,10))).orElse(new PlayDTO()).getPlayDate();
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

		LocalDate startDate = firstPlayOn == null ? LocalDate.now() : LocalDate.parse(firstPlayOn.substring(0,10), dtf);
		LocalDate today = LocalDate.now();
		daysElapsedSinceFirstPlay = (int)ChronoUnit.DAYS.between(startDate, today)+1;

		model.addAttribute("totalSongs", mapPlaysBySong.entrySet().size());
		model.addAttribute("totalPlays", allPlays.size());
		model.addAttribute("totalPlayTimeGlobalString", 
				Utils.secondsToString(allPlays.stream().parallel()
						.mapToInt(PlayDTO::getTrackLength)
						.sum())
				);
		model.addAttribute("averagePlaysPerDay", (allPlays.size()/(double)daysElapsedSinceFirstPlay));
		model.addAttribute("averageSongLength", Utils.secondsToStringColon((int)mapPlaysBySong.values().stream().mapToInt(s->s.get(0).getTrackLength()).average().orElse(0.0)));
		model.addAttribute("averagePlaysPerSong",mapPlaysBySong.values().stream().mapToInt(s->s.size()).average().orElse(0.0));

		model.addAttribute("firstPlayOn", firstPlayOn);
		model.addAttribute("daysElapsedSinceFirstPlay", (int)daysElapsedSinceFirstPlay);

		//For the main criteria Sex and Genre
		//We need a map with Criteria name (Sex or Genre) as key, and a list with every graph
		//in that criteria
		Map<String, List<DataForGraphs>> dataMap = new LinkedHashMap<>();

		List<Criterion<PlayDTO>> criteria = List.of(new Criterion<>("Sex", song -> song.getSex(), 
				(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Genre", song -> song.getGenre(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Race", song -> song.getRace(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Language", song -> song.getLanguage(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1))
				);

		for (Criterion<PlayDTO> criterion : criteria) {

			//Each element in this list, represents a graph
			List<DataForGraphs> data = new ArrayList<>();

			Map<String,List<PlayDTO>> classifiedMap= allPlays.stream().parallel().collect(Collectors.groupingBy(criterion.groupingBy));

			if(classifiedMap == null || classifiedMap.keySet() == null || classifiedMap.keySet().size()==0) {
				break;
			}

			String numberOfSongsDataMale = "";
			String numberOfSongsLabels = "";
			String numberOfSongsDataOthers = "";

			String numberOfPlaysDataMale = "";
			String numberOfPlaysLabels = "";
			String numberOfPlaysDataOthers = "";

			String playtimeDataMale = "";
			String playtimeLabels = "";
			String playtimeDataOthers = "";

			List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());

			Collections.sort(sortedList, criterion.getSortBy());

			//These loops iterate through every secondary criteria (R&B Reggaeton etc)
			for(Entry<String, List<PlayDTO>> e : sortedList) {
				Map<Boolean, List<PlayDTO>> partitionedByMale= e.getValue().stream().collect(Collectors.partitioningBy(s->s.getSex().equals("Male")));
				numberOfPlaysDataMale += partitionedByMale.get(Boolean.TRUE).size()+",";
				numberOfPlaysDataOthers += partitionedByMale.get(Boolean.FALSE).size()+",";
				numberOfPlaysLabels += ("'"+e.getKey()+"',");
			}

			Collections.sort(sortedList, (o1, o2) -> 
			o1.getValue().stream().map(p->p.getArtist()+"::"+p.getAlbum()+"::"+p.getSong()).distinct().count()>
			o2.getValue().stream().map(p->p.getArtist()+"::"+p.getAlbum()+"::"+p.getSong()).distinct().count()?-1:1
					);
			for(Entry<String, List<PlayDTO>> e : sortedList) {
				Map<Boolean, List<PlayDTO>> partitionedByMale= e.getValue().stream().collect(Collectors.partitioningBy(s->s.getSex().equals("Male")));
				numberOfSongsDataMale += partitionedByMale.get(Boolean.TRUE).stream().map(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong()).distinct().count()+",";
				numberOfSongsDataOthers += partitionedByMale.get(Boolean.FALSE).stream().map(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong()).distinct().count()+",";
				numberOfSongsLabels += ("'"+e.getKey()+"',");
			}

			Collections.sort(sortedList, (o1, o2) -> 
			o1.getValue().stream().mapToLong(p->p.getTrackLength()).sum()>
			o2.getValue().stream().mapToLong(p->p.getTrackLength()).sum()?-1:1
					);
			for(Entry<String, List<PlayDTO>> e : sortedList) {
				Map<Boolean, List<PlayDTO>> partitionedByMale= e.getValue().stream().collect(Collectors.partitioningBy(s->s.getSex().equals("Male")));
				playtimeDataMale += partitionedByMale.get(Boolean.TRUE).stream().mapToLong(s->s.getTrackLength()).sum()+",";
				playtimeDataOthers += partitionedByMale.get(Boolean.FALSE).stream().mapToLong(s->s.getTrackLength()).sum()+",";
				playtimeLabels += ("'"+e.getKey()+"',");	
			}

			DataForGraphs dataNumberSongs = new DataForGraphs(numberOfSongsDataMale.substring(0,numberOfSongsDataMale.length()-1), 
					numberOfSongsDataOthers.substring(0,numberOfSongsDataOthers.length()-1), numberOfSongsLabels.substring(0,numberOfSongsLabels.length()-1), "NumberOfSongs");

			DataForGraphs dataNumberPlays = new DataForGraphs(numberOfPlaysDataMale.substring(0,numberOfPlaysDataMale.length()-1), 
					numberOfPlaysDataOthers.substring(0,numberOfPlaysDataOthers.length()-1), numberOfPlaysLabels.substring(0,numberOfPlaysLabels.length()-1), "NumberOfPlays");

			DataForGraphs dataPlaytime = new DataForGraphs(playtimeDataMale.substring(0,playtimeDataMale.length()-1), 
					playtimeDataOthers.substring(0,playtimeDataOthers.length()-1), playtimeLabels.substring(0,playtimeLabels.length()-1), "Playtime");

			data.add(dataNumberSongs);
			data.add(dataNumberPlays);
			data.add(dataPlaytime);

			//Each element in the map is a criteria (Genre, Sex, Year etc.)
			dataMap.put(criterion.getName(), data);
		}

		String firstPlayedYear = allPlays.stream().map(s->s.getPlayDate()).min((s1, s2) -> s1.compareTo(s2)).orElse("");
		firstPlayedYear = firstPlayedYear.isBlank() ? "" : firstPlayedYear.substring(0, 4);
		List<Integer> years = firstPlayedYear.isBlank()? List.of() : IntStream.rangeClosed(Integer.parseInt(firstPlayedYear), LocalDate.now().getYear()).boxed().toList();

		Map<String,Long> playsByYearForChart = allPlays.stream().collect(Collectors.groupingBy(s->s.getPlayDate().substring(0,4),Collectors.counting()));
		for(Integer year : years) {
			if(!playsByYearForChart.containsKey(String.valueOf(year))) {
				playsByYearForChart.put(String.valueOf(year), 0L);
			}
		}
		List<Entry<String, Long>> sortedList = new ArrayList<>(playsByYearForChart.entrySet());
		Collections.sort(sortedList, (o1, o2) -> o1.getKey().compareTo(o2.getKey()));

		String chartLabels=sortedList.stream().map(e->e.getKey()).collect(Collectors.joining(","));
		String chartValues=sortedList.stream().map(e->String.valueOf(e.getValue())).collect(Collectors.joining(","));

		model.addAttribute("dataMap",dataMap);
		model.addAttribute("chartLabels",chartLabels);
		model.addAttribute("chartValues",chartValues);
		return "main";
	}// method

	@RequestMapping("/topSongs")
	public String topSongs(Model model, @RequestParam(defaultValue="10000000") int limit) {

		List<TopSongsDTO> topSongs = songRepositoryImpl.getTopSongs(limit);
		model.addAttribute("topSongs", topSongs);

		List<Criterion<TopSongsDTO>> criteria = List.of(new Criterion<>("Sex", song -> song.getSex(),
				(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Genre", song -> song.getGenre(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Race", song -> song.getRace(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Language", song -> song.getLanguage(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Release Year", song -> song.getYear(),
						(o1, o2) -> o2.getKey().compareTo(o1.getKey()))
				);


		List<TopGroupDTO> topSongsGroupList = new ArrayList<>(); 

		for (Criterion<TopSongsDTO> criterion : criteria) {
			Map<String,List<TopSongsDTO>> classifiedMap= topSongs.stream().parallel().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			List<Entry<String, List<TopSongsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<TopSongsDTO>> e : sortedList) {
				int plays = e.getValue().stream().mapToInt(topSong -> Integer.parseInt(topSong.getCount())).sum();
				long playtime = e.getValue().stream().mapToLong(topSong -> topSong.getPlaytime()).sum();
				counts.add(new TopCountDTO(
						e.getKey(), 
						e.getValue().size(), 
						(double)e.getValue().size()/(double)topSongs.size()*100,
						plays,
						(double)plays*100/topSongs.stream().mapToDouble(a->Double.parseDouble(a.getCount())).sum(),
						playtime,
						(double)playtime*100/topSongs.stream().mapToDouble(a->a.getPlaytime()).sum()));
			}

			topSongsGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		model.addAttribute("topSongsGroupList", topSongsGroupList);

		return "topsongs";

	}

	@RequestMapping("/topArtists")
	public String topArtists(Model model, @RequestParam(defaultValue="10000000") int limit) {

		List<TopArtistsDTO> topArtists = songRepositoryImpl.getTopArtists(limit);

		model.addAttribute("topArtists", topArtists);

		List<Criterion<TopArtistsDTO>> criteria = List.of(new Criterion<>("Sex", artist -> artist.getSex(),
				(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Genre", artist -> artist.getGenre(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Race", artist -> artist.getRace(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Language", artist -> artist.getLanguage(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1))
				);


		List<TopGroupDTO> topArtistsGroupList = new ArrayList<>(); 

		for (Criterion<TopArtistsDTO> criterion : criteria) {
			Map<String,List<TopArtistsDTO>> classifiedMap= topArtists.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			List<Entry<String, List<TopArtistsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<TopArtistsDTO>> e : sortedList) {
				int plays = e.getValue().stream().mapToInt(topSong -> Integer.parseInt(topSong.getCount())).sum();
				long playtime = e.getValue().stream().mapToLong(topArtist -> topArtist.getPlaytime()).sum();
				counts.add(new TopCountDTO(
						e.getKey(), 
						e.getValue().size(), 
						(double)e.getValue().size()/(double)topArtists.size()*100,
						plays,
						(double)plays*100/topArtists.stream().mapToDouble(a->Double.parseDouble(a.getCount())).sum(),
						playtime,
						(double)playtime*100/topArtists.stream().mapToDouble(a->a.getPlaytime()).sum()));
			}

			topArtistsGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		model.addAttribute("topArtistsGroupList", topArtistsGroupList);

		return "topartists";

	}

	@RequestMapping("/topAlbums")
	public String topAlbums(Model model, @RequestParam(defaultValue="10000000") int limit) {

		List<TopAlbumsDTO> topAlbums = songRepositoryImpl.getTopAlbums(limit);
		model.addAttribute("topAlbums", topAlbums);

		List<Criterion<TopAlbumsDTO>> criteria = List.of(new Criterion<>("Sex", song -> song.getSex(),
				(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Genre", song -> song.getGenre(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Race", song -> song.getRace(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Language", song -> song.getLanguage(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Release Year", song -> song.getYear(),
						(o1, o2) -> o2.getKey().compareTo(o1.getKey()))
				);


		List<TopGroupDTO> topAlbumsGroupList = new ArrayList<>(); 

		for (Criterion<TopAlbumsDTO> criterion : criteria) {
			Map<String,List<TopAlbumsDTO>> classifiedMap= topAlbums.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			List<Entry<String, List<TopAlbumsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<TopAlbumsDTO>> e : sortedList) {
				int plays = e.getValue().stream().mapToInt(topSong -> Integer.parseInt(topSong.getCount())).sum();
				long playtime = e.getValue().stream().mapToLong(topAlbum -> topAlbum.getPlaytime()).sum();
				counts.add(new TopCountDTO(e.getKey(), 
						e.getValue().size(), 
						(double)e.getValue().size()/(double)topAlbums.size()*100,
						plays,
						(double)plays*100/topAlbums.stream().mapToDouble(a->Double.parseDouble(a.getCount())).sum(),
						playtime,
						(double)playtime*100/topAlbums.stream().mapToDouble(a->a.getPlaytime()).sum()
						));
			}

			topAlbumsGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		model.addAttribute("topAlbumsGroupList", topAlbumsGroupList);

		return "topalbums";

	}

	@RequestMapping("/topGenres")
	public String topGenres(Model model, @RequestParam(defaultValue="10000000") int limit) {
		List<TopGenresDTO> topGenres = songRepositoryImpl.getTopGenres(limit);
		model.addAttribute("topGenres", topGenres);

		return "topGenres";

	}

	@RequestMapping("/songsLastFmButNotLocal")
	public String songsLastFmButNotLocal(Model model) {
		model.addAttribute("listSongs", scrobbleRepositoryImpl.songsInLastFmButNotLocal());
		return "songsLastFmButNotLocal";

	}

	@RequestMapping("/songsLocalButNotLastfm")
	public String songsLocalButNotLastFm(Model model) {
		model.addAttribute("listSongs", songRepositoryImpl.songsLocalButNotLastfm());
		return "songsLocalButNotLastfm";

	}

	@RequestMapping("/timeUnit")
	public String timeUnit(Model model, 
			@RequestParam(defaultValue="1970-01-01") String start, 
			@RequestParam(defaultValue="2400-12-31") String end,
			@RequestParam String unit) {
		List<TimeUnitStatsDTO> timeUnits = songRepositoryImpl.timeUnitStats(start, end, unit);

		model.addAttribute("timeUnits", timeUnits);

		List<TopGroupDTO> timeUnitGroupList = new ArrayList<>();
		List<TopCountDTO> counts = new ArrayList<>();

		Map<String,List<TimeUnitStatsDTO>> mapGenre = timeUnits.stream().collect(Collectors.groupingBy(ww -> ww.getGenre()));

		List<Entry<String, List<TimeUnitStatsDTO>>> sortedGenreList = new ArrayList<>(mapGenre.entrySet());
		Collections.sort(sortedGenreList, (o1,o2)-> ((o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)));

		for(Entry<String, List<TimeUnitStatsDTO>> e : sortedGenreList) {
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)timeUnits.size()*100,0,0,0,0));
		}
		timeUnitGroupList.add(new TopGroupDTO("Genre",counts));

		counts = new ArrayList<>();
		Map<String,List<TimeUnitStatsDTO>> mapRace = timeUnits.stream().collect(Collectors.groupingBy(ww -> ww.getRace()));

		List<Entry<String, List<TimeUnitStatsDTO>>> sortedRaceList = new ArrayList<>(mapRace.entrySet());
		Collections.sort(sortedRaceList, (o1,o2)->(o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));

		for(Entry<String, List<TimeUnitStatsDTO>> e : sortedRaceList) {
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)timeUnits.size()*100,0,0,0,0));
		}

		timeUnitGroupList.add(new TopGroupDTO("Race",counts));

		counts = new ArrayList<>();
		Map<String,List<TimeUnitStatsDTO>> mapSex = timeUnits.stream().collect(Collectors.groupingBy(ww -> ww.getSex()));

		List<Entry<String, List<TimeUnitStatsDTO>>> sortedSexList = new ArrayList<>(mapSex.entrySet());
		Collections.sort(sortedSexList, (o1,o2)->(o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));

		for(Entry<String, List<TimeUnitStatsDTO>> e : sortedSexList) {
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)timeUnits.size()*100,0,0,0,0));
		}

		timeUnitGroupList.add(new TopGroupDTO("Sex",counts));

		model.addAttribute("timeUnitGroupList", timeUnitGroupList);
		model.addAttribute("unit", unit);
		model.addAttribute("top", switch(unit) {
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
	public String timeUnitDetail(Model model, 
			@PathVariable String unit,
			@PathVariable String unitValue,
			@PathVariable Integer top) {

		List<PlayDTO> plays = switch(unit) {
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
		timeUnitDetailDTO.setTotalPlaytime(plays.stream().mapToInt(s->s.getTrackLength()).sum());

		timeUnitDetailDTO.setPercentageofUnitWhereMusicWasPlayed(
				switch(unit) {
				case "day" -> (double)timeUnitDetailDTO.getTotalPlaytime()*100/(double)Utils.SECONDS_IN_A_DAY;
				case "week" -> (double)timeUnitDetailDTO.getTotalPlaytime()*100/(double)Utils.SECONDS_IN_A_WEEK;
				case "month" -> (double)timeUnitDetailDTO.getTotalPlaytime()*100/(double)Utils.secondsInAMonth(unitValue.split("-")[1],Integer.parseInt(unitValue.split("-")[0]));
				case "season" -> (double)timeUnitDetailDTO.getTotalPlaytime()*100/(double)Utils.secondsInASeason(unitValue.substring(4),Integer.parseInt(unitValue.substring(0,4)));
				case "year" -> (double)timeUnitDetailDTO.getTotalPlaytime()*100/(double)Utils.secondsInAYear(Integer.parseInt(unitValue));
				case "decade" -> (double)timeUnitDetailDTO.getTotalPlaytime()*100/(double)Utils.secondsInADecade(Integer.parseInt(unitValue.substring(0,4)));
				default -> 0.0;
				}
				);

		Map<String, List<PlayDTO>> map = plays.stream().collect(Collectors.groupingBy(s->s.getArtist()));
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));

		timeUnitDetailDTO.setMostPlayedArtist(sortedList.get(0).getKey()+" - "+sortedList.get(0).getValue().size());
		timeUnitDetailDTO.setUniqueArtistsPlayed(map.entrySet().size());

		map = plays.stream().filter(s->!s.getAlbum().equals("(single)")).collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));
		timeUnitDetailDTO.setMostPlayedAlbum(sortedList.get(0).getKey()+" - "+sortedList.get(0).getValue().size());
		timeUnitDetailDTO.setUniqueAlbumsPlayed(map.entrySet().size());

		//Group by song and sort by most played
		map = plays.stream().collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));
		timeUnitDetailDTO.setMostPlayedSong(sortedList.get(0).getKey()+" - "+sortedList.get(0).getValue().size());

		timeUnitDetailDTO.setMostPlayedSongs(sortedList.stream().limit(top).map(e->{
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
			return song;
		}).toList());

		timeUnitDetailDTO.setUniqueSongsPlayed(map.entrySet().size());

		List<Criterion<PlayDTO>> criteria = List.of(new Criterion<>("Sex", play -> play.getSex(),
				(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Genre", play -> play.getGenre(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Race", play -> play.getRace(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Language", play -> play.getLanguage(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Release Year", play -> String.valueOf(play.getYear()),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey()))
				);


		List<TopGroupDTO> timeUnitGroupList = new ArrayList<>(); 

		for (Criterion<PlayDTO> criterion : criteria) {
			Map<String,List<PlayDTO>> classifiedMap= plays.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<PlayDTO>> e : sortedList) {
				int uniqueSongs = e.getValue().stream().collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong())).entrySet().size();
				int uniqueSongsMale = e.getValue().stream().filter(s->s.getSex().equals("Male")).collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong())).entrySet().size();
				int playsMale = (int)e.getValue().stream().filter(s->s.getSex().equals("Male")).count();
				long playtime = e.getValue().stream().mapToInt(play -> play.getTrackLength()).sum();
				long playtimeMale = e.getValue().stream().filter(s->s.getSex().equals("Male")).mapToInt(play -> play.getTrackLength()).sum();
				counts.add(new TopCountDTO(
						e.getKey(), 
						uniqueSongs,
						uniqueSongsMale,//numberSongsMale
						(double)uniqueSongs/(double)timeUnitDetailDTO.getUniqueSongsPlayed()*100,
						(double)uniqueSongsMale/(double)uniqueSongs*100,//percentageSongsMale
						e.getValue().size(), 
						playsMale,//playsMale
						(double)e.getValue().size()/(double)plays.size()*100,
						(double)playsMale/(double)e.getValue().size()*100,//percentagePlaysMale
						playtime,
						playtimeMale,//playtimeMale
						(double)playtime*100/(double)timeUnitDetailDTO.getTotalPlaytime(),
						(double)playtimeMale/(double)playtime*100));//percentagePlaytimeMale
			}
			timeUnitGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		model.addAttribute("timeUnitGroupList", timeUnitGroupList);
		model.addAttribute("timeUnitDetail",timeUnitDetailDTO);
		model.addAttribute("unit",unit);
		model.addAttribute("unitValue",unitValue);

		return "timeunitdetail";

	}

	@RequestMapping(value={"/insertSongForm","/insertSongForm/{artist}/{song}","/insertSongForm/{artist}/{song}/{album}"})
	public String insertSongForm(Model model, 
			@PathVariable(required=false) String artist,
			@PathVariable(required=false) String song,
			@PathVariable(required=false) String album) {

		if(artist != null && song != null) {
			Song s = new Song();
			s.setArtist(artist);
			s.setSong(song);
			s.setAlbum(album);
			model.addAttribute("songForm",s);
		}else {
			model.addAttribute("songForm",new Song());
		}
		return "insertsongform";
	}

	@RequestMapping("/insertSong")
	public String insertSong(@Valid @ModelAttribute(value="songForm") Song songForm, BindingResult result, Model model) {
		if (result.hasErrors()) {
			return "insertsongform";
		}

		songForm.setSource("Manual");
		songForm.setCloudStatus("Deleted");

		String duration[] = songForm.getDurationString().split(":");
		songForm.setDuration(Integer.parseInt(duration[0])*60+Integer.parseInt(duration[1]));

		LocalDateTime now = LocalDateTime.now();

		songForm.setCreated(java.sql.Timestamp.valueOf(now));
		songForm.setUpdated(java.sql.Timestamp.valueOf(now));

		songRepository.save(songForm);
		scrobbleRepositoryImpl.updateSongIds(songForm.getId(), songForm.getArtist(), songForm.getSong(), songForm.getAlbum());
		songForm = new Song();
		model.addAttribute("success",true);
		return "insertsongform";
	}

	@RequestMapping({"/insertAlbumForm/{artist}","/insertAlbumForm/{artist}/{album}"})
	public String insertAlbumForm(Model model, 
			@PathVariable(required=false) String artist,
			@PathVariable(required=false) String album) {

		List<Song> songsList = scrobbleRepositoryImpl.songsFromAlbum(artist, album == null ? "" : album);

		SaveAlbumDTO form = new SaveAlbumDTO();
		form.setSongs(songsList);

		model.addAttribute("form",form);

		return "insertalbumform";
	}

	@RequestMapping("/insertAlbum")
	public String insertAlbum(@ModelAttribute(value="form") SaveAlbumDTO form, Model model) {

		for(Song s : form.getSongs()) {
			s.setSource("Manual");
			s.setCloudStatus("Deleted");
			String duration[] = s.getDurationString().split(":");
			s.setDuration(Integer.parseInt(duration[0])*60+Integer.parseInt(duration[1]));

			LocalDateTime now = LocalDateTime.now();

			s.setCreated(java.sql.Timestamp.valueOf(now));
			s.setUpdated(java.sql.Timestamp.valueOf(now));

			songRepository.save(s);
			scrobbleRepositoryImpl.updateSongIds(s.getId(), s.getArtist(), s.getSong(), s.getAlbum());
		}

		model.addAttribute("success",true);
		return "insertalbumform";
	}

	@RequestMapping("/artist")
	public String artist(Model model, @RequestParam(required=true) String artist) {

		List<PlayDTO> plays = artistRepository.artistPlays(artist);
		List<ArtistSongsQueryDTO> artistSongsList = new ArrayList<>();
		List<ArtistAlbumsQueryDTO> artistAlbumsList = new ArrayList<>();

		Map<String, List<PlayDTO>> songGrouping = plays.stream().collect(Collectors.groupingBy(PlayDTO::getSong));
		for(String song : songGrouping.keySet()) {
			List<PlayDTO> sorted = songGrouping.get(song).stream()
					.sorted((sc1, sc2)->sc1.getPlayDate().compareTo(sc2.getPlayDate()))
					.toList();

			ArtistSongsQueryDTO artistSong = new ArtistSongsQueryDTO();
			artistSong.setArtist(artist);
			artistSong.setAlbum(sorted.get(0).getAlbum());
			artistSong.setSong(song);
			artistSong.setReleaseYear(sorted.stream().mapToInt(s->s.getYear()).max().getAsInt());
			artistSong.setFirstPlay(sorted.get(0).getPlayDate());
			artistSong.setLastPlay(sorted.get(sorted.size()-1).getPlayDate());
			artistSong.setTotalPlays(songGrouping.get(song).size());
			artistSong.setTrackLength(sorted.get(sorted.size()-1).getTrackLength());
			artistSong.setDaysSongWasPlayed((int)sorted.stream().map(s->s.getPlayDate().substring(0, 10)).distinct().count());
			artistSong.setWeeksSongWasPlayed((int)sorted.stream().map(s->s.getWeek()).distinct().count()); 
			artistSong.setMonthsSongWasPlayed((int)sorted.stream().map(s->s.getPlayDate().substring(0, 7)).distinct().count());
			artistSongsList.add(artistSong);
		}
		artistSongsList.sort((s1,s2)->s1.getTotalPlays()<s2.getTotalPlays()?1:s1.getTotalPlays()==s2.getTotalPlays()?0:-1);

		Map<String, List<PlayDTO>> albumGrouping = plays.stream().collect(Collectors.groupingBy(PlayDTO::getAlbum));
		for(String album : albumGrouping.keySet()) {
			List<PlayDTO> sorted = albumGrouping.get(album).stream()
					.sorted((sc1, sc2)->sc1.getPlayDate().compareTo(sc2.getPlayDate()))
					.toList();

			List<PlayDTO> distinct = sorted.stream().distinct().toList();

			ArtistAlbumsQueryDTO artistAlbum = new ArtistAlbumsQueryDTO();
			artistAlbum.setArtist(artist);
			artistAlbum.setAlbum(album);
			//Gets the release year with most ocurrences, as some albums have songs with different release years if a song was a single before being on the album
			artistAlbum.setReleaseYear(sorted.stream().collect(Collectors.groupingBy(s->s.getYear(),Collectors.counting())).entrySet().stream().sorted((e1,e2)->-e1.getValue().compareTo(e2.getValue())).findFirst().get().getKey());
			artistAlbum.setFirstPlay(sorted.get(0).getPlayDate());
			artistAlbum.setLastPlay(sorted.get(sorted.size()-1).getPlayDate());
			artistAlbum.setTotalPlays(albumGrouping.get(album).size());
			artistAlbum.setTotalPlaytime(sorted.stream().mapToInt(s->s.getTrackLength()).sum());
			artistAlbum.setAlbumLength(distinct.stream().mapToInt(s->s.getTrackLength()).sum());
			artistAlbum.setNumberOfTracks(distinct.size());
			artistAlbum.setAverageSongLength((int)distinct.stream().mapToInt(s->s.getTrackLength()).average().orElse(0.0));
			artistAlbum.setAveragePlaysPerSong((double)artistAlbum.getTotalPlays()/artistAlbum.getNumberOfTracks());
			artistAlbum.setDaysAlbumWasPlayed((int)sorted.stream().map(s->s.getPlayDate().substring(0, 10)).distinct().count());
			artistAlbum.setWeeksAlbumWasPlayed((int)sorted.stream().map(s->s.getWeek()).distinct().count());
			artistAlbum.setMonthsAlbumWasPlayed((int)sorted.stream().map(s->s.getPlayDate().substring(0, 7)).distinct().count());
			artistAlbumsList.add(artistAlbum);
		}
		artistAlbumsList.sort((s1,s2)->s1.getTotalPlays()<s2.getTotalPlays()?1:s1.getTotalPlays()==s2.getTotalPlays()?0:-1);

		int numberOfSongs = artistSongsList.size();
		int totalPlays = artistSongsList.stream().mapToInt(s -> s.getTotalPlays()).sum();
		int sumOfTrackLengths = artistSongsList.stream().mapToInt(s -> s.getTrackLength()).sum();
		double averagePlaysPerSong = (double)totalPlays/numberOfSongs;
		int totalPlaytimeInt = artistSongsList.stream().mapToInt(s ->s.getTotalPlays()*s.getTrackLength()).sum();
		String totalPlaytime = Utils.secondsToString(totalPlaytimeInt);
		String averageSongLength = Utils.secondsToStringColon(sumOfTrackLengths/numberOfSongs);

		ArtistSongsQueryDTO firstSong = artistSongsList.stream().min((s1, s2) -> s1.getFirstPlay().compareTo(s2.getFirstPlay())).orElse(new ArtistSongsQueryDTO());
		ArtistSongsQueryDTO lastSong = artistSongsList.stream().max((s1, s2) -> s1.getLastPlay().compareTo(s2.getLastPlay())).orElse(new ArtistSongsQueryDTO());

		String firstSongPlayed = firstSong.getSong() + " - " +firstSong.getFirstPlay();
		String lastSongPlayed = lastSong.getSong() + " - " +lastSong.getLastPlay();

		int daysArtistWasPlayed = (int)plays.stream().map(s->s.getPlayDate().substring(0, 10)).distinct().count();
		int weeksArtistWasPlayed = (int)plays.stream().map(s->s.getWeek()).distinct().count();  
		int monthsArtistWasPlayed = (int)plays.stream().map(s->s.getPlayDate().substring(0, 7)).distinct().count();


		//Charts
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>();

		List<Criterion<PlayDTO>> criteria = List.of(
				new Criterion<>("Release Year", play -> String.valueOf(play.getYear()),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey())),
				new Criterion<>("Play Year", play -> play.getPlayDate().substring(0,4),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey()))
				);

		List<TopGroupDTO> artistGroupList = new ArrayList<>(); 

		for (Criterion<PlayDTO> criterion : criteria) {
			Map<String,List<PlayDTO>> classifiedMap= plays.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<PlayDTO>> e : sortedList) {
				int uniqueSongs = e.getValue().stream().collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong())).entrySet().size();
				long playtime = e.getValue().stream().mapToInt(play -> play.getTrackLength()).sum();
				counts.add(new TopCountDTO(
						e.getKey(), 
						uniqueSongs, //number of songs
						0,//number of songs male
						(double)uniqueSongs/(double)numberOfSongs*100, //percentage of songs
						0.0, //percentage of songs male
						e.getValue().size(), //plays
						0,//playsMale
						(double)e.getValue().size()/(double)plays.size()*100, //percentage of plays
						0.0,//percentagePlaysMale
						playtime, //playtime
						0,//playtimeMale
						(double)playtime*100/(double)totalPlaytimeInt,//percentage of playtime
						0.0));//percentagePlaytimeMale 

			}

			artistGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		ArtistPageDTO artistInfo = new ArtistPageDTO(artistSongsList, artistAlbumsList, firstSongPlayed, lastSongPlayed, totalPlays, totalPlaytime, 
				averageSongLength,averagePlaysPerSong, numberOfSongs, daysArtistWasPlayed, weeksArtistWasPlayed, monthsArtistWasPlayed);

		model.addAttribute("artist",artist);
		model.addAttribute("artistInfo",artistInfo);
		model.addAttribute("artistGroupList", artistGroupList);

		return "artist";
	}

	@GetMapping("/album")
	public String album(Model model, @RequestParam(required=true) String artist,
			@RequestParam(required=true) String album) {

		List<PlayDTO> plays = artistRepository.albumPlays(artist, album);
		List<AlbumSongsQueryDTO> albumSongsList = new ArrayList<>();

		Map<String, List<PlayDTO>> songGrouping = plays.stream().collect(Collectors.groupingBy(PlayDTO::getSong));
		for(String song : songGrouping.keySet()) {
			List<PlayDTO> sorted = songGrouping.get(song).stream()
					.sorted((sc1, sc2)->sc1.getPlayDate().compareTo(sc2.getPlayDate()))
					.toList();

			AlbumSongsQueryDTO albumSong = new AlbumSongsQueryDTO();
			albumSong.setArtist(artist);
			albumSong.setAlbum(sorted.get(sorted.size()-1).getAlbum());
			albumSong.setReleaseYear(sorted.stream().mapToInt(s->s.getYear()).max().getAsInt());
			albumSong.setSong(song);
			albumSong.setFirstPlay(sorted.get(0).getPlayDate());
			albumSong.setLastPlay(sorted.get(sorted.size()-1).getPlayDate());
			albumSong.setTotalPlays(songGrouping.get(song).size());
			albumSong.setTrackLength(sorted.get(sorted.size()-1).getTrackLength());
			albumSong.setDaysSongWasPlayed((int)sorted.stream().map(s->s.getPlayDate().substring(0, 10)).distinct().count());
			albumSong.setWeeksSongWasPlayed((int)sorted.stream().map(s->s.getWeek()).distinct().count());
			albumSong.setMonthsSongWasPlayed((int)sorted.stream().map(s->s.getPlayDate().substring(0, 7)).distinct().count());
			albumSongsList.add(albumSong);
		}
		albumSongsList.sort((s1,s2)->s1.getTotalPlays()<s2.getTotalPlays()?1:s1.getTotalPlays()==s2.getTotalPlays()?0:-1);

		int numberOfSongs = albumSongsList.size();
		int totalPlays = albumSongsList.stream().mapToInt(s -> s.getTotalPlays()).sum();
		int sumOfTrackLengths = albumSongsList.stream().mapToInt(s -> s.getTrackLength()).sum();
		double averagePlaysPerSong = (double)totalPlays/numberOfSongs;
		int totalPlaytimeInt = albumSongsList.stream().mapToInt(s ->s.getTotalPlays()*s.getTrackLength()).sum();
		String totalPlaytime = Utils.secondsToString(totalPlaytimeInt);
		String averageSongLength = Utils.secondsToStringColon(sumOfTrackLengths/numberOfSongs);

		AlbumSongsQueryDTO firstSong = albumSongsList.stream().min((s1, s2) -> s1.getFirstPlay().compareTo(s2.getFirstPlay())).orElse(new AlbumSongsQueryDTO());
		AlbumSongsQueryDTO lastSong = albumSongsList.stream().max((s1, s2) -> s1.getLastPlay().compareTo(s2.getLastPlay())).orElse(new AlbumSongsQueryDTO());

		String firstSongPlayed = firstSong.getSong() + " - " +firstSong.getFirstPlay();
		String lastSongPlayed = lastSong.getSong() + " - " +lastSong.getLastPlay();

		int daysAlbumWasPlayed = (int)plays.stream().map(s->s.getPlayDate().substring(0, 10)).distinct().count();
		int weeksArtistWasPlayed = (int)plays.stream().map(s->s.getWeek()).distinct().count();  
		int monthsAlbumWasPlayed = (int)plays.stream().map(s->s.getPlayDate().substring(0, 7)).distinct().count();

		//Charts
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>();

		List<Criterion<PlayDTO>> criteria = List.of(
				new Criterion<>("Play Year", play -> play.getPlayDate().substring(0,4),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey()))
				);

		List<TopGroupDTO> albumGroupList = new ArrayList<>(); 

		for (Criterion<PlayDTO> criterion : criteria) {
			Map<String,List<PlayDTO>> classifiedMap= plays.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<PlayDTO>> e : sortedList) {
				long playtime = e.getValue().stream().mapToInt(play -> play.getTrackLength()).sum();
				counts.add(new TopCountDTO(
						e.getKey(), 
						0, //number of songs
						0,//number of songs male
						0.0, //percentage of songs
						0.0, //percentage of songs male
						e.getValue().size(), //plays
						0,//playsMale
						(double)e.getValue().size()/(double)plays.size()*100, //percentage of plays
						0.0,//percentagePlaysMale
						playtime, //playtime
						0,//playtimeMale
						(double)playtime*100/(double)totalPlaytimeInt,//percentage of playtime
						0.0));//percentagePlaytimeMale 

			}

			albumGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		AlbumPageDTO albumInfo = new AlbumPageDTO(albumSongsList, firstSongPlayed, lastSongPlayed, totalPlays, totalPlaytime, 
				averageSongLength,averagePlaysPerSong, numberOfSongs, Utils.secondsToStringColon(sumOfTrackLengths),
				daysAlbumWasPlayed, weeksArtistWasPlayed, monthsAlbumWasPlayed);

		model.addAttribute("artist",artist);
		model.addAttribute("album",album);
		model.addAttribute("albumInfo",albumInfo);
		model.addAttribute("albumGroupList", albumGroupList);

		return "album";
	}

	@GetMapping("/song")
	public String song(Model model, @RequestParam(required=true) String artist,
			@RequestParam(required=true) String album,
			@RequestParam(required=true) String song) {

		List<PlayDTO> plays = artistRepository.songPlays(artist, album, song);

		SongPageDTO songPage = new SongPageDTO();
		songPage.setArtist(artist);
		songPage.setAlbum(album);
		songPage.setSong(song);
		songPage.setFirstPlay(plays.stream().map(s->s.getPlayDate()).min((s1, s2) -> s1.compareTo(s2)).orElse(""));
		songPage.setLastPlay(plays.stream().map(s->s.getPlayDate()).max((s1, s2) -> s1.compareTo(s2)).orElse(""));
		songPage.setTotalPlays(plays.size());
		songPage.setTrackLength(plays.stream().findFirst().orElse(new PlayDTO()).getTrackLength());
		songPage.setDaysSongWasPlayed((int)plays.stream().map(s->s.getPlayDate().substring(0, 10)).distinct().count());
		songPage.setWeeksSongWasPlayed((int)plays.stream().map(s->s.getWeek()).distinct().count());
		songPage.setMonthsSongWasPlayed((int)plays.stream().map(s->s.getPlayDate().substring(0, 7)).distinct().count());

		//Charts
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>();

		List<Criterion<PlayDTO>> criteria = List.of(
				new Criterion<>("Play Year", play -> play.getPlayDate().substring(0,4),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey()))
				);

		List<TopGroupDTO> songGroupList = new ArrayList<>(); 

		for (Criterion<PlayDTO> criterion : criteria) {
			Map<String,List<PlayDTO>> classifiedMap= plays.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<PlayDTO>> e : sortedList) {
				long playtime = e.getValue().stream().mapToInt(play -> play.getTrackLength()).sum();
				counts.add(new TopCountDTO(
						e.getKey(), 
						0, //number of songs
						0,//number of songs male
						0.0, //percentage of songs
						0.0, //percentage of songs male
						e.getValue().size(), //plays
						0,//playsMale
						(double)e.getValue().size()/(double)plays.size()*100, //percentage of plays
						0.0,//percentagePlaysMale
						playtime, //playtime
						0,//playtimeMale
						(double)playtime*100/(double)songPage.getTotalPlaytime(),//percentage of playtime
						0.0));//percentagePlaytimeMale 

			}

			songGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		model.addAttribute("song",songPage);
		model.addAttribute("songGroupList", songGroupList);

		return "song";
	}

	@GetMapping({"/category"})
	public String song(Model model, @RequestParam(required=true) String category, @RequestParam(required=true) String value) {

		List<PlayDTO> plays = category.equalsIgnoreCase("all")?artistRepository.categoryPlaysAll()
				:artistRepository.categoryPlays(category, value);

		CategoryPageDTO categoryPage = new CategoryPageDTO();
		categoryPage.setCategoryValue(value);
		categoryPage.setTotalPlays(plays.size());
		categoryPage.setTotalPlaytime(plays.stream().mapToInt(s->s.getTrackLength()).sum());
		categoryPage.setDaysCategoryWasPlayed((int)plays.stream().map(s->s.getPlayDate().substring(0, 10)).distinct().count());
		categoryPage.setWeeksCategoryWasPlayed((int)plays.stream().map(s->s.getWeek()).distinct().count());
		categoryPage.setMonthsCategoryWasPlayed((int)plays.stream().map(s->s.getPlayDate().substring(0, 7)).distinct().count());

		//Breakdowns
		Map<String, List<PlayDTO>> map = plays.stream().collect(Collectors.groupingBy(s->s.getArtist()));
		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));

		categoryPage.setMostPlayedArtist(sortedList.get(0).getKey()+" - "+sortedList.get(0).getValue().size());
		categoryPage.setNumberOfArtists(map.entrySet().size());

		map = plays.stream().filter(s->!s.getAlbum().equals("(single)")).collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));
		categoryPage.setMostPlayedAlbum(sortedList.get(0).getKey()+" - "+sortedList.get(0).getValue().size());
		categoryPage.setNumberOfAlbums(map.entrySet().size());

		map = plays.stream().collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong()));
		sortedList = new ArrayList<>(map.entrySet());
		Collections.sort(sortedList, (o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));
		categoryPage.setMostPlayedSong(sortedList.get(0).getKey()+" - "+sortedList.get(0).getValue().size());
		categoryPage.setNumberOfSongs(map.entrySet().size());
		categoryPage.setAveragePlaysPerSong((double)categoryPage.getTotalPlays()/categoryPage.getNumberOfSongs());
		categoryPage.setAverageSongLength(sortedList.stream().mapToInt(e->e.getValue().get(0).getTrackLength()).sum()/categoryPage.getNumberOfSongs());

		List<Criterion<PlayDTO>> criteria = List.of(
				new Criterion<>("Sex", play -> play.getSex(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Genre", play -> play.getGenre(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Race", play -> play.getRace(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Language", play -> play.getLanguage(),
						(o1, o2) -> (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1)),
				new Criterion<>("Release Year", play -> String.valueOf(play.getYear()),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey())),
				new Criterion<>("Play Year", play -> play.getPlayDate().substring(0,4),
						(o1, o2) -> o1.getKey().compareTo(o2.getKey()))
				);


		List<TopGroupDTO> categoryGroupList = new ArrayList<>(); 

		for (Criterion<PlayDTO> criterion : criteria) {
			Map<String,List<PlayDTO>> classifiedMap= plays.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, criterion.getSortBy());

			for(Entry<String, List<PlayDTO>> e : sortedList) {
				int uniqueSongs = e.getValue().stream().collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong())).entrySet().size();
				int uniqueSongsMale = e.getValue().stream().filter(s->s.getSex().equals("Male")).collect(Collectors.groupingBy(s->s.getArtist()+"::"+s.getAlbum()+"::"+s.getSong())).entrySet().size();
				int playsMale = (int)e.getValue().stream().filter(s->s.getSex().equals("Male")).count();
				long playtime = e.getValue().stream().mapToInt(play -> play.getTrackLength()).sum();
				long playtimeMale = e.getValue().stream().filter(s->s.getSex().equals("Male")).mapToInt(play -> play.getTrackLength()).sum();
				counts.add(new TopCountDTO(
						e.getKey(), 
						uniqueSongs, //number of songs
						uniqueSongsMale,//number of songs male
						(double)uniqueSongs/(double)categoryPage.getNumberOfSongs()*100, //percentage of songs
						(double)uniqueSongsMale/(double)uniqueSongs*100, //percentage of songs male
						e.getValue().size(), //plays
						playsMale,//playsMale
						(double)e.getValue().size()/(double)plays.size()*100, //percentage of plays
						(double)playsMale/(double)e.getValue().size()*100,//percentagePlaysMale
						playtime, //playtime
						playtimeMale,//playtimeMale
						(double)playtime*100/(double)categoryPage.getTotalPlaytime(),//percentage of playtime
						(double)playtimeMale/(double)playtime*100));//percentagePlaytimeMale 

			}

			categoryGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}

		model.addAttribute("categoryName",category);
		model.addAttribute("category",categoryPage);
		model.addAttribute("categoryGroupList", categoryGroupList);
		model.addAttribute("daysElapsedSinceFirstPlay", daysElapsedSinceFirstPlay);

		return "category";
	}

}