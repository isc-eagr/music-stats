package library.controller;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
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
import library.dto.AllSongsExtendedDTO;
import library.dto.ArtistAlbumsQueryDTO;
import library.dto.ArtistDTO;
import library.dto.ArtistSongsQueryDTO;
import library.dto.Criterion;
import library.dto.DataForGraphs;
import library.dto.SaveAlbumDTO;
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
        scrobbles.stream().parallel().forEach(sc -> sc.setSongId(everySong.stream().parallel().filter(song -> 
						song.getArtist().equalsIgnoreCase(sc.getArtist()) &&
						song.getSong().equalsIgnoreCase(sc.getSong()) &&
						String.valueOf(song.getAlbum()).equalsIgnoreCase(String.valueOf(sc.getAlbum()))
        		).findFirst().orElse(emptySong).getId()));
        
        scrobbleRepository.saveAll(scrobbles);
        
        file.close();

		return "Success!"+ scrobbles.size()+" scrobbles inserted!";
	}
	
	@RequestMapping("/insertItunesInfo")
	@ResponseBody
	public String insertItunesInfo(Model model) throws FileNotFoundException, IOException, SAXException, ParserConfigurationException{
		
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
		
		List<AllSongsExtendedDTO> allSongsExtended = songRepositoryImpl.getAllSongsExtended(start, end);
		
		model.addAttribute("totalSongs", allSongsExtended.size());
		model.addAttribute("totalPlays", allSongsExtended.stream().parallel()
				.mapToInt(allSongsExtendedDto -> allSongsExtendedDto.getPlays())
				.sum()
				);
		model.addAttribute("totalPlayTimeGlobalString", 
				Utils.secondsToString(allSongsExtended.stream().parallel()
				.mapToInt(allSongsExtendedDto -> allSongsExtendedDto.getPlaytime())
				.sum())
				);
		
		//For the main criteria Sex and Genre
		//We need a map with Criteria name (Sex or Genre) as key, and a list with every graph
		//in that criteria (old version had 4)
		Map<String, List<DataForGraphs>> dataMap = new LinkedHashMap<>();

		List<Criterion<AllSongsExtendedDTO>> criteria = List.of(new Criterion<>("Sex", song -> song.getSex()),
				new Criterion<>("Genre", song -> song.getGenre()),
				new Criterion<>("Language", song -> song.getLanguage())
				);
				
		for (Criterion<AllSongsExtendedDTO> criterion : criteria) {
		
			//Each element in this list, represents a graph, in the old version I had 4 graphs:
			//Number of songs, number of plays, playtime, and playtime difference
			List<DataForGraphs> data = new ArrayList<>();
	
			Map<String,List<AllSongsExtendedDTO>> classifiedMap= allSongsExtended.stream().parallel().collect(Collectors.groupingBy(criterion.groupingBy));
			
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

			List<Entry<String, List<AllSongsExtendedDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			
			Collections.sort(sortedList, new Comparator<Map.Entry<String, List<AllSongsExtendedDTO>>>() {
	            public int compare(Map.Entry<String, List<AllSongsExtendedDTO>> o1,
	                               Map.Entry<String, List<AllSongsExtendedDTO>> o2) {
	                return (o1.getValue()).size()>(o2.getValue()).size()?-1:1;
	            }
	        });
			
			//This loop iterates through every secondary criteria (R&B Reggaeton etc)
			for(Entry<String, List<AllSongsExtendedDTO>> e : sortedList) {
				numberOfSongsDataMale += e.getValue().stream().filter(song -> song.getSex().equals("Male")).count()+",";
				numberOfSongsDataOthers += e.getValue().stream().filter(song -> !song.getSex().equals("Male")).count()+",";
				numberOfSongsLabels += ("'"+e.getKey()+"',");
				
				numberOfPlaysDataMale += e.getValue().stream().filter(song -> song.getSex().equals("Male")).mapToInt(song -> song.getPlays()).sum()+",";
				numberOfPlaysDataOthers += e.getValue().stream().filter(song -> !song.getSex().equals("Male")).mapToInt(song -> song.getPlays()).sum()+",";
				numberOfPlaysLabels += ("'"+e.getKey()+"',");
				
				playtimeDataMale += e.getValue().stream().filter(song -> song.getSex().equals("Male")).mapToInt(song -> song.getPlaytime()).sum()+",";
				playtimeDataOthers += e.getValue().stream().filter(song -> !song.getSex().equals("Male")).mapToInt(song -> song.getPlaytime()).sum()+",";
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
		
		model.addAttribute("dataMap",dataMap);
		return "main";
	}// method
	
	@RequestMapping("/topSongs")
	public String topSongs(Model model, @RequestParam(defaultValue="10000000") int limit) {
		
		List<TopSongsDTO> topSongs = songRepositoryImpl.getTopSongs(limit);
		model.addAttribute("topSongs", topSongs);
		
		List<Criterion<TopSongsDTO>> criteria = List.of(new Criterion<>("Sex", song -> song.getSex()),
				new Criterion<>("Genre", song -> song.getGenre()),
				new Criterion<>("Language", song -> song.getLanguage()),
				new Criterion<>("Year", song -> song.getYear())
				);
		
		
		List<TopGroupDTO> topSongsGroupList = new ArrayList<>(); 
		
		for (Criterion<TopSongsDTO> criterion : criteria) {
			Map<String,List<TopSongsDTO>> classifiedMap= topSongs.stream().parallel().collect(Collectors.groupingBy(criterion.groupingBy));
			
			List<TopCountDTO> counts = new ArrayList<>();
			
			List<Entry<String, List<TopSongsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, new Comparator<Map.Entry<String, List<TopSongsDTO>>>() {
	            public int compare(Map.Entry<String, List<TopSongsDTO>> o1,
	                               Map.Entry<String, List<TopSongsDTO>> o2) {
	            	return (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1);
	            }
	        });
			
			for(Entry<String, List<TopSongsDTO>> e : sortedList) {
				counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)topSongs.size()*100, e.getValue().stream().mapToLong(topSong -> topSong.getPlaytime()).sum()));
			}
			
			topSongsGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}
		
		model.addAttribute("topSongsGroupList", topSongsGroupList);
		
		return "topsongs";
		
	}
	
	@RequestMapping("/topArtists")
	public String topArtists(Model model, @RequestParam(defaultValue="10000000") int limit) {
		
		List<TopArtistsDTO> topArtists = songRepositoryImpl.getTopArtists(limit);
		
		//TODO this is taking a really long time. Optimize?
		/*topArtists.stream().forEach(a -> 
		{
			a.setAverageSongLength(songRepositoryImpl.averageSongDurationByArtist(a.getArtist()));
			a.setAveragePlaysPerSong(artistRepository.averageSongDurationByArtist(a.getArtist()));
		}
		);*/
		model.addAttribute("topArtists", topArtists);
		
		List<Criterion<TopArtistsDTO>> criteria = List.of(new Criterion<>("Sex", song -> song.getSex()),
				new Criterion<>("Genre", song -> song.getGenre()),
				new Criterion<>("Language", song -> song.getLanguage())
				);
		
		
		List<TopGroupDTO> topArtistsGroupList = new ArrayList<>(); 
		
		for (Criterion<TopArtistsDTO> criterion : criteria) {
			Map<String,List<TopArtistsDTO>> classifiedMap= topArtists.stream().collect(Collectors.groupingBy(criterion.groupingBy));
			
			List<TopCountDTO> counts = new ArrayList<>();
			
			List<Entry<String, List<TopArtistsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, new Comparator<Map.Entry<String, List<TopArtistsDTO>>>() {
	            public int compare(Map.Entry<String, List<TopArtistsDTO>> o1,
	                               Map.Entry<String, List<TopArtistsDTO>> o2) {
	            	return (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1);
	            }
	        });
			
			for(Entry<String, List<TopArtistsDTO>> e : sortedList) {
				counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)topArtists.size()*100,e.getValue().stream().mapToLong(topArtist -> topArtist.getPlaytime()).sum()));
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
		
		List<Criterion<TopAlbumsDTO>> criteria = List.of(new Criterion<>("Sex", song -> song.getSex()),
				new Criterion<>("Genre", song -> song.getGenre()),
				new Criterion<>("Language", song -> song.getLanguage()),
				new Criterion<>("Year", song -> song.getYear())
				);
		
		
		List<TopGroupDTO> topAlbumsGroupList = new ArrayList<>(); 
		
		for (Criterion<TopAlbumsDTO> criterion : criteria) {
			Map<String,List<TopAlbumsDTO>> classifiedMap= topAlbums.stream().collect(Collectors.groupingBy(criterion.groupingBy));
			
			List<TopCountDTO> counts = new ArrayList<>();
			
			List<Entry<String, List<TopAlbumsDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
			Collections.sort(sortedList, new Comparator<Map.Entry<String, List<TopAlbumsDTO>>>() {
	            public int compare(Map.Entry<String, List<TopAlbumsDTO>> o1,
	                               Map.Entry<String, List<TopAlbumsDTO>> o2) {
	                return (o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1);
	            }
	        });
			
			for(Entry<String, List<TopAlbumsDTO>> e : sortedList) {
				counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)topAlbums.size()*100,e.getValue().stream().mapToLong(topAlbum -> topAlbum.getPlaytime()).sum()));
			}
			
			topAlbumsGroupList.add(new TopGroupDTO(criterion.getName(),counts));
		}
		
		model.addAttribute("topAlbumsGroupList", topAlbumsGroupList);
		
		return "topalbums";
		
	}
	
	@RequestMapping("/songsLastFmButNotItunes")
	public String songsLastFmButNotItunes(Model model) {
		model.addAttribute("listSongs", scrobbleRepositoryImpl.songsInLastFmButNotItunes());
		return "songsLastFmButNotItunes";
		
	}
	
	@RequestMapping("/songsItunesButNotLastfm")
	public String songsItunesButNotLastFm(Model model) {
		model.addAttribute("listSongs", songRepositoryImpl.songsItunesButNotLastfm());
		return "songsItunesButNotLastfm";
		
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
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)timeUnits.size()*100,0));
		}
		timeUnitGroupList.add(new TopGroupDTO("Genre",counts));
		
		counts = new ArrayList<>();
		Map<String,List<TimeUnitStatsDTO>> mapSex = timeUnits.stream().collect(Collectors.groupingBy(ww -> ww.getSex()));
		
		List<Entry<String, List<TimeUnitStatsDTO>>> sortedSexList = new ArrayList<>(mapSex.entrySet());
		Collections.sort(sortedSexList, (o1,o2)->(o1.getValue()).size()>(o2.getValue()).size()?-1:(o1.getValue().size()==o2.getValue().size()?0:1));
		
		for(Entry<String, List<TimeUnitStatsDTO>> e : sortedSexList) {
			counts.add(new TopCountDTO(e.getKey(), e.getValue().size(), (double)e.getValue().size()/(double)timeUnits.size()*100,0));
		}
		
		timeUnitGroupList.add(new TopGroupDTO("Sex",counts));
		
		model.addAttribute("timeUnitGroupList", timeUnitGroupList);
		return "timeunit";
		
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
		
		List<Song> songsList = scrobbleRepositoryImpl.songsFromAlbum(artist, album == null?"":album);
		
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
	
	@RequestMapping("/artist/{artist}")
	public String insertAlbumForm(Model model, @PathVariable(required=true) String artist) {
		
		List<ArtistSongsQueryDTO> artistSongsList = artistRepository.songsByArtist(artist);
		List<ArtistAlbumsQueryDTO> artistAlbumsList = artistRepository.albumsByArtist(artist);
		
		int numberOfSongs = artistSongsList.size();
		int totalPlays = artistSongsList.stream().mapToInt(s -> s.getTotalPlays()).sum();
		int sumOfTrackLengths = artistSongsList.stream().mapToInt(s -> s.getTrackLength()).sum();
		int averagePlaysPerSong = totalPlays/numberOfSongs;
		String totalPlaytime = Utils.secondsToString(artistSongsList.stream().mapToInt(s ->s.getTotalPlays()*s.getTrackLength()).sum());
		String averageSongLength = Utils.secondsToStringColon(sumOfTrackLengths/numberOfSongs);
		
		ArtistSongsQueryDTO firstSong = artistSongsList.stream().min((s1, s2) -> s1.getFirstPlay().compareTo(s2.getFirstPlay())).orElse(new ArtistSongsQueryDTO());
		ArtistSongsQueryDTO lastSong = artistSongsList.stream().max((s1, s2) -> s1.getLastPlay().compareTo(s2.getLastPlay())).orElse(new ArtistSongsQueryDTO());
		
		String firstSongPlayed = firstSong.getSong() + " - " +firstSong.getFirstPlay();
		String lastSongPlayed = lastSong.getSong() + " - " +lastSong.getLastPlay();;
		
		ArtistDTO artistInfo = new ArtistDTO(artistSongsList, artistAlbumsList, firstSongPlayed, lastSongPlayed, totalPlays, totalPlaytime, 
											averageSongLength,averagePlaysPerSong, numberOfSongs);

		model.addAttribute("artist",artist);
		model.addAttribute("artistInfo",artistInfo);
		
		return "artist";
	}
	
	

}