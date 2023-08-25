package library.util;

import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import library.dto.Criterion;
import library.dto.MilestoneDTO;
import library.dto.PlayDTO;
import library.dto.TopCountDTO;
import library.dto.TopGroupDTO;

public class Utils {
	
	public static final int SECONDS_IN_A_DAY = 60*60*24;
	public static final int SECONDS_IN_A_WEEK = SECONDS_IN_A_DAY * 7;
	
	public static String secondsToString(long seconds) {
		Duration d = Duration.ofSeconds(seconds);
		
		long daysPart = d.toDaysPart();
		long hoursPart = d.toHoursPart();
		long minutesPart = d.toMinutesPart();
		long secondsPart = d.toSecondsPart();
		
		String daysString = daysPart == 0 ? "" : daysPart+" days, ";
		String hoursString = hoursPart == 0 ? "" : hoursPart+" hours, ";
		String minutesString = minutesPart == 0 ? "" : minutesPart+" minutes, ";
		String secondsString = secondsPart == 0 ? "" : secondsPart+" seconds, ";
		
		String fullText = daysString + hoursString + minutesString + secondsString; 
		
		return fullText.isBlank()?"":fullText.substring(0,fullText.length()-2);
		
	}
	
	public static String secondsToStringColon(long seconds) {
		Duration d = Duration.ofSeconds(seconds);
		
		long hoursPart = d.toHoursPart();
		long minutesPart = d.toMinutesPart();
		long secondsPart = d.toSecondsPart();
		
		String hoursString = hoursPart == 0 ? "" : hoursPart+":";
		String minutesString = hoursPart==0? minutesPart+":" : (minutesPart<10?"0"+minutesPart+":":minutesPart+":");
		String secondsString = secondsPart<10?"0"+secondsPart:""+secondsPart;
		secondsString = secondsString.length()==1 ? "0"+secondsString : secondsString;
		
		String fullText = hoursString + minutesString + secondsString; 
		
		return fullText;
		
	}
	
	public static int daysInAMonth(String month, int year) {
		return switch (month) {
	        case "January", "March", "May", "July", "August", "October", "December"-> {yield 31;}
	        case "April", "June", "September", "November"-> {yield 30;}
	        case "Feb"->{
	            if (isLeapYear(year)) {
	                yield 29;
	            } else {
	                yield 28;
	            }
	        }
	        default-> {yield 0;}
            
		};
	}
	
	public static int daysInASeason(String season, int year) {
		return switch (season) {
	        case "Spring", "Summer"-> {yield 92;}
	        case "Fall"-> {yield 91;}
	        case "Winter"->{
	            if (isLeapYear(year)) {
	                yield 91;
	            } else {
	                yield 90;
	            }
	        }
	        default-> {yield 0;}
            
		};
	}
	
	public static boolean isLeapYear(int year) {
		if ((year % 400 == 0) || ((year % 4 == 0) && (year % 100 != 0))) {
            return true;
        } else {
            return false;
        }
	}
	
	public static int daysInAYear(int year) {
         if (isLeapYear(year)) {
             return 366;
         } else {
             return 365;
         }
	}
	
	public static int leapYearsInADecade(int decadeStart) {
		int leapYears = 0;
        for(int year = decadeStart; year<(decadeStart+10); year++) {
        	if(isLeapYear(year))
        		leapYears++;
        }
        return leapYears;
	}
	
	public static int secondsInAMonth(String month, int year) {
		return SECONDS_IN_A_DAY * daysInAMonth(month, year);
	}
	
	public static int secondsInASeason(String season, int year) {
		return SECONDS_IN_A_DAY * daysInASeason(season, year);
	}
	
	public static int secondsInAYear(int year) {
		return SECONDS_IN_A_DAY * daysInAYear(year);
	}
	
	public static int secondsInADecade(int decadeStart) {
		return SECONDS_IN_A_DAY * (3650+leapYearsInADecade(decadeStart));
	}
	
	public static List<TopGroupDTO> generateChartData(List<Criterion<PlayDTO>> criteria, List<PlayDTO> plays, 
			int numberOfSongs, int totalPlaytime){
		List<TopGroupDTO> groupList = new ArrayList<>(); 

		for (Criterion<PlayDTO> criterion : criteria) {
			Map<String,List<PlayDTO>> classifiedMap= plays.stream().collect(Collectors.groupingBy(criterion.groupingBy));

			List<TopCountDTO> counts = new ArrayList<>();

			List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
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
						(double)uniqueSongs/(double)numberOfSongs*100, //percentage of songs
						(double)uniqueSongsMale/(double)uniqueSongs*100, //percentage of songs male
						e.getValue().size(), //plays
						playsMale,//playsMale
						(double)e.getValue().size()/(double)plays.size()*100, //percentage of plays
						(double)playsMale/(double)e.getValue().size()*100,//percentagePlaysMale
						playtime, //playtime
						playtimeMale,//playtimeMale
						(double)playtime*100/(double)totalPlaytime,//percentage of playtime
						(double)playtimeMale/(double)playtime*100));//percentagePlaytimeMale 

			}

			groupList.add(new TopGroupDTO(criterion.getName(),counts));
		}
		
		return groupList;
	}
	
	public static Map<String, Integer> generateChartDataCumulative(Criterion<PlayDTO> criterion, List<PlayDTO> plays){
		
		Map<String, Integer> data = new TreeMap<>();
		Map<String,List<PlayDTO>> classifiedMap= plays.stream().collect(Collectors.groupingBy(criterion.groupingBy));

		List<Entry<String, List<PlayDTO>>> sortedList = new ArrayList<>(classifiedMap.entrySet());
		Collections.sort(sortedList, criterion.getSortBy());
		
		
		for(Entry<String, List<PlayDTO>> e : sortedList) {
			data.put(e.getKey(),e.getValue().size());
		}
		
		int monthFirstPlayed = Integer.parseInt(sortedList.get(0).getKey().substring(5,7));
		int yearFirstPlayed = Integer.parseInt(sortedList.get(0).getKey().substring(0,4));
		
		int currentMonth = Calendar.getInstance().get(Calendar.MONTH);
		int currentYear = Calendar.getInstance().get(Calendar.YEAR);
		
		for (int year = yearFirstPlayed; year<=currentYear;year++) {
			for(int month=1; month<=12;month++) {
				
				if((year == currentYear && month > currentMonth) || (year == yearFirstPlayed && month < monthFirstPlayed))
					continue;
				
				String key = year+"-"+(String.valueOf(month).length()==1?"0"+month:month); 
				if(!data.containsKey(key))
					data.put(key, 0);
			}
		}
		
		int cumulative = 0;
		for(Entry<String,Integer> entry : data.entrySet()) {
			cumulative += entry.getValue();
			entry.setValue(cumulative);
		}
		
		return data;
		
	}
	
	public static List<MilestoneDTO> generateMilestones(List<PlayDTO> plays, int... milestonePlays){
		
		LocalDate firstPlay = LocalDate.parse(plays.get(0).getPlayDate().substring(0, 10));
		
		List<MilestoneDTO> milestonesMap = new ArrayList<>();
		
		for(int milestone : milestonePlays) {
			if(milestone -1 <= plays.size()) {
				PlayDTO play = plays.get(milestone-1);
				LocalDate milestonePlay = LocalDate.parse(play.getPlayDate().substring(0, 10));
				milestonesMap.add(new MilestoneDTO(milestone, play.getPlayDate(),ChronoUnit.DAYS.between(firstPlay, milestonePlay)));
			}
		}
		
		return milestonesMap;
	}
	
	
}
