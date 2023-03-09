package library.util;

import java.time.Duration;

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
	
}
