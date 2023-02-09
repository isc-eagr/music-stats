package library.util;

import java.time.Duration;

public class Utils {
	
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
		
		return fullText.substring(0,fullText.length()-2);
		
}
	
	public static String secondsToStringHours(long seconds) {

		Duration d = Duration.ofSeconds(seconds);
		
		return d.toHoursPart()+" hours, "+d.toMinutesPart()+" minutes, "+d.toSecondsPart()+" seconds.";
	}

}
