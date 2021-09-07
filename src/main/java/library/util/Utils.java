package library.util;

public class Utils {
	
	public static String secondsToString(long seconds) {
		long secondsInADay = 24 * 60 * 60;
		long secondsInAnHour = 60 * 60;

		long totalMinutes = seconds / 60;
		long totalHours = totalMinutes / 60;
		long finalDays = totalHours / 24;

		long remainingSecondsAfterDays = seconds - (secondsInADay * finalDays);
		long finalHours = remainingSecondsAfterDays / 60 / 60;

		long remainingSecondsAfterHours = remainingSecondsAfterDays - (secondsInAnHour * finalHours);
		long finalMinutes = remainingSecondsAfterHours / 60;

		long finalSeconds = remainingSecondsAfterHours - (60 * finalMinutes);

		return finalDays + " days, " + finalHours + " hours, " + finalMinutes + " minutes, " + finalSeconds
				+ " seconds.";
	}
	
	public static String secondsToStringHours(long seconds) {
		long secondsInAnHour = 60 * 60;

		long totalMinutes = seconds / 60;
		long finalHours = totalMinutes / 60;

		long remainingSecondsAfterHours = seconds - (secondsInAnHour * finalHours);
		long finalMinutes = remainingSecondsAfterHours / 60;

		long finalSeconds = remainingSecondsAfterHours - (60 * finalMinutes);

		return finalHours + " hours, " + finalMinutes + " minutes, " + finalSeconds
				+ " seconds.";
	}

}
