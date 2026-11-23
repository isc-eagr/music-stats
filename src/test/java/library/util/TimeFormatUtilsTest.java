package library.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeFormatUtilsTest {

    @Test
    void formatHoursMinutesKeepsHoursAndMinutesWithoutRollingIntoDays() {
        assertEquals("0m", TimeFormatUtils.formatHoursMinutes(0));
        assertEquals("0m", TimeFormatUtils.formatHoursMinutes(-5));
        assertEquals("45m", TimeFormatUtils.formatHoursMinutes(45 * 60 + 59));
        assertEquals("3h 05m", TimeFormatUtils.formatHoursMinutes(3 * 3600 + 5 * 60));
        assertEquals("1,512h 34m", TimeFormatUtils.formatHoursMinutes(1512L * 3600 + 34 * 60 + 10));
    }
}
