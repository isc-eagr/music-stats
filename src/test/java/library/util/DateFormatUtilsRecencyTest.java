package library.util;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class DateFormatUtilsRecencyTest {

    private static final LocalDate TODAY = LocalDate.of(2026, 11, 19);

    @Test
    void agoTextAndLevelMatchTheTableViewRecencyBuckets() {
        assertThat(DateFormatUtils.recency("19-Nov-2026", TODAY)).isEqualTo(new DateFormatUtils.Recency("Today", "fresh"));
        assertThat(DateFormatUtils.recency("18/11/2026", TODAY)).isEqualTo(new DateFormatUtils.Recency("Yesterday", "fresh"));
        assertThat(DateFormatUtils.recency("10-Oct-2026", TODAY)).isEqualTo(new DateFormatUtils.Recency("1 month ago", "fresh"));
        assertThat(DateFormatUtils.recency("01-Nov-2024", TODAY)).isEqualTo(new DateFormatUtils.Recency("2 years ago", "aging"));
        assertThat(DateFormatUtils.recency("26-Jul-2022", TODAY)).isEqualTo(new DateFormatUtils.Recency("4 years ago", "stale"));
        assertThat(DateFormatUtils.recency("13-Feb-2005", TODAY)).isEqualTo(new DateFormatUtils.Recency("21 years ago", "old"));
    }

    @Test
    void missingOrUnparsableDatesHaveNoRecency() {
        assertThat(DateFormatUtils.recency(null, TODAY)).isNull();
        assertThat(DateFormatUtils.recency("N/A", TODAY)).isNull();
    }
}
