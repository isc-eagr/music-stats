package library.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class OverviewCacheServiceTest {

    @Test
    void reusesRowsUntilExpiryAndReturnsMutableCopies() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger loads = new AtomicInteger();
        OverviewCacheService cache = new OverviewCacheService(Duration.ofSeconds(1), clock::get);

        List<String> first = cache.get("weekly:song", () -> List.of("load-" + loads.incrementAndGet()));
        first.add("local change");
        List<String> second = cache.get("weekly:song", () -> List.of("load-" + loads.incrementAndGet()));

        assertThat(second).containsExactly("load-1");
        assertThat(loads).hasValue(1);

        clock.set(Duration.ofSeconds(2).toNanos());
        assertThat(cache.get("weekly:song", () -> List.of("load-" + loads.incrementAndGet())))
                .containsExactly("load-2");
    }
}
