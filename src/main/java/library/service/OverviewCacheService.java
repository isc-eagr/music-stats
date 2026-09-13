package library.service;

import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Keeps expensive overview aggregates warm while the user filters, sorts, and
 * loads subsequent pages. Entries expire quickly so chart edits remain visible.
 */
@Service
public class OverviewCacheService {

    private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);

    private final ConcurrentHashMap<String, CacheEntry> entries = new ConcurrentHashMap<>();
    private final long ttlNanos;
    private final LongSupplier nanoTime;

    public OverviewCacheService() {
        this(DEFAULT_TTL, System::nanoTime);
    }

    OverviewCacheService(Duration ttl, LongSupplier nanoTime) {
        this.ttlNanos = ttl.toNanos();
        this.nanoTime = nanoTime;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> get(String key, Supplier<List<T>> loader) {
        long now = nanoTime.getAsLong();
        CacheEntry entry = entries.compute(key, (ignored, current) -> {
            if (current != null && now - current.createdAtNanos() < ttlNanos) {
                return current;
            }
            return new CacheEntry(now, List.copyOf(loader.get()));
        });
        return new ArrayList<>((List<T>) entry.rows());
    }

    public void invalidateAll() {
        entries.clear();
    }

    private record CacheEntry(long createdAtNanos, List<?> rows) {
    }
}
