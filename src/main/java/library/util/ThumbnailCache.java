package library.util;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

/**
 * Content-addressed LRU cache of resized thumbnails.
 * Entries are keyed by a fingerprint of the original image bytes, so a replaced image
 * (upload, theme switch) simply misses and no invalidation is needed.
 */
public final class ThumbnailCache {

    static final long DEFAULT_MAX_BYTES = 128L * 1024 * 1024;
    // Larger results are almost always originals that could not be decoded; don't let them flush the cache.
    private static final int MAX_ENTRY_BYTES = 2 * 1024 * 1024;

    private static final ThumbnailCache SHARED = new ThumbnailCache(DEFAULT_MAX_BYTES, ImageUtil::resizeThumbnail);

    private final long maxBytes;
    private final BiFunction<byte[], Integer, byte[]> resizer;
    private final LinkedHashMap<Key, byte[]> entries = new LinkedHashMap<>(256, 0.75f, true);
    private long totalBytes;

    record Key(long fingerprint, int length, int maxDimension) {
    }

    ThumbnailCache(long maxBytes, BiFunction<byte[], Integer, byte[]> resizer) {
        this.maxBytes = maxBytes;
        this.resizer = resizer;
    }

    public static ThumbnailCache shared() {
        return SHARED;
    }

    /** 64-bit fingerprint (CRC32C and CRC32 of the full content). */
    public static long fingerprint(byte[] data) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(data);
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (crc32c.getValue() << 32) | crc32.getValue();
    }

    public byte[] thumbnail(byte[] original, int maxDimension) {
        if (original == null || original.length == 0) {
            return original;
        }
        return thumbnail(original, fingerprint(original), maxDimension);
    }

    /** Same as {@link #thumbnail(byte[], int)} for callers that already fingerprinted the original. */
    public byte[] thumbnail(byte[] original, long originalFingerprint, int maxDimension) {
        Key key = new Key(originalFingerprint, original.length, maxDimension);
        byte[] cached = get(key);
        if (cached != null) {
            return cached;
        }
        byte[] resized = resizer.apply(original, maxDimension);
        if (resized != null && resized.length <= MAX_ENTRY_BYTES) {
            put(key, resized);
        }
        return resized;
    }

    synchronized int size() {
        return entries.size();
    }

    private synchronized byte[] get(Key key) {
        return entries.get(key);
    }

    private synchronized void put(Key key, byte[] value) {
        byte[] previous = entries.put(key, value);
        if (previous != null) {
            totalBytes -= previous.length;
        }
        totalBytes += value.length;
        Iterator<Map.Entry<Key, byte[]>> eldest = entries.entrySet().iterator();
        while (totalBytes > maxBytes && eldest.hasNext()) {
            totalBytes -= eldest.next().getValue().length;
            eldest.remove();
        }
    }
}
