package library.util;

import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * Builds image responses with a content-based ETag. Browsers revalidate every time
 * (Cache-Control: no-cache), so an unchanged image costs a body-less 304 instead of a
 * full download, and a replaced image is never served stale.
 */
public final class ImageResponses {

    public static final int THUMBNAIL_MAX_DIMENSION = 600;

    private ImageResponses() {
    }

    public static ResponseEntity<byte[]> image(byte[] image, boolean thumbnail, String ifNoneMatch) {
        if (image == null || image.length == 0) {
            return ResponseEntity.ok().body(image);
        }
        long fingerprint = ThumbnailCache.fingerprint(image);
        String etag = "\"" + Long.toHexString(fingerprint) + (thumbnail ? "-t" + THUMBNAIL_MAX_DIMENSION : "") + "\"";
        if (matches(ifNoneMatch, etag)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(etag).cacheControl(CacheControl.noCache()).build();
        }
        byte[] body = thumbnail ? ThumbnailCache.shared().thumbnail(image, fingerprint, THUMBNAIL_MAX_DIMENSION) : image;
        return ResponseEntity.ok()
                .eTag(etag)
                .cacheControl(CacheControl.noCache())
                .contentType(mediaType(body))
                .body(body);
    }

    static boolean matches(String ifNoneMatch, String etag) {
        if (ifNoneMatch == null || ifNoneMatch.isBlank()) {
            return false;
        }
        for (String candidate : ifNoneMatch.split(",")) {
            String value = candidate.trim();
            if (value.startsWith("W/")) {
                value = value.substring(2);
            }
            if (value.equals(etag) || value.equals("*")) {
                return true;
            }
        }
        return false;
    }

    static MediaType mediaType(byte[] data) {
        if (data.length >= 3 && (data[0] & 0xFF) == 0xFF && (data[1] & 0xFF) == 0xD8 && (data[2] & 0xFF) == 0xFF) {
            return MediaType.IMAGE_JPEG;
        }
        if (data.length >= 4 && (data[0] & 0xFF) == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G') {
            return MediaType.IMAGE_PNG;
        }
        if (data.length >= 3 && data[0] == 'G' && data[1] == 'I' && data[2] == 'F') {
            return MediaType.IMAGE_GIF;
        }
        if (data.length >= 12 && data[0] == 'R' && data[1] == 'I' && data[2] == 'F' && data[3] == 'F'
                && data[8] == 'W' && data[9] == 'E' && data[10] == 'B' && data[11] == 'P') {
            return MediaType.parseMediaType("image/webp");
        }
        return MediaType.APPLICATION_OCTET_STREAM;
    }
}
