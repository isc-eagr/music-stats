package library.service;

import library.dto.SaveImagesRequest;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads artwork from external providers and stores it on albums or songs.
 */
@Service
public class ExternalImageService {

    private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";

    private final AlbumService albumService;
    private final SongService songService;

    public ExternalImageService(AlbumService albumService, SongService songService) {
        this.albumService = albumService;
        this.songService = songService;
    }

    /**
     * Download image from URL and return bytes.
     */
    public byte[] downloadImage(String imageUrl) throws Exception {
        URI uri = URI.create(imageUrl);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setConnectTimeout(15000);
        connection.setReadTimeout(15000);

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("HTTP error downloading image: " + responseCode);
        }

        try (InputStream is = connection.getInputStream()) {
            return is.readAllBytes();
        }
    }

    /**
     * Save selected images to an album or song.
     * If entity has no existing image, first selected becomes default, rest are secondary.
     * If entity has existing image, all selected become secondary.
     */
    public Map<String, Object> saveImages(SaveImagesRequest request) {
        Map<String, Object> response = new HashMap<>();
        int saved = 0;
        int skippedDuplicates = 0;
        List<String> errors = new ArrayList<>();

        try {
            List<String> urls = request.imageUrls();
            Long entityId = request.entityId();
            Integer id = entityId != null ? entityId.intValue() : null;
            String entityType = request.entityType();
            boolean hasExisting = request.hasExistingImage();

            for (int i = 0; i < urls.size(); i++) {
                String url = urls.get(i);
                try {
                    byte[] imageBytes = downloadImage(url);

                    if (entityType.equals("album")) {
                        if (albumService.isDuplicateImage(id, imageBytes)) {
                            skippedDuplicates++;
                            continue;
                        }
                        if (!hasExisting && i == 0) {
                            // First image becomes default
                            albumService.updateAlbumImage(id, imageBytes);
                            hasExisting = true;
                        } else {
                            albumService.addSecondaryImage(id, imageBytes);
                        }
                    } else if (entityType.equals("song")) {
                        if (songService.isDuplicateImage(id, imageBytes)) {
                            skippedDuplicates++;
                            continue;
                        }
                        if (!hasExisting && i == 0) {
                            // First image becomes default
                            songService.updateSongImage(id, imageBytes);
                            hasExisting = true;
                        } else {
                            songService.addSecondaryImage(id, imageBytes);
                        }
                    } else {
                        errors.add("Unknown entity type: " + entityType);
                        continue;
                    }
                    saved++;
                } catch (Exception e) {
                    errors.add("Failed to download image " + (i + 1) + ": " + e.getMessage());
                }
            }

            response.put("success", true);
            response.put("savedCount", saved);
            if (skippedDuplicates > 0) {
                response.put("skippedDuplicates", skippedDuplicates);
            }
            if (!errors.isEmpty()) {
                response.put("errors", errors);
            }
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return response;
    }
}
