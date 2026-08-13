package library;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Shared behavior used by the standalone data-maintenance tools.
 */
final class MaintenanceToolSupport {

    private MaintenanceToolSupport() {
    }

    static String getJsonOrEmptyDataOnNotFound(String url, String userAgent) throws IOException {
        URI uri = URI.create(url);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", userAgent);
        connection.setRequestProperty("Accept", "application/json");

        int responseCode = connection.getResponseCode();
        if (responseCode == 404) {
            return "{\"data\":[]}";
        }
        if (responseCode != 200) {
            throw new IOException("HTTP " + responseCode + ": " + connection.getResponseMessage());
        }

        try (InputStream input = connection.getInputStream()) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static byte[] downloadImageFollowingTemporaryRedirects(String url, String userAgent) throws IOException {
        URI uri = URI.create(url);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", userAgent);
        connection.setInstanceFollowRedirects(true);

        int responseCode = connection.getResponseCode();
        if (responseCode == 302 || responseCode == 307) {
            String redirectUrl = connection.getHeaderField("Location");
            connection.disconnect();
            return downloadImageFollowingTemporaryRedirects(redirectUrl, userAgent);
        }
        if (responseCode == 404) {
            return null;
        }
        if (responseCode != 200) {
            throw new IOException("HTTP " + responseCode + ": " + connection.getResponseMessage());
        }

        try (InputStream input = connection.getInputStream()) {
            return input.readAllBytes();
        }
    }

    static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }
}
