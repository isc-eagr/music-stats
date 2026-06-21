package library.service;

import library.dto.SavedFilterDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

@Service
public class SavedFilterService {

    private static final String KEY_PREFIX = "saved.";
    private static final int MAX_NAME_LENGTH = 80;
    private static final int MAX_PAGE_KEY_LENGTH = 120;
    private static final int MAX_QUERY_LENGTH = 12000;

    private final Path storagePath;

    public SavedFilterService(@Value("${musicstats.saved-filters.file:C:/Music Stats DB/music-stats-saved-filters.properties}") String storageFile) {
        this.storagePath = Path.of(storageFile);
    }

    public synchronized List<SavedFilterDTO> list(String pageKey) {
        String safePageKey = sanitizePageKey(pageKey);
        String encodedPageKey = encode(safePageKey);
        String prefix = KEY_PREFIX + encodedPageKey + ".";
        Properties properties = loadProperties();
        List<SavedFilterDTO> filters = new ArrayList<>();

        for (String key : properties.stringPropertyNames()) {
            if (!key.startsWith(prefix)) {
                continue;
            }

            String encodedName = key.substring(prefix.length());
            filters.add(new SavedFilterDTO(safePageKey, decode(encodedName), properties.getProperty(key, "")));
        }

        filters.sort(Comparator.comparing(SavedFilterDTO::name, String.CASE_INSENSITIVE_ORDER));
        return filters;
    }

    public synchronized SavedFilterDTO save(SavedFilterDTO request) {
        String pageKey = sanitizePageKey(request != null ? request.pageKey() : null);
        String name = sanitizeName(request != null ? request.name() : null);
        String query = sanitizeQuery(request != null ? request.query() : null);
        Properties properties = loadProperties();

        properties.setProperty(buildKey(pageKey, name), query);
        storeProperties(properties);
        return new SavedFilterDTO(pageKey, name, query);
    }

    public synchronized boolean delete(String pageKey, String name) {
        String safePageKey = sanitizePageKey(pageKey);
        String safeName = sanitizeName(name);
        Properties properties = loadProperties();
        Object removed = properties.remove(buildKey(safePageKey, safeName));

        if (removed != null) {
            storeProperties(properties);
            return true;
        }

        return false;
    }

    private String buildKey(String pageKey, String name) {
        return KEY_PREFIX + encode(pageKey) + "." + encode(name);
    }

    private Properties loadProperties() {
        Properties properties = new Properties();
        if (!Files.exists(storagePath)) {
            return properties;
        }

        try (Reader reader = Files.newBufferedReader(storagePath, StandardCharsets.UTF_8)) {
            properties.load(reader);
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException("Could not read saved filters from " + storagePath, e);
        }
    }

    private void storeProperties(Properties properties) {
        try {
            Path parent = storagePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            try (Writer writer = Files.newBufferedWriter(storagePath, StandardCharsets.UTF_8)) {
                properties.store(writer, "Music Stats saved filters");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Could not write saved filters to " + storagePath, e);
        }
    }

    private String sanitizePageKey(String value) {
        String pageKey = value == null ? "" : value.trim();
        if (pageKey.isEmpty() || pageKey.length() > MAX_PAGE_KEY_LENGTH || !pageKey.startsWith("/")) {
            throw new IllegalArgumentException("Invalid saved filter page.");
        }
        String normalized = pageKey.replaceAll("/+$", "");
        return normalized.isEmpty() ? "/" : normalized;
    }

    private String sanitizeName(String value) {
        String name = value == null ? "" : value.trim();
        if (name.isEmpty() || name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Saved filter name must be 1-" + MAX_NAME_LENGTH + " characters.");
        }
        return name;
    }

    private String sanitizeQuery(String value) {
        String query = value == null ? "" : value.trim();
        if (query.startsWith("?")) {
            query = query.substring(1);
        }
        if (query.length() > MAX_QUERY_LENGTH) {
            throw new IllegalArgumentException("Saved filter query is too long.");
        }
        return query;
    }

    private String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
