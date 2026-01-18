package library;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Standalone script to populate album covers using Deezer API.
 * 
 * Features:
 * - Populates Album.image from Deezer
 * - Fetches high-quality cover art (up to 1000x1000px)
 * - Only updates if Deezer has better quality than existing
 * - Completely FREE - no API key or authentication needed
 * 
 * Deezer API:
 * - No authentication required for public search (gratis, vato!)
 * - Search endpoint: https://api.deezer.com/search/album
 * - Returns cover_xl (1000x1000), cover_big (500x500), cover_medium (250x250)
 * - Rate limit: 50 requests per 5 seconds (generous, ese)
 * 
 * Usage: Run main() method directly from IDE or via:
 * mvnw exec:java -Dexec.mainClass="library.DeezerAlbumCoverPopulator"
 * 
 * Options:
 *   --dry-run       Don't save changes to database
 *   --force         Re-populate even if image already exists
 *   --quality=xl    Use cover_xl 1000x1000 (default)
 *   --quality=big   Use cover_big 500x500
 *   --quality=med   Use cover_medium 250x250
 */
public class DeezerAlbumCoverPopulator {

	private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";

	// Deezer API Configuration
	private static final String DEEZER_API = "https://api.deezer.com";
	private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
	
	// Rate limit: 50 requests per 5 seconds = 10 per second
	// Using 150ms delay to be safe (6.6 per second)
	private static final long DEEZER_DELAY_MS = 150;
	
	// Minimum improvement in KB to consider upgrading existing cover
	// If improvement is less than this, skip the upgrade (avoid tiny improvements)
	private static final int MIN_UPGRADE_THRESHOLD_KB = 50;

	private JdbcTemplate jdbcTemplate;
	private ObjectMapper objectMapper;
	private long lastDeezerRequest = 0;

	// Statistics
	private int albumsProcessed = 0;
	private int albumsFound = 0;
	private int albumCoversUpdated = 0;
	private int albumCoversUpgraded = 0;
	private int coversSkippedSameQuality = 0;
	private int albumsNotFound = 0;
	private int apiErrors = 0;

	// Configuration flags
	private boolean dryRun = false;
	private boolean skipExisting = false;
	private String quality = "xl"; // xl, big, or med

	public DeezerAlbumCoverPopulator() {
		// Setup database connection
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("org.sqlite.JDBC");
		dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.objectMapper = new ObjectMapper();
	}

	public static void main(String[] args) {
		DeezerAlbumCoverPopulator populator = new DeezerAlbumCoverPopulator();

		// Parse command line arguments
		for (String arg : args) {
			switch (arg.toLowerCase()) {
				case "--dry-run":
					populator.dryRun = true;
					break;
				case "--force":
					populator.skipExisting = false;
					break;
				case "--quality=xl":
					populator.quality = "xl";
					break;
				case "--quality=big":
					populator.quality = "big";
					break;
				case "--quality=med":
					populator.quality = "med";
					break;
			}
		}

		populator.run();
	}

	public void run() {
		System.out.println("========================================");
		System.out.println("Deezer Album Cover Populator");
		System.out.println("========================================");
		System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
		System.out.println("Skip existing: " + skipExisting);
		String qualityDesc = quality.equals("xl") ? "1000x1000" : (quality.equals("big") ? "500x500" : "250x250");
		System.out.println("Image quality: " + qualityDesc);
		System.out.println();

		try {
			populateAlbumCovers();
		} catch (Exception e) {
			System.err.println("Error populating album covers: " + e.getMessage());
			e.printStackTrace();
		}

		printSummary();
	}

	/**
	 * Populate album covers from Deezer
	 */
	private void populateAlbumCovers() {
		String whereClause = skipExisting ? "WHERE a.image IS NULL" : "";
		String sql = """
				SELECT a.id, a.name, ar.name as artist_name, 
				       LENGTH(a.image) as current_size
				FROM Album a
				INNER JOIN Artist ar ON a.artist_id = ar.id
				""" + whereClause + " ORDER BY a.id";

		List<AlbumData> albums = jdbcTemplate.query(sql, (rs, rowNum) -> {
			AlbumData album = new AlbumData();
			album.id = rs.getInt("id");
			album.name = rs.getString("name");
			album.artistName = rs.getString("artist_name");
			Object sizeObj = rs.getObject("current_size");
			album.currentCoverSize = (sizeObj != null) ? ((Number) sizeObj).intValue() : 0;
			return album;
		});

		System.out.println("Found " + albums.size() + " albums to process");
		System.out.println();

		for (AlbumData album : albums) {
			albumsProcessed++;

			try {
				System.out.println("[" + albumsProcessed + "/" + albums.size() + "] Processing: " 
						+ album.artistName + " - " + album.name);

				// Search Deezer for the album
				DeezerAlbum deezerAlbum = searchAlbum(album.name, album.artistName);

				if (deezerAlbum != null) {
					albumsFound++;
					
					// Get the appropriate quality cover URL
					String coverUrl = getCoverUrl(deezerAlbum);
					
					if (coverUrl != null && !coverUrl.isEmpty()) {
						System.out.println("  ✓ Found cover: " + coverUrl);
						
						// Download the cover art
						byte[] imageData = downloadImage(coverUrl);

						if (imageData != null && imageData.length > 0) {
							// Check if we should update based on size
							if (shouldUpdateCover(album.currentCoverSize, imageData.length)) {
								if (album.currentCoverSize > 0) {
									int improvement = (imageData.length - album.currentCoverSize) / 1024;
									System.out.println("  ✓ Downloaded upgrade (" + formatBytes(imageData.length) 
										+ ", +" + improvement + " KB improvement)");
									albumCoversUpgraded++;
								} else {
									System.out.println("  ✓ Downloaded cover (" + formatBytes(imageData.length) + ")");
								}

								if (!dryRun) {
									updateAlbumCover(album.id, imageData);
								}
								albumCoversUpdated++;
							} else {
								// Existing cover is same or better quality
								coversSkippedSameQuality++;
							}
						} else {
							System.out.println("  ⚠ Failed to download image");
						}
					} else {
						System.out.println("  ⚠ No cover art URL available");
					}
				} else {
					System.out.println("  ✗ Not found on Deezer");
					albumsNotFound++;
				}

			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}

			System.out.println();
		}
	}

	/**
	 * Search Deezer for an album
	 * Returns the best matching album or null if not found
	 */
	private DeezerAlbum searchAlbum(String albumName, String artistName) throws IOException, InterruptedException {
		respectRateLimit();

		// Build search query: artist + album name
		String query = URLEncoder.encode("artist:\"" + artistName + "\" album:\"" + albumName + "\"", 
				StandardCharsets.UTF_8);
		String url = DEEZER_API + "/search/album?q=" + query;

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode data = root.get("data");
		if (data != null && data.isArray() && data.size() > 0) {
			// Try to find exact match first
			for (JsonNode albumNode : data) {
				String albumTitle = albumNode.get("title").asText();
				JsonNode artistNode = albumNode.get("artist");
				String albumArtist = artistNode != null ? artistNode.get("name").asText() : "";

				// Normalize and compare
				if (normalizeName(albumTitle).equals(normalizeName(albumName)) &&
					normalizeName(albumArtist).equals(normalizeName(artistName))) {
					return parseAlbum(albumNode);
				}
			}

			// If no exact match, try fuzzy match on first result
			JsonNode firstAlbum = data.get(0);
			String albumTitle = firstAlbum.get("title").asText();
			JsonNode artistNode = firstAlbum.get("artist");
			String albumArtist = artistNode != null ? artistNode.get("name").asText() : "";

			// Check if artist matches and album title is similar
			if (normalizeName(albumArtist).equals(normalizeName(artistName)) &&
				isAlbumNameMatch(albumName, albumTitle)) {
				System.out.println("  → Fuzzy match: \"" + albumTitle + "\"");
				return parseAlbum(firstAlbum);
			}
		}

		return null;
	}

	/**
	 * Parse a Deezer album JSON node
	 */
	private DeezerAlbum parseAlbum(JsonNode albumNode) {
		DeezerAlbum album = new DeezerAlbum();
		album.id = albumNode.get("id").asInt();
		album.title = albumNode.get("title").asText();

		// Get all available cover URLs
		JsonNode coverSmall = albumNode.get("cover_small");
		JsonNode coverMedium = albumNode.get("cover_medium");
		JsonNode coverBig = albumNode.get("cover_big");
		JsonNode coverXl = albumNode.get("cover_xl");

		if (coverSmall != null) album.coverSmall = coverSmall.asText();
		if (coverMedium != null) album.coverMedium = coverMedium.asText();
		if (coverBig != null) album.coverBig = coverBig.asText();
		if (coverXl != null) album.coverXl = coverXl.asText();

		return album;
	}

	/**
	 * Get the cover URL based on quality setting
	 */
	private String getCoverUrl(DeezerAlbum album) {
		switch (quality) {
			case "xl":
				return album.coverXl != null ? album.coverXl : album.coverBig;
			case "big":
				return album.coverBig != null ? album.coverBig : album.coverMedium;
			case "med":
				return album.coverMedium != null ? album.coverMedium : album.coverSmall;
			default:
				return album.coverXl;
		}
	}

	/**
	 * Check if two album names match, allowing for common variations
	 * Returns true if they're similar enough to be considered the same album
	 */
	private boolean isAlbumNameMatch(String name1, String name2) {
		String normalized1 = normalizeName(name1);
		String normalized2 = normalizeName(name2);
		
		// Exact match
		if (normalized1.equals(normalized2)) {
			return true;
		}
		
		// Check if one contains the other
		if (normalized1.contains(normalized2) || normalized2.contains(normalized1)) {
			// Allow variations with these keywords
			String lower1 = name1.toLowerCase();
			String lower2 = name2.toLowerCase();
			
			String[] allowedKeywords = {"deluxe", "remaster", "edition", "expanded", 
										 "special", "anniversary", "version", "bonus"};
			
			for (String keyword : allowedKeywords) {
				if (lower1.contains(keyword) || lower2.contains(keyword)) {
					return true; // It's a permitted variation
				}
			}
		}
		
		return false;
	}

	/**
	 * Normalize name for comparison (lowercase, trim, remove special chars)
	 */
	private String normalizeName(String name) {
		if (name == null) {
			return "";
		}
		
		// Convert to lowercase and trim
		String normalized = name.toLowerCase().trim();
		
		// Remove parenthetical/bracketed suffixes
		normalized = normalized.replaceAll("\\s*[\\(\\[].*?[\\)\\]]", "");
		
		// Remove special characters
		normalized = normalized.replaceAll("[^a-z0-9\\s]", "");
		
		// Remove multiple spaces
		normalized = normalized.replaceAll("\\s+", " ").trim();
		
		return normalized;
	}

	/**
	 * Make HTTP GET request and return response body
	 */
	private String makeHttpRequest(String urlString) throws IOException {
		URI uri = URI.create(urlString);
		HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setRequestProperty("Accept", "application/json");

		int responseCode = conn.getResponseCode();
		if (responseCode == 404) {
			return "{\"data\":[]}"; // Not found
		} else if (responseCode != 200) {
			throw new IOException("HTTP " + responseCode + ": " + conn.getResponseMessage());
		}

		try (InputStream is = conn.getInputStream()) {
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/**
	 * Download binary image data from URL
	 */
	private byte[] downloadImage(String urlString) throws IOException {
		URI uri = URI.create(urlString);
		HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setInstanceFollowRedirects(true);

		int responseCode = conn.getResponseCode();
		if (responseCode == 307 || responseCode == 302) {
			// Follow redirect
			String newUrl = conn.getHeaderField("Location");
			conn.disconnect();
			return downloadImage(newUrl);
		} else if (responseCode == 404) {
			return null;
		} else if (responseCode != 200) {
			throw new IOException("HTTP " + responseCode + ": " + conn.getResponseMessage());
		}

		try (InputStream is = conn.getInputStream()) {
			return is.readAllBytes();
		}
	}

	/**
	 * Determine if cover should be updated based on size comparison
	 * Only update if new cover is better quality (larger file size)
	 */
	private boolean shouldUpdateCover(int currentSize, int newSize) {
		if (currentSize == 0) {
			return true; // No existing cover, always update
		}
		
		// Only update if new cover is larger (better quality)
		if (newSize > currentSize) {
			// Check if improvement is significant enough
			int improvementKb = (newSize - currentSize) / 1024;
			if (improvementKb < MIN_UPGRADE_THRESHOLD_KB) {
				System.out.println("  ⚠ Skipping (improvement too small: +" + improvementKb + " KB, minimum: " + MIN_UPGRADE_THRESHOLD_KB + " KB)");
				return false;
			}
			return true;
		} else if (newSize == currentSize) {
			System.out.println("  ⚠ Skipping (same quality as existing)");
			return false;
		} else {
			int differencekb = (currentSize - newSize) / 1024;
			System.out.println("  ⚠ Skipping (existing is better: -" + differencekb + " KB)");
			return false;
		}
	}

	/**
	 * Respect Deezer rate limit (50 requests per 5 seconds)
	 */
	private void respectRateLimit() throws InterruptedException {
		long now = System.currentTimeMillis();
		long timeSinceLastRequest = now - lastDeezerRequest;

		if (timeSinceLastRequest < DEEZER_DELAY_MS) {
			long sleepTime = DEEZER_DELAY_MS - timeSinceLastRequest;
			Thread.sleep(sleepTime);
		}

		lastDeezerRequest = System.currentTimeMillis();
	}

	/**
	 * Update album image in database
	 */
	private void updateAlbumCover(int albumId, byte[] imageData) {
		String sql = "UPDATE Album SET image = ? WHERE id = ?";
		jdbcTemplate.update(sql, imageData, albumId);
	}

	/**
	 * Format bytes to human-readable format
	 */
	private String formatBytes(long bytes) {
		if (bytes < 1024) return bytes + " B";
		if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
		return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
	}

	/**
	 * Print summary statistics
	 */
	private void printSummary() {
		System.out.println("========================================");
		System.out.println("Summary");
		System.out.println("========================================");
		System.out.println("Albums processed: " + albumsProcessed);
		System.out.println("Albums found on Deezer: " + albumsFound);
		System.out.println("Album covers updated: " + albumCoversUpdated);
		if (albumCoversUpgraded > 0) {
			System.out.println("  - New covers: " + (albumCoversUpdated - albumCoversUpgraded));
			System.out.println("  - Upgrades: " + albumCoversUpgraded);
		}
		System.out.println("Covers skipped (same/worse quality): " + coversSkippedSameQuality);
		System.out.println("Albums not found: " + albumsNotFound);
		System.out.println("API errors: " + apiErrors);
		System.out.println();

		if (dryRun) {
			System.out.println("*** DRY RUN - No changes were saved to database ***");
		} else {
			System.out.println("Done! Changes saved to database.");
		}
	}

	/**
	 * Data class for album info
	 */
	private static class AlbumData {
		int id;
		String name;
		String artistName;
		int currentCoverSize; // Size in bytes of existing cover
	}

	/**
	 * Data class for Deezer album info
	 */
	private static class DeezerAlbum {
		int id;
		String title;
		String coverSmall;   // 56x56
		String coverMedium;  // 250x250
		String coverBig;     // 500x500
		String coverXl;      // 1000x1000
	}
}
