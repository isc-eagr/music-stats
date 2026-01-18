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
 * Standalone script to populate song single covers using Deezer API.
 * 
 * Features:
 * - Populates Song.single_cover from Deezer
 * - Fetches high-quality cover art (up to 1000x1000px)
 * - Completely FREE - no API key or authentication needed
 * - Good coverage for singles/tracks
 * 
 * Deezer API:
 * - No authentication required for public search (gratis, vato!)
 * - Search endpoint: https://api.deezer.com/search/track
 * - Returns cover_xl (1000x1000), cover_big (500x500), cover_medium (250x250)
 * - Rate limit: 50 requests per 5 seconds (generous, ese)
 * 
 * Usage: Run main() method directly from IDE or via:
 * mvnw exec:java -Dexec.mainClass="library.DeezerSingleCoverPopulator"
 * 
 * Options:
 *   --dry-run       Don't save changes to database
 *   --force         Re-populate even if single_cover already exists
 *   --quality=xl    Use cover_xl 1000x1000 (default)
 *   --quality=big   Use cover_big 500x500
 *   --quality=med   Use cover_medium 250x250
 */
public class DeezerSingleCoverPopulator {

	private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";

	// Deezer API Configuration
	private static final String DEEZER_API = "https://api.deezer.com";
	private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
	
	// Rate limit: 50 requests per 5 seconds = 10 per second
	// Using 150ms delay to be safe (6.6 per second)
	private static final long DEEZER_DELAY_MS = 150;
	
	// Minimum improvement in KB to consider upgrading existing cover
	// If improvement is less than this, skip the upgrade (avoid tiny improvements)
	private static final int MIN_UPGRADE_THRESHOLD_KB = 40;

	private JdbcTemplate jdbcTemplate;
	private ObjectMapper objectMapper;
	private long lastDeezerRequest = 0;

	// Statistics
	private int songsProcessed = 0;
	private int songsFound = 0;
	private int singleCoversUpdated = 0;
	private int singleCoversUpgraded = 0;
	private int coversSkippedSameQuality = 0;
	private int songsNotFound = 0;
	private int apiErrors = 0;

	// Configuration flags
	private boolean dryRun = false;
	private boolean skipExisting = false;
	private boolean singlesOnly = true; // Only populate if we can confirm it's a single, not album art
	private String quality = "xl"; // xl, big, or med

	public DeezerSingleCoverPopulator() {
		// Setup database connection
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("org.sqlite.JDBC");
		dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.objectMapper = new ObjectMapper();
	}

	public static void main(String[] args) {
		DeezerSingleCoverPopulator populator = new DeezerSingleCoverPopulator();

		// Parse command line arguments
		for (String arg : args) {
			switch (arg.toLowerCase()) {
				case "--dry-run":
					populator.dryRun = true;
					break;
				case "--force":
					populator.skipExisting = false;
					break;
				case "--allow-albums":
					populator.singlesOnly = false;
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
		System.out.println("Deezer Single Cover Populator");
		System.out.println("========================================");
		System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
		System.out.println("Skip existing: " + skipExisting);
		System.out.println("Singles only: " + singlesOnly + " (reject album art)");
		String qualityDesc = quality.equals("xl") ? "1000x1000" : (quality.equals("big") ? "500x500" : "250x250");
		System.out.println("Image quality: " + qualityDesc);
		System.out.println();

		try {
			populateSingleCovers();
		} catch (Exception e) {
			System.err.println("Error populating single covers: " + e.getMessage());
			e.printStackTrace();
		}

		printSummary();
	}

	/**
	 * Populate single covers from Deezer
	 */
	private void populateSingleCovers() {
		String whereClause = skipExisting ? "WHERE s.single_cover IS NULL" : "";
		String sql = """
				SELECT s.id, s.name, ar.name as artist_name, 
				       LENGTH(s.single_cover) as current_size
				FROM Song s
				INNER JOIN Artist ar ON s.artist_id = ar.id
				""" + whereClause + " ORDER BY s.id";

		List<SongData> songs = jdbcTemplate.query(sql, (rs, rowNum) -> {
			SongData song = new SongData();
			song.id = rs.getInt("id");
			song.name = rs.getString("name");
			song.artistName = rs.getString("artist_name");
			Object sizeObj = rs.getObject("current_size");
			song.currentCoverSize = (sizeObj != null) ? ((Number) sizeObj).intValue() : 0;
			return song;
		});

		System.out.println("Found " + songs.size() + " songs to process");
		System.out.println();

		for (SongData song : songs) {
			songsProcessed++;

			try {
				System.out.println("[" + songsProcessed + "/" + songs.size() + "] Processing: " 
						+ song.artistName + " - " + song.name);

				// Search Deezer for the track
				DeezerTrack track = searchTrack(song.name, song.artistName);

				if (track != null) {
					songsFound++;
					
					// Get the appropriate quality cover URL
					String coverUrl = getCoverUrl(track);
					
					if (coverUrl != null && !coverUrl.isEmpty()) {
						System.out.println("  ✓ Found cover: " + coverUrl);
						
						// Download the cover art
						byte[] imageData = downloadImage(coverUrl);

						if (imageData != null && imageData.length > 0) {
							// Check if we should update based on size
							if (shouldUpdateCover(song.currentCoverSize, imageData.length)) {
								if (song.currentCoverSize > 0) {
									int improvement = (imageData.length - song.currentCoverSize) / 1024;
									System.out.println("  ✓ Downloaded upgrade (" + formatBytes(imageData.length) 
										+ ", +" + improvement + " KB improvement)");
									singleCoversUpgraded++;
								} else {
									System.out.println("  ✓ Downloaded cover (" + formatBytes(imageData.length) + ")");
								}

								if (!dryRun) {
									// If no existing single_cover, set it as the primary image
									// Otherwise add to gallery
									if (song.currentCoverSize == 0) {
										updateSongCover(song.id, imageData);
									} else {
										addSongGalleryImage(song.id, imageData);
									}
								}
								singleCoversUpdated++;
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
					songsNotFound++;
				}

			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}

			System.out.println();
		}
	}

	/**
	 * Search Deezer for a track
	 * Returns the best matching track, prioritizing singles/EPs over album tracks
	 */
	private DeezerTrack searchTrack(String songName, String artistName) throws IOException, InterruptedException {
		respectRateLimit();

		// Build search query: artist + track name (strip feat. for cleaner search)
		String cleanSongName = stripFeaturedArtist(songName);
		String query = URLEncoder.encode("artist:\"" + artistName + "\" track:\"" + cleanSongName + "\"", 
				StandardCharsets.UTF_8);
		String url = DEEZER_API + "/search/track?q=" + query;

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode data = root.get("data");
		if (data != null && data.isArray() && data.size() > 0) {
			// Collect all exact matches
			List<JsonNode> exactMatches = new ArrayList<>();
			
			for (JsonNode trackNode : data) {
				String trackTitle = trackNode.get("title").asText();
				JsonNode artistNode = trackNode.get("artist");
				String trackArtist = artistNode != null ? artistNode.get("name").asText() : "";

				// Skip live/acoustic/karaoke/remix versions
				if (isLiveOrRemixVersion(trackTitle)) {
					continue;
				}

				// Normalize and compare
				if (normalizeName(trackTitle).equals(normalizeName(songName)) &&
					normalizeName(trackArtist).equals(normalizeName(artistName))) {
					exactMatches.add(trackNode);
				}
			}
			
			// Collect fuzzy matches too
			List<JsonNode> fuzzyMatches = new ArrayList<>();
			
			for (JsonNode trackNode : data) {
				String trackTitle = trackNode.get("title").asText();
				JsonNode artistNode = trackNode.get("artist");
				String trackArtist = artistNode != null ? artistNode.get("name").asText() : "";

				// Skip live/acoustic/karaoke/remix versions
				if (isLiveOrRemixVersion(trackTitle)) {
					continue;
				}

				// Skip if already in exact matches
				if (exactMatches.contains(trackNode)) {
					continue;
				}

				// Check if artist matches and song title is similar
				if (normalizeName(trackArtist).equals(normalizeName(artistName)) &&
					isSimilar(normalizeName(trackTitle), normalizeName(songName))) {
					fuzzyMatches.add(trackNode);
				}
			}
			
			// Evaluate all matches and pick the best one
			if (!exactMatches.isEmpty() || !fuzzyMatches.isEmpty()) {
				return findBestMatch(exactMatches, fuzzyMatches, songName);
			}
		}

		return null;
	}
	
	/**
	 * Find the best match from exact and fuzzy matches
	 * Priority: exact+single > fuzzy+single > exact+EP > fuzzy+EP
	 * Returns the best match or null if none are valid
	 */
	private DeezerTrack findBestMatch(List<JsonNode> exactMatches, List<JsonNode> fuzzyMatches, String songName) throws IOException, InterruptedException {
		int totalMatches = exactMatches.size() + fuzzyMatches.size();
		
		if (totalMatches > 1) {
			System.out.println("  → Found " + totalMatches + " total matches (" + exactMatches.size() + " exact, " + fuzzyMatches.size() + " fuzzy), evaluating all...");
		}
		
		// Collect all valid candidates with their priority
		List<MatchCandidate> candidates = new ArrayList<>();
		
		// Process exact matches
		for (JsonNode trackNode : exactMatches) {
			MatchCandidate candidate = evaluateTrack(trackNode, songName, true);
			if (candidate != null) {
				candidates.add(candidate);
			}
		}
		
		// Process fuzzy matches
		for (JsonNode trackNode : fuzzyMatches) {
			MatchCandidate candidate = evaluateTrack(trackNode, songName, false);
			if (candidate != null) {
				candidates.add(candidate);
			}
		}
		
		if (candidates.isEmpty()) {
			if (totalMatches > 1) {
				System.out.println("  ✗ No valid single/EP found in " + totalMatches + " matches");
			}
			return null;
		}
		
		// Sort by priority (lower number = higher priority)
		candidates.sort((a, b) -> Integer.compare(a.priority, b.priority));
		
		MatchCandidate best = candidates.get(0);
		
		if (totalMatches > 1 || candidates.size() > 1) {
			String matchType = best.isExact ? "exact" : "fuzzy";
			String recordType = best.isEP ? "EP" : "single";
			System.out.println("  ✓ Selected best match: " + matchType + " + " + recordType + " - \"" + best.track.albumTitle + "\"");
		}
		
		return best.track;
	}
	
	/**
	 * Evaluate a track and return a MatchCandidate if valid, null otherwise
	 */
	private MatchCandidate evaluateTrack(JsonNode trackNode, String songName, boolean isExact) throws IOException, InterruptedException {
		DeezerTrack track = parseTrack(trackNode);
		
		// Skip live/acoustic/karaoke/remix albums
		if (isLiveOrRemixVersion(track.albumTitle)) {
			return null;
		}
		
		// Check if album ID is valid
		if (track.albumId == 0) {
			return null;
		}
		
		// Fetch album record_type
		String recordType = fetchAlbumRecordType(track.albumId);
		
		if (recordType == null) {
			return null;
		}
		
		// Only process singles (no EPs)
		if (!recordType.equals("single")) {
			return null;
		}
		
		// For singles, verify the album/single name contains the song name
		String normalizedAlbum = normalizeName(track.albumTitle);
		String normalizedSong = normalizeName(songName);
		
		// Album name should match or contain the song name
		if (normalizedAlbum.equals(normalizedSong) || normalizedAlbum.contains(normalizedSong)) {
			// Priority: exact=1, fuzzy=2
			int priority = isExact ? 1 : 2;
			return new MatchCandidate(track, priority, isExact, false);
		}
		
		return null;
	}
	
	/**
	 * Helper class to hold match candidates with priority
	 */
	private static class MatchCandidate {
		DeezerTrack track;
		int priority;  // 1=exact match, 2=fuzzy match
		boolean isExact;
		boolean isEP;
		
		MatchCandidate(DeezerTrack track, int priority, boolean isExact, boolean isEP) {
			this.track = track;
			this.priority = priority;
			this.isExact = isExact;
			this.isEP = isEP;
		}
	}

	/**
	 * Parse a Deezer track JSON node
	 */
	private DeezerTrack parseTrack(JsonNode trackNode) {
		DeezerTrack track = new DeezerTrack();
		track.id = trackNode.get("id").asInt();
		track.title = trackNode.get("title").asText();

		JsonNode album = trackNode.get("album");
		if (album != null) {
			// Get album ID and title
			JsonNode albumId = album.get("id");
			if (albumId != null) {
				track.albumId = albumId.asInt();
			}
			
			JsonNode albumTitle = album.get("title");
			if (albumTitle != null) {
				track.albumTitle = albumTitle.asText();
			}
			
			// Get all available cover URLs
			JsonNode coverSmall = album.get("cover_small");
			JsonNode coverMedium = album.get("cover_medium");
			JsonNode coverBig = album.get("cover_big");
			JsonNode coverXl = album.get("cover_xl");

			if (coverSmall != null) track.coverSmall = coverSmall.asText();
			if (coverMedium != null) track.coverMedium = coverMedium.asText();
			if (coverBig != null) track.coverBig = coverBig.asText();
			if (coverXl != null) track.coverXl = coverXl.asText();
		}

		return track;
	}

	/**
	 * Get the cover URL based on quality setting
	 */
	private String getCoverUrl(DeezerTrack track) {
		switch (quality) {
			case "xl":
				return track.coverXl != null ? track.coverXl : track.coverBig;
			case "big":
				return track.coverBig != null ? track.coverBig : track.coverMedium;
			case "med":
				return track.coverMedium != null ? track.coverMedium : track.coverSmall;
			default:
				return track.coverXl;
		}
	}


	/**
	 * Fetch album details from Deezer API and return the record_type field
	 * Returns "single", "ep", "album", or null if error
	 */
	private String fetchAlbumRecordType(int albumId) throws IOException, InterruptedException {
		respectRateLimit();
		
		String url = DEEZER_API + "/album/" + albumId;
		
		try {
			String response = makeHttpRequest(url);
			JsonNode root = objectMapper.readTree(response);
			
			JsonNode recordType = root.get("record_type");
			if (recordType != null) {
				return recordType.asText();
			}
		} catch (Exception e) {
			System.err.println("  ⚠ Error fetching album details: " + e.getMessage());
		}
		
		return null;
	}

	/**
	 * Check if a track title is a live, acoustic, karaoke, or remix version
	 */
	private boolean isLiveOrRemixVersion(String title) {
		if (title == null) {
			return false;
		}
		
		String lower = title.toLowerCase();
		
		// Check for live versions
		if (lower.contains("live") || lower.contains("en vivo") || lower.contains("ao vivo")) {
			return true;
		}
		
		// Check for acoustic versions
		if (lower.contains("acoustic") || lower.contains("acústico") || lower.contains("acustico")) {
			return true;
		}
		
		// Check for karaoke versions
		if (lower.contains("karaoke") || lower.contains("instrumental")) {
			return true;
		}
		
		// Check for remix versions (but not if it's part of the original title)
		if (lower.contains(" remix") || lower.contains("(remix") || lower.contains("[remix")) {
			return true;
		}
		
		// Check for "Remix" and "Mix" (case variations)
		if (lower.contains(" mix") || lower.contains("(mix") || lower.contains("[mix")) {
			return true;
		}
		
		return false;
		}
	
	/**
	 * Strip featured artists from song name for cleaner API search
	 * Example: "Nike de Aretes (Feat. Uzielito Mix)" -> "Nike de Aretes"
	 */
	private String stripFeaturedArtist(String name) {
		if (name == null) {
			return "";
		}
		
		String result = name.trim();
		
		// Remove parenthetical feat: (Feat. xxx), (feat xxx), (ft. xxx)
		result = result.replaceAll("\\s*\\([Ff]eat\\.?\\s+[^)]+\\)", "");
		result = result.replaceAll("\\s*\\([Ff]t\\.?\\s+[^)]+\\)", "");
		result = result.replaceAll("\\s*\\([Ff]eaturing\\s+[^)]+\\)", "");
		
		// Remove bracketed feat: [Feat. xxx], [ft. xxx]
		result = result.replaceAll("\\s*\\[[Ff]eat\\.?\\s+[^\\]]+\\]", "");
		result = result.replaceAll("\\s*\\[[Ff]t\\.?\\s+[^\\]]+\\]", "");
		
		// Remove inline feat at end: "Song feat. Artist" or "Song ft. Artist"
		result = result.replaceAll("\\s+(feat\\.?|ft\\.?|featuring)\\s+.*$", "");
		
		return result.trim();
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
		
		// Remove "feat.", "ft.", "featuring", etc.
		normalized = normalized.replaceAll("\\s+(feat\\.|ft\\.|featuring|with)\\s+.*", "");
		
		// Remove special characters
		normalized = normalized.replaceAll("[^a-z0-9\\s]", "");
		
		// Remove multiple spaces
		normalized = normalized.replaceAll("\\s+", " ").trim();
		
		return normalized;
	}

	/**
	 * Check if two normalized strings are similar enough
	 */
	private boolean isSimilar(String s1, String s2) {
		if (s1.equals(s2)) {
			return true;
		}
		
		// Check if one contains the other
		if (s1.contains(s2) || s2.contains(s1)) {
			return true;
		}
		
		// Calculate similarity score (simple version)
		String[] words1 = s1.split("\\s+");
		String[] words2 = s2.split("\\s+");
		
		int matches = 0;
		for (String word1 : words1) {
			for (String word2 : words2) {
				if (word1.equals(word2)) {
					matches++;
					break;
				}
			}
		}
		
		// At least 60% of words match
		double minWords = Math.min(words1.length, words2.length);
		return minWords > 0 && (matches / minWords) >= 0.6;
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
	 * Update the song's primary cover (single_cover column)
	 */
	private void updateSongCover(int songId, byte[] imageData) {
		String sql = "UPDATE Song SET single_cover = ? WHERE id = ?";
		jdbcTemplate.update(sql, imageData, songId);
	}

	/**
	 * Add image to song's gallery (SongImage table)
	 */
	private void addSongGalleryImage(int songId, byte[] imageData) {
		// Get the current max display_order for this song
		String maxOrderSql = "SELECT COALESCE(MAX(display_order), 0) FROM SongImage WHERE song_id = ?";
		Integer maxOrder = jdbcTemplate.queryForObject(maxOrderSql, Integer.class, songId);
		
		// Insert new gallery image
		String sql = "INSERT INTO SongImage (song_id, image, display_order, creation_date) VALUES (?, ?, ?, datetime('now'))";
		jdbcTemplate.update(sql, songId, imageData, maxOrder + 1);
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
		System.out.println("Songs processed: " + songsProcessed);
		System.out.println("Songs found on Deezer: " + songsFound);
		System.out.println("Single covers updated: " + singleCoversUpdated);
		if (singleCoversUpgraded > 0) {
			System.out.println("  - New covers: " + (singleCoversUpdated - singleCoversUpgraded));
			System.out.println("  - Upgrades: " + singleCoversUpgraded);
		}
		System.out.println("Covers skipped (same/worse quality): " + coversSkippedSameQuality);
		System.out.println("Songs not found: " + songsNotFound);
		System.out.println("API errors: " + apiErrors);
		System.out.println();

		if (dryRun) {
			System.out.println("*** DRY RUN - No changes were saved to database ***");
		} else {
			System.out.println("Done! Changes saved to database.");
		}
	}

	/**
	 * Data class for song info
	 */
	private static class SongData {
		int id;
		String name;
		String artistName;
		int currentCoverSize; // Size in bytes of existing cover
	}

	/**
	 * Data class for Deezer track info
	 */
	private static class DeezerTrack {
		int id;
		String title;
		int albumId;        // Album ID for fetching album details
		String albumTitle;   // Album name for validation
		String coverSmall;   // 56x56
		String coverMedium;  // 250x250
		String coverBig;     // 500x500
		String coverXl;      // 1000x1000
	}
}
