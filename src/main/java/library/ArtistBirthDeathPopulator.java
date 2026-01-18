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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * Standalone script to populate artist birth dates and death dates using
 * MusicBrainz and Wikidata APIs.
 * 
 * Features:
 * - Populates Artist.birth_date from MusicBrainz, falls back to Wikidata
 * - Populates Artist.death_date from MusicBrainz, falls back to Wikidata
 * - Only processes solo artists (is_band = 0 or NULL)
 * 
 * Rate Limits:
 * - MusicBrainz: 1 request per second
 * - Wikidata: 1 request per second (to be polite)
 * 
 * Usage: Run main() method directly from IDE or via:
 * mvnw exec:java -Dexec.mainClass="library.ArtistBirthDeathPopulator"
 * 
 * Options:
 *   --dry-run       Don't save changes to database
 *   --force         Re-populate even if data already exists
 *   --birth-only    Only populate birth dates
 *   --death-only    Only populate death dates
 *   --wikidata-only Skip MusicBrainz, only use Wikidata
 */
public class ArtistBirthDeathPopulator {

	private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";

	// API Configuration
	private static final String MUSICBRAINZ_API = "https://musicbrainz.org/ws/2";
	private static final String WIKIDATA_API = "https://www.wikidata.org/w/api.php";
	private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
	
	// Rate limits: 1.1 seconds to be safe
	private static final long MUSICBRAINZ_DELAY_MS = 1100;
	private static final long WIKIDATA_DELAY_MS = 1100;

	private JdbcTemplate jdbcTemplate;
	private ObjectMapper objectMapper;
	private long lastMusicBrainzRequest = 0;
	private long lastWikidataRequest = 0;

	// Statistics
	private int artistsProcessed = 0;
	private int birthDatesFromMusicBrainz = 0;
	private int birthDatesFromWikidata = 0;
	private int deathDatesFromMusicBrainz = 0;
	private int deathDatesFromWikidata = 0;
	private int countriesUpdated = 0;
	private int artistsSkipped = 0;
	private int artistsNotFound = 0;
	private int apiErrors = 0;

	// Configuration flags
	private boolean dryRun = false;
	private boolean skipExisting = true;
	private boolean populateBirthDates = true;
	private boolean populateDeathDates = true;
	private boolean populateCountries = true;
	private boolean wikidataOnly = false;

	public ArtistBirthDeathPopulator() {
		// Setup database connection
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("org.sqlite.JDBC");
		dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.objectMapper = new ObjectMapper();
	}

	public static void main(String[] args) {
		ArtistBirthDeathPopulator populator = new ArtistBirthDeathPopulator();

		// Parse command line arguments
		for (String arg : args) {
			switch (arg.toLowerCase()) {
				case "--dry-run":
					populator.dryRun = true;
					break;
				case "--force":
					populator.skipExisting = false;
					break;
				case "--birth-only":
					populator.populateDeathDates = false;
					break;
				case "--death-only":
					populator.populateBirthDates = false;
					break;
				case "--skip-countries":
					populator.populateCountries = false;
					break;
				case "--wikidata-only":
					populator.wikidataOnly = true;
					break;
			}
		}

		populator.run();
	}

	public void run() {
		System.out.println("========================================");
		System.out.println("Artist Birth/Death Date Populator");
		System.out.println("========================================");
		System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
		System.out.println("Skip existing: " + skipExisting);
		System.out.println("Populate birth dates: " + populateBirthDates);
		System.out.println("Populate death dates: " + populateDeathDates);
		System.out.println("Populate countries: " + populateCountries + " (always overwrites)");
		System.out.println("Wikidata only: " + wikidataOnly);
		System.out.println();

		try {
			populateArtistDates();
		} catch (Exception e) {
			System.err.println("Error populating artist dates: " + e.getMessage());
			e.printStackTrace();
		}

		printSummary();
	}

	/**
	 * Populate birth and death dates for solo artists
	 */
	private void populateArtistDates() {
		// Build WHERE clause based on options
		StringBuilder whereBuilder = new StringBuilder();
		whereBuilder.append("WHERE (is_band = 0 OR is_band IS NULL) "); // Only solo artists
		
		if (skipExisting) {
			List<String> conditions = new ArrayList<>();
			if (populateBirthDates) {
				conditions.add("birth_date IS NULL");
			}
			if (populateDeathDates) {
				conditions.add("death_date IS NULL");
			}
			if (!conditions.isEmpty()) {
				whereBuilder.append("AND (").append(String.join(" OR ", conditions)).append(") ");
			}
		}

		String sql = "SELECT id, name, birth_date, death_date, country FROM Artist " + whereBuilder + " ORDER BY id";

		List<ArtistData> artists = jdbcTemplate.query(sql, (rs, rowNum) -> {
			ArtistData artist = new ArtistData();
			artist.id = rs.getInt("id");
			artist.name = rs.getString("name");
			artist.birthDate = rs.getString("birth_date");
			artist.deathDate = rs.getString("death_date");
			artist.country = rs.getString("country");
			return artist;
		});

		System.out.println("Found " + artists.size() + " solo artists to process");
		System.out.println();

		for (ArtistData artist : artists) {
			artistsProcessed++;
			
			try {
				System.out.println("[" + artistsProcessed + "/" + artists.size() + "] Processing: " + artist.name);
				
				boolean needsBirthDate = populateBirthDates && (artist.birthDate == null || !skipExisting);
				boolean needsDeathDate = populateDeathDates && (artist.deathDate == null || !skipExisting);
				
				// Get artist's songs from our database for verification
				List<String> knownSongs = getArtistSongs(artist.id);
				
				LocalDate birthDate = null;
				LocalDate deathDate = null;
				String country = null;
				String birthSource = null;
				String deathSource = null;
				
				// Try MusicBrainz first (unless wikidata-only mode)
				if (!wikidataOnly) {
					String mbid = searchArtistMBID(artist.name, artist.country, knownSongs);
					
					if (mbid != null) {
						Map<String, Object> artistData = getArtistDataFromMusicBrainz(mbid);
						
						if (needsBirthDate && artistData.containsKey("birth")) {
							birthDate = (LocalDate) artistData.get("birth");
							birthSource = "MusicBrainz";
						}
						if (needsDeathDate && artistData.containsKey("death")) {
							deathDate = (LocalDate) artistData.get("death");
							deathSource = "MusicBrainz";
						}
						if (populateCountries && artistData.containsKey("country")) {
							country = (String) artistData.get("country");
						}
					}
				}
				
				// Fall back to Wikidata if needed
				if ((needsBirthDate && birthDate == null) || (needsDeathDate && deathDate == null)) {
					String wikidataId = searchWikidataId(artist.name);
					
					if (wikidataId != null) {
						Map<String, LocalDate> dates = getArtistDatesFromWikidata(wikidataId);
						
						if (needsBirthDate && birthDate == null && dates.containsKey("birth")) {
							birthDate = dates.get("birth");
							birthSource = "Wikidata";
						}
						if (needsDeathDate && deathDate == null && dates.containsKey("death")) {
							deathDate = dates.get("death");
							deathSource = "Wikidata";
						}
					}
				}
				
				// Update database if we found dates
				boolean foundAnything = false;
				
				if (birthDate != null) {
					System.out.println("  ✓ Birth date: " + birthDate + " (" + birthSource + ")");
					if (!dryRun) {
						updateArtistBirthDate(artist.id, birthDate);
					}
					if ("MusicBrainz".equals(birthSource)) {
						birthDatesFromMusicBrainz++;
					} else {
						birthDatesFromWikidata++;
					}
					foundAnything = true;
				}
				
				if (deathDate != null) {
					System.out.println("  ✓ Death date: " + deathDate + " (" + deathSource + ")");
					if (!dryRun) {
						updateArtistDeathDate(artist.id, deathDate);
					}
					if ("MusicBrainz".equals(deathSource)) {
						deathDatesFromMusicBrainz++;
					} else {
						deathDatesFromWikidata++;
					}
					foundAnything = true;
				}
				
				if (country != null) {
					System.out.println("  ✓ Country: " + country + " (MusicBrainz)");
					if (!dryRun) {
						updateArtistCountry(artist.id, country);
					}
					countriesUpdated++;
					foundAnything = true;
				}
				
				if (!foundAnything) {
					if (needsBirthDate || needsDeathDate) {
						System.out.println("  ⚠ No date information found");
						artistsNotFound++;
					} else {
						System.out.println("  → Already has dates, skipping");
						artistsSkipped++;
					}
				}
				
			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}
			
			System.out.println();
		}
	}

	/**
	 * Get artist's songs from our database for verification
	 */
	private List<String> getArtistSongs(int artistId) {
		String sql = "SELECT name FROM Song WHERE artist_id = ? LIMIT 20";
		return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("name"), artistId);
	}

	/**
	 * Search MusicBrainz for an artist and return the MBID
	 * Uses discography verification to disambiguate artists with the same name
	 */
	private String searchArtistMBID(String artistName, String country, List<String> knownSongs) 
			throws IOException, InterruptedException {
		respectMusicBrainzRateLimit();

		String query = URLEncoder.encode("artist:\"" + artistName + "\"", StandardCharsets.UTF_8);
		String url = MUSICBRAINZ_API + "/artist?query=" + query + "&fmt=json&limit=10";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode artists = root.get("artists");
		if (artists == null || !artists.isArray() || artists.size() == 0) {
			return null;
		}

		// Collect all exact name matches that are persons
		List<ArtistCandidate> candidates = new ArrayList<>();
		for (JsonNode artist : artists) {
			String name = artist.get("name").asText();
			if (name.equalsIgnoreCase(artistName)) {
				// Check that it's a person, not a group
				JsonNode type = artist.get("type");
				if (type == null || "Person".equalsIgnoreCase(type.asText())) {
					ArtistCandidate candidate = new ArtistCandidate();
					candidate.mbid = artist.get("id").asText();
					candidate.name = name;
					
					// Get country if available
					JsonNode area = artist.get("area");
					if (area != null) {
						JsonNode areaName = area.get("name");
						if (areaName != null) {
							candidate.country = areaName.asText();
						}
					}
					
					// Get disambiguation if available
					JsonNode disambiguation = artist.get("disambiguation");
					if (disambiguation != null && !disambiguation.isNull()) {
						candidate.disambiguation = disambiguation.asText();
					}
					
					candidates.add(candidate);
				}
			}
		}

		if (candidates.isEmpty()) {
			return null;
		}

		// If only one candidate, use it
		if (candidates.size() == 1) {
			return candidates.get(0).mbid;
		}

		// Multiple candidates - use discography verification
		System.out.println("  ! Found " + candidates.size() + " artists with name \"" + artistName + "\"");
		
		// If we have no songs to verify with, we can't disambiguate
		if (knownSongs == null || knownSongs.isEmpty()) {
			System.out.println("  ⚠ Can't disambiguate - no songs in database for verification");
			return null;
		}

		// Verify each candidate's discography
		System.out.println("  → Verifying discography against " + knownSongs.size() + " known songs...");
		ArtistCandidate bestMatch = null;
		int bestScore = 0;

		for (ArtistCandidate candidate : candidates) {
			try {
				int matchScore = verifyArtistDiscography(candidate.mbid, knownSongs);
				candidate.matchScore = matchScore;
				
				String disambInfo = "";
				if (candidate.disambiguation != null) {
					disambInfo = " (" + candidate.disambiguation + ")";
				}
				if (candidate.country != null) {
					disambInfo += " [" + candidate.country + "]";
				}
				
				System.out.println("    - " + candidate.name + disambInfo + ": " + matchScore + " song matches");
				
				if (matchScore > bestScore) {
					bestScore = matchScore;
					bestMatch = candidate;
				}
			} catch (Exception e) {
				System.out.println("    - " + candidate.name + ": Error verifying (" + e.getMessage() + ")");
			}
		}

		// Require at least 2 matching songs or 25% match rate to be confident
		int minMatches = Math.min(2, Math.max(1, knownSongs.size() / 4));
		if (bestMatch != null && bestScore >= minMatches) {
			System.out.println("  ✓ Best match: " + bestMatch.name + " (" + bestScore + " matches)");
			return bestMatch.mbid;
		} else {
			System.out.println("  ⚠ No confident match found (best score: " + bestScore + ", required: " + minMatches + ")");
			return null;
		}
	}

	/**
	 * Verify an artist's discography by checking how many of their MusicBrainz recordings
	 * match songs we have in our database.
	 * Returns the number of matching songs.
	 */
	private int verifyArtistDiscography(String mbid, List<String> knownSongs) 
			throws IOException, InterruptedException {
		respectMusicBrainzRateLimit();

		// Get artist's recordings from MusicBrainz (limit to 100 most popular)
		String url = MUSICBRAINZ_API + "/recording?artist=" + mbid + "&limit=100&fmt=json";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode recordings = root.get("recordings");
		if (recordings == null || !recordings.isArray()) {
			return 0;
		}

		// Normalize our known songs for comparison
		Set<String> normalizedKnownSongs = new HashSet<>();
		for (String song : knownSongs) {
			normalizedKnownSongs.add(normalizeSongTitle(song));
		}

		// Count how many MusicBrainz recordings match our songs
		int matches = 0;
		for (JsonNode recording : recordings) {
			JsonNode titleNode = recording.get("title");
			if (titleNode != null) {
				String title = normalizeSongTitle(titleNode.asText());
				if (normalizedKnownSongs.contains(title)) {
					matches++;
				}
			}
		}

		return matches;
	}

	/**
	 * Normalize song title for comparison
	 * Removes common variations like "(Radio Edit)", "[Live]", etc.
	 */
	private String normalizeSongTitle(String title) {
		if (title == null) {
			return "";
		}
		
		// Convert to lowercase
		String normalized = title.toLowerCase().trim();
		
		// Remove parenthetical/bracketed suffixes
		normalized = normalized.replaceAll("\\s*[\\(\\[].*?[\\)\\]]", "");
		
		// Remove "feat.", "ft.", "featuring", etc.
		normalized = normalized.replaceAll("\\s+(feat\\.|ft\\.|featuring|with)\\s+.*", "");
		
		// Remove multiple spaces
		normalized = normalized.replaceAll("\\s+", " ").trim();
		
		return normalized;
	}

	/**
	 * Get birth date, death date, and country from MusicBrainz using MBID
	 */
	private Map<String, Object> getArtistDataFromMusicBrainz(String mbid) throws IOException, InterruptedException {
		respectMusicBrainzRateLimit();

		String url = MUSICBRAINZ_API + "/artist/" + mbid + "?fmt=json";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		Map<String, Object> data = new HashMap<>();

		// Parse life-span object
		JsonNode lifeSpan = root.get("life-span");
		if (lifeSpan != null) {
			JsonNode beginNode = lifeSpan.get("begin");
			if (beginNode != null && !beginNode.isNull()) {
				LocalDate birthDate = parseDate(beginNode.asText());
				if (birthDate != null) {
					data.put("birth", birthDate);
				}
			}

			JsonNode endNode = lifeSpan.get("end");
			if (endNode != null && !endNode.isNull()) {
				LocalDate deathDate = parseDate(endNode.asText());
				if (deathDate != null) {
					data.put("death", deathDate);
				}
			}
		}

		// Get country/area
		JsonNode area = root.get("area");
		if (area != null) {
			JsonNode areaName = area.get("name");
			if (areaName != null && !areaName.isNull()) {
				data.put("country", areaName.asText());
			} else {
				// Fallback: try ISO code and convert to country name
				JsonNode isoCodes = area.get("iso-3166-1-codes");
				if (isoCodes != null && isoCodes.isArray() && isoCodes.size() > 0) {
					String isoCode = isoCodes.get(0).asText();
					String countryName = convertIsoCodeToCountryName(isoCode);
					if (countryName != null) {
						data.put("country", countryName);
					}
				}
			}
		}

		return data;
	}

	/**
	 * Convert ISO country code to full country name
	 */
	private String convertIsoCodeToCountryName(String isoCode) {
		if (isoCode == null || isoCode.length() != 2) {
			return isoCode;
		}

		@SuppressWarnings("deprecation")
		Locale locale = new Locale("", isoCode.toUpperCase());
		String countryName = locale.getDisplayCountry(Locale.ENGLISH);

		// If we got a valid country name, return it; otherwise return the code
		return (countryName != null && !countryName.isEmpty()) ? countryName : isoCode;
	}

	/**
	 * Search Wikidata for an artist by name
	 * Returns the Wikidata ID (e.g., Q12345)
	 */
	private String searchWikidataId(String artistName) throws IOException, InterruptedException {
		respectWikidataRateLimit();

		String query = URLEncoder.encode(artistName, StandardCharsets.UTF_8);
		String url = WIKIDATA_API + "?action=wbsearchentities&search=" + query 
				+ "&language=en&type=item&format=json&limit=5";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode results = root.get("search");
		if (results != null && results.isArray() && results.size() > 0) {
			// Try to find a musician/singer in results
			for (JsonNode result : results) {
				String id = result.get("id").asText();
				JsonNode description = result.get("description");
				if (description != null) {
					String desc = description.asText().toLowerCase();
					// Look for musician-related descriptions
					if (desc.contains("singer") || desc.contains("musician") || 
						desc.contains("rapper") || desc.contains("songwriter") ||
						desc.contains("artist") || desc.contains("composer") ||
						desc.contains("performer") || desc.contains("band member")) {
						return id;
					}
				}
			}
			// Fall back to first result
			return results.get(0).get("id").asText();
		}

		return null;
	}

	/**
	 * Get birth and death dates from Wikidata using entity ID
	 * P569 = date of birth
	 * P570 = date of death
	 */
	private Map<String, LocalDate> getArtistDatesFromWikidata(String entityId) throws IOException, InterruptedException {
		respectWikidataRateLimit();

		String url = WIKIDATA_API + "?action=wbgetentities&ids=" + entityId 
				+ "&props=claims&format=json";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		Map<String, LocalDate> dates = new HashMap<>();

		JsonNode entities = root.get("entities");
		if (entities != null && entities.has(entityId)) {
			JsonNode entity = entities.get(entityId);
			JsonNode claims = entity.get("claims");

			if (claims != null) {
				// P569 = date of birth
				LocalDate birthDate = extractWikidataDate(claims, "P569");
				if (birthDate != null) {
					dates.put("birth", birthDate);
				}

				// P570 = date of death
				LocalDate deathDate = extractWikidataDate(claims, "P570");
				if (deathDate != null) {
					dates.put("death", deathDate);
				}
			}
		}

		return dates;
	}

	/**
	 * Extract a date from Wikidata claims structure
	 */
	private LocalDate extractWikidataDate(JsonNode claims, String property) {
		JsonNode propertyClaims = claims.get(property);
		if (propertyClaims != null && propertyClaims.isArray() && propertyClaims.size() > 0) {
			JsonNode firstClaim = propertyClaims.get(0);
			JsonNode mainsnak = firstClaim.get("mainsnak");
			if (mainsnak != null) {
				JsonNode datavalue = mainsnak.get("datavalue");
				if (datavalue != null) {
					JsonNode value = datavalue.get("value");
					if (value != null) {
						JsonNode timeNode = value.get("time");
						if (timeNode != null) {
							// Wikidata time format: +1985-03-02T00:00:00Z
							String timeStr = timeNode.asText();
							return parseWikidataTime(timeStr);
						}
					}
				}
			}
		}
		return null;
	}

	/**
	 * Parse Wikidata time format: +1985-03-02T00:00:00Z
	 */
	private LocalDate parseWikidataTime(String timeStr) {
		try {
			// Remove leading + and trailing time portion
			if (timeStr.startsWith("+")) {
				timeStr = timeStr.substring(1);
			}
			if (timeStr.contains("T")) {
				timeStr = timeStr.substring(0, timeStr.indexOf("T"));
			}
			
			// Handle partial dates (year only, year-month only)
			String[] parts = timeStr.split("-");
			int year = Integer.parseInt(parts[0]);
			int month = parts.length > 1 ? Integer.parseInt(parts[1]) : 1;
			int day = parts.length > 2 ? Integer.parseInt(parts[2]) : 1;
			
			// Validate date components
			if (month < 1 || month > 12) month = 1;
			if (day < 1 || day > 31) day = 1;
			
			return LocalDate.of(year, month, day);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Parse date string in various formats (YYYY, YYYY-MM, YYYY-MM-DD)
	 */
	private LocalDate parseDate(String dateStr) {
		if (dateStr == null || dateStr.isEmpty()) {
			return null;
		}

		// Try full date first
		try {
			return LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
		} catch (DateTimeParseException e) {
			// Try year-month
			try {
				String[] parts = dateStr.split("-");
				if (parts.length == 2) {
					int year = Integer.parseInt(parts[0]);
					int month = Integer.parseInt(parts[1]);
					return LocalDate.of(year, month, 1);
				} else if (parts.length == 1) {
					// Year only
					int year = Integer.parseInt(parts[0]);
					return LocalDate.of(year, 1, 1);
				}
			} catch (NumberFormatException nfe) {
				return null;
			}
		}
		return null;
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
			return "{}"; // Not found
		} else if (responseCode != 200) {
			throw new IOException("HTTP " + responseCode + ": " + conn.getResponseMessage());
		}

		try (InputStream is = conn.getInputStream()) {
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/**
	 * Respect MusicBrainz rate limit of 1 request per second
	 */
	private void respectMusicBrainzRateLimit() throws InterruptedException {
		long now = System.currentTimeMillis();
		long timeSinceLastRequest = now - lastMusicBrainzRequest;

		if (timeSinceLastRequest < MUSICBRAINZ_DELAY_MS) {
			long sleepTime = MUSICBRAINZ_DELAY_MS - timeSinceLastRequest;
			Thread.sleep(sleepTime);
		}

		lastMusicBrainzRequest = System.currentTimeMillis();
	}

	/**
	 * Respect Wikidata rate limit (being polite with 1 request per second)
	 */
	private void respectWikidataRateLimit() throws InterruptedException {
		long now = System.currentTimeMillis();
		long timeSinceLastRequest = now - lastWikidataRequest;

		if (timeSinceLastRequest < WIKIDATA_DELAY_MS) {
			long sleepTime = WIKIDATA_DELAY_MS - timeSinceLastRequest;
			Thread.sleep(sleepTime);
		}

		lastWikidataRequest = System.currentTimeMillis();
	}

	/**
	 * Update artist birth date in database
	 */
	private void updateArtistBirthDate(int artistId, LocalDate birthDate) {
		String sql = "UPDATE Artist SET birth_date = ? WHERE id = ?";
		jdbcTemplate.update(sql, birthDate.toString(), artistId);
	}

	/**
	 * Update artist death date in database
	 */
	private void updateArtistDeathDate(int artistId, LocalDate deathDate) {
		String sql = "UPDATE Artist SET death_date = ? WHERE id = ?";
		jdbcTemplate.update(sql, deathDate.toString(), artistId);
	}

	/**
	 * Update artist country in database (always overwrites)
	 */
	private void updateArtistCountry(int artistId, String country) {
		String sql = "UPDATE Artist SET country = ? WHERE id = ?";
		jdbcTemplate.update(sql, country, artistId);
	}

	/**
	 * Print summary statistics
	 */
	private void printSummary() {
		System.out.println("========================================");
		System.out.println("Summary");
		System.out.println("========================================");
		System.out.println("Artists processed: " + artistsProcessed);
		System.out.println("Artists skipped (already had data): " + artistsSkipped);
		System.out.println("Artists not found in APIs: " + artistsNotFound);
		System.out.println();
		System.out.println("Birth dates from MusicBrainz: " + birthDatesFromMusicBrainz);
		System.out.println("Birth dates from Wikidata: " + birthDatesFromWikidata);
		System.out.println("Death dates from MusicBrainz: " + deathDatesFromMusicBrainz);
		System.out.println("Death dates from Wikidata: " + deathDatesFromWikidata);
		System.out.println();
		System.out.println("Total birth dates found: " + (birthDatesFromMusicBrainz + birthDatesFromWikidata));
		System.out.println("Total death dates found: " + (deathDatesFromMusicBrainz + deathDatesFromWikidata));
		System.out.println("Countries updated: " + countriesUpdated);
		System.out.println("API errors: " + apiErrors);
		System.out.println();

		if (dryRun) {
			System.out.println("*** DRY RUN - No changes were saved to database ***");
		} else {
			System.out.println("Done! Changes saved to database.");
		}
	}

	/**
	 * Data class for artist info
	 */
	private static class ArtistData {
		int id;
		String name;
		String birthDate;
		String deathDate;
		String country;
	}

	/**
	 * Data class for MusicBrainz artist candidates during disambiguation
	 */
	private static class ArtistCandidate {
		String mbid;
		String name;
		String country;
		String disambiguation;
		int matchScore;
	}
}
