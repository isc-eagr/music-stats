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
 * Standalone script to populate music metadata using MusicBrainz and Cover Art
 * Archive APIs.
 * 
 * Features: - Populates Artist.country from MusicBrainz - Populates
 * Artist.is_band from MusicBrainz artist type (Person vs Group/Orchestra/Choir)
 * - Populates ArtistMember table from MusicBrainz band member relationships -
 * Populates Album.image from Cover Art Archive - Populates Song.single_cover
 * for singles from Cover Art Archive
 * 
 * Requirements: - Respects MusicBrainz rate limit (1 request per second) - No
 * rate limit for Cover Art Archive - Requires internet connection
 * 
 * Usage: Run main() method directly from IDE or via: mvnw exec:java
 * -Dexec.mainClass="library.MusicBrainzMetadataPopulator"
 * 
 * Options: --dry-run Don't save changes to database --artists-only Only
 * populate artist data (country, type, members) --artist-types Only populate
 * artist is_band field --band-members Only populate band member relationships
 * --albums-only Only populate album images --songs-only Only populate single
 * cover art --force Re-populate even if data already exists --image-small Use
 * 250px images --image-medium Use 500px images (default: 1200px) --image-large
 * Use 1200px images --image-original Use original full-quality images
 */
public class MusicBrainzMetadataPopulator {

	private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";

	// API Configuration
	private static final String MUSICBRAINZ_API = "https://musicbrainz.org/ws/2";
	private static final String COVERART_API = "https://coverartarchive.org";
	private static final String USER_AGENT = "MusicStatsApp/1.0 ( isc.eagr@gmail.com )";
	private static final long MUSICBRAINZ_DELAY_MS = 1100; // 1.1 seconds to be safe

	// Image quality options: "250", "500", "1200", or "" for full original
	private String imageSize = "1200"; // Default to high quality

	private JdbcTemplate jdbcTemplate;
	private ObjectMapper objectMapper;
	private long lastMusicBrainzRequest = 0;

	// Statistics
	private int artistsProcessed = 0;
	private int artistCountriesUpdated = 0;
	private int artistTypesUpdated = 0;
	private int bandMembersAdded = 0;
	private int albumsProcessed = 0;
	private int albumImagesUpdated = 0;
	private int songsProcessed = 0;
	private int songImagesUpdated = 0;
	private int apiErrors = 0;

	// Configuration flags
	private boolean dryRun = false;
	private boolean populateArtistCountries = false;
	private boolean populateArtistTypes = false;
	private boolean populateBandMembers = false;
	private boolean populateAlbumImages = false;
	private boolean populateSongImages = true;
	private boolean skipExisting = true;

	public MusicBrainzMetadataPopulator() {
		// Setup database connection
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("org.sqlite.JDBC");
		dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.objectMapper = new ObjectMapper();
	}

	public static void main(String[] args) {
		MusicBrainzMetadataPopulator populator = new MusicBrainzMetadataPopulator();

		// Parse command line arguments
		for (String arg : args) {
			switch (arg.toLowerCase()) {
			case "--dry-run":
				populator.dryRun = true;
				break;
			case "--artists-only":
				populator.populateAlbumImages = false;
				populator.populateSongImages = false;
				break;
			case "--albums-only":
				populator.populateArtistCountries = false;
				populator.populateArtistTypes = false;
				populator.populateBandMembers = false;
				populator.populateSongImages = false;
				break;
			case "--songs-only":
				populator.populateArtistCountries = false;
				populator.populateArtistTypes = false;
				populator.populateBandMembers = false;
				populator.populateAlbumImages = false;
				break;
			case "--artist-types":
				populator.populateArtistTypes = true;
				populator.populateArtistCountries = false;
				populator.populateAlbumImages = false;
				populator.populateSongImages = false;
				populator.populateBandMembers = false;
				break;
			case "--band-members":
				populator.populateBandMembers = true;
				populator.populateArtistCountries = false;
				populator.populateArtistTypes = false;
				populator.populateAlbumImages = false;
				populator.populateSongImages = false;
				break;
			case "--force":
				populator.skipExisting = false;
				break;
			case "--image-small":
				populator.imageSize = "250";
				break;
			case "--image-medium":
				populator.imageSize = "500";
				break;
			case "--image-large":
				populator.imageSize = "1200";
				break;
			case "--image-original":
				populator.imageSize = "";
				break;
			}
		}

		populator.run();
	}

	public void run() {
		System.out.println("========================================");
		System.out.println("MusicBrainz Metadata Populator");
		System.out.println("========================================");
		System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
		System.out.println("Skip existing: " + skipExisting);
		String imageSizeDesc = imageSize.isEmpty() ? "Original (full quality)" : imageSize + "px";
		System.out.println("Image quality: " + imageSizeDesc);
		System.out.println();

		if (populateArtistCountries) {
			try {
				System.out.println(">>> Populating Artist Countries...");
				populateArtistCountriesFromMusicBrainz();
				System.out.println();
			} catch (Exception e) {
				System.err.println("Error populating artist countries: " + e.getMessage());
				e.printStackTrace();
				System.out.println();
			}
		}

		if (populateArtistTypes) {
			try {
				System.out.println(">>> Populating Artist Types (is_band)...");
				populateArtistTypesFromMusicBrainz();
				System.out.println();
			} catch (Exception e) {
				System.err.println("Error populating artist types: " + e.getMessage());
				e.printStackTrace();
				System.out.println();
			}
		}

		if (populateBandMembers) {
			try {
				System.out.println(">>> Populating Band Members...");
				populateBandMembersFromMusicBrainz();
				System.out.println();
			} catch (Exception e) {
				System.err.println("Error populating band members: " + e.getMessage());
				e.printStackTrace();
				System.out.println();
			}
		}

		if (populateAlbumImages) {
			try {
				System.out.println(">>> Populating Album Cover Art...");
				populateAlbumImagesFromCoverArt();
				System.out.println();
			} catch (Exception e) {
				System.err.println("Error populating album images: " + e.getMessage());
				e.printStackTrace();
				System.out.println();
			}
		}

		if (populateSongImages) {
			try {
				System.out.println(">>> Populating Single Cover Art...");
				populateSongImagesFromCoverArt();
				System.out.println();
			} catch (Exception e) {
				System.err.println("Error populating song images: " + e.getMessage());
				e.printStackTrace();
				System.out.println();
			}
		}

		printSummary();
	}

	/**
	 * Populate artist countries from MusicBrainz
	 */
	private void populateArtistCountriesFromMusicBrainz() {
		String whereClause = skipExisting ? "WHERE country IS NULL OR country = ''" : "";
		String sql = "SELECT id, name FROM Artist " + whereClause + " ORDER BY id";

		List<ArtistData> artists = jdbcTemplate.query(sql, (rs, rowNum) -> {
			ArtistData artist = new ArtistData();
			artist.id = rs.getInt("id");
			artist.name = rs.getString("name");
			return artist;
		});

		System.out.println("Found " + artists.size() + " artists to process");
		System.out.println();

		for (ArtistData artist : artists) {
			artistsProcessed++;

			try {
				System.out.println("[" + artistsProcessed + "/" + artists.size() + "] Processing: " + artist.name);

				// Search MusicBrainz for artist
				String mbid = searchArtistMBID(artist.name);

				if (mbid != null) {
					// Get artist details including country
					String country = getArtistCountry(mbid);

					if (country != null && !country.isEmpty()) {
						System.out.println("  ✓ Found country: " + country);

						if (!dryRun) {
							updateArtistCountry(artist.id, country);
						}
						artistCountriesUpdated++;
					} else {
						System.out.println("  ⚠ No country information available");
					}
				} else {
					System.out.println("  ✗ Not found in MusicBrainz");
				}

			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}

			System.out.println();
		}
	}

	/**
	 * Populate album images from Cover Art Archive
	 */
	private void populateAlbumImagesFromCoverArt() {
		String whereClause = skipExisting ? "WHERE (a.image IS NULL OR LENGTH(a.image) = 0)" : "";
		String sql = """
				SELECT a.id, a.name, a.artist_id, ar.name as artist_name
				FROM Album a
				INNER JOIN Artist ar ON a.artist_id = ar.id
				""" + whereClause + " ORDER BY a.id";

		List<AlbumData> albums = jdbcTemplate.query(sql, (rs, rowNum) -> {
			AlbumData album = new AlbumData();
			album.id = rs.getInt("id");
			album.name = rs.getString("name");
			album.artistId = rs.getInt("artist_id");
			album.artistName = rs.getString("artist_name");
			return album;
		});

		System.out.println("Found " + albums.size() + " albums to process");
		System.out.println();

		for (AlbumData album : albums) {
			albumsProcessed++;

			try {
				System.out.println("[" + albumsProcessed + "/" + albums.size() + "] Processing: " + album.artistName
						+ " - " + album.name);

				// Search for release MBID
				String mbid = searchReleaseMBID(album.name, album.artistName);

				if (mbid != null) {
					// Try to get cover art
					byte[] imageData = getCoverArtImage(mbid);

					if (imageData != null && imageData.length > 0) {
						System.out.println("  ✓ Downloaded cover art (" + formatBytes(imageData.length) + ")");

						if (!dryRun) {
							updateAlbumImage(album.id, imageData);
						}
						albumImagesUpdated++;
					} else {
						System.out.println("  ⚠ No cover art available");
					}
				} else {
					System.out.println("  ✗ Release not found in MusicBrainz");
				}

			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}

			System.out.println();
		}
	}

	/**
	 * Populate song images for singles from Cover Art Archive
	 */
	private void populateSongImagesFromCoverArt() {
		String whereClause = skipExisting ? "WHERE (s.single_cover IS NULL OR LENGTH(s.single_cover) = 0) " : "";
		String sql = """
				SELECT s.id, s.name, s.artist_id, ar.name as artist_name
				FROM Song s
				INNER JOIN Artist ar ON s.artist_id = ar.id
				""" + whereClause + " ORDER BY s.id";

		List<SongData> songs = jdbcTemplate.query(sql, (rs, rowNum) -> {
			SongData song = new SongData();
			song.id = rs.getInt("id");
			song.name = rs.getString("name");
			song.artistId = rs.getInt("artist_id");
			song.artistName = rs.getString("artist_name");
			return song;
		});

		System.out.println("Found " + songs.size() + " singles to process");
		System.out.println();

		for (SongData song : songs) {
			songsProcessed++;

			try {
				System.out.println("[" + songsProcessed + "/" + songs.size() + "] Processing: " + song.artistName
						+ " - " + song.name);

				// Search for recording/release MBID
				String mbid = searchReleaseMBID(song.name, song.artistName);

				if (mbid != null) {
					// Try to get cover art
					byte[] imageData = getCoverArtImage(mbid);

					if (imageData != null && imageData.length > 0) {
						System.out.println("  ✓ Downloaded single cover (" + formatBytes(imageData.length) + ")");

						if (!dryRun) {
							updateSongImage(song.id, imageData);
							// Mark as single since we found a single cover
							markSongAsSingle(song.id);
						}
						songImagesUpdated++;
					} else {
						System.out.println("  ⚠ No cover art available");
					}
				} else {
					System.out.println("  ✗ Single not found in MusicBrainz");
				}

			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}

			System.out.println();
		}
	}

	/**
	 * Populate artist types (is_band field) from MusicBrainz
	 */
	private void populateArtistTypesFromMusicBrainz() {
		String sql = "SELECT id, name FROM Artist ORDER BY id";

		List<ArtistData> artists = jdbcTemplate.query(sql, (rs, rowNum) -> {
			ArtistData artist = new ArtistData();
			artist.id = rs.getInt("id");
			artist.name = rs.getString("name");
			return artist;
		});

		System.out.println("Found " + artists.size() + " artists to process");
		System.out.println();

		for (ArtistData artist : artists) {
			try {
				System.out.println(
						"[" + (artistTypesUpdated + 1) + "/" + artists.size() + "] Processing: " + artist.name);

				// Search MusicBrainz for artist
				String mbid = searchArtistMBID(artist.name);

				if (mbid != null) {
					// Get artist type
					String artistType = getArtistType(mbid);

					if (artistType != null) {
						// Determine if this is a band/group
						// Group, Orchestra, Choir = band (is_band = 1)
						// Person, Character, Other = solo (is_band = 0)
						boolean isBand = artistType.equalsIgnoreCase("Group")
								|| artistType.equalsIgnoreCase("Orchestra") || artistType.equalsIgnoreCase("Choir");

						System.out.println("  ✓ Type: " + artistType + " (is_band=" + (isBand ? 1 : 0) + ")");

						if (!dryRun) {
							updateArtistType(artist.id, isBand);
						}
						artistTypesUpdated++;
					} else {
						System.out.println("  ⚠ No type information available");
					}
				} else {
					System.out.println("  ✗ Not found in MusicBrainz");
				}

			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}

			System.out.println();
		}
	}

	/**
	 * Populate band member relationships from MusicBrainz
	 */
	private void populateBandMembersFromMusicBrainz() {
		// Only process artists that are bands
		String sql = "SELECT id, name FROM Artist WHERE is_band = 1 ORDER BY id";

		List<ArtistData> bands = jdbcTemplate.query(sql, (rs, rowNum) -> {
			ArtistData artist = new ArtistData();
			artist.id = rs.getInt("id");
			artist.name = rs.getString("name");
			return artist;
		});

		System.out.println("Found " + bands.size() + " bands to process");
		System.out.println();

		int bandsProcessed = 0;
		for (ArtistData band : bands) {
			bandsProcessed++;

			try {
				System.out.println("[" + bandsProcessed + "/" + bands.size() + "] Processing: " + band.name);

				// Search MusicBrainz for band
				String mbid = searchArtistMBID(band.name);

				if (mbid != null) {
					// Get band members
					List<String> memberNames = getBandMembers(mbid);

					if (memberNames != null && !memberNames.isEmpty()) {
						System.out.println("  ✓ Found " + memberNames.size() + " member(s)");

						for (String memberName : memberNames) {
							// Try to find this member in our database
							Integer memberId = findArtistByName(memberName);

							if (memberId != null) {
								System.out.println("    - " + memberName + " (ID: " + memberId + ")");

								if (!dryRun) {
									addBandMember(band.id, memberId);
								}
								bandMembersAdded++;
							} else {
								System.out.println("    - " + memberName + " (not in database, skipping)");
							}
						}
					} else {
						System.out.println("  ⚠ No member information available");
					}
				} else {
					System.out.println("  ✗ Not found in MusicBrainz");
				}

			} catch (Exception e) {
				System.err.println("  ✗ Error: " + e.getMessage());
				apiErrors++;
			}

			System.out.println();
		}
	}

	/**
	 * Search MusicBrainz for an artist and return the MBID
	 */
	private String searchArtistMBID(String artistName) throws IOException, InterruptedException {
		respectRateLimit();

		String query = URLEncoder.encode(artistName, StandardCharsets.UTF_8);
		String url = MUSICBRAINZ_API + "/artist?query=artist:" + query + "&fmt=json&limit=1";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode artists = root.get("artists");
		if (artists != null && artists.isArray() && artists.size() > 0) {
			JsonNode firstArtist = artists.get(0);
			return firstArtist.get("id").asText();
		}

		return null;
	}

	/**
	 * Get artist country from MusicBrainz using MBID Returns the full country name
	 * (e.g., "United States") instead of ISO code
	 */
	private String getArtistCountry(String mbid) throws IOException, InterruptedException {
		respectRateLimit();

		String url = MUSICBRAINZ_API + "/artist/" + mbid + "?inc=area-rels&fmt=json";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		// Try to get country from area
		JsonNode area = root.get("area");
		if (area != null) {
			// First try to get the area name directly
			JsonNode areaName = area.get("name");
			if (areaName != null && !areaName.isNull()) {
				return areaName.asText();
			}

			// Fallback: get ISO code and convert to country name
			JsonNode isoCodes = area.get("iso-3166-1-codes");
			if (isoCodes != null && isoCodes.isArray() && isoCodes.size() > 0) {
				String isoCode = isoCodes.get(0).asText();
				return convertIsoCodeToCountryName(isoCode);
			}
		}

		return null;
	}

	/**
	 * Get artist type from MusicBrainz using MBID Returns: "Person", "Group",
	 * "Orchestra", "Choir", "Character", "Other", or null
	 */
	private String getArtistType(String mbid) throws IOException, InterruptedException {
		respectRateLimit();

		String url = MUSICBRAINZ_API + "/artist/" + mbid + "?fmt=json";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode typeNode = root.get("type");
		if (typeNode != null && !typeNode.isNull()) {
			return typeNode.asText();
		}

		return null;
	}

	/**
	 * Get band members from MusicBrainz using MBID Returns list of member artist
	 * names
	 */
	private List<String> getBandMembers(String mbid) throws IOException, InterruptedException {
		respectRateLimit();

		String url = MUSICBRAINZ_API + "/artist/" + mbid + "?inc=artist-rels&fmt=json";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		List<String> members = new ArrayList<>();

		JsonNode relations = root.get("relations");
		if (relations != null && relations.isArray()) {
			for (JsonNode relation : relations) {
				// Look for "member of band" relationships
				JsonNode type = relation.get("type");
				if (type != null && type.asText().equals("member of band")) {
					// Get the target artist (the member)
					JsonNode artist = relation.get("artist");
					if (artist != null) {
						JsonNode name = artist.get("name");
						if (name != null) {
							members.add(name.asText());
						}
					}
				}
			}
		}

		return members;
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
	 * Search MusicBrainz for a release and return the MBID
	 */
	private String searchReleaseMBID(String releaseName, String artistName) throws IOException, InterruptedException {
		respectRateLimit();

		String query = URLEncoder.encode("release:\"" + releaseName + "\" AND artist:\"" + artistName + "\"",
				StandardCharsets.UTF_8);
		String url = MUSICBRAINZ_API + "/release?query=" + query + "&fmt=json&limit=1";

		String response = makeHttpRequest(url);
		JsonNode root = objectMapper.readTree(response);

		JsonNode releases = root.get("releases");
		if (releases != null && releases.isArray() && releases.size() > 0) {
			JsonNode firstRelease = releases.get(0);
			return firstRelease.get("id").asText();
		}

		return null;
	}

	/**
	 * Get cover art image from Cover Art Archive No rate limiting needed for Cover
	 * Art Archive
	 */
	private byte[] getCoverArtImage(String releaseMbid) throws IOException {
		String url;

		if (imageSize.isEmpty()) {
			// Get original full-quality image
			url = COVERART_API + "/release/" + releaseMbid + "/front";
		} else {
			// Get specific size (250, 500, or 1200)
			url = COVERART_API + "/release/" + releaseMbid + "/front-" + imageSize;
		}

		try {
			return downloadImage(url);
		} catch (IOException e) {
			// Fallback: try original if sized version fails
			if (!imageSize.isEmpty()) {
				url = COVERART_API + "/release/" + releaseMbid + "/front";
				return downloadImage(url);
			}
			throw e;
		}
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
	 * Download binary image data
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
	 * Respect MusicBrainz rate limit of 1 request per second
	 */
	private void respectRateLimit() throws InterruptedException {
		long now = System.currentTimeMillis();
		long timeSinceLastRequest = now - lastMusicBrainzRequest;

		if (timeSinceLastRequest < MUSICBRAINZ_DELAY_MS) {
			long sleepTime = MUSICBRAINZ_DELAY_MS - timeSinceLastRequest;
			Thread.sleep(sleepTime);
		}

		lastMusicBrainzRequest = System.currentTimeMillis();
	}

	/**
	 * Update artist country in database
	 */
	private void updateArtistCountry(int artistId, String country) {
		String sql = "UPDATE Artist SET country = ? WHERE id = ?";
		jdbcTemplate.update(sql, country, artistId);
	}

	/**
	 * Update artist type (is_band) in database
	 */
	private void updateArtistType(int artistId, boolean isBand) {
		String sql = "UPDATE Artist SET is_band = ? WHERE id = ?";
		jdbcTemplate.update(sql, isBand ? 1 : 0, artistId);
	}

	/**
	 * Find artist by name in database (case-insensitive)
	 */
	private Integer findArtistByName(String name) {
		String sql = "SELECT id FROM Artist WHERE LOWER(name) = LOWER(?) LIMIT 1";
		try {
			return jdbcTemplate.queryForObject(sql, Integer.class, name);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Add band member relationship to ArtistMember table
	 */
	private void addBandMember(int groupArtistId, int memberArtistId) {
		String checkSql = "SELECT COUNT(*) FROM ArtistMember WHERE group_artist_id = ? AND member_artist_id = ?";
		Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, groupArtistId, memberArtistId);

		if (count == null || count == 0) {
			String insertSql = "INSERT INTO ArtistMember (group_artist_id, member_artist_id) VALUES (?, ?)";
			jdbcTemplate.update(insertSql, groupArtistId, memberArtistId);
		}
	}

	/**
	 * Update album image in database
	 */
	private void updateAlbumImage(int albumId, byte[] imageData) {
		String sql = "UPDATE Album SET image = ? WHERE id = ?";
		jdbcTemplate.update(sql, imageData, albumId);
	}

	/**
	 * Update song image in database
	 */
	private void updateSongImage(int songId, byte[] imageData) {
		String sql = "UPDATE Song SET single_cover = ? WHERE id = ?";
		jdbcTemplate.update(sql, imageData, songId);
	}

	/**
	 * Mark a song as a single
	 */
	private void markSongAsSingle(int songId) {
		String sql = "UPDATE Song SET is_single = 1 WHERE id = ?";
		jdbcTemplate.update(sql, songId);
	}

	/**
	 * Format bytes to human-readable format
	 */
	private String formatBytes(long bytes) {
		if (bytes < 1024)
			return bytes + " B";
		if (bytes < 1024 * 1024)
			return String.format("%.1f KB", bytes / 1024.0);
		return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
	}

	/**
	 * Print summary statistics
	 */
	private void printSummary() {
		System.out.println("========================================");
		System.out.println("Summary");
		System.out.println("========================================");

		if (populateArtistCountries) {
			System.out.println("Artists processed: " + artistsProcessed);
			System.out.println("Artist countries updated: " + artistCountriesUpdated);
		}

		if (populateArtistTypes) {
			System.out.println("Artist types updated: " + artistTypesUpdated);
		}

		if (populateBandMembers) {
			System.out.println("Band member relationships added: " + bandMembersAdded);
		}

		if (populateAlbumImages) {
			System.out.println("Albums processed: " + albumsProcessed);
			System.out.println("Album images updated: " + albumImagesUpdated);
		}

		if (populateSongImages) {
			System.out.println("Singles processed: " + songsProcessed);
			System.out.println("Single images updated: " + songImagesUpdated);
		}

		System.out.println("API errors: " + apiErrors);
		System.out.println();

		if (dryRun) {
			System.out.println("*** DRY RUN - No changes were saved to database ***");
		} else {
			System.out.println("Done! Changes saved to database.");
		}
	}

	/**
	 * Data classes
	 */
	private static class ArtistData {
		int id;
		String name;
	}

	private static class AlbumData {
		int id;
		String name;
		int artistId;
		String artistName;
	}

	private static class SongData {
		int id;
		String name;
		int artistId;
		String artistName;
	}
}
