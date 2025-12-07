package library;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.List;
import java.util.Locale;

/**
 * Standalone script to convert existing ISO country codes to full country names in the Artist table.
 * 
 * Converts codes like "US", "MX", "GB" to "United States", "Mexico", "United Kingdom"
 * 
 * Usage: Run main() method directly from IDE or via:
 *   mvnw exec:java -Dexec.mainClass="library.CountryCodeToNameConverter"
 */
public class CountryCodeToNameConverter {
    
    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    
    private JdbcTemplate jdbcTemplate;
    private boolean dryRun = false;
    
    // Statistics
    private int artistsProcessed = 0;
    private int artistsUpdated = 0;
    private int artistsSkipped = 0;
    private int artistsAlreadyNames = 0;
    
    public CountryCodeToNameConverter() {
        // Setup database connection
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + DB_PATH);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    public static void main(String[] args) {
        CountryCodeToNameConverter converter = new CountryCodeToNameConverter();
        
        // Parse command line arguments
        for (String arg : args) {
            if ("--dry-run".equalsIgnoreCase(arg)) {
                converter.dryRun = true;
            }
        }
        
        converter.run();
    }
    
    public void run() {
        System.out.println("========================================");
        System.out.println("Country Code to Name Converter");
        System.out.println("========================================");
        System.out.println("Mode: " + (dryRun ? "DRY RUN (no changes will be saved)" : "LIVE"));
        System.out.println();
        
        // Get all artists with non-null country values
        String sql = "SELECT id, name, country FROM Artist WHERE country IS NOT NULL AND country != ''";
        
        List<ArtistData> artists = jdbcTemplate.query(sql, (rs, rowNum) -> {
            ArtistData artist = new ArtistData();
            artist.id = rs.getInt("id");
            artist.name = rs.getString("name");
            artist.country = rs.getString("country");
            return artist;
        });
        
        System.out.println("Found " + artists.size() + " artists with country data");
        System.out.println();
        
        for (ArtistData artist : artists) {
            artistsProcessed++;
            
            String currentCountry = artist.country.trim();
            
            // Check if it's already a country name (length > 2 or not all uppercase)
            if (currentCountry.length() > 2 || !currentCountry.equals(currentCountry.toUpperCase())) {
                artistsAlreadyNames++;
                if (artistsProcessed <= 10) {
                    System.out.println("[" + artistsProcessed + "/" + artists.size() + "] " + artist.name + 
                                     ": Already a name (\"" + currentCountry + "\")");
                }
                continue;
            }
            
            // Try to convert the code to a country name
            String countryName = convertIsoCodeToCountryName(currentCountry);
            
            if (countryName != null && !countryName.equals(currentCountry)) {
                System.out.println("[" + artistsProcessed + "/" + artists.size() + "] " + artist.name);
                System.out.println("  Converting: \"" + currentCountry + "\" → \"" + countryName + "\"");
                
                if (!dryRun) {
                    updateArtistCountry(artist.id, countryName);
                }
                artistsUpdated++;
            } else {
                System.out.println("[" + artistsProcessed + "/" + artists.size() + "] " + artist.name + 
                                 ": Could not convert \"" + currentCountry + "\"");
                artistsSkipped++;
            }
        }
        
        System.out.println();
        printSummary();
    }
    
    /**
     * Convert ISO country code to full country name
     */
    private String convertIsoCodeToCountryName(String isoCode) {
        if (isoCode == null || isoCode.trim().isEmpty()) {
            return null;
        }
        
        String code = isoCode.trim().toUpperCase();
        
        // Must be exactly 2 characters to be a valid ISO code
        if (code.length() != 2) {
            return null;
        }
        
        // Verify it's a valid ISO country code
        String[] isoCodes = Locale.getISOCountries();
        boolean isValid = false;
        for (String validCode : isoCodes) {
            if (validCode.equals(code)) {
                isValid = true;
                break;
            }
        }
        
        if (!isValid) {
            return null;
        }
        
        // Convert to country name
        @SuppressWarnings("deprecation")
        Locale locale = new Locale("", code);
        String countryName = locale.getDisplayCountry(Locale.ENGLISH);
        
        // Return the name if valid, otherwise return null
        return (countryName != null && !countryName.isEmpty()) ? countryName : null;
    }
    
    /**
     * Update artist country in database
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
        System.out.println("Artists updated: " + artistsUpdated);
        System.out.println("Artists already names: " + artistsAlreadyNames);
        System.out.println("Artists skipped (invalid code): " + artistsSkipped);
        System.out.println();
        
        if (dryRun) {
            System.out.println("*** DRY RUN - No changes were saved to database ***");
        } else {
            System.out.println("Done! Changes saved to database.");
        }
    }
    
    /**
     * Data class to hold artist information
     */
    private static class ArtistData {
        int id;
        String name;
        String country;
    }
}
