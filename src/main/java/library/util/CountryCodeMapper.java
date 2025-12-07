package library.util;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Component("countryCodeMapper")
public class CountryCodeMapper {
    private final Map<String, String> nameToCode = new HashMap<>();

    public CountryCodeMapper() {
        String[] iso = Locale.getISOCountries();
        for (String code : iso) {
            @SuppressWarnings("deprecation")
            Locale loc = new Locale("", code);
            String name = loc.getDisplayCountry(Locale.ENGLISH);
            if (name != null && !name.isBlank()) {
                nameToCode.put(normalize(name), code.toLowerCase());
            }
        }
        // Add some common aliases
        nameToCode.put("usa", "us");
        nameToCode.put("united states", "us");
        nameToCode.put("uk", "gb");
        nameToCode.put("great britain", "gb");
        nameToCode.put("russia", "ru");
    }

    private String normalize(String s) {
        return s == null ? null : s.trim().toLowerCase();
    }

    public String getCode(String countryNameOrCode) {
        if (countryNameOrCode == null) return null;
        
        String normalized = normalize(countryNameOrCode);
        
        // Check if it's already a valid ISO code (2 letters)
        if (normalized.length() == 2) {
            // Verify it's a valid ISO country code
            String[] isoCodes = Locale.getISOCountries();
            for (String code : isoCodes) {
                if (code.equalsIgnoreCase(normalized)) {
                    return normalized;
                }
            }
        }
        
        // Try to look up by country name
        String code = nameToCode.get(normalized);
        
        // If found, return it
        if (code != null) {
            return code;
        }
        
        // Last resort: check if the input is already a country name that matches a Locale
        // Try to find the code by checking all ISO countries
        for (String isoCode : Locale.getISOCountries()) {
            @SuppressWarnings("deprecation")
            Locale locale = new Locale("", isoCode);
            String countryName = locale.getDisplayCountry(Locale.ENGLISH);
            if (countryName.equalsIgnoreCase(countryNameOrCode)) {
                return isoCode.toLowerCase();
            }
        }
        
        return null;
    }

    public Map<String, String> getNameToCodeMap() {
        return nameToCode;
    }
}
