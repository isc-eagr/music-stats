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

    public String getCode(String countryName) {
        if (countryName == null) return null;
        String code = nameToCode.get(normalize(countryName));
        return code;
    }

    public Map<String, String> getNameToCodeMap() {
        return nameToCode;
    }
}
