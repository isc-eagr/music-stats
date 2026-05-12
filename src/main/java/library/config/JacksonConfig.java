package library.config;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.module.SimpleModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Jackson configuration to handle custom date formats.
 * Supports parsing dates in dd/MM/yyyy format for JSON requests.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public JacksonModule sqlDateModule() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Date.class, new SqlDateDeserializer());
        return module;
    }

    /**
     * Custom deserializer for java.sql.Date that handles multiple date formats.
     */
    public static class SqlDateDeserializer extends ValueDeserializer<Date> {

        private static final SimpleDateFormat[] FORMATS = {
            new SimpleDateFormat("dd/MM/yyyy"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("dd-MM-yyyy"),
            new SimpleDateFormat("dd MMM yyyy")
        };

        @Override
        public Date deserialize(JsonParser p, DeserializationContext ctxt) {
            String text;
            try {
                text = p.getText();
            } catch (Exception e) {
                return null;
            }
            if (text == null || text.trim().isEmpty()) {
                return null;
            }

            text = text.trim();

            // Try each format in order
            for (SimpleDateFormat format : FORMATS) {
                try {
                    synchronized (format) {  // SimpleDateFormat is not thread-safe
                        java.util.Date parsed = format.parse(text);
                        return new Date(parsed.getTime());
                    }
                } catch (ParseException e) {
                    // Try next format
                }
            }

            // If no format worked, throw an error with helpful message
            throw new IllegalArgumentException("Cannot parse date '" + text +
                "'. Supported formats: dd/MM/yyyy, yyyy-MM-dd, dd-MM-yyyy, dd MMM yyyy");
        }
    }
}
