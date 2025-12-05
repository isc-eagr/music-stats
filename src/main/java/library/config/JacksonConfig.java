package library.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.io.IOException;
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
    public ObjectMapper objectMapper(Jackson2ObjectMapperBuilder builder) {
        ObjectMapper mapper = builder.build();
        
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Date.class, new SqlDateDeserializer());
        mapper.registerModule(module);
        
        return mapper;
    }
    
    /**
     * Custom deserializer for java.sql.Date that handles multiple date formats.
     */
    public static class SqlDateDeserializer extends JsonDeserializer<Date> {
        
        private static final SimpleDateFormat[] FORMATS = {
            new SimpleDateFormat("dd/MM/yyyy"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("dd-MM-yyyy"),
            new SimpleDateFormat("dd MMM yyyy")
        };
        
        @Override
        public Date deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getText();
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
            throw new IOException("Cannot parse date '" + text + 
                "'. Supported formats: dd/MM/yyyy, yyyy-MM-dd, dd-MM-yyyy, dd MMM yyyy");
        }
    }
}
