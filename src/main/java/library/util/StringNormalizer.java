package library.util;

import java.text.Normalizer;

/**
 * Utility class for normalizing strings for accent-insensitive search.
 * Handles Spanish and Portuguese diacritical marks (á, é, í, ó, ú, ñ, ç, ü, ã, õ, etc.)
 */
public final class StringNormalizer {
    
    private StringNormalizer() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Removes diacritical marks (accents) from a string using Unicode normalization.
     * E.g., "José María" → "Jose Maria", "São Paulo" → "Sao Paulo", "Ñejo" → "Nejo"
     * 
     * Note: Some characters like ñ and ç are not decomposable in Unicode NFD,
     * so we handle them with explicit replacements.
     * 
     * @param input The input string
     * @return The string with accents removed, or null if input is null
     */
    public static String stripAccents(String input) {
        if (input == null) {
            return null;
        }
        // First, handle special characters that don't decompose in NFD
        // Using Unicode escapes to avoid encoding issues: ñ=\u00F1, Ñ=\u00D1, ç=\u00E7, Ç=\u00C7
        String result = input
            .replace('\u00F1', 'n').replace('\u00D1', 'N')  // ñ, Ñ
            .replace('\u00E7', 'c').replace('\u00C7', 'C'); // ç, Ç
        
        // Normalize to NFD (decomposed form) - separates base characters from combining marks
        String normalized = Normalizer.normalize(result, Normalizer.Form.NFD);
        // Remove all combining diacritical marks (Unicode category M)
        return normalized.replaceAll("\\p{M}", "");
    }
    
    /**
     * Normalizes a string for search: lowercase + strip accents + trim.
     * Use this for building lookup keys and preparing search terms.
     * 
     * @param input The input string
     * @return The normalized string, or null if input is null
     */
    public static String normalizeForSearch(String input) {
        if (input == null) {
            return null;
        }
        String result = stripAccents(input.toLowerCase().trim());
        // Remove content in parentheses and brackets
        result = result.replaceAll("\\([^)]*\\)", "");
        result = result.replaceAll("\\[[^\\]]*\\]", "");
        // Remove common featuring tokens
        result = result.replaceAll("\\bfeaturing\\b", "");
        result = result.replaceAll("\\bfeat\\.?\\b", "");
        result = result.replaceAll("\\bft\\.?\\b", "");
        // Remove punctuation characters
        result = result.replaceAll("[\\\\.,'!\"\\-_:;\\/()\\[\\]&%]", "");
        // Collapse whitespace
        result = result.replaceAll("\\s+", " ").trim();
        return result;
    }
    
    /**
     * Generates a SQLite REPLACE chain expression to normalize a column for accent-insensitive comparison.
     * This wraps a column name with nested REPLACE calls to convert accented characters to their base form.
     * 
     * Covers the most common Spanish and Portuguese characters (limited to avoid SQLite parser stack overflow):
     * - Vowels with common accents: á, é, í, ó, ú, ã, õ (both cases)
     * - Special characters: ñ, ç (both cases)
     * 
     * Note: SQLite's LOWER() function does NOT handle non-ASCII characters properly,
     * so we must include both uppercase and lowercase replacements.
     * 
     * @param columnName The SQL column expression to normalize (e.g., "a.name", "s.name")
     * @return A SQL expression that normalizes the column value
     */
    public static String sqlNormalizeColumn(String columnName) {
        // Build nested REPLACE chain for common Spanish/Portuguese accented characters
        // SQLite's LOWER() does NOT convert non-ASCII characters like Ñ→ñ, so we need both cases
        // Limited set to avoid SQLite parser stack overflow (max ~30 nested calls is safe)
        // Using Unicode escapes to avoid encoding issues
        
        String[][] replacements = {
            // Uppercase special characters (MUST come before LOWER() is applied)
            {"\u00D1", "n"}, // Ñ → n
            {"\u00C7", "c"}, // Ç → c
            // Uppercase accented vowels
            {"\u00C1", "a"}, // Á
            {"\u00C9", "e"}, // É
            {"\u00CD", "i"}, // Í
            {"\u00D3", "o"}, // Ó
            {"\u00DA", "u"}, // Ú
            // Lowercase special characters
            {"\u00F1", "n"}, // ñ → n
            {"\u00E7", "c"}, // ç → c
            // Lowercase accented vowels
            {"\u00E1", "a"}, // á
            {"\u00E0", "a"}, // à
            {"\u00E3", "a"}, // ã
            {"\u00E2", "a"}, // â
            {"\u00E9", "e"}, // é
            {"\u00EA", "e"}, // ê
            {"\u00ED", "i"}, // í
            {"\u00F3", "o"}, // ó
            {"\u00F5", "o"}, // õ
            {"\u00F4", "o"}, // ô
            {"\u00FA", "u"}, // ú
            {"\u00FC", "u"} // ü
        };
        
        // Build nested REPLACE chain, then apply LOWER at the end
        // LOWER(REPLACE(REPLACE(column, 'Ñ', 'n'), 'ñ', 'n')...)
        String expr = columnName;
        for (String[] replacement : replacements) {
            expr = "REPLACE(" + expr + ", '" + replacement[0] + "', '" + replacement[1] + "')";
        }
        // Apply LOWER at the end for case-insensitive comparison
        return "LOWER(" + expr + ")";
    }
}
