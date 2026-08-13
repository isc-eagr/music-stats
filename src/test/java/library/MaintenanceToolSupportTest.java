package library;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MaintenanceToolSupportTest {

    @Test
    void formatsByteUnitsAtExistingBoundaries() {
        assertEquals("1023 B", MaintenanceToolSupport.formatBytes(1023));
        assertEquals(String.format("%.1f KB", 1.0), MaintenanceToolSupport.formatBytes(1024));
        assertEquals(String.format("%.1f MB", 1.0), MaintenanceToolSupport.formatBytes(1024L * 1024));
    }
}
