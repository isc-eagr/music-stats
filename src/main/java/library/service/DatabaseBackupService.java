package library.service;

import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DatabaseBackupService {

    private static final DateTimeFormatter FILE_TIMESTAMP = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss");
    private static final ZoneId BACKUP_TIME_ZONE = ZoneId.of("America/Mexico_City");

    private final DataSource dataSource;
    private final ReentrantLock backupLock = new ReentrantLock();

    public DatabaseBackupService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Path getDatabasePath() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            return getDatabasePath(connection);
        }
    }

    public Path backupDatabase() throws SQLException, IOException {
        if (!backupLock.tryLock()) {
            throw new IllegalStateException("A database backup is already in progress. Please wait for it to finish.");
        }

        Path partialBackup = null;
        try (Connection connection = dataSource.getConnection()) {
            Path database = getDatabasePath(connection);
            String filename = database.getFileName().toString();
            int extension = filename.lastIndexOf('.');
            String basename = extension > 0 ? filename.substring(0, extension) : filename;
            String timestamp = ZonedDateTime.now(BACKUP_TIME_ZONE).format(FILE_TIMESTAMP);
            partialBackup = Files.createTempFile(database.getParent(), basename + "-backup-" + timestamp + "-", ".partial");

            // Use SQLite's snapshot operation so committed WAL data is included in the backup.
            try (PreparedStatement statement = connection.prepareStatement("VACUUM main INTO ?")) {
                statement.setString(1, partialBackup.toString());
                statement.execute();
            }

            String backupFilename = partialBackup.getFileName().toString().replaceFirst("\\.partial$", ".db");
            Path backup = partialBackup.resolveSibling(backupFilename);
            Files.move(partialBackup, backup);
            return backup;
        } catch (SQLException | IOException | RuntimeException exception) {
            if (partialBackup != null) {
                try {
                    Files.deleteIfExists(partialBackup);
                } catch (IOException cleanupException) {
                    exception.addSuppressed(cleanupException);
                }
            }
            throw exception;
        } finally {
            backupLock.unlock();
        }
    }

    private Path getDatabasePath(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet databases = statement.executeQuery("PRAGMA database_list")) {
            while (databases.next()) {
                if ("main".equals(databases.getString("name"))) {
                    String filename = databases.getString("file");
                    if (filename != null && !filename.isBlank()) {
                        return Path.of(filename).toAbsolutePath().normalize();
                    }
                }
            }
        }
        throw new SQLException("Database backups require a database stored on disk.");
    }
}
