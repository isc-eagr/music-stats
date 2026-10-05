package library.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DatabaseBackupService {

    private static final ZoneId BACKUP_TIME_ZONE = ZoneId.of("America/Mexico_City");

    private final DataSource dataSource;
    private final Clock clock;
    private final ReentrantLock backupLock = new ReentrantLock();

    @Autowired
    public DatabaseBackupService(DataSource dataSource) {
        this(dataSource, Clock.system(BACKUP_TIME_ZONE));
    }

    DatabaseBackupService(DataSource dataSource, Clock clock) {
        this.dataSource = dataSource;
        this.clock = clock.withZone(BACKUP_TIME_ZONE);
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
            String backupBasename = basename + "_" + LocalDate.now(clock);
            partialBackup = Files.createTempFile(database.getParent(), backupBasename + "-", ".partial");

            // Use SQLite's snapshot operation so committed WAL data is included in the backup.
            try (PreparedStatement statement = connection.prepareStatement("VACUUM main INTO ?")) {
                statement.setString(1, partialBackup.toString());
                statement.execute();
            }

            return moveBackup(partialBackup, backupBasename);
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

    private Path moveBackup(Path partialBackup, String basename) throws IOException {
        for (int number = 1; ; number++) {
            String suffix = number == 1 ? "" : "-" + number;
            Path backup = partialBackup.resolveSibling(basename + suffix + ".db");
            try {
                // Moving without replacement preserves existing backups, including concurrent creations.
                return Files.move(partialBackup, backup);
            } catch (FileAlreadyExistsException exception) {
                // Try the next numbered filename.
            }
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
