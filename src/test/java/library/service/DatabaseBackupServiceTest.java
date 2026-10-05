package library.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatabaseBackupServiceTest {

    @TempDir
    Path tempDirectory;

    @Test
    void backupIncludesCommittedWalDataAndPreservesEarlierBackups() throws Exception {
        Path directory = Files.createDirectory(tempDirectory.resolve("Music Stats' DB"));
        Path database = directory.resolve("music-stats.db");
        var dataSource = dataSource(database);
        try {
            var jdbc = new JdbcTemplate(dataSource);
            jdbc.execute("PRAGMA journal_mode=WAL");
            jdbc.execute("PRAGMA wal_autocheckpoint=0");
            jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY, title TEXT, image BLOB)");
            jdbc.execute("CREATE INDEX song_title ON Song(title)");
            jdbc.update("INSERT INTO Song VALUES (1, 'First song', X'010203')");
            assertThat(Files.size(Path.of(database + "-wal"))).isPositive();

            var service = new DatabaseBackupService(dataSource);
            assertThat(service.getDatabasePath()).isEqualTo(database.toAbsolutePath());
            Path firstBackup = service.backupDatabase();
            jdbc.update("INSERT INTO Song VALUES (2, 'Second song', NULL)");
            Path secondBackup = service.backupDatabase();

            assertThat(firstBackup.getParent()).isEqualTo(directory);
            assertThat(secondBackup.getParent()).isEqualTo(directory);
            assertThat(firstBackup).isNotEqualTo(secondBackup).isNotEqualTo(database);
            assertThat(firstBackup.getFileName().toString())
                    .matches("music-stats-backup-\\d{2}-\\d{2}-\\d{4}_\\d{2}-\\d{2}-\\d{2}-\\d+\\.db");
            var firstDataSource = dataSource(firstBackup);
            var secondDataSource = dataSource(secondBackup);
            try {
                var first = new JdbcTemplate(firstDataSource);
                var second = new JdbcTemplate(secondDataSource);
                assertThat(first.queryForObject("SELECT COUNT(*) FROM Song", Integer.class)).isEqualTo(1);
                assertThat(first.queryForObject("SELECT hex(image) FROM Song WHERE id=1", String.class)).isEqualTo("010203");
                assertThat(second.queryForList("SELECT title FROM Song ORDER BY id", String.class))
                        .containsExactly("First song", "Second song");
                assertThat(second.queryForObject("PRAGMA integrity_check", String.class)).isEqualTo("ok");
                assertThat(second.queryForObject("SELECT COUNT(*) FROM sqlite_master WHERE name='song_title'", Integer.class)).isEqualTo(1);
            } finally {
                firstDataSource.destroy();
                secondDataSource.destroy();
            }
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM Song", Integer.class)).isEqualTo(2);
            try (var files = Files.list(directory)) {
                assertThat(files).noneMatch(path -> path.toString().endsWith(".partial"));
            }
        } finally {
            dataSource.destroy();
        }
    }

    @Test
    void failedSnapshotRemovesPartialFileAndAllowsRetry() throws Exception {
        Path database = tempDirectory.resolve("music-stats.db");
        var dataSource = dataSource(database);
        try {
            var jdbc = new JdbcTemplate(dataSource);
            jdbc.execute("CREATE TABLE Song (id INTEGER PRIMARY KEY)");
            var connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            jdbc.update("INSERT INTO Song VALUES (1)");
            var service = new DatabaseBackupService(dataSource);

            assertThatThrownBy(service::backupDatabase).isInstanceOf(SQLException.class);
            try (var files = Files.list(tempDirectory)) {
                assertThat(files).noneMatch(path -> path.getFileName().toString().contains("-backup-"));
            }
            connection.rollback();
            connection.setAutoCommit(true);
            assertThat(service.backupDatabase()).exists();
        } finally {
            dataSource.destroy();
        }
    }

    @Test
    void inMemoryDatabaseCannotChooseABackupDirectory() throws Exception {
        var dataSource = new SingleConnectionDataSource("jdbc:sqlite::memory:", true);
        try {
            var service = new DatabaseBackupService(dataSource);
            assertThatThrownBy(service::backupDatabase).isInstanceOf(SQLException.class)
                    .hasMessage("Database backups require a database stored on disk.");
            try (var files = Files.list(tempDirectory)) {
                assertThat(files).isEmpty();
            }
        } finally {
            dataSource.destroy();
        }
    }

    private SingleConnectionDataSource dataSource(Path database) {
        return new SingleConnectionDataSource("jdbc:sqlite:" + database, true);
    }
}
