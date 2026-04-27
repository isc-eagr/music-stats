package library;

import library.util.BillboardHot100ImportSupport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BillboardHot100ImportRunner {

    private static final String DB_PATH = "C:/Music Stats DB/music-stats.db";
    private static final String DB_URL = "jdbc:sqlite:" + DB_PATH;

    public static void main(String[] args) {
        System.out.println("=== Billboard Hot 100 Import Runner ===");
        System.out.println("Database: " + DB_PATH);
        System.out.println();

        try (Connection connection = DriverManager.getConnection(DB_URL)) {
            BillboardHot100ImportSupport.ImportReport importReport = BillboardHot100ImportSupport.importAllCharts(connection);
            int linked = BillboardHot100ImportSupport.autoLinkExactMatches(connection);
            BillboardHot100ImportSupport.NameIssueReport issueReport =
                BillboardHot100ImportSupport.analyzePotentialNameIssues(connection, 20);

            System.out.println("Imported charts:       " + importReport.chartCount());
            System.out.println("Imported chart rows:   " + importReport.entryCount());
            System.out.println("Preserved song links:  " + importReport.preservedLinks());
            System.out.println("Exact matches linked:  " + linked);
            System.out.println("Source variant groups: " + issueReport.sourceVariantGroupCount());
            System.out.println("Normalized candidates: " + issueReport.normalizedOnlyCandidateCount());
            System.out.println();

            if (!issueReport.samples().isEmpty()) {
                System.out.println("Potential normalized-only matches:");
                System.out.println("----------------------------------");
                for (BillboardHot100ImportSupport.NameIssueSample sample : issueReport.samples()) {
                    System.out.printf(
                        "Billboard: \"%s\" by %s [weeks=%d, peak=%d]%n",
                        sample.billboardSongTitle(),
                        sample.billboardArtistName(),
                        sample.weeksOnChart(),
                        sample.peakPosition()
                    );
                    System.out.printf(
                        "Library:   \"%s\" by %s (song_id=%d)%n%n",
                        sample.librarySongTitle(),
                        sample.libraryArtistName(),
                        sample.songId()
                    );
                }
            }
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Import failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}