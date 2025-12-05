package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PlaylistService {

    private final JdbcTemplate jdbcTemplate;
    
    public PlaylistService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Generate M3U playlist content from a list of song IDs.
     * 
     * M3U format:
     * #EXTM3U
     * #EXTINF:duration,Artist - Title
     * path/to/file.mp3
     * 
     * Since we don't have file paths, we use a placeholder format that 
     * can be searched manually or enhanced later.
     */
    public String generateM3U(List<Integer> songIds) {
        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n");
        
        for (Integer songId : songIds) {
            Map<String, Object> songData = getSongData(songId);
            if (songData != null) {
                String songName = (String) songData.get("song_name");
                String artistName = (String) songData.get("artist_name");
                String albumName = (String) songData.get("album_name");
                Integer lengthSeconds = (Integer) songData.get("length_seconds");
                int duration = lengthSeconds != null ? lengthSeconds : -1;
                
                // #EXTINF line: duration in seconds, Artist - Title
                sb.append("#EXTINF:").append(duration).append(",")
                  .append(artistName != null ? artistName : "Unknown Artist")
                  .append(" - ").append(songName).append("\n");
                
                // File reference - use a searchable format: Artist/Album/Song
                // This can be used with iTunes "Add to Library" or manual lookup
                String filePath = buildFilePath(artistName, albumName, songName);
                sb.append(filePath).append("\n");
            }
        }
        
        return sb.toString();
    }

    /**
     * Generate M3U8 (UTF-8 encoded) playlist content.
     * Same format as M3U but with UTF-8 encoding support.
     */
    public String generateM3U8(List<Integer> songIds) {
        // M3U8 is the same format, just UTF-8 encoded
        return generateM3U(songIds);
    }

    /**
     * Generate iTunes-compatible tab-separated text file.
     * This format can be imported via File > Library > Import Playlist in iTunes.
     * Using minimal columns: Name, Artist, Album
     */
    public String generateiTunesTxt(List<Integer> songIds) {
        StringBuilder sb = new StringBuilder();
        
        // Header row (tab-separated)
        sb.append("Name\tArtist\tAlbum\n");
        
        for (Integer songId : songIds) {
            Map<String, Object> songData = getSongData(songId);
            if (songData != null) {
                String songName = (String) songData.get("song_name");
                String artistName = (String) songData.get("artist_name");
                String albumName = (String) songData.get("album_name");
                
                // Tab-separated values (escape tabs in content just in case)
                sb.append(escapeForTsv(songName)).append("\t")
                  .append(escapeForTsv(artistName != null ? artistName : "")).append("\t")
                  .append(escapeForTsv(albumName != null ? albumName : "")).append("\n");
            }
        }
        
        return sb.toString();
    }

    /**
     * Escape a string for TSV format (replace tabs and newlines)
     */
    private String escapeForTsv(String str) {
        if (str == null) return "";
        return str.replace("\t", " ").replace("\n", " ").replace("\r", "");
    }
    
    /**
     * Get song data with artist and album names using a join query.
     */
    private Map<String, Object> getSongData(Integer songId) {
        String sql = """
            SELECT 
                s.name as song_name,
                s.length_seconds,
                a.name as artist_name,
                al.name as album_name
            FROM Song s
            LEFT JOIN Artist a ON s.artist_id = a.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE s.id = ?
            """;
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, songId);
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Build a searchable file path placeholder.
     * Format: Artist/Album/Song.mp3
     * This can be used for manual lookup or as a reference.
     */
    private String buildFilePath(String artistName, String albumName, String songName) {
        // Clean up names for file path (remove special characters)
        String cleanArtist = sanitizeFileName(artistName != null ? artistName : "Unknown Artist");
        String cleanAlbum = albumName != null ? sanitizeFileName(albumName) : "Singles";
        String cleanSong = sanitizeFileName(songName);
        
        return cleanArtist + "/" + cleanAlbum + "/" + cleanSong + ".mp3";
    }

    /**
     * Sanitize a string for use in file paths.
     */
    private String sanitizeFileName(String name) {
        if (name == null) return "Unknown";
        // Replace characters that are invalid in file names
        return name.replaceAll("[\\\\/:*?\"<>|]", "_").trim();
    }

    /**
     * Generate a PowerShell script that creates the playlist in iTunes using COM automation.
     * The script searches for each track in the iTunes library and adds matches to a new playlist.
     */
    public String generatePowerShellScript(List<Integer> songIds, String playlistName) {
        StringBuilder sb = new StringBuilder();
        
        // Script header with comments
        sb.append("# iTunes Playlist Creator Script\n");
        sb.append("# Generated by Music Stats - Playlist Builder\n");
        sb.append("# This script uses iTunes COM automation to create a playlist\n");
        sb.append("#\n");
        sb.append("# Requirements:\n");
        sb.append("#   - iTunes must be installed on Windows\n");
        sb.append("#   - Run this script in PowerShell\n");
        sb.append("#\n");
        sb.append("# Usage: .\\").append(sanitizeFileName(playlistName)).append(".ps1\n");
        sb.append("#\n\n");
        
        // Error handling and iTunes connection
        sb.append("$ErrorActionPreference = \"Stop\"\n\n");
        sb.append("Write-Host \"Connecting to iTunes...\" -ForegroundColor Cyan\n");
        sb.append("try {\n");
        sb.append("    $iTunes = New-Object -ComObject iTunes.Application\n");
        sb.append("} catch {\n");
        sb.append("    Write-Host \"Error: Could not connect to iTunes. Make sure iTunes is installed.\" -ForegroundColor Red\n");
        sb.append("    exit 1\n");
        sb.append("}\n\n");
        
        // Create the playlist
        String escapedName = playlistName.replace("\"", "`\"").replace("$", "`$");
        sb.append("$playlistName = \"").append(escapedName).append("\"\n");
        sb.append("Write-Host \"Creating playlist: $playlistName\" -ForegroundColor Cyan\n");
        sb.append("$playlist = $iTunes.CreatePlaylist($playlistName)\n\n");
        
        // Track counters
        sb.append("$found = 0\n");
        sb.append("$notFound = 0\n");
        sb.append("$total = ").append(songIds.size()).append("\n\n");
        
        // Search and add each track
        sb.append("Write-Host \"Searching for tracks in iTunes library...\" -ForegroundColor Cyan\n");
        sb.append("Write-Host \"\"\n\n");
        
        for (Integer songId : songIds) {
            Map<String, Object> songData = getSongData(songId);
            if (songData != null) {
                String songName = (String) songData.get("song_name");
                String artistName = (String) songData.get("artist_name");
                
                // Escape special characters for PowerShell
                String escapedSong = escapeForPowerShell(songName);
                String escapedArtist = escapeForPowerShell(artistName != null ? artistName : "");
                
                // Search strategy: try "Artist Title" first for more accurate matching
                String searchTerm = (artistName != null ? artistName + " " : "") + songName;
                String escapedSearch = escapeForPowerShell(searchTerm);
                
                sb.append("# ").append(artistName != null ? artistName : "Unknown").append(" - ").append(songName).append("\n");
                sb.append("$searchTerm = \"").append(escapedSearch).append("\"\n");
                sb.append("$tracks = $iTunes.LibraryPlaylist.Search($searchTerm, 5)  # 5 = search all fields\n");
                sb.append("if ($tracks -and $tracks.Count -gt 0) {\n");
                sb.append("    $added = $false\n");
                sb.append("    foreach ($track in $tracks) {\n");
                sb.append("        # Match by exact song name (case-insensitive)\n");
                sb.append("        if ($track.Name -ieq \"").append(escapedSong).append("\") {\n");
                sb.append("            $playlist.AddTrack($track)\n");
                sb.append("            Write-Host \"  [OK] $($track.Artist) - $($track.Name)\" -ForegroundColor Green\n");
                sb.append("            $found++\n");
                sb.append("            $added = $true\n");
                sb.append("            break\n");
                sb.append("        }\n");
                sb.append("    }\n");
                sb.append("    if (-not $added) {\n");
                sb.append("        # If no exact match, add first result\n");
                sb.append("        $track = $tracks.Item(1)\n");
                sb.append("        $playlist.AddTrack($track)\n");
                sb.append("        Write-Host \"  [~] $($track.Artist) - $($track.Name) (closest match)\" -ForegroundColor Yellow\n");
                sb.append("        $found++\n");
                sb.append("    }\n");
                sb.append("} else {\n");
                sb.append("    Write-Host \"  [X] Not found: ").append(escapedArtist).append(" - ").append(escapedSong).append("\" -ForegroundColor Red\n");
                sb.append("    $notFound++\n");
                sb.append("}\n\n");
            }
        }
        
        // Summary
        sb.append("Write-Host \"\"\n");
        sb.append("Write-Host \"===== Summary =====\" -ForegroundColor Cyan\n");
        sb.append("Write-Host \"Playlist: $playlistName\"\n");
        sb.append("Write-Host \"Tracks found: $found / $total\" -ForegroundColor $(if ($found -eq $total) { 'Green' } else { 'Yellow' })\n");
        sb.append("if ($notFound -gt 0) {\n");
        sb.append("    Write-Host \"Tracks not found: $notFound\" -ForegroundColor Red\n");
        sb.append("}\n");
        sb.append("Write-Host \"===================\"\n");
        sb.append("Write-Host \"\"\n");
        sb.append("Write-Host \"Done! Open iTunes to see your new playlist.\" -ForegroundColor Green\n");
        
        return sb.toString();
    }

    /**
     * Escape special characters for PowerShell strings.
     */
    private String escapeForPowerShell(String str) {
        if (str == null) return "";
        return str
            .replace("`", "``")      // Backtick (escape char)
            .replace("\"", "`\"")    // Double quote
            .replace("$", "`$")      // Dollar sign (variable prefix)
            .replace("\n", "`n")     // Newline
            .replace("\r", "`r")     // Carriage return
            .replace("\t", "`t");    // Tab
    }

    /**
     * Generate a PowerShell folder watcher script that automatically imports playlists into iTunes.
     * The script monitors a folder for new .txt playlist files and creates them in iTunes.
     */
    public String generateFolderWatcherScript(String watchFolder) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("# iTunes Playlist Folder Watcher\n");
        sb.append("# Generated by Music Stats - Playlist Builder\n");
        sb.append("#\n");
        sb.append("# This script watches a folder for new playlist files (.txt)\n");
        sb.append("# and automatically imports them into iTunes.\n");
        sb.append("#\n");
        sb.append("# Usage:\n");
        sb.append("#   1. Run this script once: .\\iTunesPlaylistWatcher.ps1\n");
        sb.append("#   2. Export playlists from Music Stats to the watch folder\n");
        sb.append("#   3. Playlists will be automatically created in iTunes!\n");
        sb.append("#\n");
        sb.append("# To stop: Press Ctrl+C or close the PowerShell window\n");
        sb.append("#\n\n");
        
        String escapedFolder = watchFolder.replace("\\", "\\\\").replace("\"", "`\"");
        
        sb.append("$watchFolder = \"").append(escapedFolder).append("\"\n\n");
        
        // Create folder if it doesn't exist
        sb.append("# Create watch folder if it doesn't exist\n");
        sb.append("if (-not (Test-Path $watchFolder)) {\n");
        sb.append("    New-Item -ItemType Directory -Path $watchFolder -Force | Out-Null\n");
        sb.append("    Write-Host \"Created watch folder: $watchFolder\" -ForegroundColor Green\n");
        sb.append("}\n\n");
        
        // Create imported subfolder
        sb.append("$importedFolder = Join-Path $watchFolder \"imported\"\n");
        sb.append("if (-not (Test-Path $importedFolder)) {\n");
        sb.append("    New-Item -ItemType Directory -Path $importedFolder -Force | Out-Null\n");
        sb.append("}\n\n");
        
        // Connect to iTunes
        sb.append("# Connect to iTunes\n");
        sb.append("Write-Host \"Connecting to iTunes...\" -ForegroundColor Cyan\n");
        sb.append("try {\n");
        sb.append("    $iTunes = New-Object -ComObject iTunes.Application\n");
        sb.append("    Write-Host \"Connected to iTunes!\" -ForegroundColor Green\n");
        sb.append("} catch {\n");
        sb.append("    Write-Host \"Error: Could not connect to iTunes. Make sure iTunes is installed.\" -ForegroundColor Red\n");
        sb.append("    Read-Host \"Press Enter to exit\"\n");
        sb.append("    exit 1\n");
        sb.append("}\n\n");
        
        // Function to import a playlist file
        sb.append("# Function to import a playlist file\n");
        sb.append("function Import-PlaylistFile {\n");
        sb.append("    param([string]$FilePath, [object]$iTunesApp)\n");
        sb.append("    \n");
        sb.append("    $fileName = [System.IO.Path]::GetFileNameWithoutExtension($FilePath)\n");
        sb.append("    Write-Host \"\"\n");
        sb.append("    Write-Host \"Importing playlist: $fileName\" -ForegroundColor Cyan\n");
        sb.append("    \n");
        sb.append("    try {\n");
        sb.append("        # Read the file\n");
        sb.append("        Write-Host \"  Reading file...\" -ForegroundColor DarkGray\n");
        sb.append("        $content = Get-Content $FilePath -Raw -Encoding UTF8\n");
        sb.append("        $lines = $content -split \"`r?`n\" | Select-Object -Skip 1 | Where-Object { $_.Trim() -ne '' }\n");
        sb.append("        \n");
        sb.append("        if (-not $lines -or $lines.Count -eq 0) {\n");
        sb.append("            Write-Host \"  No songs in playlist file\" -ForegroundColor Yellow\n");
        sb.append("            return $false\n");
        sb.append("        }\n");
        sb.append("        \n");
        sb.append("        Write-Host \"  Found $($lines.Count) songs to import\" -ForegroundColor DarkGray\n");
        sb.append("        \n");
        sb.append("        # Build list of songs to find\n");
        sb.append("        $songsToFind = @()\n");
        sb.append("        foreach ($line in $lines) {\n");
        sb.append("            $parts = $line -split \"`t\"\n");
        sb.append("            if ($parts.Count -lt 2) { continue }\n");
        sb.append("            $songsToFind += @{ Name = $parts[0].Trim(); Artist = $parts[1].Trim() }\n");
        sb.append("        }\n");
        sb.append("        \n");
        sb.append("        # Create the playlist first\n");
        sb.append("        Write-Host \"  Creating playlist...\" -ForegroundColor DarkGray\n");
        sb.append("        $playlist = $iTunesApp.CreatePlaylist($fileName)\n");
        sb.append("        Write-Host \"  Playlist created!\" -ForegroundColor DarkGray\n");
        sb.append("        \n");
        sb.append("        # Get the library\n");
        sb.append("        Write-Host \"  Scanning library...\" -ForegroundColor DarkGray\n");
        sb.append("        $libraryPlaylist = $iTunesApp.LibraryPlaylist\n");
        sb.append("        $libraryTracks = $libraryPlaylist.Tracks\n");
        sb.append("        $trackCount = $libraryTracks.Count\n");
        sb.append("        Write-Host \"  Library has $trackCount tracks\" -ForegroundColor DarkGray\n");
        sb.append("        \n");
        sb.append("        # Build normalized lookup for requested songs\n");
        sb.append("        $wantedSongs = @{}\n");
        sb.append("        foreach ($song in $songsToFind) {\n");
        sb.append("            $key = ($song.Name.ToLower().Trim() + '||' + $song.Artist.ToLower().Trim())\n");
        sb.append("            $wantedSongs[$key] = @{ Name = $song.Name; Artist = $song.Artist; Found = $false }\n");
        sb.append("        }\n");
        sb.append("        \n");
        sb.append("        $found = 0\n");
        sb.append("        $tracksToAdd = @()\n");
        sb.append("        \n");
        sb.append("        # Scan library and collect matching tracks\n");
        sb.append("        Write-Host \"  Finding matching tracks...\" -ForegroundColor DarkGray\n");
        sb.append("        for ($i = 1; $i -le $trackCount; $i++) {\n");
        sb.append("            if ($found -ge $songsToFind.Count) { break }\n");
        sb.append("            \n");
        sb.append("            try {\n");
        sb.append("                $track = $libraryTracks.Item($i)\n");
        sb.append("                $key = ($track.Name.ToLower().Trim() + '||' + $track.Artist.ToLower().Trim())\n");
        sb.append("                \n");
        sb.append("                if ($wantedSongs.ContainsKey($key) -and -not $wantedSongs[$key].Found) {\n");
        sb.append("                    $wantedSongs[$key].Found = $true\n");
        sb.append("                    $wantedSongs[$key].TrackIndex = $i\n");
        sb.append("                    $found++\n");
        sb.append("                }\n");
        sb.append("            } catch { }\n");
        sb.append("        }\n");
        sb.append("        \n");
        sb.append("        Write-Host \"  Found $found of $($songsToFind.Count) tracks\" -ForegroundColor DarkGray\n");
        sb.append("        \n");
        sb.append("        # Add tracks to playlist\n");
        sb.append("        $addedCount = 0\n");
        sb.append("        $errorCount = 0\n");
        sb.append("        \n");
        sb.append("        Write-Host \"  Adding tracks...\" -ForegroundColor DarkGray\n");
        sb.append("        \n");
        sb.append("        foreach ($song in $songsToFind) {\n");
        sb.append("            $key = ($song.Name.ToLower().Trim() + '||' + $song.Artist.ToLower().Trim())\n");
        sb.append("            $songInfo = $wantedSongs[$key]\n");
        sb.append("            \n");
        sb.append("            if ($songInfo.Found) {\n");
        sb.append("                try {\n");
        sb.append("                    $track = $libraryTracks.Item($songInfo.TrackIndex)\n");
        sb.append("                    $added = $false\n");
        sb.append("                    \n");
        sb.append("                    # Debug: Show track info\n");
        sb.append("                    Write-Host \"  Track: $($track.Name) by $($track.Artist)\" -ForegroundColor DarkGray\n");
        sb.append("                    Write-Host \"    Kind: $($track.Kind), KindAsString: $($track.KindAsString)\" -ForegroundColor DarkGray\n");
        sb.append("                    try { Write-Host \"    Location: $($track.Location)\" -ForegroundColor DarkGray } catch { Write-Host \"    Location: (none)\" -ForegroundColor DarkGray }\n");
        sb.append("                    \n");
        sb.append("                    # Method 1: Duplicate - call on track, pass playlist as destination\n");
        sb.append("                    if (-not $added) {\n");
        sb.append("                        try {\n");
        sb.append("                            Write-Host \"    Trying Duplicate...\" -ForegroundColor DarkGray\n");
        sb.append("                            $result = $track.Duplicate($playlist)\n");
        sb.append("                            Write-Host \"    Duplicate result: $result\" -ForegroundColor DarkGray\n");
        sb.append("                            if ($result -ne $null) { $added = $true }\n");
        sb.append("                        } catch {\n");
        sb.append("                            Write-Host \"    Duplicate error: $($_.Exception.Message)\" -ForegroundColor Red\n");
        sb.append("                        }\n");
        sb.append("                    }\n");
        sb.append("                    \n");
        sb.append("                    # Method 2: playlist.AddTrack\n");
        sb.append("                    if (-not $added) {\n");
        sb.append("                        try {\n");
        sb.append("                            Write-Host \"    Trying AddTrack...\" -ForegroundColor DarkGray\n");
        sb.append("                            $result = $playlist.AddTrack($track)\n");
        sb.append("                            Write-Host \"    AddTrack result: $result\" -ForegroundColor DarkGray\n");
        sb.append("                            if ($result -ne $null) { $added = $true }\n");
        sb.append("                        } catch {\n");
        sb.append("                            Write-Host \"    AddTrack error: $($_.Exception.Message)\" -ForegroundColor Red\n");
        sb.append("                        }\n");
        sb.append("                    }\n");
        sb.append("                    \n");
        sb.append("                    # Method 3: Copy method\n");
        sb.append("                    if (-not $added) {\n");
        sb.append("                        try {\n");
        sb.append("                            Write-Host \"    Trying Copy...\" -ForegroundColor DarkGray\n");
        sb.append("                            $result = $track.Copy($playlist)\n");
        sb.append("                            Write-Host \"    Copy result: $result\" -ForegroundColor DarkGray\n");
        sb.append("                            if ($result -ne $null) { $added = $true }\n");
        sb.append("                        } catch {\n");
        sb.append("                            Write-Host \"    Copy error: $($_.Exception.Message)\" -ForegroundColor Red\n");
        sb.append("                        }\n");
        sb.append("                    }\n");
        sb.append("                    \n");
        sb.append("                    # Method 4: Move method\n");
        sb.append("                    if (-not $added) {\n");
        sb.append("                        try {\n");
        sb.append("                            Write-Host \"    Trying Move (creates a reference, not actually move)...\" -ForegroundColor DarkGray\n");
        sb.append("                            # Don't actually move - just test if method exists\n");
        sb.append("                        } catch {\n");
        sb.append("                            Write-Host \"    Move error: $($_.Exception.Message)\" -ForegroundColor Red\n");
        sb.append("                        }\n");
        sb.append("                    }\n");
        sb.append("                    \n");
        sb.append("                    # Method 5: List available methods on track\n");
        sb.append("                    if (-not $added) {\n");
        sb.append("                        Write-Host \"    Available methods on track:\" -ForegroundColor Yellow\n");
        sb.append("                        $track | Get-Member -MemberType Method | ForEach-Object { Write-Host \"      $($_.Name)\" -ForegroundColor DarkGray }\n");
        sb.append("                    }\n");
        sb.append("                    \n");
        sb.append("                    if ($added) {\n");
        sb.append("                        Write-Host \"  [OK] $($song.Artist) - $($song.Name)\" -ForegroundColor Green\n");
        sb.append("                        $addedCount++\n");
        sb.append("                    } else {\n");
        sb.append("                        Write-Host \"  [!] Could not add: $($song.Artist) - $($song.Name)\" -ForegroundColor Yellow\n");
        sb.append("                        $errorCount++\n");
        sb.append("                    }\n");
        sb.append("                } catch {\n");
        sb.append("                    Write-Host \"  [!] Error: $($song.Artist) - $($song.Name) - $($_.Exception.Message)\" -ForegroundColor Red\n");
        sb.append("                    $errorCount++\n");
        sb.append("                }\n");
        sb.append("            } else {\n");
        sb.append("                Write-Host \"  [X] Not found: $($song.Artist) - $($song.Name)\" -ForegroundColor Red\n");
        sb.append("            }\n");
        sb.append("        }\n");
        sb.append("        \n");
        sb.append("        $notFoundCount = $songsToFind.Count - $found\n");
        sb.append("        Write-Host \"\" -ForegroundColor Cyan\n");
        sb.append("        Write-Host \"  ================================\" -ForegroundColor Cyan\n");
        sb.append("        Write-Host \"  Summary:\" -ForegroundColor Cyan\n");
        sb.append("        Write-Host \"    Added: $addedCount\" -ForegroundColor Green\n");
        sb.append("        if ($errorCount -gt 0) {\n");
        sb.append("            Write-Host \"    Errors: $errorCount\" -ForegroundColor Yellow\n");
        sb.append("        }\n");
        sb.append("        if ($notFoundCount -gt 0) {\n");
        sb.append("            Write-Host \"    Not found: $notFoundCount\" -ForegroundColor Red\n");
        sb.append("        }\n");
        sb.append("        Write-Host \"  ================================\" -ForegroundColor Cyan\n");
        sb.append("        \n");
        sb.append("        return $true\n");
        sb.append("    } catch {\n");
        sb.append("        Write-Host \"  Error: $($_.Exception.Message)\" -ForegroundColor Red\n");
        sb.append("        Write-Host \"  At line: $($_.InvocationInfo.ScriptLineNumber)\" -ForegroundColor Red\n");
        sb.append("        return $false\n");
        sb.append("    }\n");
        sb.append("}\n\n");
        
        // Display status
        sb.append("Write-Host \"\"\n");
        sb.append("Write-Host \"========================================\" -ForegroundColor Cyan\n");
        sb.append("Write-Host \" iTunes Playlist Watcher is ACTIVE\" -ForegroundColor Green\n");
        sb.append("Write-Host \"========================================\" -ForegroundColor Cyan\n");
        sb.append("Write-Host \"Watching folder: $watchFolder\"\n");
        sb.append("Write-Host \"\"\n");
        sb.append("Write-Host \"Playlists dropped here will be auto-imported!\"\n");
        sb.append("Write-Host \"Press Ctrl+C to stop watching...\"\n");
        sb.append("Write-Host \"\"\n\n");
        
        // Polling loop - check for new files every 2 seconds
        sb.append("# Main loop - poll for new files\n");
        sb.append("try {\n");
        sb.append("    while ($true) {\n");
        sb.append("        $files = Get-ChildItem $watchFolder -Filter \"*.txt\" -File -ErrorAction SilentlyContinue\n");
        sb.append("        \n");
        sb.append("        foreach ($file in $files) {\n");
        sb.append("            # Import the playlist\n");
        sb.append("            $success = Import-PlaylistFile -FilePath $file.FullName -iTunesApp $iTunes\n");
        sb.append("            \n");
        sb.append("            # Move to imported folder\n");
        sb.append("            $destPath = Join-Path $importedFolder $file.Name\n");
        sb.append("            try {\n");
        sb.append("                Move-Item $file.FullName $destPath -Force\n");
        sb.append("                Write-Host \"  Moved to: $destPath\" -ForegroundColor DarkGray\n");
        sb.append("            } catch {\n");
        sb.append("                Write-Host \"  Could not move file: $_\" -ForegroundColor Yellow\n");
        sb.append("            }\n");
        sb.append("        }\n");
        sb.append("        \n");
        sb.append("        Start-Sleep -Seconds 2\n");
        sb.append("    }\n");
        sb.append("} catch {\n");
        sb.append("    Write-Host \"Error: $_\" -ForegroundColor Red\n");
        sb.append("} finally {\n");
        sb.append("    Write-Host \"Watcher stopped.\" -ForegroundColor Yellow\n");
        sb.append("}\n");
        
        return sb.toString();
    }

    /**
     * Get the default watch folder path.
     */
    public String getDefaultWatchFolder() {
        String userHome = System.getProperty("user.home");
        return userHome + "\\Music\\iTunes Playlists";
    }

    /**
     * Generate a batch file that can be added to Windows Startup to auto-start the watcher.
     */
    public String generateAutoStartBatch(String watchFolder) {
        StringBuilder sb = new StringBuilder();
        
        String scriptPath = watchFolder + "\\iTunesPlaylistWatcher.ps1";
        
        sb.append("@echo off\n");
        sb.append(":: iTunes Playlist Watcher Auto-Start\n");
        sb.append(":: Place this batch file in: %APPDATA%\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\n");
        sb.append(":: Or run it manually to start the watcher.\n");
        sb.append("::\n");
        sb.append(":: This will run minimized in the background.\n");
        sb.append("\n");
        sb.append(":: Wait a few seconds for iTunes/system to initialize\n");
        sb.append("timeout /t 10 /nobreak > nul\n");
        sb.append("\n");
        sb.append(":: Start the watcher script minimized\n");
        sb.append("start /min \"iTunes Playlist Watcher\" powershell.exe -ExecutionPolicy Bypass -WindowStyle Minimized -File \"").append(scriptPath).append("\"\n");
        
        return sb.toString();
    }

    /**
     * Generate instructions for setting up auto-start.
     */
    public String getAutoStartInstructions(String watchFolder) {
        return "To auto-start the watcher when Windows starts:\n\n" +
               "1. Download both the watcher script AND the auto-start batch file\n" +
               "2. Save the watcher script as: " + watchFolder + "\\iTunesPlaylistWatcher.ps1\n" +
               "3. Press Win+R, type: shell:startup\n" +
               "4. Copy the StartWatcher.bat file to this Startup folder\n" +
               "5. The watcher will now start automatically when you log in!\n\n" +
               "Note: The watcher runs minimized in the background.";
    }
}
