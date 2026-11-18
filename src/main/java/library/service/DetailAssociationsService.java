package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Data for the Artist Associations tab (artist detail) and the Featured Artists tab (album and song detail).
 *
 * Two artists are associated through a song when both are credited on it: as its main artist or as a
 * featured artist. On the artist tab, members of the same group are also associated through the group's
 * songs (bandmates). Shared plays are the plays of those songs. "Their plays" are the plays of every song
 * the other artist is credited on, so the share that comes through a pair never goes above 100%.
 */
@Service
public class DetailAssociationsService {

    /** Collaborators drawn on the artist map (the table lists them all). */
    static final int MAP_SIZE = 20;
    /** Map canvas (the template's SVG viewBox and centre use the same numbers). */
    static final int MAP_W = 600;
    static final int MAP_H = 680;
    static final int MAP_CX = 300;
    static final int MAP_CY = 330;
    /** Map rings: first rank (0-based, inclusive), last rank (exclusive), radius, starting angle. */
    private static final int[][] MAP_TIERS = {{0, 5, 150, -45}, {5, 12, 218, -90}, {12, MAP_SIZE, 278, -62}};
    /** Tallest album track bar, px. */
    static final int TRACK_BAR_PX = 200;
    private static final String LEGACY_ACCOUNT = "robertlover";

    private final JdbcTemplate jdbcTemplate;

    public DetailAssociationsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // ── Shared shapes ──

    /** One artist as the tabs show it: gender is m / f / o (the blue / pink / gray theme). */
    public record ArtistRef(int id, String name, String gender, String genderLabel, String genre) {
        public String initials() {
            return DetailAssociationsService.initials(name);
        }
    }

    /** A share of plays: by gender (m / f / o) or by link type, with how many artists fall in it. */
    public record Split(String key, String label, int artists, long plays, double pct) {}

    record SongStat(int id, String name, long plays, long legacy) {}

    /**
     * Association types, from the page artist's side of each shared song: guest (featured on the artist's
     * song), host (the artist is featured on theirs), co (both featured on someone else's song), group (credited
     * with one of the artist's groups on that group's song), band (both members of a group: the group's songs).
     * Each song counts once per pair, under one type, in that order of precedence.
     */
    static final String GUEST = "guest";
    static final String HOST = "host";
    static final String CO = "co";
    static final String VIA_GROUP = "group";
    static final String BAND = "band";
    static final List<String> LINK_ORDER = List.of(GUEST, HOST, CO, VIA_GROUP, BAND);

    // ── Artist tab ──

    /** The shared songs of one association type with a partner, and their plays. */
    public record LinkCount(String key, String label, int songs, long plays) {}

    /** One collaborator of the artist; links holds every association type the pair has, in LINK_ORDER. */
    public record Partner(ArtistRef artist, List<LinkCount> links, int songs, long plays, long primary, long legacy,
                          Integer topSongId, String topSongName, long topSongPlays, String songList) {
        /** The type with the most shared songs (sorting the Link column). */
        public String mainLink() {
            return links.stream().max(Comparator.comparingInt(LinkCount::songs)).map(LinkCount::label).orElse("");
        }

        public String typeTip() {
            return String.join(" · ", links.stream().map(l -> l.label() + " " + l.songs()).toList())
                    + (songs == 1 ? " (1 song)" : " (" + songs + " songs)");
        }
    }

    /** A bubble on the map: centre (x, y), diameter d, the edge weight back to the centre, the label baseline. */
    public record MapNode(Partner partner, int rank, int x, int y, int d, double edge, int labelY, String label) {}

    public record MapRing(String label, int r) {}

    /** A group member: plays and songs are their own songs (main artist); self = the page's artist. */
    public record Member(ArtistRef artist, long plays, int songs, boolean self) {}

    /** One group the page's artist belongs to (or the artist itself when it is a group) and all its members. */
    public record Family(ArtistRef group, long plays, int songs, boolean self, List<Member> members) {}

    public record ArtistTab(ArtistRef self, List<Partner> partners, int songsTogether, int creditedSongs, long sharedPlays,
                            long creditedPlays, List<Split> byGender, List<Split> byLink,
                            List<MapNode> map, List<MapRing> rings, List<Family> families) {
        public double sharedPct() {
            return creditedPlays == 0 ? 0 : sharedPlays * 100.0 / creditedPlays;
        }

        public double songsTogetherPct() {
            return creditedSongs == 0 ? 0 : songsTogether * 100.0 / creditedSongs;
        }

        public long count(String gender) {
            return partners.stream().filter(p -> p.artist().gender().equals(gender)).count();
        }

        public long maxPlays() {
            return partners.isEmpty() ? 0 : partners.getFirst().plays();
        }
    }

    public ArtistTab artistTab(int artistId) {
        Map<Integer, String> myRole = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT id, 'main' AS role FROM Song WHERE artist_id = ? "
                        + "UNION ALL SELECT song_id, 'feat' FROM SongFeaturedArtist WHERE artist_id = ?",
                (RowCallbackHandler) rs -> myRole.merge(rs.getInt(1), rs.getString(2), DetailAssociationsService::mainFirst),
                artistId, artistId);

        // Groups the artist is in: their songs count as the artist's too
        Map<Integer, Set<Integer>> bandmatesByGroup = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT m.group_artist_id, o.member_artist_id FROM ArtistMember m "
                        + "LEFT JOIN ArtistMember o ON o.group_artist_id = m.group_artist_id AND o.member_artist_id != m.member_artist_id "
                        + "WHERE m.member_artist_id = ?",
                (RowCallbackHandler) rs -> {
                    Set<Integer> mates = bandmatesByGroup.computeIfAbsent(rs.getInt(1), k -> new LinkedHashSet<>());
                    int mate = rs.getInt(2);
                    if (!rs.wasNull()) mates.add(mate);
                }, artistId);
        Map<Integer, Set<Integer>> groupSongs = creditedSongs(bandmatesByGroup.keySet());

        Set<Integer> universe = new LinkedHashSet<>(myRole.keySet());
        groupSongs.values().forEach(universe::addAll);
        Map<Integer, Map<Integer, String>> credits = songCredits(universe);
        Map<Integer, SongStat> stats = songStats(universe);
        // Never partners: the artist and its own groups; bandmates are always linked as bandmates
        Set<Integer> own = new HashSet<>(bandmatesByGroup.keySet());
        own.add(artistId);
        Set<Integer> mates = new HashSet<>();
        bandmatesByGroup.values().forEach(mates::addAll);

        // Partner -> song -> association type, one per pair and song: direct credit, then via a group, then bandmate
        Map<Integer, Map<Integer, String>> typeBySong = new LinkedHashMap<>();
        myRole.forEach((songId, mine) -> credits.getOrDefault(songId, Map.of()).forEach((other, theirs) -> {
            if (own.contains(other)) return;
            String type = "main".equals(mine) ? GUEST : ("main".equals(theirs) ? HOST : CO);
            typeBySong.computeIfAbsent(other, k -> new LinkedHashMap<>()).put(songId, type);
        }));
        groupSongs.values().forEach(songIds -> songIds.forEach(songId ->
                credits.getOrDefault(songId, Map.of()).keySet().forEach(other -> {
                    if (!own.contains(other) && !mates.contains(other)) {
                        typeBySong.computeIfAbsent(other, k -> new LinkedHashMap<>()).putIfAbsent(songId, VIA_GROUP);
                    }
                })));
        bandmatesByGroup.forEach((groupId, groupMates) -> groupSongs.getOrDefault(groupId, Set.of()).forEach(songId ->
                groupMates.forEach(mate -> typeBySong.computeIfAbsent(mate, k -> new LinkedHashMap<>()).putIfAbsent(songId, BAND))));

        Set<Integer> together = new HashSet<>();
        typeBySong.values().forEach(songs -> together.addAll(songs.keySet()));

        Map<Integer, ArtistRef> refs = artistRefs(typeBySong.keySet());
        List<Partner> partners = new ArrayList<>();
        typeBySong.forEach((other, types) -> {
            ArtistRef ref = refs.get(other);
            if (ref == null) return;
            List<SongStat> songs = types.keySet().stream().map(stats::get).filter(java.util.Objects::nonNull)
                    .sorted(Comparator.comparingLong(SongStat::plays).reversed().thenComparing(SongStat::name)).toList();
            long plays = songs.stream().mapToLong(SongStat::plays).sum();
            long legacy = songs.stream().mapToLong(SongStat::legacy).sum();
            SongStat top = songs.isEmpty() ? null : songs.getFirst();
            List<LinkCount> links = new ArrayList<>();
            for (String key : LINK_ORDER) {
                List<Integer> ofType = types.entrySet().stream().filter(e -> e.getValue().equals(key)).map(Map.Entry::getKey).toList();
                if (!ofType.isEmpty()) links.add(new LinkCount(key, linkLabel(key), ofType.size(), playsOf(ofType, stats)));
            }
            partners.add(new Partner(ref, links, songs.size(), plays, plays - legacy, legacy,
                    top == null ? null : top.id(), top == null ? null : top.name(), top == null ? 0 : top.plays(),
                    String.join(", ", songs.stream().map(SongStat::name).toList())));
        });
        partners.sort(Comparator.comparingLong(Partner::plays).reversed()
                .thenComparing(Comparator.comparingInt(Partner::songs).reversed())
                .thenComparing(p -> p.artist().name().toLowerCase()));

        long creditedPlays = stats.values().stream().mapToLong(SongStat::plays).sum();
        long sharedPlays = playsOf(together, stats);

        long pairPlays = partners.stream().mapToLong(Partner::plays).sum();
        List<Split> byGender = splits(partners, p -> p.artist().gender(), pairPlays,
                List.of("m", "f", "o"), Map.of("m", "Male", "f", "Female", "o", "Other"));
        // By type: each pair's plays go to the type of each shared song, so the parts add up to the whole
        List<Split> byLink = new ArrayList<>();
        for (String key : LINK_ORDER) {
            List<LinkCount> ofType = partners.stream().flatMap(p -> p.links().stream()).filter(l -> l.key().equals(key)).toList();
            if (ofType.isEmpty()) continue;
            long plays = ofType.stream().mapToLong(LinkCount::plays).sum();
            byLink.add(new Split(key, VIA_GROUP.equals(key) ? linkLabel(key) : linkLabel(key) + "s", ofType.size(), plays,
                    pairPlays == 0 ? 0 : plays * 100.0 / pairPlays));
        }

        List<MapNode> map = new ArrayList<>();
        List<MapRing> rings = new ArrayList<>();
        layoutMap(partners, map, rings);

        ArtistRef self = artistRefs(List.of(artistId)).getOrDefault(artistId,
                new ArtistRef(artistId, "?", "o", "Unknown", null));
        return new ArtistTab(self, partners, together.size(), universe.size(), sharedPlays, creditedPlays,
                byGender, byLink, map, rings, families(artistId));
    }

    /**
     * The groups the artist is a member of, each with every member (the artist first, then by plays), plus
     * the artist itself when it is a group with members.
     */
    List<Family> families(int artistId) {
        Map<Integer, List<Integer>> membersByGroup = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT m.group_artist_id, m.member_artist_id FROM ArtistMember m "
                        + "WHERE m.group_artist_id IN (SELECT group_artist_id FROM ArtistMember WHERE member_artist_id = ?) "
                        + "OR m.group_artist_id = ? ORDER BY m.group_artist_id = ? DESC, m.group_artist_id",
                (RowCallbackHandler) rs -> membersByGroup.computeIfAbsent(rs.getInt(1), k -> new ArrayList<>()).add(rs.getInt(2)),
                artistId, artistId, artistId);
        if (membersByGroup.isEmpty()) return List.of();

        Set<Integer> ids = new LinkedHashSet<>(membersByGroup.keySet());
        membersByGroup.values().forEach(ids::addAll);
        Map<Integer, ArtistRef> refs = artistRefs(ids);
        Map<Integer, long[]> own = new HashMap<>();
        jdbcTemplate.query("SELECT s.artist_id, COUNT(DISTINCT s.id), COUNT(p.id) FROM Song s LEFT JOIN Play p ON p.song_id = s.id "
                        + "WHERE s.artist_id IN (" + placeholders(ids.size()) + ") GROUP BY s.artist_id",
                (RowCallbackHandler) rs -> own.put(rs.getInt(1), new long[]{rs.getLong(2), rs.getLong(3)}), ids.toArray());

        List<Family> families = new ArrayList<>();
        membersByGroup.forEach((groupId, memberIds) -> {
            ArtistRef group = refs.get(groupId);
            if (group == null) return;
            List<Member> members = memberIds.stream().distinct().filter(refs::containsKey)
                    .map(id -> new Member(refs.get(id), own.getOrDefault(id, new long[2])[1],
                            (int) own.getOrDefault(id, new long[2])[0], id == artistId))
                    .sorted(Comparator.comparing((Member m) -> !m.self()).thenComparing(Comparator.comparingLong(Member::plays).reversed())
                            .thenComparing(m -> m.artist().name().toLowerCase()))
                    .toList();
            long[] g = own.getOrDefault(groupId, new long[2]);
            families.add(new Family(group, g[1], (int) g[0], groupId == artistId, members));
        });
        return families;
    }

    private static long playsOf(Collection<Integer> songIds, Map<Integer, SongStat> stats) {
        return songIds.stream().map(stats::get).filter(java.util.Objects::nonNull).mapToLong(SongStat::plays).sum();
    }

    /** Rings of bubbles around the artist: closer rings hold the bigger partners. */
    public static void layoutMap(List<Partner> partners, List<MapNode> nodes, List<MapRing> rings) {
        int shown = Math.min(MAP_SIZE, partners.size());
        long max = partners.isEmpty() ? 0 : Math.max(1, partners.getFirst().plays());
        for (int[] tier : MAP_TIERS) {
            int start = tier[0];
            int end = Math.min(tier[1], shown);
            int count = end - start;
            if (count <= 0) break;
            int r = tier[2];
            rings.add(new MapRing("#" + (start + 1) + (count > 1 ? "-" + end : ""), r));
            for (int i = start; i < end; i++) {
                Partner p = partners.get(i);
                double angle = Math.toRadians(tier[3] + (i - start) * 360.0 / count);
                int x = (int) Math.round(MAP_CX + r * Math.cos(angle));
                int y = (int) Math.round(MAP_CY + r * Math.sin(angle));
                int d = (int) Math.round(30 + 40 * Math.sqrt((double) p.plays() / max));
                double edge = Math.round(Math.min(7, 1.2 + p.songs() * 1.1) * 10) / 10.0;
                String name = p.artist().name();
                String label = name.length() > 18 ? name.substring(0, 17) + "…" : name;
                nodes.add(new MapNode(p, i + 1, x, y, d, edge, y + d / 2 + 15, label));
            }
        }
    }

    // ── Album tab ──

    /** One track of the album strip; gender is the first guest's (blank for a solo track). */
    public record Track(int songId, int no, String name, long plays, List<ArtistRef> guests, String gender, int barPx) {
        public boolean hasGuests() {
            return !guests.isEmpty();
        }

        public String tip() {
            String feat = guests.isEmpty() ? "" : " (with " + String.join(", ", guests.stream().map(ArtistRef::name).toList()) + ")";
            return no + ". " + name + feat + " · " + String.format("%,d", plays) + " plays";
        }
    }

    /** A guest on the album: featured on its songs, or the main artist of a song on another artist's album. */
    public record AlbumGuest(ArtistRef artist, String role, int tracks, String trackNames, String trackNos,
                             Integer firstSongId, long plays, long primary, long legacy, double albumPct,
                             long theirPlays, double viaPct) {}

    public record AlbumTab(List<Track> tracks, List<AlbumGuest> guests, long total, long guestPlays, int guestTracks) {
        public long soloPlays() {
            return total - guestPlays;
        }

        public double guestPct() {
            return total == 0 ? 0 : guestPlays * 100.0 / total;
        }

        public double guestTrackPct() {
            return tracks.isEmpty() ? 0 : guestTracks * 100.0 / tracks.size();
        }
    }

    public AlbumTab albumTab(int albumId) {
        Integer owner = jdbcTemplate.query("SELECT artist_id FROM Album WHERE id = ?",
                rs -> rs.next() ? rs.getInt(1) : null, albumId);
        record Row(int id, String name, int no, long plays, long legacy) {}
        List<Row> rows = new ArrayList<>();
        jdbcTemplate.query("SELECT s.id, s.name, s.track_number, COUNT(p.id) AS plays, "
                        + "SUM(CASE WHEN p.account = '" + LEGACY_ACCOUNT + "' THEN 1 ELSE 0 END) AS legacy "
                        + "FROM Song s LEFT JOIN Play p ON p.song_id = s.id WHERE s.album_id = ? "
                        + "GROUP BY s.id ORDER BY s.track_number IS NULL, s.track_number, s.id",
                (RowCallbackHandler) rs -> {
                    int no = rs.getInt("track_number");
                    rows.add(new Row(rs.getInt("id"), rs.getString("name"), rs.wasNull() ? rows.size() + 1 : no,
                            rs.getLong("plays"), rs.getLong("legacy")));
                }, albumId);

        Map<Integer, Map<Integer, String>> credits = songCredits(rows.stream().map(Row::id).toList());
        Map<Integer, List<Integer>> guestIdsBySong = new HashMap<>();
        Map<Integer, Set<String>> roles = new HashMap<>();
        Set<Integer> guestIds = new LinkedHashSet<>();
        for (Row row : rows) {
            credits.getOrDefault(row.id(), Map.of()).forEach((artist, role) -> {
                if (owner != null && artist.equals(owner)) return;
                guestIdsBySong.computeIfAbsent(row.id(), k -> new ArrayList<>()).add(artist);
                roles.computeIfAbsent(artist, k -> new HashSet<>()).add("main".equals(role) ? "Track artist" : "Featured");
                guestIds.add(artist);
            });
        }
        Map<Integer, ArtistRef> refs = artistRefs(guestIds);
        Map<Integer, Long> theirPlays = creditedPlays(guestIds);

        long total = rows.stream().mapToLong(Row::plays).sum();
        long max = Math.max(1, rows.stream().mapToLong(Row::plays).max().orElse(0));
        List<Track> tracks = new ArrayList<>();
        Map<Integer, List<Row>> rowsByGuest = new LinkedHashMap<>();
        for (Row row : rows) {
            List<ArtistRef> guests = guestIdsBySong.getOrDefault(row.id(), List.of()).stream().map(refs::get)
                    .filter(java.util.Objects::nonNull).sorted(Comparator.comparing(a -> a.name().toLowerCase())).toList();
            guests.forEach(g -> rowsByGuest.computeIfAbsent(g.id(), k -> new ArrayList<>()).add(row));
            int bar = row.plays() == 0 ? 2 : (int) Math.max(3, Math.round(row.plays() * (double) TRACK_BAR_PX / max));
            tracks.add(new Track(row.id(), row.no(), row.name(), row.plays(), guests,
                    guests.isEmpty() ? "" : guests.getFirst().gender(), bar));
        }

        List<AlbumGuest> guests = new ArrayList<>();
        rowsByGuest.forEach((id, guestRows) -> {
            long plays = guestRows.stream().mapToLong(Row::plays).sum();
            long legacy = guestRows.stream().mapToLong(Row::legacy).sum();
            long theirs = theirPlays.getOrDefault(id, 0L);
            guests.add(new AlbumGuest(refs.get(id), String.join(" · ", new java.util.TreeSet<>(roles.get(id))), guestRows.size(),
                    String.join(", ", guestRows.stream().map(Row::name).toList()),
                    String.join(", ", guestRows.stream().map(r -> String.valueOf(r.no())).toList()),
                    guestRows.getFirst().id(), plays, plays - legacy, legacy, total == 0 ? 0 : plays * 100.0 / total,
                    theirs, theirs == 0 ? 0 : plays * 100.0 / theirs));
        });
        guests.sort(Comparator.comparingLong(AlbumGuest::plays).reversed().thenComparing(g -> g.artist().name().toLowerCase()));

        List<Track> withGuests = tracks.stream().filter(Track::hasGuests).toList();
        long guestPlays = withGuests.stream().mapToLong(Track::plays).sum();
        return new AlbumTab(tracks, guests, total, guestPlays, withGuests.size());
    }

    // ── Song tab ──

    /** One credited artist: plays and songs of everything they are credited on, and their overall artist rank. */
    public record Credit(ArtistRef artist, boolean main, long theirPlays, int theirSongs, Integer overallRank) {}

    public record PairSong(int id, String name, long plays) {}

    /** Two artists credited on this song and every song they share across the library (this one included). */
    public record Pair(ArtistRef a, ArtistRef b, int songs, long plays, List<PairSong> others) {}

    public record SongTab(long plays, List<Credit> credits, List<Pair> pairs) {
        public boolean hasFeatures() {
            return credits.size() > 1;
        }

        public long maxPairPlays() {
            return pairs.stream().mapToLong(Pair::plays).max().orElse(0);
        }

        /** Whether any pair shares a song besides this one. */
        public boolean hasOtherSongs() {
            return pairs.stream().anyMatch(p -> !p.others().isEmpty());
        }
    }

    public SongTab songTab(int songId) {
        Map<Integer, String> credits = songCredits(List.of(songId)).getOrDefault(songId, Map.of());
        Map<Integer, ArtistRef> refs = artistRefs(credits.keySet());
        List<Integer> order = credits.keySet().stream().filter(refs::containsKey)
                .sorted(Comparator.comparing((Integer id) -> !"main".equals(credits.get(id)))
                        .thenComparing(id -> refs.get(id).name().toLowerCase()))
                .toList();

        Map<Integer, Set<Integer>> songsByArtist = creditedSongs(order);
        Set<Integer> allSongs = new HashSet<>();
        songsByArtist.values().forEach(allSongs::addAll);
        allSongs.add(songId);
        Map<Integer, SongStat> stats = songStats(allSongs);
        long plays = stats.containsKey(songId) ? stats.get(songId).plays() : 0;
        Map<Integer, Integer> overall = overallArtistRanks(order);

        List<Credit> result = new ArrayList<>();
        for (Integer id : order) {
            Set<Integer> theirs = songsByArtist.getOrDefault(id, Set.of());
            result.add(new Credit(refs.get(id), "main".equals(credits.get(id)), playsOf(theirs, stats), theirs.size(),
                    overall.get(id)));
        }

        List<Pair> pairs = new ArrayList<>();
        for (int i = 0; i < order.size(); i++) {
            for (int j = i + 1; j < order.size(); j++) {
                Set<Integer> shared = new HashSet<>(songsByArtist.getOrDefault(order.get(i), Set.of()));
                shared.retainAll(songsByArtist.getOrDefault(order.get(j), Set.of()));
                shared.add(songId);
                long pairPlays = shared.stream().map(stats::get).filter(java.util.Objects::nonNull).mapToLong(SongStat::plays).sum();
                List<PairSong> others = shared.stream().filter(s -> s != songId).map(stats::get).filter(java.util.Objects::nonNull)
                        .sorted(Comparator.comparingLong(SongStat::plays).reversed().thenComparing(SongStat::name))
                        .map(s -> new PairSong(s.id(), s.name(), s.plays())).toList();
                pairs.add(new Pair(refs.get(order.get(i)), refs.get(order.get(j)), shared.size(), pairPlays, others));
            }
        }
        pairs.sort(Comparator.comparingLong(Pair::plays).reversed());
        return new SongTab(plays, result, pairs);
    }

    // ── Queries ──

    /** Song id -> (artist id -> main / feat) for the given songs. */
    Map<Integer, Map<Integer, String>> songCredits(Collection<Integer> songIds) {
        Map<Integer, Map<Integer, String>> result = new LinkedHashMap<>();
        if (songIds.isEmpty()) return result;
        String in = placeholders(songIds.size());
        List<Object> params = new ArrayList<>(songIds);
        params.addAll(songIds);
        jdbcTemplate.query("SELECT id, artist_id, 'main' AS role FROM Song WHERE id IN (" + in + ") "
                        + "UNION ALL SELECT song_id, artist_id, 'feat' FROM SongFeaturedArtist WHERE song_id IN (" + in + ")",
                (RowCallbackHandler) rs -> result.computeIfAbsent(rs.getInt(1), k -> new LinkedHashMap<>())
                        .merge(rs.getInt(2), rs.getString(3), DetailAssociationsService::mainFirst),
                params.toArray());
        return result;
    }

    /** Artist id -> every song they are credited on (main or featured). */
    Map<Integer, Set<Integer>> creditedSongs(Collection<Integer> artistIds) {
        Map<Integer, Set<Integer>> result = new HashMap<>();
        if (artistIds.isEmpty()) return result;
        String in = placeholders(artistIds.size());
        List<Object> params = new ArrayList<>(artistIds);
        params.addAll(artistIds);
        jdbcTemplate.query("SELECT artist_id, id FROM Song WHERE artist_id IN (" + in + ") "
                        + "UNION SELECT artist_id, song_id FROM SongFeaturedArtist WHERE artist_id IN (" + in + ")",
                (RowCallbackHandler) rs -> result.computeIfAbsent(rs.getInt(1), k -> new HashSet<>()).add(rs.getInt(2)),
                params.toArray());
        return result;
    }

    /** Artist id -> plays of every song they are credited on. */
    Map<Integer, Long> creditedPlays(Collection<Integer> artistIds) {
        Map<Integer, Long> result = new HashMap<>();
        if (artistIds.isEmpty()) return result;
        String in = placeholders(artistIds.size());
        List<Object> params = new ArrayList<>(artistIds);
        params.addAll(artistIds);
        jdbcTemplate.query("SELECT c.artist_id, COUNT(p.id) FROM ("
                        + "SELECT artist_id, id AS song_id FROM Song WHERE artist_id IN (" + in + ") "
                        + "UNION SELECT artist_id, song_id FROM SongFeaturedArtist WHERE artist_id IN (" + in + ")"
                        + ") c JOIN Play p ON p.song_id = c.song_id GROUP BY c.artist_id",
                (RowCallbackHandler) rs -> result.put(rs.getInt(1), rs.getLong(2)), params.toArray());
        return result;
    }

    Map<Integer, SongStat> songStats(Collection<Integer> songIds) {
        Map<Integer, SongStat> result = new HashMap<>();
        if (songIds.isEmpty()) return result;
        jdbcTemplate.query("SELECT s.id, s.name, COUNT(p.id) AS plays, "
                        + "SUM(CASE WHEN p.account = '" + LEGACY_ACCOUNT + "' THEN 1 ELSE 0 END) AS legacy "
                        + "FROM Song s LEFT JOIN Play p ON p.song_id = s.id "
                        + "WHERE s.id IN (" + placeholders(songIds.size()) + ") GROUP BY s.id",
                (RowCallbackHandler) rs -> result.put(rs.getInt("id"), new SongStat(rs.getInt("id"), rs.getString("name"),
                        rs.getLong("plays"), rs.getLong("legacy"))),
                songIds.toArray());
        return result;
    }

    Map<Integer, ArtistRef> artistRefs(Collection<Integer> artistIds) {
        Map<Integer, ArtistRef> result = new HashMap<>();
        if (artistIds.isEmpty()) return result;
        jdbcTemplate.query("SELECT a.id, a.name, g.name AS gender, gr.name AS genre FROM Artist a "
                        + "LEFT JOIN Gender g ON g.id = a.gender_id LEFT JOIN Genre gr ON gr.id = a.genre_id "
                        + "WHERE a.id IN (" + placeholders(artistIds.size()) + ")",
                (RowCallbackHandler) rs -> {
                    String gender = rs.getString("gender");
                    result.put(rs.getInt("id"), new ArtistRef(rs.getInt("id"), rs.getString("name"), genderKey(gender),
                            gender == null ? "Unknown" : gender, rs.getString("genre")));
                }, artistIds.toArray());
        return result;
    }

    /** Overall artist rank by plays of their own songs (the General tab standing), ties broken by id. */
    Map<Integer, Integer> overallArtistRanks(Collection<Integer> artistIds) {
        Map<Integer, Integer> result = new HashMap<>();
        if (artistIds.isEmpty()) return result;
        jdbcTemplate.query("WITH t AS (SELECT s.artist_id AS id, COUNT(*) AS n FROM Play p JOIN Song s ON s.id = p.song_id "
                        + "GROUP BY s.artist_id) SELECT id, rk FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY n DESC, id) AS rk FROM t) "
                        + "WHERE id IN (" + placeholders(artistIds.size()) + ")",
                (RowCallbackHandler) rs -> result.put(rs.getInt(1), rs.getInt(2)), artistIds.toArray());
        return result;
    }

    // ── Helpers ──

    private static List<Split> splits(List<Partner> partners, java.util.function.Function<Partner, String> key,
                                      long total, List<String> keys, Map<String, String> labels) {
        Map<String, long[]> sums = new TreeMap<>();
        partners.forEach(p -> {
            long[] s = sums.computeIfAbsent(key.apply(p), k -> new long[2]);
            s[0]++;
            s[1] += p.plays();
        });
        List<Split> result = new ArrayList<>();
        for (String k : keys) {
            long[] s = sums.get(k);
            if (s == null) continue;
            result.add(new Split(k, labels.get(k), (int) s[0], s[1], total == 0 ? 0 : s[1] * 100.0 / total));
        }
        return result;
    }

    static String linkLabel(String key) {
        return switch (key) {
            case GUEST -> "Guest";
            case HOST -> "Host";
            case VIA_GROUP -> "Via group";
            case BAND -> "Bandmate";
            default -> "Co-guest";
        };
    }

    static String genderKey(String genderName) {
        if (genderName == null) return "o";
        return switch (genderName.trim().toLowerCase()) {
            case "male" -> "m";
            case "female" -> "f";
            default -> "o";
        };
    }

    static String initials(String name) {
        StringBuilder sb = new StringBuilder();
        for (String word : name.replace(".", " ").trim().split("\\s+")) {
            if (!word.isEmpty() && Character.isLetterOrDigit(word.codePointAt(0))) {
                sb.appendCodePoint(Character.toUpperCase(word.codePointAt(0)));
            }
            if (sb.length() >= 2) break;
        }
        return sb.isEmpty() ? "?" : sb.toString();
    }

    private static String mainFirst(String a, String b) {
        return "main".equals(a) || "main".equals(b) ? "main" : a;
    }

    private static String placeholders(int n) {
        return String.join(",", java.util.Collections.nCopies(n, "?"));
    }
}
