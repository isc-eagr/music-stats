package library.service;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class TopPlayedTimelineTop3Test {

    @Test
    void incrementalTop3MatchesRankingEveryItemAfterEveryPlay() {
        Random random = new Random(20260929L);
        for (int sequence = 0; sequence < 300; sequence++) {
            int itemCount = 2 + random.nextInt(12);
            Map<Integer, Integer> playCounts = new HashMap<>();
            List<Integer> top3 = List.of();
            for (int play = 0; play < 400; play++) {
                // Skewed picks produce plenty of ties and lead changes.
                int itemId = 1 + (int) (itemCount * Math.pow(random.nextDouble(), 2));
                playCounts.merge(itemId, 1, Integer::sum);

                List<Integer> expected = rankEveryItem(playCounts, top3);
                List<Integer> actual = TopPlayedTimelineService.computeTop3(playCounts, top3, itemId);

                assertThat(actual).as("sequence %d, play %d", sequence, play).isEqualTo(expected);
                top3 = actual;
            }
        }
    }

    /** The previous implementation: sort every item on each call. */
    private static List<Integer> rankEveryItem(Map<Integer, Integer> playCounts, List<Integer> previousTop3) {
        Map<Integer, Integer> previousPosition = new HashMap<>();
        for (int i = 0; i < previousTop3.size(); i++) {
            previousPosition.put(previousTop3.get(i), i);
        }
        return new ArrayList<>(playCounts.entrySet().stream()
                .sorted(Map.Entry.<Integer, Integer>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(entry -> previousPosition.getOrDefault(entry.getKey(), Integer.MAX_VALUE))
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(3)
                .map(Map.Entry::getKey)
                .toList());
    }
}
