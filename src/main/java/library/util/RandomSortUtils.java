package library.util;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public final class RandomSortUtils {
    private static final long MODULUS = 2_147_483_647L;

    private RandomSortUtils() {
    }

    public static String sqliteNumericExpression(String stableExpression, Integer randomSeed) {
        if (randomSeed == null) {
            return "RANDOM()";
        }
        long seed = normalizeSeed(randomSeed);
        return "ABS((((COALESCE(" + stableExpression + ", 0) + " + seed + ") * 1103515245 + "
                + seed + " * 12345) % " + MODULUS + "))";
    }

    public static String sqliteTextExpression(String stableExpression, Integer randomSeed) {
        if (randomSeed == null) {
            return "RANDOM()";
        }
        long seed = normalizeSeed(randomSeed);
        return "ABS(((COALESCE(LENGTH(" + stableExpression + "), 0) * 1103515245 + "
                + "COALESCE(unicode(substr(" + stableExpression + ", 1, 1)), 0) * 12345 + "
                + "COALESCE(unicode(substr(" + stableExpression + ", -1)), 0) * 110351 + "
                + seed + " * 1013904223) % " + MODULUS + "))";
    }

    public static <T> void shuffle(List<T> values, Integer randomSeed) {
        if (randomSeed == null) {
            Collections.shuffle(values);
        } else {
            Collections.shuffle(values, new Random(randomSeed));
        }
    }

    private static long normalizeSeed(Integer randomSeed) {
        return Math.floorMod(randomSeed.longValue(), MODULUS);
    }
}
