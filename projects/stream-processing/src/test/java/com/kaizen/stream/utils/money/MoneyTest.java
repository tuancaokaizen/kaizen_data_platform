package com.kaizen.stream.utils.money;

import com.kaizen.stream.utils.time.IctTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MoneyTest {

    @Test
    void allocatesProportionallyAndExactly() {
        assertArrayEquals(new long[]{24_000, 11_000, 0}, Money.allocate(35_000, new long[]{240_000, 110_000, 0}));
        assertArrayEquals(new long[]{34, 33, 33}, Money.allocate(100, new long[]{1, 1, 1}));
        assertArrayEquals(new long[]{0, 0}, Money.allocate(0, new long[]{0, 0}));
    }

    @Test
    void partsAlwaysSumToTotal() {
        Random rnd = new Random(42);
        for (int i = 0; i < 1_000; i++) {
            long total = rnd.nextInt(5_000_000);
            long[] weights = rnd.longs(1 + rnd.nextInt(8), 0, 2_000_000).toArray();
            weights[0] += 1;
            long[] parts = Money.allocate(total, weights);
            assertEquals(total, Arrays.stream(parts).sum());
            for (int j = 0; j < parts.length; j++) {
                assertTrue(parts[j] >= 0);
                if (weights[j] == 0) {
                    assertEquals(0, parts[j]);
                }
            }
        }
    }

    @Test
    void rejectsImpossibleSplits() {
        assertThrows(IllegalArgumentException.class, () -> Money.allocate(10, new long[]{0, 0}));
        assertThrows(IllegalArgumentException.class, () -> Money.allocate(-1, new long[]{1}));
        assertThrows(IllegalArgumentException.class, () -> Money.allocate(1, new long[]{-1, 2}));
    }

    @Test
    void businessDateUsesVietnamTime() {
        // 2026-10-10T17:30Z = 2026-10-11 00:30 ICT
        long ms = java.time.Instant.parse("2026-10-10T17:30:00Z").toEpochMilli();
        assertEquals(LocalDate.of(2026, 10, 11), IctTime.businessDate(ms));
        assertEquals(java.time.Instant.parse("2026-10-10T17:00:00Z").toEpochMilli(),
                IctTime.startOfDay(LocalDate.of(2026, 10, 11)));
    }
}
