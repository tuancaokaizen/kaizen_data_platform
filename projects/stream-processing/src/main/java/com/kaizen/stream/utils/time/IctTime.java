package com.kaizen.stream.utils.time;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Business dates are Vietnam dates of UTC epoch-ms instants.
 * Ngày nghiệp vụ là ngày theo giờ Việt Nam của thời điểm epoch-ms (UTC).
 */
public final class IctTime {

    public static final ZoneId ZONE = ZoneId.of("Asia/Ho_Chi_Minh");

    private IctTime() {
    }

    public static LocalDate businessDate(long epochMs) {
        return Instant.ofEpochMilli(epochMs).atZone(ZONE).toLocalDate();
    }

    public static long startOfDay(LocalDate date) {
        return date.atStartOfDay(ZONE).toInstant().toEpochMilli();
    }
}
