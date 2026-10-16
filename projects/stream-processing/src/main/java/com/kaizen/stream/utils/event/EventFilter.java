package com.kaizen.stream.utils.event;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Header-level filter on eventType, applied before JSON parsing.
 * Lọc theo header eventType, chạy trước khi parse JSON.
 */
public final class EventFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Empty = accept everything. / Rỗng = nhận tất cả. */
    private final Set<String> allowed;

    private EventFilter(Set<String> allowed) {
        this.allowed = allowed;
    }

    public static EventFilter all() {
        return new EventFilter(Collections.emptySet());
    }

    public static EventFilter only(Enum<?>... eventTypes) {
        if (eventTypes.length == 0) {
            throw new IllegalArgumentException("EventFilter.only needs at least one event type");
        }
        return new EventFilter(Arrays.stream(eventTypes).map(Enum::name)
                .collect(Collectors.toCollection(TreeSet::new)));
    }

    /**
     * A record without eventType header is accepted so the parser can send it to the DLQ.
     * Record thiếu header eventType vẫn được nhận để parser đẩy sang DLQ.
     */
    public boolean accepts(String eventType) {
        return allowed.isEmpty() || eventType == null || allowed.contains(eventType);
    }

    @Override
    public String toString() {
        return allowed.isEmpty() ? "EventFilter[all]" : "EventFilter" + allowed;
    }
}
