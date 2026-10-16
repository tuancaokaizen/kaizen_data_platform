package com.kaizen.stream.processing.rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Collects invariant violations as readable messages for the DLQ header.
 * Gom các vi phạm bất biến thành thông điệp dễ đọc cho header DLQ.
 */
public final class Violations {

    private final List<String> messages = new ArrayList<>();

    public Violations require(String field, Object value) {
        if (value == null || (value instanceof String && ((String) value).isEmpty())) {
            messages.add(field + " is missing");
        }
        return this;
    }

    public Violations requireNonEmpty(String field, Collection<?> values) {
        if (values == null || values.isEmpty()) {
            messages.add(field + " is empty");
        }
        return this;
    }

    public Violations check(boolean ok, String message) {
        if (!ok) {
            messages.add(message);
        }
        return this;
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    /** Use with {@link #grewSince(int)} to tell whether a block added violations. / Đánh dấu để biết một khối có thêm vi phạm không. */
    public int mark() {
        return messages.size();
    }

    public boolean grewSince(int mark) {
        return messages.size() > mark;
    }

    public List<String> list() {
        return messages;
    }
}
