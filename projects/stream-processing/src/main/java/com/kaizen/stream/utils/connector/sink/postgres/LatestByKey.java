package com.kaizen.stream.utils.connector.sink.postgres;

import com.kaizen.stream.model.envelope.Envelope;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Write buffer keeping only the newest snapshot per aggregate; snapshots are full state, so this loses nothing.
 * Bộ đệm ghi chỉ giữ snapshot mới nhất của mỗi aggregate; snapshot là trạng thái đầy đủ nên không mất gì.
 */
final class LatestByKey<T> {

    private final Map<String, Envelope<T>> byKey = new LinkedHashMap<>();

    void add(String key, Envelope<T> envelope) {
        Envelope<T> current = byKey.get(key);
        // Equal versions replace, so a corrected re-send of the same version wins
        // Cùng version thì thay, để bản gửi lại đã sửa của cùng version được ghi
        if (current == null || envelope.getMeta().getVersion() >= current.getMeta().getVersion()) {
            byKey.put(key, envelope);
        }
    }

    int size() {
        return byKey.size();
    }

    boolean isEmpty() {
        return byKey.isEmpty();
    }

    List<Envelope<T>> drain() {
        List<Envelope<T>> out = new ArrayList<>(byKey.values());
        byKey.clear();
        return out;
    }
}
