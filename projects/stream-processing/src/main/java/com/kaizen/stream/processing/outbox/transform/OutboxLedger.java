package com.kaizen.stream.processing.outbox.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Which outbox rows are emitted but not yet marked published. A row may be marked only after a checkpoint
 * taken after its emission completes, because only then has the Kafka sink flushed it. Not thread-safe.
 * Theo dõi dòng outbox đã phát nhưng chưa đánh dấu. Chỉ được đánh dấu khi một checkpoint chụp sau lúc phát
 * đã hoàn tất, vì khi đó sink Kafka mới flush xong. Không an toàn đa luồng.
 */
public final class OutboxLedger {

    /** Emitted, not yet marked; never polled again. / Đã phát, chưa đánh dấu; không quét lại. */
    private final Set<Long> inFlight = new TreeSet<>();
    private List<Long> sinceSnapshot = new ArrayList<>();
    private final NavigableMap<Long, List<Long>> byCheckpoint = new TreeMap<>();
    /** Delivered for sure, waiting for the PG update. / Chắc chắn đã gửi, chờ cập nhật PG. */
    private final Set<Long> confirmed = new LinkedHashSet<>();

    /**
     * State comes from a completed checkpoint, so everything in it reached Kafka.
     * State lấy từ checkpoint đã hoàn tất nên mọi dòng trong đó đã tới Kafka.
     */
    public void restore(Iterable<Long> seqs) {
        for (Long s : seqs) {
            inFlight.add(s);
            confirmed.add(s);
        }
    }

    public void emitted(long seq) {
        if (inFlight.add(seq)) {
            sinceSnapshot.add(seq);
        }
    }

    /** Rows emitted so far belong to this checkpoint; returns the state to store. / Trả về state cần lưu. */
    public List<Long> snapshot(long checkpointId) {
        if (!sinceSnapshot.isEmpty()) {
            byCheckpoint.put(checkpointId, sinceSnapshot);
            sinceSnapshot = new ArrayList<>();
        }
        return new ArrayList<>(inFlight);
    }

    /** Also confirms earlier checkpoints that were aborted. / Xác nhận luôn các checkpoint trước bị huỷ. */
    public void checkpointCompleted(long checkpointId) {
        Map<Long, List<Long>> done = byCheckpoint.headMap(checkpointId, true);
        for (List<Long> seqs : done.values()) {
            confirmed.addAll(seqs);
        }
        done.clear();
    }

    public List<Long> toMark() {
        return new ArrayList<>(confirmed);
    }

    public void marked(Collection<Long> seqs) {
        confirmed.removeAll(seqs);
        inFlight.removeAll(seqs);
    }

    public List<Long> inFlight() {
        return new ArrayList<>(inFlight);
    }
}
