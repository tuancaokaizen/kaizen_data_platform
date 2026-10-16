package com.kaizen.stream.processing.outbox.source.store;

import com.kaizen.stream.model.outbox.OutboxRow;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

/**
 * Access to inventory."Outbox"; a seam so the relay source can be tested without PG.
 * Truy cập inventory."Outbox"; tách ra để test source relay không cần PG.
 */
public interface OutboxStore extends Serializable {

    /**
     * Unpublished rows due now, oldest Seq first, skipping {@code exclude}.
     * Dòng chưa phát đã tới giờ, Seq nhỏ trước, bỏ qua {@code exclude}.
     */
    List<OutboxRow> poll(Collection<Long> exclude, int limit) throws SQLException;

    void markPublished(Collection<Long> seqs) throws SQLException;

    /** Deletes rows published longer ago than retentionMs; returns the count. / Xoá dòng đã phát quá hạn. */
    int purge(long retentionMs) throws SQLException;

    void close();
}
