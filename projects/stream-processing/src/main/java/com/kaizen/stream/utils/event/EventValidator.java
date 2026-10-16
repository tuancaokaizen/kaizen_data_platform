package com.kaizen.stream.utils.event;

import com.kaizen.stream.model.envelope.EventMeta;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Business invariant check for one payload type; violations route the record to the DLQ.
 * Kiểm tra bất biến nghiệp vụ cho một loại payload; có vi phạm thì record sang DLQ.
 */
@FunctionalInterface
public interface EventValidator<T> extends Serializable {

    /** Empty list = valid. / Danh sách rỗng = hợp lệ. */
    List<String> violations(EventMeta meta, T data);

    static <T> EventValidator<T> none() {
        return (meta, data) -> Collections.emptyList();
    }
}
