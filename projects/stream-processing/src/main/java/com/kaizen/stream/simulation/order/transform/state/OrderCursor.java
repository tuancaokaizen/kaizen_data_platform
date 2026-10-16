package com.kaizen.stream.simulation.order.transform.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Checkpointed position of the order stream: restoring it replays exactly the same orders.
 * Vị trí của luồng đơn được checkpoint: restore lại thì phát lại đúng các đơn cũ.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCursor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Sequence number of the next order. / Số thứ tự của đơn kế tiếp. */
    private long seq;
    /** When the next order is placed (epoch ms). / Thời điểm đặt đơn kế tiếp. */
    private long nextAt;
    /** Business day of the shop counters (yyyy-MM-dd). / Ngày nghiệp vụ của bộ đếm shop. */
    private String day;
    /** Orders already numbered per shop on that day. / Số đơn đã đánh số theo shop trong ngày. */
    private HashMap<String, Integer> shopCounters = new HashMap<>();
    /**
     * Voucher codes handed out that master data may not show as closed yet; null in state written before this
     * field existed.
     * Mã voucher đã phát ra mà master data có thể chưa thấy là đã đóng; null với state ghi trước khi có trường này.
     */
    private HashSet<String> usedSeries = new HashSet<>();

    public OrderCursor copy() {
        return new OrderCursor(seq, nextAt, day, new HashMap<>(shopCounters),
                usedSeries == null ? new HashSet<>() : new HashSet<>(usedSeries));
    }
}
