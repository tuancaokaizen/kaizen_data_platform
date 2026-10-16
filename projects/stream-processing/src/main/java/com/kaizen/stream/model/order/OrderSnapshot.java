package com.kaizen.stream.model.order;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Full order state carried by every order-bundles event (kafka_topic_model.md 7.3).
 * Trạng thái đầy đủ của đơn trong mọi sự kiện order-bundles (kafka_topic_model.md mục 7.3).
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class OrderSnapshot implements Serializable {
    private OrderHeader order;
    @Builder.Default
    private List<OrderLine> items = new ArrayList<>();
    @Builder.Default
    private List<ReturnDocument> returns = new ArrayList<>();

    /** Σ returnAmount over all returns. / Tổng tiền đã trả lại. */
    @JsonIgnore
    public long returnedAmount() {
        long sum = 0;
        if (returns != null) {
            for (ReturnDocument r : returns) {
                sum += r.getReturnAmount() == null ? 0 : r.getReturnAmount();
            }
        }
        return sum;
    }
}
