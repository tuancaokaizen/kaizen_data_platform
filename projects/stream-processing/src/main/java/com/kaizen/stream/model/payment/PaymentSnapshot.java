package com.kaizen.stream.model.payment;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Full payment state carried by every payment-bundles event (kafka_topic_model.md 7.4).
 * Trạng thái đầy đủ của phiếu trong mọi sự kiện payment-bundles (kafka_topic_model.md mục 7.4).
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class PaymentSnapshot implements Serializable {
    private PaymentHeader payment;
    @Builder.Default
    private List<PaymentTransaction> transactions = new ArrayList<>();
}
