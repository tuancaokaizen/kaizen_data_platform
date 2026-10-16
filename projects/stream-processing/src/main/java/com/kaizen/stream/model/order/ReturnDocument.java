package com.kaizen.stream.model.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * "order"."Return" document with its lines. / Phiếu trả "order"."Return" kèm các dòng.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ReturnDocument implements Serializable {
    private String id;
    private Long createdDate;
    private String reason;
    /** Σ line lineAmount; refunded by exactly one REFUND payment. / Hoàn bằng đúng 1 phiếu chi. */
    private Long returnAmount;
    @Builder.Default
    private List<ReturnLine> items = new ArrayList<>();
}
