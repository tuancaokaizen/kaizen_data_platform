package com.kaizen.stream.model.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * "order"."ReturnItem" row. / Dòng "order"."ReturnItem".
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ReturnLine implements Serializable {
    private String id;
    private String orderItemId;
    private String productId;
    private Integer quantity;
    /** Units back on the shelf. / Số đơn vị nhập lại kệ. */
    private Integer restockQty;
    /** Units written off. / Số đơn vị huỷ. */
    private Integer damageQty;
    private Long unitPrice;
    private Long unitCost;
    /** Refunded amount for this line. / Số tiền hoàn cho dòng. */
    private Long lineAmount;
}
