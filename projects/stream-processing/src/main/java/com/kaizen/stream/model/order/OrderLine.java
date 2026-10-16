package com.kaizen.stream.model.order;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * "order"."OrderItem" row. / Dòng "order"."OrderItem".
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class OrderLine implements Serializable {
    private String id;
    private Integer lineNo;
    private String productId;
    private Integer quantity;
    private Long unitPrice;
    /** Share of order discount allocated to this line. / Phần chiết khấu đơn phân bổ cho dòng. */
    private Long discountAmount;
    /** quantity × unitPrice − discountAmount. */
    private Long lineAmount;
    /** Gift lines have unitPrice = 0. / Dòng quà tặng có unitPrice = 0. */
    @JsonProperty("isGift")
    private Boolean gift;
    /** Cost price at sale time. / Giá vốn tại thời điểm bán. */
    private Long unitCost;
}
