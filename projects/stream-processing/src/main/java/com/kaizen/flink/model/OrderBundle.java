package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Bundle gom Order + OrderItems + Return (optional) + ReturnItems (optional)
 * vào một event duy nhất để đảm bảo thứ tự insert và RI khi fan-out stream.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderBundle implements Serializable {
    private Order order;
    private List<OrderItem> orderItems;
    private Return returnRecord;       // null nếu không có hoàn trả
    private List<ReturnItem> returnItems; // null nếu không có hoàn trả
}
