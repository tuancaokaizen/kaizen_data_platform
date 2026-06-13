package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng order."Return" – PK composite ("Id", "OrderId").
 * Lưu ý: "ModifiedDate" trong DB là time(6) without time zone.
 * Trường modifiedTimeMs lưu epoch-ms, khi sink sẽ extract phần time-of-day.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Return implements Serializable {
    private String id;            // PK part 1
    private String orderId;       // PK part 2 + FK → Order
    private Integer returnAmount; // nullable
    private Long createdDate;     // epoch-ms, nullable
    private Long modifiedTimeMs;  // epoch-ms → mapped to SQL TIME (time-of-day)
}
