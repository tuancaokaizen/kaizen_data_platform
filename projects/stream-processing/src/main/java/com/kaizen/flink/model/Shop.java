package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng shop."Shop"
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Shop implements Serializable {
    private String code;       // PK
    private String name;
    private String address;
    private String wardId;     // FK → Geography, nullable
    private String managerId;  // FK → Employee, nullable (tránh circular-insert)
    private long createdDate;
    private long modifiedDate;
}
