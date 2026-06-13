package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Ánh xạ bảng shop."Employee" */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Employee implements Serializable {
    private String id;        // PK
    private String name;
    private String degree;    // e.g. "Dược sĩ", "Dược tá"
    private String phone;
    private String address;   // nullable
    private boolean status;
    private String shopCode;  // FK → Shop
    private long createdDate;
    private long modifiedDate;
    private boolean isManager;
}
