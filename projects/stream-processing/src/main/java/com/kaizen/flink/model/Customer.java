package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Ánh xạ bảng customer."Customer" */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer implements Serializable {
    private String id;           // PK
    private String fullName;
    private String phone;
    private String wardId;       // FK → Geography, nullable
    private String address;      // nullable
    private Integer yearOfBirth; // nullable
    private String gender;       // "M" | "F" | null
    private long createdDate;
    private long modifiedDate;
}
