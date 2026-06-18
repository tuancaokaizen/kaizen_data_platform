package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng public."Geography"
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Geography implements Serializable {
    private String id;           // PK
    private String name;
    private Integer population;  // nullable
    private long createdDate;    // epoch-ms
    private long modifiedDate;   // epoch-ms
}
