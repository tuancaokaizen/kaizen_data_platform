package com.kaizen.stream.utils.connector.sink.clickhouse;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClickHouseSinkFactoryTest {

    @Test
    void insertListsColumnsAndPlaceholders() {
        assertEquals("INSERT INTO report.t (A, B, C) VALUES (?, ?, ?)",
                ClickHouseSinkFactory.insertSql("report.t", Arrays.asList("A", "B", "C")));
    }

    @Test
    void rejectsEmptyColumnList() {
        assertThrows(IllegalArgumentException.class,
                () -> ClickHouseSinkFactory.insertSql("report.t", Collections.emptyList()));
    }
}
