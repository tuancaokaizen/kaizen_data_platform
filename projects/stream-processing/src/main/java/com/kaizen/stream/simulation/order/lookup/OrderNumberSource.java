package com.kaizen.stream.simulation.order.lookup;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Map;

/**
 * Highest order number already used per shop on a business day, so a fresh start continues the numbering
 * instead of reusing ids; tests pass a fixed map.
 * Số đơn lớn nhất đã dùng theo shop trong một ngày nghiệp vụ, để khởi động mới đánh số tiếp thay vì dùng lại id;
 * test truyền một map cố định.
 */
@FunctionalInterface
public interface OrderNumberSource extends Serializable {

    Map<String, Integer> highestPerShop(LocalDate businessDate) throws SQLException;
}
