package com.kaizen.stream.simulation.order.lookup;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * Where the order generator gets its master data; tests pass a fixed snapshot.
 * Nguồn master data cho bộ sinh đơn; test truyền một ảnh chụp cố định.
 */
@FunctionalInterface
public interface MasterDataSource extends Serializable {

    MasterData load() throws SQLException;
}
