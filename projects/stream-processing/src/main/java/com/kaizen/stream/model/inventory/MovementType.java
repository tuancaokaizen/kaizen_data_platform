package com.kaizen.stream.model.inventory;

/**
 * Event types of inventory-movements; the sign rule mirrors ck_stock_movement_sign in inventory.sql.
 * Loại sự kiện của inventory-movements; quy tắc dấu khớp ck_stock_movement_sign trong inventory.sql.
 */
public enum MovementType {
    SALE(-1),
    RETURN_RESTOCK(1),
    RECEIPT(1),
    TRANSFER_OUT(-1),
    TRANSFER_IN(1),
    EXPIRE(-1),
    DAMAGE(-1),
    ADJUST(0);

    /** −1 outflow, +1 inflow, 0 either way but non-zero. / −1 xuất, +1 nhập, 0 hai chiều nhưng khác 0. */
    private final int sign;

    MovementType(int sign) {
        this.sign = sign;
    }

    public boolean allows(int qty) {
        return qty != 0 && (sign == 0 || Integer.signum(qty) == sign);
    }

    /** Null for unknown names instead of throwing. / Trả null khi tên lạ thay vì ném lỗi. */
    public static MovementType parse(String name) {
        for (MovementType t : values()) {
            if (t.name().equals(name)) {
                return t;
            }
        }
        return null;
    }
}
