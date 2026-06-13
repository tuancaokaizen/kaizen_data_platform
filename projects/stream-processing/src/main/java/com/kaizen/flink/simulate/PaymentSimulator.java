package com.kaizen.flink.simulate;

import com.kaizen.flink.model.OrderPayment;
import com.kaizen.flink.model.Payment;
import com.kaizen.flink.model.PaymentBundle;
import com.kaizen.flink.model.PaymentTransaction;
import com.kaizen.flink.model.ReturnPayment;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Sinh PaymentBundle: Payment → 1-2 Transactions → OrderPayment → (20%) ReturnPayment.
 * Simulate các phương thức thanh toán kết hợp (tiền mặt + ví điện tử).
 */
public final class PaymentSimulator {

    private static final String[] STATUSES = {"SUCCESS", "SUCCESS", "SUCCESS", "PENDING", "FAILED"};

    private PaymentSimulator() {}

    public static PaymentBundle generate() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();

        // ── Payment ───────────────────────────────────────────────────────────
        String payId   = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        short payType  = IdPool.pickShort(IdPool.PAYMENT_TYPE_IDS);
        String status  = STATUSES[rnd.nextInt(STATUSES.length)];
        int total      = rnd.nextInt(50_000, 5_000_000);
        Payment payment = new Payment(payId, payType, status, total, now, now);

        // ── Transactions (thanh toán có thể chia nhiều phương thức) ──────────
        List<PaymentTransaction> txs = new ArrayList<>();
        boolean splitPayment = rnd.nextInt(5) == 0; // 20% thanh toán 2 phương thức
        if (splitPayment) {
            int part1 = (int) (total * rnd.nextDouble(0.3, 0.7));
            int part2 = total - part1;
            short m1  = IdPool.pickShort(IdPool.PAYMENT_METHOD_IDS);
            short m2  = (m1 == 1) ? (short) 2 : (short) 1; // khác phương thức
            txs.add(new PaymentTransaction("TX-" + uid(), payId, m1, part1, now, now));
            txs.add(new PaymentTransaction("TX-" + uid(), payId, m2, part2, now, now));
        } else {
            short method = IdPool.pickShort(IdPool.PAYMENT_METHOD_IDS);
            txs.add(new PaymentTransaction("TX-" + uid(), payId, method, total, now, now));
        }

        // ── OrderPayment (liên kết payment → order) ───────────────────────────
        // Sử dụng order ID từ Postgres nếu đã có, fallback sang sinh ngẫu nhiên nếu trống
        String fakeOrderId;
        if (!IdPool.ORDER_IDS.isEmpty()) {
            fakeOrderId = IdPool.pick(IdPool.ORDER_IDS);
        } else {
            fakeOrderId = "ORD-" + String.format("%08d", rnd.nextInt(100_000_000));
        }
        OrderPayment op = new OrderPayment("OP-" + uid(), payId, fakeOrderId, now, now);

        // ── ReturnPayment (20% xác suất – hoàn tiền cho đơn hàng trả lại) ────
        ReturnPayment rp = null;
        if (rnd.nextInt(5) == 0) {
            String retId = "RET-" + uid();
            rp = new ReturnPayment("RP-" + uid(), payId, retId, fakeOrderId, now, now);
        }

        return new PaymentBundle(payment, txs, op, rp);
    }

    private static String uid() {
        return UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}
