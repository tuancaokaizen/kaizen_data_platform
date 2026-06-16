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
 * Sinh PaymentBundle: 
 * - Thu (Receipt/OrderPayment): Thanh toán tiền mua hàng. Status có thể là SUCCESS, PENDING, FAILED.
 * - Chi (Refund/ReturnPayment): Hoàn tiền cho đơn hàng trả lại. Chỉ tạo khi có Return thực tế. Status luôn là SUCCESS.
 */
public final class PaymentSimulator {

    private static final String[] PURCHASE_STATUSES = {"SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "PENDING", "FAILED"};

    private PaymentSimulator() {}

    public static PaymentBundle generate(com.kaizen.flink.config.FlinkConfig config) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();

        // 1. Ưu tiên xử lý các đơn trả hàng chưa có phiếu chi hoàn tiền thành công (Chi)
        List<IdPool.ReturnRef> unpaidReturns = IdPool.fetchUnpaidReturns(config);
        if (!unpaidReturns.isEmpty()) {
            IdPool.ReturnRef returnRef = unpaidReturns.get(0);
            
            // ── HOÀN TIỀN TRẢ HÀNG (CHI) ──
            String payId   = "SPAY-REF-" + uid12();
            short payType  = 2; // Phiếu chi
            // Hoàn tiền thì status mặc định luôn là SUCCESS (Giao dịch hoàn tất)
            String status  = "SUCCESS";
            int total      = returnRef.returnAmount;
            Payment payment = new Payment(payId, payType, status, total, now, now);

            // ── Transactions (giao dịch hoàn tiền) ──
            List<PaymentTransaction> txs = new ArrayList<>();
            short method = IdPool.pickShort(IdPool.PAYMENT_METHOD_IDS);
            txs.add(new PaymentTransaction("STX-" + uid12(), payId, method, total, now, now));

            // ── ReturnPayment (Liên kết chi trả hoàn tiền -> Đơn trả) ──
            ReturnPayment rp = new ReturnPayment("SRP-" + uid12(), payId, returnRef.returnId, returnRef.orderId, now, now);

            // Hoàn tiền thì KHÔNG có OrderPayment (bán hàng)
            return new PaymentBundle(payment, txs, null, rp);
        }

        // 2. Tiếp theo xử lý các đơn hàng chưa có thanh toán thành công (Thu)
        List<IdPool.UnpaidOrderRef> unpaidOrders = IdPool.fetchUnpaidOrders(config);
        if (!unpaidOrders.isEmpty()) {
            IdPool.UnpaidOrderRef orderRef = unpaidOrders.get(0);
            String orderId = orderRef.orderId;
            int total = orderRef.totalPayment;

            // ── THANH TOÁN MUA HÀNG (THU) ──
            String payId   = "SPAY-" + uid12();
            short payType  = 1; // Phiếu thu
            
            // Giả lập 90% thành công, 10% thất bại/chờ xử lý để mô phỏng thực tế.
            // Nếu FAILED/PENDING, đơn hàng vẫn nằm trong tập "chưa thanh toán" và sẽ được thanh toán lại ở các vòng sau.
            String status;
            double randVal = rnd.nextDouble();
            if (randVal < 0.90) {
                status = "SUCCESS";
            } else if (randVal < 0.95) {
                status = "FAILED";
            } else {
                status = "PENDING";
            }

            Payment payment = new Payment(payId, payType, status, total, now, now);

            // ── Transactions ──
            List<PaymentTransaction> txs = new ArrayList<>();
            boolean splitPayment = rnd.nextInt(5) == 0; // 20% thanh toán 2 phương thức kết hợp
            if (splitPayment) {
                int part1 = (int) (total * rnd.nextDouble(0.3, 0.7));
                int part2 = total - part1;
                short m1  = IdPool.pickShort(IdPool.PAYMENT_METHOD_IDS);
                short m2  = (m1 == 1) ? (short) 2 : (short) 1;
                txs.add(new PaymentTransaction("STX-" + uid12(), payId, m1, part1, now, now));
                txs.add(new PaymentTransaction("STX-" + uid12(), payId, m2, part2, now, now));
            } else {
                short method = IdPool.pickShort(IdPool.PAYMENT_METHOD_IDS);
                txs.add(new PaymentTransaction("STX-" + uid12(), payId, method, total, now, now));
            }

            // ── OrderPayment (Liên kết thanh toán mua hàng -> Đơn hàng) ──
            OrderPayment op = new OrderPayment("SOP-" + uid12(), payId, orderId, now, now);

            // Thanh toán mua hàng mới thì chưa có ReturnPayment
            return new PaymentBundle(payment, txs, op, null);
        }

        // 3. Nếu tất cả đơn hàng & trả hàng đã có thanh toán thành công, tạm thời không sinh thêm
        return null;
    }

    private static String uid12() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
    }
}
