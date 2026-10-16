package com.kaizen.stream.simulation.payment.transform;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.payment.PaymentTransaction;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.utils.time.IctTime;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Decides every payment of an order from the order data alone: same order → same ids, times, outcomes
 * and method split. Replays and lost state therefore re-create identical events, which the sinks ignore.
 * Quyết định mọi phiếu của một đơn chỉ từ dữ liệu đơn: cùng đơn → cùng id, thời điểm, kết quả và cách chia
 * phương thức. Nhờ vậy phát lại hay mất state đều sinh ra sự kiện giống hệt, sink idempotent sẽ bỏ qua.
 */
public final class PaymentDecider implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int CASH = 1;
    public static final int CARD = 2;
    public static final int WALLET = 3;

    /** Split parts are whole thousands of VND. / Các phần chia là bội số 1.000đ. */
    static final long SPLIT_UNIT = 1_000L;

    private final StreamSettings.Payment profile;

    public PaymentDecider(StreamSettings.Payment profile) {
        this.profile = profile;
    }

    /**
     * Receipt events in emit order; the last one is the only SUCCESS. Empty when nothing is owed.
     * Các sự kiện phiếu thu theo thứ tự phát; sự kiện cuối là SUCCESS duy nhất. Rỗng khi đơn không phải trả tiền.
     */
    public List<PaymentSnapshot> receipts(OrderHeader order) {
        long total = order.getTotalPayment();
        if (total <= 0) {
            return Collections.emptyList();
        }
        SplittableRandom rnd = random(order.getId() + "|receipt");
        List<PaymentSnapshot> out = new ArrayList<>();
        long t = order.getTransactionDate() + between(rnd, profile.getFirstDelayMinMs(), profile.getFirstDelayMaxMs());
        for (int attempt = 1; ; attempt++) {
            String id = "PAY-" + order.getId() + "-" + attempt;
            PaymentStatus outcome = attempt >= profile.getMaxAttempts() ? PaymentStatus.SUCCESS : draw(rnd);
            if (outcome == PaymentStatus.FAILED) {
                out.add(receipt(order, id, attempt, PaymentStatus.FAILED, t, t, Collections.emptyList()));
                t += between(rnd, profile.getRetryDelayMinMs(), profile.getRetryDelayMaxMs());
                continue;
            }
            long settledAt = t;
            if (outcome == PaymentStatus.PENDING) {
                // Same payment id moves PENDING → SUCCESS / Cùng id phiếu chuyển PENDING → SUCCESS
                out.add(receipt(order, id, attempt, PaymentStatus.PENDING, t, t, Collections.emptyList()));
                settledAt = t + between(rnd, profile.getPendingMinMs(), profile.getPendingMaxMs());
            }
            out.add(receipt(order, id, attempt, PaymentStatus.SUCCESS, t, settledAt,
                    transactions(rnd, id, total, IctTime.businessDate(order.getTransactionDate()))));
            return out;
        }
    }

    /**
     * The refund of one return, never before the receipt succeeded. Empty when nothing is refunded.
     * Phiếu chi cho một phiếu trả, không sớm hơn lúc phiếu thu thành công. Rỗng khi không có tiền hoàn.
     */
    public Optional<PaymentSnapshot> refund(OrderHeader order, ReturnDocument ret, int methodId, long notBefore) {
        long amount = ret.getReturnAmount() == null ? 0 : ret.getReturnAmount();
        if (amount <= 0) {
            return Optional.empty();
        }
        SplittableRandom rnd = random(ret.getId() + "|refund");
        long t = Math.max(ret.getCreatedDate()
                + between(rnd, profile.getRefundDelayMinMs(), profile.getRefundDelayMaxMs()), notBefore);
        String id = "REF-" + ret.getId();
        PaymentHeader header = PaymentHeader.builder()
                .id(id).type(PaymentType.REFUND).status(PaymentStatus.SUCCESS)
                .orderId(order.getId()).returnId(ret.getId()).shopCode(order.getShopCode())
                .totalAmount(amount).attempt(1).createdDate(t).modifiedDate(t)
                .build();
        List<PaymentTransaction> txns = new ArrayList<>();
        txns.add(PaymentTransaction.builder().id(id + "-1").paymentMethodId(methodId).amount(amount).build());
        return Optional.of(PaymentSnapshot.builder().payment(header).transactions(txns).build());
    }

    /**
     * Method that received most of the money; refunds go back to it (ties → lower id).
     * Phương thức nhận nhiều tiền nhất; phiếu chi hoàn về phương thức này (bằng nhau → id nhỏ hơn).
     */
    public static int mainMethod(PaymentSnapshot success) {
        PaymentTransaction best = null;
        for (PaymentTransaction t : success.getTransactions()) {
            if (best == null || t.getAmount() > best.getAmount()
                    || (t.getAmount().equals(best.getAmount()) && t.getPaymentMethodId() < best.getPaymentMethodId())) {
                best = t;
            }
        }
        return best == null ? CASH : best.getPaymentMethodId();
    }

    /** E-wallet share on a business date (E7). / Tỷ trọng ví điện tử theo ngày (E7). */
    double walletShare(LocalDate date) {
        double span = ChronoUnit.DAYS.between(profile.getWalletRampStart(), profile.getWalletRampEnd());
        double x = ChronoUnit.DAYS.between(profile.getWalletRampStart(), date) / span;
        x = Math.max(0, Math.min(1, x));
        return profile.getWalletShareFrom() + (profile.getWalletShareTo() - profile.getWalletShareFrom()) * x;
    }

    private PaymentStatus draw(SplittableRandom rnd) {
        double u = rnd.nextDouble();
        if (u < profile.getSuccessRate()) {
            return PaymentStatus.SUCCESS;
        }
        return u < profile.getSuccessRate() + profile.getFailedRate() ? PaymentStatus.FAILED : PaymentStatus.PENDING;
    }

    private List<PaymentTransaction> transactions(SplittableRandom rnd, String paymentId, long total, LocalDate date) {
        double[] weights = methodWeights(date);
        int first = pick(rnd, weights);
        List<PaymentTransaction> out = new ArrayList<>();
        if (rnd.nextDouble() < profile.getSplitRate()) {
            long part = Math.round(total * (0.3 + 0.4 * rnd.nextDouble()) / SPLIT_UNIT) * SPLIT_UNIT;
            weights[first - 1] = 0;
            // Split only into two positive parts over two different methods
            // Chỉ chia khi được 2 phần dương trên 2 phương thức khác nhau
            if (part > 0 && part < total && weights[0] + weights[1] + weights[2] > 0) {
                int second = pick(rnd, weights);
                out.add(PaymentTransaction.builder().id(paymentId + "-1").paymentMethodId(first).amount(part).build());
                out.add(PaymentTransaction.builder().id(paymentId + "-2").paymentMethodId(second).amount(total - part).build());
                return out;
            }
        }
        out.add(PaymentTransaction.builder().id(paymentId + "-1").paymentMethodId(first).amount(total).build());
        return out;
    }

    /** Weights indexed by method id − 1. / Trọng số theo id phương thức − 1. */
    private double[] methodWeights(LocalDate date) {
        double wallet = walletShare(date);
        double rest = 1 - wallet;
        return new double[]{rest * profile.getCashShareOfRest(), rest * (1 - profile.getCashShareOfRest()), wallet};
    }

    private static int pick(SplittableRandom rnd, double[] weights) {
        double sum = 0;
        for (double w : weights) {
            sum += w;
        }
        double u = rnd.nextDouble() * sum;
        for (int i = 0; i < weights.length; i++) {
            u -= weights[i];
            if (u < 0 && weights[i] > 0) {
                return i + 1;
            }
        }
        // Rounding left u ≥ 0: take the last method with weight / Sai số làm tròn: lấy phương thức cuối có trọng số
        for (int i = weights.length - 1; i >= 0; i--) {
            if (weights[i] > 0) {
                return i + 1;
            }
        }
        return CASH;
    }

    private static PaymentSnapshot receipt(OrderHeader order, String id, int attempt, PaymentStatus status,
                                           long created, long modified, List<PaymentTransaction> txns) {
        PaymentHeader header = PaymentHeader.builder()
                .id(id).type(PaymentType.RECEIPT).status(status)
                .orderId(order.getId()).shopCode(order.getShopCode())
                .totalAmount(order.getTotalPayment()).attempt(attempt)
                .createdDate(created).modifiedDate(modified)
                .build();
        return PaymentSnapshot.builder().payment(header).transactions(new ArrayList<>(txns)).build();
    }

    private static long between(SplittableRandom rnd, long min, long max) {
        return min == max ? min : rnd.nextLong(min, max + 1);
    }

    /** Stable seed per business id (name-based UUID). / Seed cố định theo id nghiệp vụ. */
    private static SplittableRandom random(String name) {
        UUID u = UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
        return new SplittableRandom(u.getMostSignificantBits() ^ u.getLeastSignificantBits());
    }
}
