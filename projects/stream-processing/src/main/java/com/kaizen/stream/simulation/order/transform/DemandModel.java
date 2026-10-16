package com.kaizen.stream.simulation.order.transform;

import java.io.Serializable;

/**
 * Order intensity of the chain over time and how it splits over shops. The rate must be constant within
 * each clock hour (ICT is UTC+7, so hours line up with epoch hours).
 * Cường độ đơn của chuỗi theo thời gian và cách chia cho các shop. Tốc độ phải cố định trong mỗi giờ đồng hồ
 * (ICT là UTC+7 nên ranh giới giờ trùng với giờ epoch).
 */
public interface DemandModel extends Serializable {

    long HOUR_MS = 3_600_000L;

    /** Expected orders per millisecond over all shops at this instant. / Số đơn kỳ vọng mỗi ms toàn chuỗi. */
    double rate(long epochMs);

    /**
     * Relative weight at this instant of the shop at this rank among shop codes sorted ascending; 0 = closed.
     * Trọng số tại thời điểm này của shop theo thứ hạng mã (tăng dần); 0 = đóng cửa.
     */
    double shopWeight(int rank, long epochMs);

    /**
     * Next arrival of a Poisson process with this rate, strictly after {@code from}; {@code exp1} is an Exp(1) draw.
     * Lần đến tiếp theo của quá trình Poisson với tốc độ này, sau {@code from}; {@code exp1} là một mẫu Exp(1).
     */
    static long nextArrival(DemandModel model, long from, double exp1) {
        double left = exp1;
        long t = from;
        // Walk hour by hour, spending the exponential budget / Đi từng giờ, tiêu dần "ngân sách" mũ
        for (int i = 0; i < 24 * 14; i++) {
            long hourEnd = (Math.floorDiv(t, HOUR_MS) + 1) * HOUR_MS;
            double r = model.rate(t);
            double capacity = r * (hourEnd - t);
            if (r > 0 && capacity >= left) {
                return t + Math.max(1L, (long) Math.ceil(left / r));
            }
            left -= capacity;
            t = hourEnd;
        }
        throw new IllegalStateException("Demand rate is zero for 14 days after " + from);
    }
}
