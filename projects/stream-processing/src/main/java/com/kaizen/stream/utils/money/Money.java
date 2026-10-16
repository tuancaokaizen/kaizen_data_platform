package com.kaizen.stream.utils.money;

/**
 * Money is whole VND in a long. / Tiền là số nguyên VND kiểu long.
 */
public final class Money {

    private Money() {
    }

    /**
     * Split total proportionally to weights so parts sum exactly to total (largest remainder; ties → lower index).
     * Chia total theo tỷ trọng, tổng các phần đúng bằng total (phần dư lớn nhất; hoà thì ưu tiên chỉ số nhỏ).
     */
    public static long[] allocate(long total, long[] weights) {
        if (total < 0) {
            throw new IllegalArgumentException("total must be >= 0: " + total);
        }
        long weightSum = 0;
        for (long w : weights) {
            if (w < 0) {
                throw new IllegalArgumentException("weights must be >= 0");
            }
            weightSum = Math.addExact(weightSum, w);
        }
        long[] parts = new long[weights.length];
        if (total == 0) {
            return parts;
        }
        if (weightSum == 0) {
            throw new IllegalArgumentException("cannot allocate " + total + " over zero weights");
        }
        long[] remainders = new long[weights.length];
        long assigned = 0;
        for (int i = 0; i < weights.length; i++) {
            // Exact integer floor of total * w / sum without overflow / Lấy phần nguyên chính xác, không tràn số
            java.math.BigInteger[] qr = java.math.BigInteger.valueOf(total)
                    .multiply(java.math.BigInteger.valueOf(weights[i]))
                    .divideAndRemainder(java.math.BigInteger.valueOf(weightSum));
            parts[i] = qr[0].longValueExact();
            remainders[i] = qr[1].longValueExact();
            assigned += parts[i];
        }
        // Hand out the leftover units to the largest remainders / Chia phần còn thiếu cho phần dư lớn nhất
        long leftover = total - assigned;
        while (leftover > 0) {
            int best = -1;
            for (int i = 0; i < weights.length; i++) {
                if (weights[i] > 0 && (best < 0 || remainders[i] > remainders[best])) {
                    best = i;
                }
            }
            parts[best]++;
            remainders[best] = -1;
            leftover--;
        }
        return parts;
    }
}
