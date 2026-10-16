package com.kaizen.stream.config;

import lombok.Value;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * Typed settings for all jobs. Groups are validated on first use, so a job only needs the groups it reads.
 * Cấu hình có kiểu cho mọi job. Mỗi nhóm chỉ kiểm tra khi được dùng, job không dùng PG thì không cần mật khẩu PG.
 */
public final class StreamSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RawSettings raw;

    public StreamSettings(RawSettings raw) {
        this.raw = raw;
    }

    public RawSettings raw() {
        return raw;
    }

    public Kafka kafka() {
        return new Kafka(raw.require("kafka", "bootstrap_servers"));
    }

    public Flink flink() {
        int parallelism = raw.getInt("flink", "parallelism", 3);
        return new Flink(parallelism, raw.getInt("flink", "source_parallelism", parallelism));
    }

    public Postgres postgres() {
        return new Postgres(
                raw.require("postgres", "host"),
                raw.getInt("postgres", "port", 5432),
                raw.require("postgres", "database"),
                raw.require("postgres", "username"),
                raw.require("postgres", "password"),
                raw.getInt("postgres", "batch_size", 500),
                raw.getLong("postgres", "flush_interval_ms", 1_000L));
    }

    public Elasticsearch elasticsearch() {
        return new Elasticsearch(
                raw.require("elasticsearch", "hosts"),
                raw.getInt("elasticsearch", "bulk_max_actions", 500),
                raw.getLong("elasticsearch", "bulk_interval_ms", 1_000L));
    }

    public ClickHouse clickhouse() {
        return new ClickHouse(
                raw.require("clickhouse", "host"),
                raw.getInt("clickhouse", "port", 8123),
                raw.get("clickhouse", "database", "default"),
                raw.require("clickhouse", "username"),
                raw.require("clickhouse", "password"));
    }

    /** BusinessRealtimeJob → report.realtime_business_by_order. */
    public BusinessRealtime businessRealtime() {
        int hotDays = raw.getInt("business_realtime", "hot_days", 14);
        return new BusinessRealtime(
                hotDays,
                raw.getInt("business_realtime", "state_days", hotDays + 2),
                raw.getInt("business_realtime", "batch_size", 500),
                raw.getLong("business_realtime", "batch_interval_ms", 2_000L),
                raw.getLong("business_realtime", "shop_refresh_ms", 600_000L));
    }

    /** Payment simulation profile (supply_chain_forecast_optimization.md 3.6, E7). / Hồ sơ mô phỏng thanh toán. */
    public Payment payment() {
        Payment p = new Payment(
                raw.getInt("payment", "state_days", 16),
                raw.getDouble("payment", "success_rate", 0.92),
                raw.getDouble("payment", "failed_rate", 0.05),
                raw.getInt("payment", "max_attempts", 3),
                raw.getLong("payment", "first_delay_min_ms", 2_000L),
                raw.getLong("payment", "first_delay_max_ms", 10_000L),
                raw.getLong("payment", "retry_delay_min_ms", 60_000L),
                raw.getLong("payment", "retry_delay_max_ms", 300_000L),
                raw.getLong("payment", "pending_min_ms", 600_000L),
                raw.getLong("payment", "pending_max_ms", 1_800_000L),
                raw.getLong("payment", "refund_delay_min_ms", 2_000L),
                raw.getLong("payment", "refund_delay_max_ms", 10_000L),
                raw.getDouble("payment", "split_rate", 0.15),
                raw.getDouble("payment", "wallet_share_from", 0.15),
                raw.getDouble("payment", "wallet_share_to", 0.45),
                LocalDate.parse(raw.get("payment", "wallet_ramp_start", "2024-01-01")),
                LocalDate.parse(raw.get("payment", "wallet_ramp_end", "2026-09-30")),
                raw.getDouble("payment", "cash_share_of_rest", 0.6));
        p.validate();
        return p;
    }

    /** Live order simulation profile (supply_chain_forecast_optimization.md 3.1–3.6). / Hồ sơ mô phỏng đơn live. */
    public Order order() {
        Order o = new Order(
                raw.getLong("order", "seed", 20_261_011L),
                raw.getDouble("order", "orders_per_day", 7_000),
                raw.getInt("order", "tier_a_count", 10),
                raw.getInt("order", "tier_b_count", 20),
                raw.getDouble("order", "tier_a_weight", 230),
                raw.getDouble("order", "tier_b_weight", 150),
                raw.getDouble("order", "tier_c_weight", 90),
                raw.getDouble("order", "extra_lines_mean", 1.4),
                raw.getInt("order", "max_lines", 6),
                raw.getDouble("order", "walk_in_rate", 0.20),
                raw.getDouble("order", "voucher_rate", 0.10),
                raw.getDouble("order", "gift_rate", 0.30),
                raw.getDouble("order", "return_rate", 0.04),
                raw.getDouble("order", "return_full_rate", 0.50),
                raw.getLong("order", "return_delay_min_ms", 86_400_000L),
                raw.getLong("order", "return_delay_max_ms", 1_209_600_000L),
                raw.getDouble("order", "restock_rate", 0.70),
                raw.getInt("order", "state_days", 16),
                raw.getLong("order", "max_catchup_ms", 21_600_000L),
                raw.getLong("order", "master_refresh_ms", 3_600_000L));
        o.validate();
        return o;
    }

    /** OutboxRelayJob polling and clean-up. / Chu kỳ quét và dọn của OutboxRelayJob. */
    public Outbox outbox() {
        Outbox o = new Outbox(
                raw.getLong("outbox", "poll_interval_ms", 1_000L),
                raw.getInt("outbox", "batch_size", 500),
                raw.getLong("outbox", "retention_ms", 259_200_000L),
                raw.getLong("outbox", "purge_interval_ms", 3_600_000L),
                raw.getInt("outbox", "max_consecutive_failures", 60));
        o.validate();
        return o;
    }

    /** SupplyChainRealtimeJob → report.realtime_inventory_position / report.realtime_shipment_status. */
    public SupplyChainRealtime supplyChainRealtime() {
        return new SupplyChainRealtime(
                raw.getInt("supply_chain_realtime", "batch_size", 500),
                raw.getLong("supply_chain_realtime", "batch_interval_ms", 2_000L),
                raw.getInt("supply_chain_realtime", "shipment_state_days", 35));
    }

    @Override
    public String toString() {
        return "StreamSettings" + raw;
    }

    @Value
    public static class Outbox implements Serializable {
        long pollIntervalMs;
        /** Rows per poll. / Số dòng mỗi lần quét. */
        int batchSize;
        /** Published rows older than this are deleted. / Dòng đã phát cũ hơn mốc này bị xoá. */
        long retentionMs;
        long purgeIntervalMs;
        /** PG errors in a row before the job fails. / Số lỗi PG liên tiếp trước khi job dừng. */
        int maxConsecutiveFailures;

        void validate() {
            // Fail at startup rather than relaying wrongly / Dừng lúc khởi động thay vì relay sai
            if (pollIntervalMs <= 0 || batchSize <= 0 || purgeIntervalMs <= 0 || maxConsecutiveFailures <= 0) {
                throw new IllegalStateException("Setting [outbox] intervals, batch_size and failures must be > 0");
            }
            // Kafka keeps inventory/logistics 1 day; PG must cover it / PG phải giữ lâu hơn retention Kafka (1 ngày)
            if (retentionMs < 86_400_000L) {
                throw new IllegalStateException("Setting [outbox] retention_ms must be >= 1 day");
            }
        }
    }

    @Value
    public static class SupplyChainRealtime implements Serializable {
        int batchSize;
        long batchIntervalMs;
        /** Keyed-state TTL of a shipment, longer than the CH TTL (30 days). / TTL state của chuyến, dài hơn TTL CH. */
        int shipmentStateDays;
    }

    @Value
    public static class Kafka implements Serializable {
        String bootstrapServers;
    }

    @Value
    public static class Flink implements Serializable {
        /** Default operator parallelism. / Parallelism mặc định của operator. */
        int parallelism;
        /** Kafka source parallelism; must divide every source topic's partition count. */
        int sourceParallelism;
    }

    @Value
    public static class Postgres implements Serializable {
        String host;
        int port;
        String database;
        String username;
        @lombok.ToString.Exclude
        String password;
        /** Aggregates per transaction. / Số aggregate mỗi transaction. */
        int batchSize;
        /** Max wait before a partial batch is written. / Thời gian chờ tối đa trước khi ghi batch chưa đầy. */
        long flushIntervalMs;

        public String jdbcUrl() {
            return "jdbc:postgresql://" + host + ":" + port + "/" + database;
        }
    }

    @Value
    public static class Elasticsearch implements Serializable {
        /** Comma-separated URLs, e.g. http://es:9200. / Danh sách URL cách nhau dấu phẩy. */
        String hosts;
        int bulkMaxActions;
        long bulkIntervalMs;
    }

    @Value
    public static class ClickHouse implements Serializable {
        String host;
        int port;
        String database;
        String username;
        @lombok.ToString.Exclude
        String password;

        public String jdbcUrl() {
            return "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        }
    }

    @Value
    public static class BusinessRealtime implements Serializable {
        /** Days served from the hot CH table; older days come from cold. / Số ngày đọc từ bảng hot. */
        int hotDays;
        /**
         * Keyed-state TTL and CH table TTL; rows older than this are frozen (never re-emitted).
         * TTL của keyed state và bảng CH; dòng cũ hơn mốc này bị đóng băng (không phát lại).
         */
        int stateDays;
        int batchSize;
        long batchIntervalMs;
        /** How often shop names are reloaded from PG. / Chu kỳ nạp lại tên cửa hàng từ PG. */
        long shopRefreshMs;
    }

    @Value
    public static class Payment implements Serializable {
        /** Keyed-state TTL; covers returns up to 14 days after the sale. / TTL state; bao trọn trả hàng tới 14 ngày. */
        int stateDays;
        /** First-attempt outcome; PENDING gets 1 − success − failed. / Kết quả lần đầu; PENDING = phần còn lại. */
        double successRate;
        double failedRate;
        /** The last attempt always succeeds. / Lần thử cuối luôn thành công. */
        int maxAttempts;
        long firstDelayMinMs;
        long firstDelayMaxMs;
        long retryDelayMinMs;
        long retryDelayMaxMs;
        /** PENDING → SUCCESS delay. / Thời gian từ PENDING tới SUCCESS. */
        long pendingMinMs;
        long pendingMaxMs;
        long refundDelayMinMs;
        long refundDelayMaxMs;
        /** Share of receipts paid with two methods. / Tỷ lệ phiếu thu trả bằng 2 phương thức. */
        double splitRate;
        /** E-wallet share ramps linearly between these dates (E7). / Tỷ trọng ví tăng tuyến tính (E7). */
        double walletShareFrom;
        double walletShareTo;
        LocalDate walletRampStart;
        LocalDate walletRampEnd;
        /** Cash share of the non-wallet part; card gets the rest. / Tỷ trọng tiền mặt trong phần không phải ví. */
        double cashShareOfRest;

        void validate() {
            // Fail at startup rather than emitting impossible payments / Dừng lúc khởi động thay vì phát phiếu sai
            check(stateDays >= 1, "state_days must be >= 1");
            check(successRate >= 0 && failedRate >= 0 && successRate + failedRate <= 1,
                    "success_rate + failed_rate must be within [0, 1]");
            check(maxAttempts >= 1, "max_attempts must be >= 1");
            check(range(firstDelayMinMs, firstDelayMaxMs) && range(retryDelayMinMs, retryDelayMaxMs)
                    && range(pendingMinMs, pendingMaxMs) && range(refundDelayMinMs, refundDelayMaxMs),
                    "every *_min_ms must be >= 0 and <= its *_max_ms");
            check(unit(splitRate) && unit(walletShareFrom) && unit(walletShareTo) && unit(cashShareOfRest),
                    "rates and shares must be within [0, 1]");
            check(walletRampEnd.isAfter(walletRampStart), "wallet_ramp_end must be after wallet_ramp_start");
        }

        private static boolean range(long min, long max) {
            return min >= 0 && min <= max;
        }

        private static boolean unit(double x) {
            return x >= 0 && x <= 1;
        }

        private static void check(boolean ok, String message) {
            if (!ok) {
                throw new IllegalStateException("Setting [payment] " + message);
            }
        }
    }

    @Value
    public static class Order implements Serializable {
        /** Same seed + same master data → same orders. / Cùng seed + cùng master → cùng đơn. */
        long seed;
        /** Mean orders per day over all shops before weekday factors. / Số đơn TB mỗi ngày toàn chuỗi. */
        double ordersPerDay;
        /** Shops ranked by code: first A, next B, the rest C (3.1). / Xếp shop theo mã: A, B, còn lại C. */
        int tierACount;
        int tierBCount;
        double tierAWeight;
        double tierBWeight;
        double tierCWeight;
        /** Lines per order = 1 + Poisson(mean), capped. / Số dòng = 1 + Poisson(mean), có trần. */
        double extraLinesMean;
        int maxLines;
        /** Orders without customerId. / Tỷ lệ đơn khách vãng lai. */
        double walkInRate;
        double voucherRate;
        /** Orders getting a gift line while a GIFT promotion runs. / Tỷ lệ đơn được tặng quà khi có CT quà tặng. */
        double giftRate;
        double returnRate;
        /** Share of returns that give back everything. / Tỷ lệ phiếu trả toàn bộ. */
        double returnFullRate;
        long returnDelayMinMs;
        long returnDelayMaxMs;
        /** Returned units back on the shelf; the rest is damage. / Tỷ lệ hàng trả nhập lại kệ. */
        double restockRate;
        /** Keyed-state TTL of pending returns. / TTL state của phiếu trả đang chờ. */
        int stateDays;
        /** After downtime, regenerate at most this much of the gap. / Sau khi dừng, sinh bù tối đa chừng này. */
        long maxCatchupMs;
        long masterRefreshMs;

        void validate() {
            // Fail at startup rather than emitting impossible orders / Dừng lúc khởi động thay vì phát đơn sai
            check(ordersPerDay > 0, "orders_per_day must be > 0");
            check(tierACount >= 0 && tierBCount >= 0, "tier counts must be >= 0");
            check(tierAWeight > 0 && tierBWeight > 0 && tierCWeight > 0, "tier weights must be > 0");
            check(extraLinesMean >= 0 && maxLines >= 1, "extra_lines_mean must be >= 0 and max_lines >= 1");
            check(unit(walkInRate) && unit(voucherRate) && unit(giftRate) && unit(returnRate)
                    && unit(returnFullRate) && unit(restockRate), "rates must be within [0, 1]");
            check(returnDelayMinMs >= 0 && returnDelayMinMs <= returnDelayMaxMs,
                    "return_delay_min_ms must be >= 0 and <= return_delay_max_ms");
            check(stateDays * 86_400_000L > returnDelayMaxMs, "state_days must outlast return_delay_max_ms");
            check(maxCatchupMs >= 0 && masterRefreshMs > 0, "max_catchup_ms >= 0 and master_refresh_ms > 0");
        }

        private static boolean unit(double x) {
            return x >= 0 && x <= 1;
        }

        private static void check(boolean ok, String message) {
            if (!ok) {
                throw new IllegalStateException("Setting [order] " + message);
            }
        }
    }
}
