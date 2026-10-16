package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderEventType;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.rule.OrderRules;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.simulation.order.transform.state.OrderCursor;
import com.kaizen.stream.utils.time.IctTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Turns the wall clock into ORDER_PLACED events: every order whose arrival time has passed is built and
 * emitted, then the cursor moves on. Flink-free so it can be tested with a fake clock.
 * Biến đồng hồ thực thành sự kiện ORDER_PLACED: mọi đơn đã tới giờ được dựng và phát, rồi con trỏ tiến lên.
 * Không phụ thuộc Flink nên test được với đồng hồ giả.
 */
public final class OrderEmitter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OrderEmitter.class);

    private final OrderGenerator generator;
    private final long maxCatchupMs;
    private final String eventSource;

    public OrderEmitter(OrderGenerator generator, long maxCatchupMs, String eventSource) {
        this.generator = generator;
        this.maxCatchupMs = maxCatchupMs;
        this.eventSource = eventSource;
    }

    /**
     * Cursor to run from: a fresh one starts now; a restored one resumes, but a gap longer than max_catchup_ms
     * is cut to its last max_catchup_ms.
     * Con trỏ để chạy: mới thì bắt đầu từ bây giờ; restore thì chạy tiếp, nhưng khoảng trống dài hơn
     * max_catchup_ms chỉ được sinh bù max_catchup_ms cuối.
     */
    public OrderCursor start(OrderCursor restored, long now) {
        return start(restored, now, Map.of());
    }

    /**
     * Same as {@link #start(OrderCursor, long)}; a fresh cursor continues each shop from {@code usedToday}, the
     * highest number that shop already has today, so a restart without state never reuses an order id.
     * Như {@link #start(OrderCursor, long)}; con trỏ mới đánh số tiếp mỗi shop từ {@code usedToday}, số lớn nhất
     * shop đó đã có trong hôm nay, để khởi động lại không có state cũng không dùng lại id đơn.
     */
    public OrderCursor start(OrderCursor restored, long now, Map<String, Integer> usedToday) {
        if (restored == null) {
            OrderCursor c = new OrderCursor();
            c.setDay(IctTime.businessDate(now).toString());
            c.getShopCounters().putAll(usedToday);
            c.setNextAt(generator.nextArrival(now, 0));
            return c;
        }
        // copy() also fills usedSeries for state written before the field existed
        // copy() cũng tạo usedSeries cho state ghi trước khi có trường này
        OrderCursor c = restored.copy();
        long resumeFrom = now - maxCatchupMs;
        if (c.getNextAt() < resumeFrom) {
            LOG.warn("Order stream was down since {}; skipping to {} (max_catchup_ms={})",
                    c.getNextAt(), resumeFrom, maxCatchupMs);
            c.setNextAt(generator.nextArrival(resumeFrom, c.getSeq()));
        }
        return c;
    }

    /**
     * Emits up to {@code max} orders due at {@code now}; returns how many were emitted.
     * Phát tối đa {@code max} đơn đã tới giờ tại {@code now}; trả về số đơn đã phát.
     */
    public int emitDue(OrderCursor c, MasterData m, long now, int max, Consumer<Envelope<OrderSnapshot>> out) {
        int emitted = 0;
        while (emitted < max && c.getNextAt() <= now) {
            long at = c.getNextAt();
            // Shop counters restart every business day / Bộ đếm shop đánh lại từ đầu mỗi ngày
            String day = IctTime.businessDate(at).toString();
            if (!day.equals(c.getDay())) {
                c.setDay(day);
                c.getShopCounters().clear();
            }
            OrderSnapshot order = generator.place(m, c.getSeq(), at,
                    shop -> c.getShopCounters().merge(shop, 1, Integer::sum), c.getUsedSeries());
            List<String> broken = OrderRules.violations(OrderEventType.ORDER_PLACED.name(), at, order);
            // A generator bug must not reach the topic / Lỗi bộ sinh không được lọt lên topic
            if (broken.isEmpty()) {
                out.accept(Envelope.of(OrderEventType.ORDER_PLACED, eventSource, order.getOrder().getId(), at, at,
                        order));
                emitted++;
                // A voucher code is single-use / Mỗi mã voucher chỉ dùng một lần
                if (order.getOrder().getVoucherSeriesCode() != null) {
                    c.getUsedSeries().add(order.getOrder().getVoucherSeriesCode());
                }
            } else {
                LOG.error("Dropping generated order {}: {}", order.getOrder().getId(), broken);
            }
            c.setSeq(c.getSeq() + 1);
            c.setNextAt(generator.nextArrival(at, c.getSeq()));
        }
        return emitted;
    }

    /**
     * After a master reload, forgets handed-out codes that master data already rules out from the next order on
     * (closed before it, or no longer loaded), so the cursor stays small. Which codes are free does not change.
     * Sau khi nạp lại master, bỏ các mã đã phát mà master data đã loại từ đơn kế tiếp trở đi (đóng trước đó, hoặc
     * không còn được nạp), để con trỏ luôn nhỏ. Tập mã còn trống không đổi.
     */
    public void forgetSettledSeries(OrderCursor c, MasterData m) {
        Map<String, MasterData.Series> loaded = new HashMap<>();
        for (MasterData.Voucher v : m.getVouchers()) {
            for (MasterData.Series s : v.getSeries()) {
                loaded.put(s.getCode(), s);
            }
        }
        c.getUsedSeries().removeIf(code -> {
            MasterData.Series s = loaded.get(code);
            return s == null || s.getClosedAt() < c.getNextAt();
        });
    }
}
