package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.order.OrderEventType;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.OrderLine;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.OrderStatus;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.order.ReturnLine;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Invariants every order snapshot must satisfy before any sink sees it; broken snapshots go to the DLQ.
 * Bất biến mọi snapshot đơn phải thoả trước khi tới sink; snapshot sai sang DLQ.
 */
public final class OrderRules {

    private OrderRules() {
    }

    /**
     * @param eventType meta.eventType
     * @param version   meta.version, must equal order.modifiedDate / phải bằng order.modifiedDate
     */
    public static List<String> violations(String eventType, Long version, OrderSnapshot s) {
        Violations v = new Violations();
        OrderHeader o = s.getOrder();
        if (o == null) {
            return v.check(false, "order is missing").list();
        }
        v.require("order.id", o.getId())
                .require("order.shopCode", o.getShopCode())
                .require("order.employeeSaleId", o.getEmployeeSaleId())
                .require("order.transactionDate", o.getTransactionDate())
                .require("order.totalBill", o.getTotalBill())
                .require("order.totalDiscount", o.getTotalDiscount())
                .require("order.totalPayment", o.getTotalPayment())
                .require("order.status", o.getStatus())
                .require("order.createdDate", o.getCreatedDate())
                .require("order.modifiedDate", o.getModifiedDate())
                .requireNonEmpty("items", s.getItems());
        // Stop here: later checks need these fields / Dừng: các bước sau cần các trường này
        if (!v.isEmpty()) {
            return v.list();
        }

        List<ReturnDocument> returns = s.getReturns() == null ? Collections.emptyList() : s.getReturns();
        checkEvent(v, eventType, version, o, returns);
        Map<String, OrderLine> linesById = checkLines(v, o, s.getItems());
        // Return checks need every line indexed / Kiểm tra phiếu trả cần đủ mọi dòng
        if (linesById.size() != s.getItems().size()) {
            return v.list();
        }
        checkReturns(v, o, returns, linesById);

        // Property 4: status follows returns / Trạng thái theo phiếu trả
        long returned = s.returnedAmount();
        OrderStatus expected = OrderStatus.of(o.getTotalPayment(), returns.size(), returned);
        v.check(o.getStatus() == expected,
                "order.status " + o.getStatus().label() + " but returns imply " + expected.label());
        v.check(returned <= o.getTotalPayment(),
                "Σ returnAmount " + returned + " > totalPayment " + o.getTotalPayment());
        return v.list();
    }

    private static void checkEvent(Violations v, String eventType, Long version, OrderHeader o,
                                   List<ReturnDocument> returns) {
        v.check(Objects.equals(version, o.getModifiedDate()),
                "meta.version " + version + " != order.modifiedDate " + o.getModifiedDate());
        v.check(o.getModifiedDate() >= o.getCreatedDate(), "order.modifiedDate < createdDate");
        if (OrderEventType.ORDER_PLACED.name().equals(eventType)) {
            v.check(returns.isEmpty(), "ORDER_PLACED must not carry returns");
        } else if (OrderEventType.ORDER_RETURNED.name().equals(eventType)) {
            v.check(!returns.isEmpty(), "ORDER_RETURNED needs at least one return");
        } else {
            v.check(false, "unknown order eventType " + eventType);
        }
    }

    /** Line math and order totals. / Kiểm tra tiền từng dòng và tổng đơn. */
    private static Map<String, OrderLine> checkLines(Violations v, OrderHeader o, List<OrderLine> items) {
        Map<String, OrderLine> byId = new HashMap<>();
        Set<Integer> lineNos = new HashSet<>();
        long bill = 0;
        long discount = 0;
        long payment = 0;
        for (OrderLine l : items) {
            String p = "items[" + l.getId() + "]";
            int mark = v.mark();
            v.require(p + ".id", l.getId())
                    .require(p + ".lineNo", l.getLineNo())
                    .require(p + ".productId", l.getProductId())
                    .require(p + ".quantity", l.getQuantity())
                    .require(p + ".unitPrice", l.getUnitPrice())
                    .require(p + ".discountAmount", l.getDiscountAmount())
                    .require(p + ".lineAmount", l.getLineAmount())
                    .require(p + ".isGift", l.getGift())
                    .require(p + ".unitCost", l.getUnitCost());
            if (v.grewSince(mark)) {
                return byId;
            }
            v.check(byId.put(l.getId(), l) == null, p + " duplicate id");
            v.check(lineNos.add(l.getLineNo()), p + " duplicate lineNo " + l.getLineNo());
            v.check(l.getQuantity() > 0, p + ".quantity must be > 0");
            v.check(l.getUnitPrice() >= 0 && l.getDiscountAmount() >= 0 && l.getUnitCost() >= 0,
                    p + " money fields must be >= 0");
            v.check(l.getLineAmount() == l.getQuantity() * l.getUnitPrice() - l.getDiscountAmount(),
                    p + ".lineAmount != quantity × unitPrice − discountAmount");
            v.check(!l.getGift() || l.getUnitPrice() == 0, p + " gift line must have unitPrice 0");
            bill += l.getQuantity() * l.getUnitPrice();
            discount += l.getDiscountAmount();
            payment += l.getLineAmount();
        }
        v.check(bill == o.getTotalBill(), "Σ quantity × unitPrice " + bill + " != totalBill " + o.getTotalBill());
        v.check(discount == o.getTotalDiscount(),
                "Σ discountAmount " + discount + " != totalDiscount " + o.getTotalDiscount());
        v.check(payment == o.getTotalPayment(),
                "Σ lineAmount " + payment + " != totalPayment " + o.getTotalPayment());
        return byId;
    }

    /** Returns reference real lines and never exceed what was sold. / Phiếu trả trỏ đúng dòng, không vượt số đã bán. */
    private static void checkReturns(Violations v, OrderHeader o, List<ReturnDocument> returns,
                                     Map<String, OrderLine> linesById) {
        Set<String> ids = new HashSet<>();
        Map<String, Integer> returnedQty = new HashMap<>();
        Map<String, Long> returnedAmount = new HashMap<>();
        for (ReturnDocument r : returns) {
            String p = "returns[" + r.getId() + "]";
            int mark = v.mark();
            v.require(p + ".id", r.getId())
                    .require(p + ".createdDate", r.getCreatedDate())
                    .require(p + ".returnAmount", r.getReturnAmount())
                    .requireNonEmpty(p + ".items", r.getItems());
            if (v.grewSince(mark)) {
                return;
            }
            v.check(ids.add(r.getId()), p + " duplicate id");
            v.check(r.getReturnAmount() > 0, p + ".returnAmount must be > 0");
            v.check(r.getCreatedDate() >= o.getCreatedDate() && r.getCreatedDate() <= o.getModifiedDate(),
                    p + ".createdDate outside [order.createdDate, order.modifiedDate]");
            long sum = 0;
            for (ReturnLine rl : r.getItems()) {
                String q = p + ".items[" + rl.getId() + "]";
                int lineMark = v.mark();
                v.require(q + ".id", rl.getId())
                        .require(q + ".orderItemId", rl.getOrderItemId())
                        .require(q + ".quantity", rl.getQuantity())
                        .require(q + ".restockQty", rl.getRestockQty())
                        .require(q + ".damageQty", rl.getDamageQty())
                        .require(q + ".lineAmount", rl.getLineAmount());
                if (v.grewSince(lineMark)) {
                    return;
                }
                OrderLine sold = linesById.get(rl.getOrderItemId());
                if (sold == null) {
                    v.check(false, q + " references unknown orderItemId " + rl.getOrderItemId());
                    continue;
                }
                v.check(Objects.equals(rl.getProductId(), sold.getProductId()), q + ".productId differs from sold line");
                v.check(rl.getQuantity() > 0, q + ".quantity must be > 0");
                v.check(rl.getRestockQty() >= 0 && rl.getDamageQty() >= 0
                                && rl.getRestockQty() + rl.getDamageQty() == rl.getQuantity(),
                        q + " restockQty + damageQty != quantity");
                v.check(rl.getLineAmount() >= 0, q + ".lineAmount must be >= 0");
                returnedQty.merge(sold.getId(), rl.getQuantity(), Integer::sum);
                returnedAmount.merge(sold.getId(), rl.getLineAmount(), Long::sum);
                sum += rl.getLineAmount();
            }
            v.check(sum == r.getReturnAmount(), p + " Σ lineAmount " + sum + " != returnAmount " + r.getReturnAmount());
        }
        // Cumulative over all returns of the order / Cộng dồn qua mọi phiếu trả của đơn
        returnedQty.forEach((lineId, qty) -> v.check(qty <= linesById.get(lineId).getQuantity(),
                "returned quantity " + qty + " > sold quantity on line " + lineId));
        returnedAmount.forEach((lineId, amount) -> v.check(amount <= linesById.get(lineId).getLineAmount(),
                "returned amount " + amount + " > lineAmount on line " + lineId));
    }
}
