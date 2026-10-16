#!/bin/bash
# One-off repair: on 2026-10-15 20:16 VN simulation-order restarted without state, numbered shops from 1 again
# and overwrote 264 morning orders. Removes those orders and everything hanging off them (option A).
# Sửa một lần: 20:16 ngày 2026-10-15 simulation-order chạy lại không có state, đánh số shop lại từ 1
# và ghi đè 264 đơn buổi sáng. Xoá các đơn đó cùng mọi thứ gắn với chúng (phương án A).
#
#   check  Postgres part inside a transaction that is rolled back, plus counts in Elasticsearch / ClickHouse
#          Phần Postgres chạy trong transaction rồi rollback, cộng số đếm ở Elasticsearch / ClickHouse
#   apply  Commit Postgres, then delete the same ids in Elasticsearch and ClickHouse
#          Commit Postgres, rồi xoá đúng các id đó ở Elasticsearch và ClickHouse
#
# Only Kaizen data: DB information_system, ES indexes order-bundles / payment-bundles,
# ClickHouse report.realtime_business_by_order.
# Chỉ dữ liệu Kaizen: DB information_system, index ES order-bundles / payment-bundles,
# ClickHouse report.realtime_business_by_order.
set -euo pipefail

export KUBECONFIG="${K3S_CONF:-$HOME/.kube/config.k3s-new}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." >/dev/null 2>&1 && pwd)"
WORK="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.repair-20261015"
MODE="${1:-check}"

# The bad set: header touched by the restarted run, created before it / Tập lỗi: đầu đơn bị lần chạy lại sửa, tạo trước đó
BAD_ORDERS="SELECT \"Id\" FROM \"order\".\"Order\" WHERE \"ModifiedDate\" >= '2026-10-15 20:16+07' AND \"CreatedDate\" < '2026-10-15 20:16+07'"

# Read one key of dags/config.ini; the value only goes into the pod env / Đọc một khoá của dags/config.ini; giá trị chỉ vào env của pod
cfg() {
    awk -F' *= *' -v sec="[$1]" -v k="$2" '$0==sec{f=1;next} /^\[/{f=0} f&&$1==k{print $2;exit}' "$ROOT/dags/config.ini"
}

PG_POD="$(kubectl get pods -n databases -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')"
CH_POD="$(kubectl get pods -n databases -o name | grep my-clickhouse | head -1)"

psql_run() {
    kubectl exec -i -n databases "$PG_POD" -- env PGPASSWORD="$(cfg postgres password)" \
        psql -U "$(cfg postgres username)" -d information_system -v ON_ERROR_STOP=1 -X -q -P pager=off "$@"
}

clickhouse_run() {
    kubectl exec -i -n databases "$CH_POD" -- sh -c \
        'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery'
}

elasticsearch_run() {
    kubectl exec -i -n logging deploy/elasticsearch -- curl -s -H 'Content-Type: application/json' "$@"
}

# Comma-separated quoted list for SQL / Danh sách có nháy, phân cách dấu phẩy cho SQL
sql_list() {
    awk 'NF {printf "%s'\''%s'\''", (n++ ? "," : ""), $1}' "$1"
}

# JSON array for an Elasticsearch ids query / Mảng JSON cho truy vấn ids của Elasticsearch
json_list() {
    awk 'NF {printf "%s\"%s\"", (n++ ? "," : ""), $1}' "$1"
}

# Save the ids before Postgres loses them; apply re-runs reuse the saved files
# Lưu id trước khi Postgres xoá; chạy apply lại sẽ dùng lại file đã lưu
export_ids() {
    mkdir -p "$WORK"
    if [ -s "$WORK/order_ids.txt" ]; then
        echo "Reusing saved ids in $WORK"
        return
    fi
    psql_run -t -A -c "$BAD_ORDERS ORDER BY 1" > "$WORK/order_ids.txt"
    psql_run -t -A -c "SELECT \"Id\" FROM payment.\"Payment\" WHERE \"OrderId\" IN ($BAD_ORDERS) ORDER BY 1" \
        > "$WORK/payment_ids.txt"
    echo "Saved $(wc -l < "$WORK/order_ids.txt" | tr -d ' ') order ids, $(wc -l < "$WORK/payment_ids.txt" | tr -d ' ') payment ids"
}

# Postgres part; ends with COMMIT or ROLLBACK / Phần Postgres; kết thúc bằng COMMIT hoặc ROLLBACK
repair_postgres() {
    local finish="$1"
    psql_run <<SQL
BEGIN;
CREATE TEMP TABLE bad_order ON COMMIT DROP AS SELECT unnest(ARRAY[$(sql_list "$WORK/order_ids.txt")]::text[]) AS "Id";
CREATE TEMP TABLE bad_payment ON COMMIT DROP AS
    SELECT "Id" FROM payment."Payment" WHERE "OrderId" IN (SELECT "Id" FROM bad_order);

-- Give back the stock of every sale movement of these orders (Qty is negative)
-- Trả lại tồn của mọi dòng xuất bán của các đơn này (Qty âm)
WITH sold AS (
    SELECT "LocationType", "LocationId", "ProductId", "LotId", sum("Qty") AS qty
    FROM inventory."StockMovement"
    WHERE "RefDocType" = 'ORDER' AND "RefDocId" IN (SELECT "Id" FROM bad_order)
    GROUP BY 1, 2, 3, 4)
UPDATE inventory."InventoryBalance" b
SET "OnHandQty" = b."OnHandQty" - s.qty, "ModifiedDate" = now()
FROM sold s
WHERE b."LocationType" = s."LocationType" AND b."LocationId" = s."LocationId"
  AND b."ProductId" = s."ProductId" AND b."LotId" IS NOT DISTINCT FROM s."LotId";

DELETE FROM inventory."StockMovement" WHERE "RefDocType" = 'ORDER' AND "RefDocId" IN (SELECT "Id" FROM bad_order);
DELETE FROM payment."Transaction" WHERE "PaymentId" IN (SELECT "Id" FROM bad_payment);
DELETE FROM payment."OrderPayment" WHERE "OrderId" IN (SELECT "Id" FROM bad_order);
DELETE FROM payment."Payment" WHERE "Id" IN (SELECT "Id" FROM bad_payment);
DELETE FROM "order"."OrderItem" WHERE "OrderId" IN (SELECT "Id" FROM bad_order);
DELETE FROM "order"."Order" WHERE "Id" IN (SELECT "Id" FROM bad_order);

-- Voucher codes marked USED today that no order holds any more go back to ACTIVE
-- Mã voucher đánh dấu USED hôm nay mà không còn đơn nào giữ thì trả về ACTIVE
UPDATE promotion."VoucherSeries" s
SET "Status" = 'ACTIVE', "ModifiedDate" = now()
WHERE s."Status" = 'USED' AND s."ModifiedDate" >= '2026-10-15 00:00+07'
  AND NOT EXISTS (SELECT 1 FROM "order"."Order" o WHERE o."VoucherSeriesCode" = s."SeriesCode");

-- Checks: nothing left, ledger still equals balance / Kiểm tra: không còn gì, sổ kho vẫn khớp tồn
SELECT 'orders left' AS check_name, count(*) FROM "order"."Order" WHERE "Id" IN (SELECT "Id" FROM bad_order)
UNION ALL SELECT 'payments left', count(*) FROM payment."Payment" WHERE "Id" IN (SELECT "Id" FROM bad_payment)
UNION ALL SELECT 'movements left', count(*) FROM inventory."StockMovement" WHERE "RefDocId" IN (SELECT "Id" FROM bad_order)
UNION ALL SELECT 'orders today', count(*) FROM "order"."Order" WHERE "OrderDate" = DATE '2026-10-15'
UNION ALL SELECT 'used vouchers without order', count(*) FROM promotion."VoucherSeries" s
    WHERE s."Status" = 'USED' AND NOT EXISTS (SELECT 1 FROM "order"."Order" o WHERE o."VoucherSeriesCode" = s."SeriesCode")
UNION ALL SELECT 'ledger <> balance', count(*) FROM (
    SELECT "LocationType", "LocationId", "ProductId", coalesce("LotId", '') AS lot, sum("Qty") AS qty
    FROM inventory."StockMovement" GROUP BY 1, 2, 3, 4) l
    FULL JOIN (SELECT "LocationType", "LocationId", "ProductId", coalesce("LotId", '') AS lot, "OnHandQty" AS qty
               FROM inventory."InventoryBalance") b USING ("LocationType", "LocationId", "ProductId", lot)
    WHERE coalesce(l.qty, 0) <> coalesce(b.qty, 0)
UNION ALL SELECT 'negative balance', count(*) FROM inventory."InventoryBalance" WHERE "OnHandQty" < 0;
$finish;
SQL
}

count_downstream() {
    echo "-- Elasticsearch order-bundles / payment-bundles:"
    elasticsearch_run 'localhost:9200/order-bundles/_count' -d "{\"query\":{\"ids\":{\"values\":[$(json_list "$WORK/order_ids.txt")]}}}"
    echo
    elasticsearch_run 'localhost:9200/payment-bundles/_count' -d "{\"query\":{\"ids\":{\"values\":[$(json_list "$WORK/payment_ids.txt")]}}}"
    echo
    echo "-- ClickHouse report.realtime_business_by_order rows:"
    echo "SELECT count() FROM report.realtime_business_by_order WHERE OrderId IN ($(sql_list "$WORK/order_ids.txt"))" \
        | clickhouse_run
}

delete_downstream() {
    elasticsearch_run -X POST 'localhost:9200/order-bundles/_delete_by_query?refresh=true' \
        -d "{\"query\":{\"ids\":{\"values\":[$(json_list "$WORK/order_ids.txt")]}}}"
    echo
    elasticsearch_run -X POST 'localhost:9200/payment-bundles/_delete_by_query?refresh=true' \
        -d "{\"query\":{\"ids\":{\"values\":[$(json_list "$WORK/payment_ids.txt")]}}}"
    echo
    echo "ALTER TABLE report.realtime_business_by_order DELETE WHERE OrderId IN ($(sql_list "$WORK/order_ids.txt")) SETTINGS mutations_sync = 1" \
        | clickhouse_run
}

case "$MODE" in
    check)
        export_ids
        repair_postgres ROLLBACK
        count_downstream
        ;;
    apply)
        export_ids
        repair_postgres COMMIT
        delete_downstream
        count_downstream
        ;;
    *)
        echo "Usage: $0 {check|apply}" >&2
        exit 1
        ;;
esac
