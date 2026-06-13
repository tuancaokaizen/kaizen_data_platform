#!/bin/bash
# clean_data.sh
# Script dọn dẹp sạch dữ liệu trên PostgreSQL và Elasticsearch (giữ nguyên cấu trúc/mappings)
set -euo pipefail

# Get directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Prioritize K3S_CONF or config.k3s-new over standard KUBECONFIG
if [ -n "${K3S_CONF:-}" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$DIR/../dags/config.k3s-new" ]; then
    export KUBECONFIG="$DIR/../dags/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

DB_NAMESPACE="databases"
ES_NAMESPACE="logging"
ES_SERVICE="http://elasticsearch.logging.svc.cluster.local:9200"

echo "=== 1. Dọn dẹp dữ liệu PostgreSQL ==="
echo "Đang tìm Pod Postgres..."
POSTGRES_POD=$(kubectl get pods -n "$DB_NAMESPACE" -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')
echo "Đã tìm thấy Pod: $POSTGRES_POD"

echo "Đang thực thi TRUNCATE các bảng trên Postgres..."
kubectl exec -i -n "$DB_NAMESPACE" "$POSTGRES_POD" -- env PGPASSWORD=datateam123 psql -U postgres -d information_system -c "
  TRUNCATE TABLE 
    payment.\"OrderPayment\",
    payment.\"ReturnPayment\",
    payment.\"Transaction\",
    payment.\"Payment\",
    \"order\".\"ReturnItem\",
    \"order\".\"Return\",
    \"order\".\"OrderItem\",
    \"order\".\"Order\",
    promotion.\"Gift\",
    promotion.\"VoucherSeries\",
    promotion.\"Voucher\",
    promotion.\"Promotion\",
    customer.\"Customer\",
    shop.\"EmployeeSalary\",
    shop.\"Employee\",
    shop.\"Shop\",
    product.\"ProductSupplement\",
    product.\"ProductMedicine\",
    product.\"Product\",
    public.\"Geography\"
  CASCADE;
"
echo "✔ Đã dọn sạch các bảng dữ liệu trên PostgreSQL."
echo ""

echo "=== 2. Dọn dẹp dữ liệu Elasticsearch ==="
echo "Đang tạo Pod tạm để gửi yêu cầu xóa tài liệu trên Elasticsearch..."

# Sử dụng _delete_by_query để xóa sạch documents của mọi index mà không làm mất mappings/settings
kubectl run curl-temp-clean -n "$ES_NAMESPACE" --image=curlimages/curl:8.5.0 --rm -i --restart=Never -- \
  curl -s -X POST "$ES_SERVICE/*/_delete_by_query?conflicts=proceed&pretty" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match_all": {}}}'

echo ""
echo "✔ Đã dọn sạch tài liệu trên mọi Index Elasticsearch."
echo ""
echo "=== Hoàn tất dọn dẹp dữ liệu! ==="
