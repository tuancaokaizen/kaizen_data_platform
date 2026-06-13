# Schema Manager

Thư mục này quản lý toàn bộ các schema định nghĩa dữ liệu cho PostgreSQL (DB nguồn & giao dịch) và Elasticsearch (nơi lưu các payloads đã sink thành công).

## Cấu trúc thư mục

```text
schema_manager/
├── postgres_source/           # Định nghĩa cấu trúc bảng DDL PostgreSQL
│   ├── schema_postgres.sql    # Kịch bản DDL hợp nhất toàn bộ cấu trúc DB trong một transaction block
│   ├── public.sql             # Schema public (Geography)
│   ├── product.sql            # Schema product (Product, ProductSupplement, ProductMedicine)
│   ├── shop.sql               # Schema shop (Shop, Employee, EmployeeSalary)
│   ├── customer.sql           # Schema customer (Customer)
│   ├── promotion.sql          # Schema promotion (Promotion, Voucher, VoucherSeries, Gift)
│   ├── order.sql              # Schema order (Order, OrderItem, Return, ReturnItem)
│   └── payment.sql            # Schema payment (Payment, PaymentType, PaymentMethod, Transaction, OrderPayment, ReturnPayment)
│
└── elasticsearch/             # Định nghĩa Elasticsearch Index Mappings
    ├── geography.json         # Mapping index geography
    ├── products.json          # Mapping index products
    ├── product-supplements.json # Mapping index product-supplements
    ├── product-medicines.json # Mapping index product-medicines
    ├── shops.json             # Mapping index shops
    ├── employees.json         # Mapping index employees
    ├── employee-salaries.json # Mapping index employee-salaries
    ├── customers.json         # Mapping index customers
    ├── promotions.json        # Mapping index promotions
    ├── vouchers.json          # Mapping index vouchers
    ├── voucher-series.json    # Mapping index voucher-series
    ├── gifts.json             # Mapping index gifts
    ├── payment-types.json     # Mapping index payment-types
    ├── payment-methods.json   # Mapping index payment-methods
    ├── order-bundles.json     # Mapping index order-bundles (chứa nested OrderItems & Returns)
    └── payment-bundles.json   # Mapping index payment-bundles (chứa nested Transactions, OrderPayments & ReturnPayments)
```

---

## 1. PostgreSQL Source (postgres_source/)
Chứa các câu lệnh DDL để khởi tạo các schema (`public`, `product`, `shop`, `customer`, `promotion`, `order`, `payment`) và thiết lập ràng buộc toàn vẹn khóa ngoại (Foreign Key Constraints) phù hợp với mã nguồn Java model cùng các ràng buộc được áp dụng trong Flink stream.

* **Master Data & Reference Tables:** Sinh và nạp bởi Airflow (`generate_master_data.py`).
* **Transactional Tables:** Được cập nhật từ luồng Flink thông qua các Custom Transaction Sinks (`OrderBundlePgSink` và `PaymentBundlePgSink`).
* **Indexes (Chỉ mục tối ưu):** Bổ sung đầy đủ hệ thống Index tối ưu hóa cho các câu hỏi JOIN (khóa ngoại), bộ lọc khoảng thời gian, và tìm kiếm thông tin nhanh theo context hệ thống thực tế.
* **Unified Script (`schema_postgres.sql`):** Cho phép khởi tạo nhanh toàn bộ cấu trúc cơ sở dữ liệu Postgres trong một khối transaction block an toàn.

---

## 2. Elasticsearch Mappings (elasticsearch/)
Chứa cấu hình mapping chi tiết cho từng Elasticsearch Index tương ứng với các topic Kafka được đồng bộ bởi Flink (`MasterDataEsConsumerJob` & `TransactionEsConsumerJob`).

* **Nested Objects:** Các trường như `orderItems` và `transactions` được khai báo kiểu `"type": "nested"` nhằm giữ mối quan hệ chặt chẽ giữa tài liệu chính và tài liệu con trong Elasticsearch phục vụ cho việc truy vấn Full-Text Search tối ưu.
* **Keyword vs Text:**
  * Các trường định danh ID, trạng thái, mã code được khai báo là `"type": "keyword"` để tìm kiếm chính xác và tăng hiệu năng.
  * Các trường tên, địa chỉ được khai báo `"type": "text"` kèm sub-field `"keyword"` để hỗ trợ tìm kiếm mờ (fuzzy search) lẫn sắp xếp/aggregation.

---

## 3. Script Dọn dẹp Dữ liệu (`clean_data.sh`)

Nhằm hỗ trợ quá trình phát triển và kiểm thử chạy lại các kịch bản sinh dữ liệu từ đầu, hệ thống cung cấp script dọn sạch dữ liệu nhanh mà **không làm mất cấu trúc bảng hay index mappings**:

```bash
cd schema_manager
./clean_data.sh
```

### Chức năng chi tiết:
1. **PostgreSQL**: Thực hiện lệnh `TRUNCATE TABLE ... CASCADE` để dọn sạch toàn bộ 20 bảng dữ liệu thuộc mọi schema (`public`, `product`, `shop`, `customer`, `promotion`, `order`, `payment`) một cách an toàn và giữ nguyên cấu trúc bảng/khóa ngoại.
2. **Elasticsearch**: Khởi chạy một container curl tạm thời trong cluster để gửi yêu cầu `_delete_by_query` khớp `match_all` xóa sạch tài liệu (documents) trên mọi Index, đảm bảo giữ nguyên cấu hình mapping/settings của Elasticsearch phục vụ các lần chạy tiếp theo.
