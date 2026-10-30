import { supersetSlugForReportKey } from "./biDashboardRegistry";

export type ReportCategory = "financial" | "fund" | "kd" | "supply";

export type FinancialView = "shop" | "detail" | "product" | "order";
export type FundView = "payment" | "shop" | "order";
export type KdView = "order";
export type SupplyView = "inventory" | "capacity" | "carrier" | "supplier";
export type ReportViewKey =
  | `financial:${FinancialView}`
  | `fund:${FundView}`
  | `kd:${KdView}`
  | `supply:${SupplyView}`;

export type FilterField =
  | "dateRange"
  | "shopCode"
  | "productId"
  | "productLineCode"
  | "orderId"
  | "documentType"
  | "orderStatus"
  | "paymentMethodName"
  | "carrierId"
  | "supplierId";

export type ReportViewConfig = {
  key: ReportViewKey;
  category: ReportCategory;
  view: FinancialView | FundView | KdView | SupplyView;
  label: string;
  description: string;
  filters: FilterField[];
  /** Superset dashboard slug — omit when BI dashboard not ready yet / Bỏ qua nếu chưa có dashboard Superset */
  supersetSlug?: string;
};

/** Full Vietnamese labels for filter fields / Nhãn đầy đủ cho bộ lọc */
export const FILTER_LABELS: Record<FilterField, string> = {
  dateRange: "Khoảng ngày",
  shopCode: "Mã cửa hàng",
  productId: "Mã sản phẩm",
  productLineCode: "Mã ngành hàng",
  orderId: "Mã đơn hàng",
  documentType: "Loại chứng từ",
  orderStatus: "Trạng thái đơn hàng",
  paymentMethodName: "Phương thức thanh toán",
  carrierId: "Mã nhà vận chuyển",
  supplierId: "Mã nhà cung cấp"
};

export const FILTER_PLACEHOLDERS: Partial<Record<FilterField, string>> = {
  shopCode: "Chọn mã cửa hàng",
  productId: "Nhập mã sản phẩm",
  productLineCode: "Chọn mã ngành hàng",
  orderId: "Nhập mã đơn hàng",
  documentType: "Chọn loại chứng từ",
  orderStatus: "Chọn trạng thái đơn hàng",
  paymentMethodName: "Chọn phương thức thanh toán",
  carrierId: "Nhập mã nhà vận chuyển",
  supplierId: "Nhập mã nhà cung cấp"
};

export const MENU_GROUP_LABELS: Record<ReportCategory, string> = {
  financial: "Báo Cáo Tài Chính",
  fund: "Báo Cáo Quỹ",
  kd: "Báo Cáo Tình Hình Kinh Doanh Realtime",
  supply: "Báo Cáo Chuỗi Cung Ứng"
};

/** Product lines in master data / Ngành hàng trong dữ liệu nguồn */
export const PRODUCT_LINE_OPTIONS = [
  { label: "Thuốc", value: "medicine" },
  { label: "Thực phẩm chức năng", value: "supplement" }
] as const;

export function formatProductLine(value?: string): string {
  if (!value) return "-";
  const found = PRODUCT_LINE_OPTIONS.find((o) => o.value === value);
  return found?.label ?? value;
}

// Prefer registry helper; fall back safely if slug missing during partial deploys
// Ưu tiên registry; bỏ qua an toàn nếu slug chưa có khi deploy từng phần
function safeSlug(key: string): string | undefined {
  try {
    return supersetSlugForReportKey(key);
  } catch {
    return undefined;
  }
}

export const REPORT_VIEWS: ReportViewConfig[] = [
  {
    key: "financial:shop",
    category: "financial",
    view: "shop",
    label: "Tổng hợp theo cửa hàng",
    description:
      "Doanh thu, chi phí và lợi nhuận gộp theo từng ngày và từng cửa hàng",
    filters: ["dateRange", "shopCode"],
    supersetSlug: safeSlug("financial:shop")
  },
  {
    key: "financial:detail",
    category: "financial",
    view: "detail",
    label: "Chi tiết theo sản phẩm và cửa hàng",
    description:
      "Phân tích doanh thu và lợi nhuận theo sản phẩm, ngành hàng trong từng cửa hàng",
    filters: ["dateRange", "shopCode", "productId", "productLineCode"],
    supersetSlug: safeSlug("financial:detail")
  },
  {
    key: "financial:product",
    category: "financial",
    view: "product",
    label: "Tổng hợp theo sản phẩm",
    description: "Tổng hợp toàn hệ thống theo sản phẩm và ngành hàng",
    filters: ["dateRange", "productId", "productLineCode"],
    supersetSlug: safeSlug("financial:product")
  },
  {
    key: "financial:order",
    category: "financial",
    view: "order",
    label: "Chi tiết theo đơn hàng",
    description:
      "Chi tiết từng đơn bán hoặc trả hàng (dữ liệu nóng 7 ngày gần nhất)",
    filters: ["dateRange", "shopCode", "orderId", "documentType", "orderStatus"],
    supersetSlug: safeSlug("financial:order")
  },
  {
    key: "fund:payment",
    category: "fund",
    view: "payment",
    label: "Theo phương thức thanh toán và cửa hàng",
    description:
      "Thu, chi và dòng tiền ròng theo phương thức thanh toán và cửa hàng",
    filters: ["dateRange", "shopCode", "paymentMethodName"],
    supersetSlug: safeSlug("fund:payment")
  },
  {
    key: "fund:shop",
    category: "fund",
    view: "shop",
    label: "Tổng hợp quỹ theo cửa hàng",
    description: "Tổng thu, tổng chi và dòng tiền ròng theo từng cửa hàng",
    filters: ["dateRange", "shopCode"],
    supersetSlug: safeSlug("fund:shop")
  },
  {
    key: "fund:order",
    category: "fund",
    view: "order",
    label: "Chi tiết quỹ theo đơn hàng",
    description:
      "Thu chi theo từng đơn hàng và phương thức thanh toán (dữ liệu nóng 7 ngày gần nhất)",
    filters: ["dateRange", "shopCode", "orderId", "paymentMethodName"],
    supersetSlug: safeSlug("fund:order")
  },
  {
    key: "kd:order",
    category: "kd",
    view: "order",
    label: "Theo đơn hàng",
    description: "Doanh thu, thu chi và dòng tiền theo từng đơn bán hoặc trả hàng",
    filters: ["dateRange", "shopCode", "orderId", "documentType", "orderStatus"],
    supersetSlug: safeSlug("kd:order")
  },
  {
    key: "supply:inventory",
    category: "supply",
    view: "inventory",
    label: "Sức khỏe tồn kho theo cửa hàng và ngành hàng",
    description:
      "Tồn kho, tỷ lệ hết hàng, số ngày đủ hàng và tỷ lệ đáp ứng theo cửa hàng × ngành hàng",
    filters: ["dateRange", "shopCode", "productLineCode"],
    supersetSlug: safeSlug("supply:inventory")
  },
  {
    key: "supply:capacity",
    category: "supply",
    view: "capacity",
    label: "Mức sử dụng công suất cửa hàng",
    description:
      "Tỷ lệ sử dụng kho, slot nhận hàng và số mặt hàng vượt sức chứa theo cửa hàng",
    filters: ["dateRange", "shopCode"],
    supersetSlug: safeSlug("supply:capacity")
  },
  {
    key: "supply:carrier",
    category: "supply",
    view: "carrier",
    label: "Thẻ điểm nhà vận chuyển",
    description:
      "Tỷ lệ giao đúng hạn đủ hàng, thời gian trễ và chi phí vận chuyển theo nhà vận chuyển",
    filters: ["dateRange", "carrierId"],
    supersetSlug: safeSlug("supply:carrier")
  },
  {
    key: "supply:supplier",
    category: "supply",
    view: "supplier",
    label: "Hiệu suất nhà cung cấp",
    description:
      "Lead time thực tế so với cam kết, tỷ lệ đúng hạn và tỷ lệ đáp ứng theo nhà cung cấp",
    filters: ["dateRange", "supplierId"],
    supersetSlug: safeSlug("supply:supplier")
  }
];

export const DEFAULT_VIEW_KEY: ReportViewKey = "financial:shop";

export function getViewConfig(key: ReportViewKey): ReportViewConfig {
  return REPORT_VIEWS.find((v) => v.key === key) ?? REPORT_VIEWS[0];
}
