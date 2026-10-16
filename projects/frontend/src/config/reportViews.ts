import { supersetSlugForReportKey } from "./biDashboardRegistry";

export type ReportCategory = "financial" | "fund" | "kd";

export type FinancialView = "shop" | "detail" | "product" | "order";
export type FundView = "payment" | "shop" | "order";
export type KdView = "order";
export type ReportViewKey =
  | `financial:${FinancialView}`
  | `fund:${FundView}`
  | `kd:${KdView}`;

export type FilterField =
  | "dateRange"
  | "shopCode"
  | "productId"
  | "productLineCode"
  | "orderId"
  | "documentType"
  | "orderStatus"
  | "paymentMethodName";

export type ReportViewConfig = {
  key: ReportViewKey;
  category: ReportCategory;
  view: FinancialView | FundView | KdView;
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
  paymentMethodName: "Phương thức thanh toán"
};

export const FILTER_PLACEHOLDERS: Partial<Record<FilterField, string>> = {
  shopCode: "Chọn mã cửa hàng",
  productId: "Nhập mã sản phẩm",
  productLineCode: "Chọn mã ngành hàng",
  orderId: "Nhập mã đơn hàng",
  documentType: "Chọn loại chứng từ",
  orderStatus: "Chọn trạng thái đơn hàng",
  paymentMethodName: "Chọn phương thức thanh toán"
};

export const MENU_GROUP_LABELS: Record<ReportCategory, string> = {
  financial: "Báo Cáo Tài Chính",
  fund: "Báo Cáo Quỹ",
  kd: "Báo Cáo Tình Hình Kinh Doanh Realtime"
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

export const REPORT_VIEWS: ReportViewConfig[] = [
  {
    key: "financial:shop",
    category: "financial",
    view: "shop",
    label: "Tổng hợp theo cửa hàng",
    description:
      "Doanh thu, chi phí và lợi nhuận gộp theo từng ngày và từng cửa hàng",
    filters: ["dateRange", "shopCode"],
    supersetSlug: supersetSlugForReportKey("financial:shop")
  },
  {
    key: "financial:detail",
    category: "financial",
    view: "detail",
    label: "Chi tiết theo sản phẩm và cửa hàng",
    description:
      "Phân tích doanh thu và lợi nhuận theo sản phẩm, ngành hàng trong từng cửa hàng",
    filters: ["dateRange", "shopCode", "productId", "productLineCode"],
    supersetSlug: supersetSlugForReportKey("financial:detail")
  },
  {
    key: "financial:product",
    category: "financial",
    view: "product",
    label: "Tổng hợp theo sản phẩm",
    description:
      "Tổng hợp toàn hệ thống theo sản phẩm và ngành hàng",
    filters: ["dateRange", "productId", "productLineCode"],
    supersetSlug: supersetSlugForReportKey("financial:product")
  },
  {
    key: "financial:order",
    category: "financial",
    view: "order",
    label: "Chi tiết theo đơn hàng",
    description:
      "Chi tiết từng đơn bán hoặc trả hàng (dữ liệu nóng 7 ngày gần nhất)",
    filters: ["dateRange", "shopCode", "orderId", "documentType", "orderStatus"],
    supersetSlug: supersetSlugForReportKey("financial:order")
  },
  {
    key: "fund:payment",
    category: "fund",
    view: "payment",
    label: "Theo phương thức thanh toán và cửa hàng",
    description:
      "Thu, chi và dòng tiền ròng theo phương thức thanh toán và cửa hàng",
    filters: ["dateRange", "shopCode", "paymentMethodName"],
    supersetSlug: supersetSlugForReportKey("fund:payment")
  },
  {
    key: "fund:shop",
    category: "fund",
    view: "shop",
    label: "Tổng hợp quỹ theo cửa hàng",
    description:
      "Tổng thu, tổng chi và dòng tiền ròng theo từng cửa hàng",
    filters: ["dateRange", "shopCode"],
    supersetSlug: supersetSlugForReportKey("fund:shop")
  },
  {
    key: "fund:order",
    category: "fund",
    view: "order",
    label: "Chi tiết quỹ theo đơn hàng",
    description:
      "Thu chi theo từng đơn hàng và phương thức thanh toán (dữ liệu nóng 7 ngày gần nhất)",
    filters: ["dateRange", "shopCode", "orderId", "paymentMethodName"],
    supersetSlug: supersetSlugForReportKey("fund:order")
  },
  {
    key: "kd:order",
    category: "kd",
    view: "order",
    label: "Theo đơn hàng",
    description: "Doanh thu, thu chi và dòng tiền theo từng đơn bán hoặc trả hàng",
    filters: ["dateRange", "shopCode", "orderId", "documentType", "orderStatus"],
    supersetSlug: supersetSlugForReportKey("kd:order")
  }
];

export const DEFAULT_VIEW_KEY: ReportViewKey = "financial:shop";

export function getViewConfig(key: ReportViewKey): ReportViewConfig {
  return REPORT_VIEWS.find((v) => v.key === key) ?? REPORT_VIEWS[0];
}
