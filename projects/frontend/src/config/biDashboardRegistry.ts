// AUTO-GENERATED — do not edit by hand
// Sinh tự động bởi: python -m superset_deploy export-registry

export type BiDashboardEntry = {
  slug: string;
  reportKey: string;
  title: string;
};

export const BI_DASHBOARDS: BiDashboardEntry[] = [
  { slug: "hieu-suat-nha-cung-cap", reportKey: "supply:supplier", title: "Hiệu suất nhà cung cấp" },
  { slug: "kinh-doanh-realtime-theo-don", reportKey: "kd:order", title: "Tình hình kinh doanh realtime theo đơn" },
  { slug: "muc-su-dung-cong-suat-cua-hang", reportKey: "supply:capacity", title: "Mức sử dụng công suất cửa hàng" },
  { slug: "quy-chi-tiet-don-hang", reportKey: "fund:order", title: "Chi tiết quỹ theo đơn hàng" },
  { slug: "quy-pttt-cua-hang", reportKey: "fund:payment", title: "Theo phương thức thanh toán và cửa hàng" },
  { slug: "quy-tong-hop-cua-hang", reportKey: "fund:shop", title: "Tổng hợp quỹ theo cửa hàng" },
  { slug: "suc-khoe-ton-kho-cua-hang-nganh-hang", reportKey: "supply:inventory", title: "Sức khỏe tồn kho theo cửa hàng và ngành hàng" },
  { slug: "tc-chi-tiet-don-hang", reportKey: "financial:order", title: "Chi tiết theo đơn hàng" },
  { slug: "tc-chi-tiet-san-pham-cua-hang", reportKey: "financial:detail", title: "Chi tiết theo sản phẩm và cửa hàng" },
  { slug: "tc-tong-hop-cua-hang", reportKey: "financial:shop", title: "Tổng hợp theo cửa hàng" },
  { slug: "tc-tong-hop-san-pham", reportKey: "financial:product", title: "Tổng hợp theo sản phẩm" },
  { slug: "the-diem-nha-van-chuyen", reportKey: "supply:carrier", title: "Thẻ điểm nhà vận chuyển" },
];

const SLUG_BY_REPORT_KEY = new Map<string, string>(
  BI_DASHBOARDS.map((d) => [d.reportKey, d.slug])
);

/** Resolve Superset slug from report view key / Lấy slug Superset từ report key */
export function supersetSlugForReportKey(key: string): string {
  const slug = SLUG_BY_REPORT_KEY.get(key);
  if (!slug) throw new Error(`No Superset dashboard for report key: ${key}`);
  return slug;
}

export function biDashboardTitle(slug: string): string | undefined {
  return BI_DASHBOARDS.find((d) => d.slug === slug)?.title;
}
