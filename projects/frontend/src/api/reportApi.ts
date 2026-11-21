import axios from "axios";
import type { ReportCategory } from "../config/reportViews";
import type { ListResponse, ReportRow } from "../types/report";

const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "",
  timeout: 20000
});

export type ReportFilter = {
  fromDate: string;
  toDate: string;
  view?: string;
  shopCode?: string;
  productId?: string;
  productLineCode?: string;
  orderId?: string;
  documentType?: string;
  orderStatus?: string;
  paymentMethodName?: string;
  carrierId?: string;
  supplierId?: string;
  segmentCode?: string;
  clusterCode?: string;
  page?: number;
  pageSize?: number;
};

function reportPath(category: ReportCategory): string {
  switch (category) {
    case "financial":
      return "/api/v1/reports/financial";
    case "fund":
      return "/api/v1/reports/fund";
    case "kd":
      return "/api/v1/reports/kd";
    case "supply":
      return "/api/v1/reports/supply";
    case "analytics":
      return "/api/v1/reports/analytics";
    default:
      return "/api/v1/reports/financial";
  }
}

export async function fetchReport(
  category: ReportCategory,
  filter: ReportFilter,
  _opts?: { biOnly?: boolean }
): Promise<ListResponse<ReportRow>> {
  const timeout = filter.pageSize === 0 ? 120_000 : 20_000;
  const response = await apiClient.get<ListResponse<ReportRow>>(reportPath(category), {
    params: filter,
    timeout
  });
  return response.data;
}

export type FinancialDetailFilterOptions = {
  view: string;
  shops: { shopCode: string; shopName: string }[];
  products: { productId: string; productName: string }[];
};

/** Distinct shop/product values for financial detail view (not paginated) */
export async function fetchFinancialDetailFilterOptions(
  fromDate: string,
  toDate: string
): Promise<FinancialDetailFilterOptions> {
  const response = await apiClient.get<FinancialDetailFilterOptions>(
    "/api/v1/reports/financial/filter-options",
    {
      params: { view: "detail", fromDate, toDate }
    }
  );
  return response.data;
}

export type KdFilterOptions = {
  view: string;
  shops: { shopCode: string; shopName: string }[];
  orderStatuses: string[];
};

/** Distinct shop values for KD realtime view / Cửa hàng distinct cho KD realtime */
export async function fetchKdFilterOptions(
  fromDate: string,
  toDate: string
): Promise<KdFilterOptions> {
  const response = await apiClient.get<KdFilterOptions>("/api/v1/reports/kd/filter-options", {
    params: { fromDate, toDate }
  });
  return response.data;
}

export type AnalyticsFilterOptions = {
  view: string;
  segments: { segmentCode: string; segmentName: string }[];
  clusters: { clusterCode: string; clusterName: string }[];
};

/** Distinct segment/cluster values for analytics views / Phân khúc & nhóm cho analytics */
export async function fetchAnalyticsFilterOptions(
  fromDate: string,
  toDate: string,
  view: string
): Promise<AnalyticsFilterOptions> {
  const response = await apiClient.get<AnalyticsFilterOptions>(
    "/api/v1/reports/analytics/filter-options",
    { params: { fromDate, toDate, view } }
  );
  return response.data;
}

export type SupplyFilterOptions = {
  view: string;
  shops: { shopCode: string; shopName: string }[];
  products: { productId: string; productName: string }[];
};

/** Distinct shop/product values for supply views / Cửa hàng & sản phẩm cho chuỗi cung ứng */
export async function fetchSupplyFilterOptions(
  fromDate: string,
  toDate: string,
  view: string
): Promise<SupplyFilterOptions> {
  const response = await apiClient.get<SupplyFilterOptions>(
    "/api/v1/reports/supply/filter-options",
    { params: { fromDate, toDate, view } }
  );
  return response.data;
}
