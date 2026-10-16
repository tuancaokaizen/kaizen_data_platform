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
  page?: number;
  pageSize?: number;
};

export async function fetchReport(
  category: ReportCategory,
  filter: ReportFilter
): Promise<ListResponse<ReportRow>> {
  const path =
    category === "financial"
      ? "/api/v1/reports/financial"
      : category === "fund"
        ? "/api/v1/reports/fund"
        : "/api/v1/reports/kd";
  const timeout = filter.pageSize === 0 ? 120_000 : 20_000;
  const response = await apiClient.get<ListResponse<ReportRow>>(path, {
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
