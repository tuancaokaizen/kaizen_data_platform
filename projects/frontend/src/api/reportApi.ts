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
    category === "financial" ? "/api/v1/reports/financial" : "/api/v1/reports/fund";
  const timeout = filter.pageSize === 0 ? 120_000 : 20_000;
  const response = await apiClient.get<ListResponse<ReportRow>>(path, {
    params: filter,
    timeout
  });
  return response.data;
}
