import axios from "axios";
import type { FinancialRow, FundRow, ListResponse } from "../types/report";

const apiClient = axios.create({
  // Empty baseURL uses Vite proxy in dev; set VITE_API_BASE_URL for direct backend URL
  // baseURL rỗng dùng Vite proxy khi dev; set VITE_API_BASE_URL để gọi thẳng backend
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "",
  timeout: 20000
});

export type ReportFilter = {
  fromDate: string;
  toDate: string;
  shopCode?: string;
  productCode?: string;
  page?: number;
  pageSize?: number;
};

export async function fetchFinancialReport(filter: ReportFilter) {
  const response = await apiClient.get<ListResponse<FinancialRow>>(
    "/api/v1/reports/financial",
    {
      params: filter
    }
  );
  return response.data;
}

export async function fetchFundReport(filter: ReportFilter) {
  const response = await apiClient.get<ListResponse<FundRow>>(
    "/api/v1/reports/fund",
    {
      params: filter
    }
  );
  return response.data;
}
