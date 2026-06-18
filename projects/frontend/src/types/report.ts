export type FinancialRow = {
  reportDate: string;
  shopCode: string;
  shopName: string;
  salesQuantity: number;
  returnQuantity: number;
  grossRevenue: number;
  returnAmount: number;
  netRevenue: number;
  totalCogs: number;
  totalPromotionCost: number;
  totalCost: number;
  grossProfit: number;
  profitMargin: number;
};

export type FundRow = {
  reportDate: string;
  shopCode: string;
  shopName: string;
  paymentMethodName: string;
  totalReceipts: number;
  totalExpenditures: number;
  netCashFlow: number;
  totalOrderAmount: number;
  totalReturnAmount: number;
  reconciliationDiff: number;
};

export type Pagination = {
  page: number;
  pageSize: number;
  total: number;
};

export type ListResponse<T> = {
  data: T[];
  pagination: Pagination;
};
