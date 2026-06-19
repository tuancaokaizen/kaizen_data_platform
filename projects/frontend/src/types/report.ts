export type ReportRow = {
  reportDate: string;
  shopCode?: string;
  shopName?: string;
  productId?: string;
  productName?: string;
  productLineCode?: string;
  productLineName?: string;
  documentType?: string;
  orderId?: string;
  returnId?: string;
  employeeSaleId?: string;
  orderStatus?: string;
  paymentMethodName?: string;
  salesQuantity?: number;
  returnQuantity?: number;
  grossRevenue?: number;
  returnAmount?: number;
  netRevenue?: number;
  totalCogs?: number;
  totalPromotionCost?: number;
  totalCost?: number;
  grossProfit?: number;
  profitMargin?: number;
  totalReceipts?: number;
  totalExpenditures?: number;
  netCashFlow?: number;
  totalOrderAmount?: number;
  totalReturnAmount?: number;
  reconciliationDiff?: number;
};

export type Pagination = {
  page: number;
  pageSize: number;
  total: number;
};

export type ListResponse<T> = {
  data: T[];
  view?: string;
  pagination: Pagination;
};
