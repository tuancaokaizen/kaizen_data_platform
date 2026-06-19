import type { ColumnsType } from "antd/es/table";
import type { ReportRow } from "../types/report";
import { formatNumber } from "../utils/formatNumber";
import { formatReportDate } from "../utils/formatDate";
import type { ReportViewKey } from "./reportViews";
import { formatProductLine } from "./reportViews";

const numCell = (value?: number) => (value == null ? "-" : formatNumber(value));

const financialMetricColumns: ColumnsType<ReportRow> = [
  {
    title: "Số lượng bán",
    dataIndex: "salesQuantity",
    render: numCell,
    align: "right",
    width: 110
  },
  {
    title: "Số lượng trả",
    dataIndex: "returnQuantity",
    render: numCell,
    align: "right",
    width: 110
  },
  {
    title: "Doanh thu gộp",
    dataIndex: "grossRevenue",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Giá trị trả hàng",
    dataIndex: "returnAmount",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Doanh thu thuần",
    dataIndex: "netRevenue",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Tổng giá vốn",
    dataIndex: "totalCogs",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Chi phí khuyến mãi",
    dataIndex: "totalPromotionCost",
    render: numCell,
    align: "right",
    width: 140
  },
  {
    title: "Tổng chi phí",
    dataIndex: "totalCost",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Lợi nhuận gộp",
    dataIndex: "grossProfit",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Biên lợi nhuận",
    dataIndex: "profitMargin",
    render: numCell,
    align: "right",
    width: 110
  }
];

const fundMetricColumns: ColumnsType<ReportRow> = [
  {
    title: "Tổng thu",
    dataIndex: "totalReceipts",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Tổng chi",
    dataIndex: "totalExpenditures",
    render: numCell,
    align: "right",
    width: 130
  },
  {
    title: "Dòng tiền ròng",
    dataIndex: "netCashFlow",
    render: numCell,
    align: "right",
    width: 140
  },
  {
    title: "Tổng tiền đơn hàng",
    dataIndex: "totalOrderAmount",
    render: numCell,
    align: "right",
    width: 150
  },
  {
    title: "Tổng tiền hoàn trả",
    dataIndex: "totalReturnAmount",
    render: numCell,
    align: "right",
    width: 150
  },
  {
    title: "Chênh lệch đối soát",
    dataIndex: "reconciliationDiff",
    render: numCell,
    align: "right",
    width: 150
  }
];

export function getColumnsForView(viewKey: ReportViewKey): ColumnsType<ReportRow> {
  const dateCol = {
    title: "Ngày báo cáo",
    dataIndex: "reportDate",
    width: 120,
    fixed: "left" as const,
    render: (value: string) => formatReportDate(value)
  };
  const shopCols: ColumnsType<ReportRow> = [
    { title: "Mã cửa hàng", dataIndex: "shopCode", width: 110 },
    { title: "Tên cửa hàng", dataIndex: "shopName", width: 180, ellipsis: true }
  ];
  const productCols: ColumnsType<ReportRow> = [
    { title: "Mã sản phẩm", dataIndex: "productId", width: 120 },
    { title: "Tên sản phẩm", dataIndex: "productName", width: 200, ellipsis: true },
    { title: "Mã ngành hàng", dataIndex: "productLineCode", width: 140, render: formatProductLine },
    { title: "Tên ngành hàng", dataIndex: "productLineName", width: 180, ellipsis: true, render: formatProductLine },
  ];
  const orderCols: ColumnsType<ReportRow> = [
    {
      title: "Loại chứng từ",
      dataIndex: "documentType",
      width: 120,
      render: (value: string) => formatDocumentType(value)
    },
    { title: "Mã đơn hàng", dataIndex: "orderId", width: 140 },
    { title: "Mã phiếu trả hàng", dataIndex: "returnId", width: 140 },
    { title: "Mã nhân viên bán", dataIndex: "employeeSaleId", width: 140 },
    { title: "Trạng thái đơn hàng", dataIndex: "orderStatus", width: 140 }
  ];
  const paymentCol = {
    title: "Phương thức thanh toán",
    dataIndex: "paymentMethodName",
    width: 180,
    ellipsis: true
  };

  switch (viewKey) {
    case "financial:shop":
      return [dateCol, ...shopCols, ...financialMetricColumns];
    case "financial:detail":
      return [dateCol, ...shopCols, ...productCols, ...financialMetricColumns];
    case "financial:product":
      return [dateCol, ...productCols, ...financialMetricColumns];
    case "financial:order":
      return [dateCol, ...shopCols, ...orderCols, ...financialMetricColumns];
    case "fund:payment":
      return [dateCol, ...shopCols, paymentCol, ...fundMetricColumns];
    case "fund:shop":
      return [dateCol, ...shopCols, ...fundMetricColumns];
    case "fund:order":
      return [
        dateCol,
        { title: "Mã đơn hàng", dataIndex: "orderId", width: 140 },
        { title: "Mã phiếu trả hàng", dataIndex: "returnId", width: 140 },
        ...shopCols,
        paymentCol,
        ...fundMetricColumns
      ];
    default:
      return [dateCol, ...shopCols, ...financialMetricColumns];
  }
}

export function rowKeyForView(viewKey: ReportViewKey, row: ReportRow, index: number): string {
  const parts = [row.reportDate, viewKey, String(index)];
  if (row.shopCode) parts.push(row.shopCode);
  if (row.productId) parts.push(row.productId);
  if (row.orderId) parts.push(row.orderId);
  if (row.paymentMethodName) parts.push(row.paymentMethodName);
  if (row.documentType) parts.push(row.documentType);
  return parts.join("-");
}

/** Human-readable document type in table cells / Hiển thị loại chứng từ đầy đủ */
export function formatDocumentType(value?: string): string {
  if (value === "SALE") return "Bán hàng";
  if (value === "RETURN") return "Trả hàng";
  return value ?? "-";
}
