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
    case "kd:order":
      return [
        dateCol,
        { title: "Mã đơn hàng", dataIndex: "orderId", width: 140 },
        { title: "Mã phiếu trả hàng", dataIndex: "returnId", width: 140 },
        {
          title: "Loại chứng từ",
          dataIndex: "documentType",
          width: 120,
          render: (value: string) => formatDocumentType(value)
        },
        ...shopCols,
        { title: "Trạng thái đơn hàng", dataIndex: "orderStatus", width: 140 },
        {
          title: "Doanh thu bán",
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
        ...fundMetricColumns.slice(0, 3),
        {
          title: "Tóm tắt PTTT",
          dataIndex: "paymentMethodSummary",
          width: 220,
          ellipsis: true
        },
        ...fundMetricColumns.slice(3),
        {
          title: "Chờ đơn hàng",
          dataIndex: "pendingOrder",
          width: 110,
          render: (value?: number) => (value === 1 ? "Có" : value === 0 ? "Không" : "-")
        }
      ];
    case "supply:inventory":
      return [
        dateCol,
        ...shopCols,
        {
          title: "Mã ngành hàng",
          dataIndex: "productLineCode",
          width: 140,
          render: formatProductLine
        },
        {
          title: "Tên ngành hàng",
          dataIndex: "productLineName",
          width: 180,
          ellipsis: true
        },
        { title: "Số lượng tồn", dataIndex: "onHandQuantity", render: numCell, align: "right", width: 120 },
        { title: "Giá trị tồn", dataIndex: "onHandValue", render: numCell, align: "right", width: 130 },
        { title: "Thể tích tồn (m³)", dataIndex: "onHandVolumeM3", render: numCell, align: "right", width: 140 },
        { title: "Số lượng hết hạn", dataIndex: "expiredQuantity", render: numCell, align: "right", width: 130 },
        { title: "Số mặt hàng", dataIndex: "skuCount", render: numCell, align: "right", width: 110 },
        { title: "Số mặt hàng hết hàng", dataIndex: "stockoutSkuCount", render: numCell, align: "right", width: 160 },
        { title: "Tỷ lệ hết hàng", dataIndex: "stockoutRate", render: numCell, align: "right", width: 120 },
        {
          title: "Số ngày đủ hàng trung bình",
          dataIndex: "averageDaysOfSupply",
          render: numCell,
          align: "right",
          width: 180
        },
        { title: "Số lượng yêu cầu", dataIndex: "requestedQuantity", render: numCell, align: "right", width: 130 },
        { title: "Số lượng mất bán", dataIndex: "lostQuantity", render: numCell, align: "right", width: 130 },
        {
          title: "Doanh thu mất ước tính",
          dataIndex: "lostRevenueEstimate",
          render: numCell,
          align: "right",
          width: 160
        },
        { title: "Tỷ lệ đáp ứng", dataIndex: "fillRate", render: numCell, align: "right", width: 120 }
      ];
    case "supply:capacity":
      return [
        dateCol,
        ...shopCols,
        { title: "Thể tích tồn (m³)", dataIndex: "onHandVolumeM3", render: numCell, align: "right", width: 140 },
        { title: "Giá trị tồn", dataIndex: "onHandValue", render: numCell, align: "right", width: 130 },
        { title: "Sức chứa kho (m³)", dataIndex: "storageM3", render: numCell, align: "right", width: 140 },
        { title: "Sức chứa kho lạnh (lít)", dataIndex: "coldStorageL", render: numCell, align: "right", width: 160 },
        { title: "Số kệ hàng", dataIndex: "shelfSlots", render: numCell, align: "right", width: 110 },
        {
          title: "Slot nhận hàng mỗi ngày",
          dataIndex: "receivingSlotsPerDay",
          render: numCell,
          align: "right",
          width: 170
        },
        { title: "Số lượt nhận hàng", dataIndex: "receivingCount", render: numCell, align: "right", width: 140 },
        {
          title: "Số mặt hàng vượt sức chứa",
          dataIndex: "overCapacitySkuCount",
          render: numCell,
          align: "right",
          width: 180
        },
        { title: "Tỷ lệ sử dụng kho", dataIndex: "storageUtilization", render: numCell, align: "right", width: 140 },
        {
          title: "Tỷ lệ sử dụng slot nhận",
          dataIndex: "receivingUtilization",
          render: numCell,
          align: "right",
          width: 170
        }
      ];
    case "supply:carrier":
      return [
        dateCol,
        { title: "Mã nhà vận chuyển", dataIndex: "carrierId", width: 140 },
        { title: "Tên nhà vận chuyển", dataIndex: "carrierName", width: 200, ellipsis: true },
        { title: "Số chuyến hàng", dataIndex: "shipmentCount", render: numCell, align: "right", width: 120 },
        { title: "Số điểm giao", dataIndex: "stopCount", render: numCell, align: "right", width: 120 },
        { title: "Số điểm đã giao", dataIndex: "deliveredStopCount", render: numCell, align: "right", width: 130 },
        { title: "Số điểm đúng hạn", dataIndex: "onTimeStopCount", render: numCell, align: "right", width: 130 },
        {
          title: "Số điểm đúng hạn đủ hàng",
          dataIndex: "onTimeInFullStopCount",
          render: numCell,
          align: "right",
          width: 180
        },
        { title: "Tỷ lệ đúng hạn đủ hàng", dataIndex: "onTimeInFullRate", render: numCell, align: "right", width: 160 },
        {
          title: "Thời gian trễ trung bình (phút)",
          dataIndex: "averageDelayMinutes",
          render: numCell,
          align: "right",
          width: 200
        },
        { title: "Chi phí vận chuyển", dataIndex: "transportCost", render: numCell, align: "right", width: 140 },
        { title: "Tổng trọng lượng (kg)", dataIndex: "totalWeightKg", render: numCell, align: "right", width: 150 },
        { title: "Tổng thể tích (m³)", dataIndex: "totalVolumeM3", render: numCell, align: "right", width: 140 },
        { title: "Chi phí trên kg", dataIndex: "costPerKg", render: numCell, align: "right", width: 120 },
        { title: "Chi phí trên điểm giao", dataIndex: "costPerStop", render: numCell, align: "right", width: 160 },
        {
          title: "Hệ số chất tải trọng lượng",
          dataIndex: "averageWeightLoadFactor",
          render: numCell,
          align: "right",
          width: 180
        },
        {
          title: "Hệ số chất tải thể tích",
          dataIndex: "averageVolumeLoadFactor",
          render: numCell,
          align: "right",
          width: 170
        }
      ];
    case "supply:supplier":
      return [
        dateCol,
        { title: "Mã nhà cung cấp", dataIndex: "supplierId", width: 140 },
        { title: "Tên nhà cung cấp", dataIndex: "supplierName", width: 200, ellipsis: true },
        { title: "Số dòng đơn mua", dataIndex: "purchaseLineCount", render: numCell, align: "right", width: 130 },
        { title: "Số dòng đã nhận", dataIndex: "receivedLineCount", render: numCell, align: "right", width: 130 },
        { title: "Số dòng đúng hạn", dataIndex: "onTimeLineCount", render: numCell, align: "right", width: 130 },
        { title: "Tỷ lệ đúng hạn", dataIndex: "onTimeRate", render: numCell, align: "right", width: 120 },
        {
          title: "Lead time thực tế (ngày)",
          dataIndex: "averageLeadTimeDays",
          render: numCell,
          align: "right",
          width: 170
        },
        {
          title: "Lead time cam kết (ngày)",
          dataIndex: "averagePromisedLeadTimeDays",
          render: numCell,
          align: "right",
          width: 180
        },
        {
          title: "Chênh lệch lead time (ngày)",
          dataIndex: "leadTimeVarianceDays",
          render: numCell,
          align: "right",
          width: 190
        },
        {
          title: "Tỷ lệ đáp ứng trung bình",
          dataIndex: "averageFillRate",
          render: numCell,
          align: "right",
          width: 170
        },
        { title: "Số lượng đặt", dataIndex: "orderedQuantity", render: numCell, align: "right", width: 120 },
        { title: "Số lượng nhận", dataIndex: "receivedQuantity", render: numCell, align: "right", width: 120 },
        { title: "Giá trị đặt", dataIndex: "orderedAmount", render: numCell, align: "right", width: 120 },
        { title: "Giá trị nhận", dataIndex: "receivedAmount", render: numCell, align: "right", width: 120 }
      ];
    default:
      return [dateCol, ...shopCols, ...financialMetricColumns];
  }
}

export function rowKeyForView(viewKey: ReportViewKey, row: ReportRow, index: number): string {
  const parts = [row.reportDate, viewKey, String(index)];
  if (row.shopCode) parts.push(row.shopCode);
  if (row.productId) parts.push(row.productId);
  if (row.productLineCode) parts.push(row.productLineCode);
  if (row.orderId) parts.push(row.orderId);
  if (row.paymentMethodName) parts.push(row.paymentMethodName);
  if (row.paymentMethodSummary) parts.push(row.paymentMethodSummary);
  if (row.documentType) parts.push(row.documentType);
  if (row.carrierId) parts.push(row.carrierId);
  if (row.supplierId) parts.push(row.supplierId);
  return parts.join("-");
}

/** Human-readable document type in table cells / Hiển thị loại chứng từ đầy đủ */
export function formatDocumentType(value?: string): string {
  if (value === "SALE") return "Bán hàng";
  if (value === "RETURN") return "Trả hàng";
  return value ?? "-";
}
