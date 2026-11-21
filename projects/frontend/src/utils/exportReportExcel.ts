import type { ColumnsType, ColumnType } from "antd/es/table";
import type { ReportRow } from "../types/report";
import { formatDocumentType } from "../config/reportColumns";
import { formatProductLine } from "../config/reportViews";
import { formatReportDate } from "./formatDate";

export type ExcelColumn = {
  header: string;
  dataIndex: keyof ReportRow;
};

/** Build Excel columns from the same table column defs / Lấy cột Excel từ cùng định nghĩa bảng */
export function excelColumnsFromTable(columns: ColumnsType<ReportRow>): ExcelColumn[] {
  const result: ExcelColumn[] = [];
  for (const col of columns) {
    if (!isLeafColumn(col)) continue;
    const header = typeof col.title === "string" ? col.title.trim() : "";
    const dataIndex = normalizeDataIndex(col.dataIndex);
    if (!header || !dataIndex) continue;
    result.push({ header, dataIndex });
  }
  return result;
}

function isLeafColumn(col: ColumnsType<ReportRow>[number]): col is ColumnType<ReportRow> {
  return Boolean(col && !Array.isArray(col) && "dataIndex" in col && col.dataIndex != null);
}

function normalizeDataIndex(dataIndex: ColumnType<ReportRow>["dataIndex"]): keyof ReportRow | null {
  if (typeof dataIndex === "string") return dataIndex as keyof ReportRow;
  if (Array.isArray(dataIndex) && typeof dataIndex[0] === "string") {
    return dataIndex[0] as keyof ReportRow;
  }
  return null;
}

/** Format cell for Excel (dates/enums as text; keep numbers numeric) / Định dạng ô Excel */
export function formatExcelCell(dataIndex: keyof ReportRow, row: ReportRow): string | number | null {
  const value = row[dataIndex];
  if (dataIndex === "reportDate") {
    const formatted = formatReportDate(value as string | undefined);
    return formatted === "-" ? null : formatted;
  }
  if (dataIndex === "documentType") {
    const formatted = formatDocumentType(value as string | undefined);
    return formatted === "-" ? null : formatted;
  }
  if (dataIndex === "productLineCode" || dataIndex === "productLineName") {
    const formatted = formatProductLine(value as string | undefined);
    return formatted === "-" ? null : formatted;
  }
  if (dataIndex === "labelSource") {
    if (value === "model" || value === "Mô hình RF") return "Mô hình dự đoán";
    if (value === "rule" || value === "Luật RFM") return "Luật phân loại";
    return (value as string) || null;
  }
  if (value == null || value === "") return null;
  if (typeof value === "number" || typeof value === "boolean") return value as number;
  return String(value);
}

export type DownloadReportExcelInput = {
  fileName: string;
  sheetName: string;
  columns: ExcelColumn[];
  rows: ReportRow[];
};

/** Write .xlsx with bold header row and download in browser / Ghi xlsx header đậm rồi tải về */
export async function downloadReportExcel(input: DownloadReportExcelInput): Promise<void> {
  // Lazy-load ExcelJS only on export click / Chỉ nạp ExcelJS khi bấm xuất
  const ExcelJS = (await import("exceljs")).default;
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "Kaizen Report";
  const sheet = workbook.addWorksheet(sanitizeSheetName(input.sheetName));

  // Header template: bold + freeze / Template header: in đậm + đóng băng hàng 1
  const headerRow = sheet.addRow(input.columns.map((col) => col.header));
  headerRow.font = { bold: true };
  headerRow.alignment = { vertical: "middle", wrapText: true };
  sheet.views = [{ state: "frozen", ySplit: 1 }];

  for (const row of input.rows) {
    sheet.addRow(input.columns.map((col) => formatExcelCell(col.dataIndex, row)));
  }

  // Autosize within a sane cap / Auto độ rộng cột, giới hạn hợp lý
  input.columns.forEach((col, index) => {
    const lengths = [
      col.header.length,
      ...input.rows.slice(0, 200).map((row) => String(formatExcelCell(col.dataIndex, row) ?? "").length)
    ];
    const width = Math.min(48, Math.max(12, ...lengths) + 2);
    sheet.getColumn(index + 1).width = width;
  });

  const buffer = await workbook.xlsx.writeBuffer();
  triggerBrowserDownload(buffer, ensureXlsxExtension(input.fileName));
}

function sanitizeSheetName(name: string): string {
  const cleaned = name.replace(/[\\/*?:\[\]]/g, " ").trim() || "BaoCao";
  return cleaned.slice(0, 31);
}

function ensureXlsxExtension(fileName: string): string {
  return fileName.toLowerCase().endsWith(".xlsx") ? fileName : `${fileName}.xlsx`;
}

function triggerBrowserDownload(buffer: ArrayBuffer | Uint8Array | Blob, fileName: string): void {
  const blob =
    buffer instanceof Blob
      ? buffer
      : new Blob([buffer as BlobPart], {
          type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName;
  anchor.click();
  URL.revokeObjectURL(url);
}

/** Safe file stem from report title + date range / Tên file an toàn từ tiêu đề + khoảng ngày */
export function buildExportFileName(label: string, fromDate: string, toDate: string): string {
  const stem = label
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80);
  return `${stem || "BaoCao"}_${fromDate}_${toDate}.xlsx`;
}
