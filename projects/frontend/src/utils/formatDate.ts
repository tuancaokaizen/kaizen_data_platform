import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";

dayjs.extend(utc);

/** Format report date for table cells / Định dạng ngày báo cáo trên bảng */
export function formatReportDate(value?: string | null): string {
  if (!value) return "-";
  const parsed = dayjs.utc(value);
  if (!parsed.isValid()) return value;
  return parsed.format("DD/MM/YYYY");
}
