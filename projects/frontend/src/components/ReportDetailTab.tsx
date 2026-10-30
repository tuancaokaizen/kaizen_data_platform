import {
  Button,
  Card,
  Col,
  DatePicker,
  Form,
  Input,
  Row,
  Select,
  Statistic,
  Table
} from "antd";
import type { FormInstance } from "antd/es/form";
import type { TablePaginationConfig } from "antd/es/table";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import {
  FILTER_LABELS,
  FILTER_PLACEHOLDERS,
  PRODUCT_LINE_OPTIONS,
  type FilterField,
  type ReportViewConfig
} from "../config/reportViews";
import type { ListResponse, ReportRow } from "../types/report";
import { formatNumber } from "../utils/formatNumber";

const { RangePicker } = DatePicker;

const DOCUMENT_TYPE_OPTIONS = [
  { label: "Bán hàng", value: "SALE" },
  { label: "Trả hàng", value: "RETURN" }
];

export type FilterFormValues = {
  dateRange?: [dayjs.Dayjs, dayjs.Dayjs];
  shopCode?: string;
  productId?: string;
  productLineCode?: string;
  orderId?: string;
  documentType?: string;
  orderStatus?: string;
  paymentMethodName?: string;
  carrierId?: string;
  supplierId?: string;
};

type ReportDetailTabProps = {
  viewConfig: ReportViewConfig;
  form: FormInstance<FilterFormValues>;
  loading: boolean;
  shopOptions: { label: string; value: string }[];
  productOptions: { label: string; value: string }[];
  paymentOptions: { label: string; value: string }[];
  orderStatusOptions: { label: string; value: string }[];
  showFilter: (field: FilterField) => boolean;
  onSearch: () => void;
  summary: {
    netRevenue: number;
    grossProfit: number;
    totalReceipts: number;
    netCashFlow: number;
    reconciliationDiff?: number;
    pendingOrderCount?: number;
    onHandValue?: number;
    fillRate?: number;
    stockoutRate?: number;
    storageUtilization?: number;
    receivingUtilization?: number;
    onTimeInFullRate?: number;
    transportCost?: number;
    onTimeRate?: number;
    receivedAmount?: number;
  };
  columns: ColumnsType<ReportRow>;
  reportData?: ListResponse<ReportRow>;
  paginationConfig: TablePaginationConfig;
  rowKey: (row: ReportRow, index?: number) => string;
};

const filterSelectOption = (input: string, option?: { label?: string; value?: string }) =>
  (option?.label ?? "").toLowerCase().includes(input.toLowerCase()) ||
  String(option?.value ?? "")
    .toLowerCase()
    .includes(input.toLowerCase());

export function ReportDetailTab({
  viewConfig,
  form,
  loading,
  shopOptions,
  productOptions,
  paymentOptions,
  orderStatusOptions,
  showFilter,
  onSearch,
  summary,
  columns,
  reportData,
  paginationConfig,
  rowKey
}: ReportDetailTabProps) {
  return (
    <>
      <Card title="Bộ lọc" size="small">
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            dateRange: [dayjs().subtract(30, "day"), dayjs()]
          }}
        >
          <Row gutter={[12, 0]}>
            {showFilter("dateRange") && (
              <Col xs={24} sm={12} lg={6}>
                <Form.Item label={FILTER_LABELS.dateRange} name="dateRange">
                  <RangePicker className="full-width" />
                </Form.Item>
              </Col>
            )}
            {showFilter("shopCode") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.shopCode} name="shopCode">
                  <Select
                    allowClear
                    showSearch
                    options={shopOptions}
                    placeholder={FILTER_PLACEHOLDERS.shopCode}
                    optionFilterProp="label"
                    filterOption={filterSelectOption}
                  />
                </Form.Item>
              </Col>
            )}
            {showFilter("productId") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.productId} name="productId">
                  <Select
                    allowClear
                    showSearch
                    options={productOptions}
                    placeholder={FILTER_PLACEHOLDERS.productId}
                  />
                </Form.Item>
              </Col>
            )}
            {showFilter("productLineCode") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.productLineCode} name="productLineCode">
                  <Select
                    allowClear
                    showSearch
                    options={[...PRODUCT_LINE_OPTIONS]}
                    placeholder={FILTER_PLACEHOLDERS.productLineCode}
                  />
                </Form.Item>
              </Col>
            )}
            {showFilter("orderId") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.orderId} name="orderId">
                  <Input placeholder={FILTER_PLACEHOLDERS.orderId} allowClear />
                </Form.Item>
              </Col>
            )}
            {showFilter("documentType") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.documentType} name="documentType">
                  <Select
                    allowClear
                    options={DOCUMENT_TYPE_OPTIONS}
                    placeholder={FILTER_PLACEHOLDERS.documentType}
                  />
                </Form.Item>
              </Col>
            )}
            {showFilter("orderStatus") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.orderStatus} name="orderStatus">
                  <Select
                    allowClear
                    showSearch
                    options={orderStatusOptions}
                    placeholder={FILTER_PLACEHOLDERS.orderStatus}
                    optionFilterProp="label"
                    filterOption={filterSelectOption}
                  />
                </Form.Item>
              </Col>
            )}
            {showFilter("paymentMethodName") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.paymentMethodName} name="paymentMethodName">
                  <Select
                    allowClear
                    showSearch
                    options={paymentOptions}
                    placeholder={FILTER_PLACEHOLDERS.paymentMethodName}
                  />
                </Form.Item>
              </Col>
            )}
            {showFilter("carrierId") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.carrierId} name="carrierId">
                  <Input placeholder={FILTER_PLACEHOLDERS.carrierId} allowClear />
                </Form.Item>
              </Col>
            )}
            {showFilter("supplierId") && (
              <Col xs={24} sm={12} lg={4}>
                <Form.Item label={FILTER_LABELS.supplierId} name="supplierId">
                  <Input placeholder={FILTER_PLACEHOLDERS.supplierId} allowClear />
                </Form.Item>
              </Col>
            )}
            <Col xs={24} sm={12} lg={3} className="search-col">
              <Button type="primary" onClick={onSearch} loading={loading} block>
                Áp dụng
              </Button>
            </Col>
          </Row>
        </Form>
      </Card>

      <Row gutter={[12, 12]} className="stats-row">
        {viewConfig.category === "financial" ? (
          <>
            <Col xs={24} sm={12}>
              <Card size="small">
                <Statistic
                  title="Doanh thu thuần (trang hiện tại)"
                  value={summary.netRevenue}
                  formatter={(v) => formatNumber(v as number)}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12}>
              <Card size="small">
                <Statistic
                  title="Lợi nhuận gộp (trang hiện tại)"
                  value={summary.grossProfit}
                  formatter={(v) => formatNumber(v as number)}
                />
              </Card>
            </Col>
          </>
        ) : viewConfig.category === "kd" ? (
          <>
            <Col xs={24} sm={12} lg={6}>
              <Card size="small">
                <Statistic
                  title="Doanh thu thuần (trang hiện tại)"
                  value={summary.netRevenue}
                  formatter={(v) => formatNumber(v as number)}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card size="small">
                <Statistic
                  title="Dòng tiền ròng (trang hiện tại)"
                  value={summary.netCashFlow}
                  formatter={(v) => formatNumber(v as number)}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card size="small">
                <Statistic
                  title="Chênh lệch đối soát (trang hiện tại)"
                  value={summary.reconciliationDiff ?? 0}
                  formatter={(v) => formatNumber(v as number)}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12} lg={6}>
              <Card size="small">
                <Statistic
                  title="Đơn chờ xử lý (trang hiện tại)"
                  value={summary.pendingOrderCount ?? 0}
                />
              </Card>
            </Col>
          </>
        ) : viewConfig.category === "supply" ? (
          <>
            {viewConfig.view === "inventory" && (
              <>
                <Col xs={24} sm={12} lg={8}>
                  <Card size="small">
                    <Statistic
                      title="Giá trị tồn (trang hiện tại)"
                      value={summary.onHandValue ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
                <Col xs={24} sm={12} lg={8}>
                  <Card size="small">
                    <Statistic
                      title="Tỷ lệ hết hàng TB (trang hiện tại)"
                      value={summary.stockoutRate ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
                <Col xs={24} sm={12} lg={8}>
                  <Card size="small">
                    <Statistic
                      title="Tỷ lệ đáp ứng TB (trang hiện tại)"
                      value={summary.fillRate ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
              </>
            )}
            {viewConfig.view === "capacity" && (
              <>
                <Col xs={24} sm={12}>
                  <Card size="small">
                    <Statistic
                      title="Tỷ lệ sử dụng kho TB (trang hiện tại)"
                      value={summary.storageUtilization ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
                <Col xs={24} sm={12}>
                  <Card size="small">
                    <Statistic
                      title="Tỷ lệ sử dụng slot nhận TB (trang hiện tại)"
                      value={summary.receivingUtilization ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
              </>
            )}
            {viewConfig.view === "carrier" && (
              <>
                <Col xs={24} sm={12}>
                  <Card size="small">
                    <Statistic
                      title="Tỷ lệ đúng hạn đủ hàng TB (trang hiện tại)"
                      value={summary.onTimeInFullRate ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
                <Col xs={24} sm={12}>
                  <Card size="small">
                    <Statistic
                      title="Chi phí vận chuyển (trang hiện tại)"
                      value={summary.transportCost ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
              </>
            )}
            {viewConfig.view === "supplier" && (
              <>
                <Col xs={24} sm={12}>
                  <Card size="small">
                    <Statistic
                      title="Tỷ lệ đúng hạn TB (trang hiện tại)"
                      value={summary.onTimeRate ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
                <Col xs={24} sm={12}>
                  <Card size="small">
                    <Statistic
                      title="Giá trị nhận (trang hiện tại)"
                      value={summary.receivedAmount ?? 0}
                      formatter={(v) => formatNumber(v as number)}
                    />
                  </Card>
                </Col>
              </>
            )}
          </>
        ) : (
          <>
            <Col xs={24} sm={12}>
              <Card size="small">
                <Statistic
                  title="Tổng thu (trang hiện tại)"
                  value={summary.totalReceipts}
                  formatter={(v) => formatNumber(v as number)}
                />
              </Card>
            </Col>
            <Col xs={24} sm={12}>
              <Card size="small">
                <Statistic
                  title="Dòng tiền ròng (trang hiện tại)"
                  value={summary.netCashFlow}
                  formatter={(v) => formatNumber(v as number)}
                />
              </Card>
            </Col>
          </>
        )}
      </Row>

      <Card size="small" className="table-card">
        <Table
          rowKey={(row, index) => rowKey(row, index)}
          columns={columns}
          dataSource={reportData?.data ?? []}
          loading={loading}
          pagination={paginationConfig}
          scroll={{ x: "max-content" }}
          size="small"
        />
      </Card>
    </>
  );
}
