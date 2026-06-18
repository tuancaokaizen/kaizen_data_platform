import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  App as AntdApp,
  Button,
  Card,
  Col,
  DatePicker,
  Form,
  Input,
  Layout,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tabs,
  Typography
} from "antd";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import axios from "axios";
import { fetchFinancialReport, fetchFundReport, type ReportFilter } from "./api/reportApi";
import type { FinancialRow, FundRow, ListResponse } from "./types/report";
import { formatNumber } from "./utils/formatNumber";

const { Header, Content } = Layout;
const { Title, Text } = Typography;
const { RangePicker } = DatePicker;

// Table cell renderer for numeric columns / Render ô số trong bảng
const numCell = (value: number) => formatNumber(value);

function App() {
  const { message } = AntdApp.useApp();
  const [loading, setLoading] = useState(false);
  const [financialData, setFinancialData] = useState<ListResponse<FinancialRow>>();
  const [fundData, setFundData] = useState<ListResponse<FundRow>>();
  const [activeTab, setActiveTab] = useState("financial");
  const [form] = Form.useForm();

  const shopOptions = useMemo(() => {
    const sourceRows = activeTab === "financial" ? financialData?.data : fundData?.data;
    const unique = new Set((sourceRows ?? []).map((row) => row.shopCode));
    return Array.from(unique).map((shopCode) => ({ label: shopCode, value: shopCode }));
  }, [activeTab, financialData?.data, fundData?.data]);

  const financialColumns: ColumnsType<FinancialRow> = [
    { title: "Ngày", dataIndex: "reportDate" },
    { title: "Mã CH", dataIndex: "shopCode" },
    { title: "Tên cửa hàng", dataIndex: "shopName" },
    { title: "SL bán", dataIndex: "salesQuantity", render: numCell, align: "right" },
    { title: "SL trả", dataIndex: "returnQuantity", render: numCell, align: "right" },
    { title: "Doanh thu thuần", dataIndex: "netRevenue", render: numCell, align: "right" },
    { title: "Lợi nhuận gộp", dataIndex: "grossProfit", render: numCell, align: "right" }
  ];

  const fundColumns: ColumnsType<FundRow> = [
    { title: "Ngày", dataIndex: "reportDate" },
    { title: "Mã CH", dataIndex: "shopCode" },
    { title: "Tên cửa hàng", dataIndex: "shopName" },
    { title: "PTTT", dataIndex: "paymentMethodName" },
    { title: "Tổng thu", dataIndex: "totalReceipts", render: numCell, align: "right" },
    { title: "Tổng chi", dataIndex: "totalExpenditures", render: numCell, align: "right" },
    { title: "Dòng tiền ròng", dataIndex: "netCashFlow", render: numCell, align: "right" }
  ];

  // Convert filter form values into backend query params / Chuyển giá trị filter thành query params cho backend
  const buildFilter = (values: {
    dateRange?: [dayjs.Dayjs, dayjs.Dayjs];
    shopCode?: string;
    productCode?: string;
  }): ReportFilter => {
    const fromDate = (values.dateRange?.[0] ?? dayjs().subtract(30, "day")).format("YYYY-MM-DD");
    const toDate = (values.dateRange?.[1] ?? dayjs()).format("YYYY-MM-DD");

    return {
      fromDate,
      toDate,
      shopCode: values.shopCode || undefined,
      productCode: values.productCode || undefined,
      page: 1,
      pageSize: 100
    };
  };

  // Load report data for a tab / Tải dữ liệu theo tab
  const loadReportForTab = async (
    tab: string,
    values?: {
      dateRange?: [dayjs.Dayjs, dayjs.Dayjs];
      shopCode?: string;
      productCode?: string;
    }
  ) => {
    const filter = buildFilter(values ?? form.getFieldsValue());
    if (tab === "financial") {
      const data = await fetchFinancialReport(filter);
      setFinancialData(data);
      return;
    }
    const data = await fetchFundReport(filter);
    setFundData(data);
  };

  const handleSearch = async () => {
    try {
      setLoading(true);
      await loadReportForTab(activeTab);
    } catch (error) {
      let detail = "Không tải được dữ liệu report từ backend.";
      if (axios.isAxiosError(error)) {
        const apiError = error.response?.data as { error?: string } | undefined;
        if (apiError?.error) {
          detail = apiError.error;
        } else if (error.message) {
          detail = error.message;
        }
      }
      message.error(detail);
    } finally {
      setLoading(false);
    }
  };

  const handleTabChange = (tab: string) => {
    setActiveTab(tab);
    void (async () => {
      try {
        setLoading(true);
        await loadReportForTab(tab);
      } catch (error) {
        let detail = "Không tải được dữ liệu report từ backend.";
        if (axios.isAxiosError(error)) {
          const apiError = error.response?.data as { error?: string } | undefined;
          if (apiError?.error) {
            detail = apiError.error;
          } else if (error.message) {
            detail = error.message;
          }
        }
        message.error(detail);
      } finally {
        setLoading(false);
      }
    })();
  };

  // Auto-load both reports on first render / Tự tải cả 2 báo cáo khi mở trang
  useEffect(() => {
    const initialFilter = {
      dateRange: [dayjs().subtract(30, "day"), dayjs()] as [dayjs.Dayjs, dayjs.Dayjs]
    };
    void (async () => {
      try {
        setLoading(true);
        await Promise.all([
          loadReportForTab("financial", initialFilter),
          loadReportForTab("fund", initialFilter)
        ]);
      } catch (error) {
        let detail = "Không tải được dữ liệu report từ backend.";
        if (axios.isAxiosError(error)) {
          const apiError = error.response?.data as { error?: string } | undefined;
          if (apiError?.error) {
            detail = apiError.error;
          } else if (error.message) {
            detail = error.message;
          }
        }
        message.error(detail);
      } finally {
        setLoading(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const financialSummary = useMemo(() => {
    const rows = financialData?.data ?? [];
    return rows.reduce(
      (acc, row) => {
        acc.netRevenue += row.netRevenue;
        acc.grossProfit += row.grossProfit;
        return acc;
      },
      { netRevenue: 0, grossProfit: 0 }
    );
  }, [financialData?.data]);

  const fundSummary = useMemo(() => {
    const rows = fundData?.data ?? [];
    return rows.reduce(
      (acc, row) => {
        acc.totalReceipts += row.totalReceipts;
        acc.netCashFlow += row.netCashFlow;
        return acc;
      },
      { totalReceipts: 0, netCashFlow: 0 }
    );
  }, [fundData?.data]);

  return (
    <Layout className="page">
      <Header className="header">
        <Title level={3} style={{ color: "#fff", margin: 0 }}>
          Kaizen Report Dashboard
        </Title>
      </Header>
      <Content className="content">
        <Card>
          <Form
            form={form}
            layout="vertical"
            initialValues={{
              dateRange: [dayjs().subtract(30, "day"), dayjs()]
            }}
          >
            <Row gutter={16}>
              <Col xs={24} md={10}>
                <Form.Item label="Khoảng ngày" name="dateRange">
                  <RangePicker className="full-width" />
                </Form.Item>
              </Col>
              <Col xs={24} md={6}>
                <Form.Item label="Shop" name="shopCode">
                  <Select allowClear options={shopOptions} placeholder="Chọn shop" />
                </Form.Item>
              </Col>
              <Col xs={24} md={6}>
                <Form.Item label="Sản phẩm" name="productCode">
                  <Input placeholder="Tạm thời chưa áp dụng ở backend" />
                </Form.Item>
              </Col>
              <Col xs={24} md={2} className="search-col">
                <Button type="primary" onClick={handleSearch} loading={loading}>
                  Lọc
                </Button>
              </Col>
            </Row>
          </Form>
          <Alert
            type="info"
            showIcon
            message="Filter sản phẩm cần batch report có ProductCode trong view tổng hợp (đề xuất bên dưới)."
          />
        </Card>

        <Tabs
          activeKey={activeTab}
          onChange={handleTabChange}
          items={[
            {
              key: "financial",
              label: "Báo cáo tài chính",
              children: (
                <Space direction="vertical" size="middle" className="full-width">
                  <Row gutter={16}>
                    <Col span={12}>
                      <Card>
                        <Statistic
                          title="Doanh thu thuần"
                          value={financialSummary.netRevenue}
                          formatter={(v) => formatNumber(v as number)}
                        />
                      </Card>
                    </Col>
                    <Col span={12}>
                      <Card>
                        <Statistic
                          title="Lợi nhuận gộp"
                          value={financialSummary.grossProfit}
                          formatter={(v) => formatNumber(v as number)}
                        />
                      </Card>
                    </Col>
                  </Row>
                  <Table
                    rowKey={(row) => `${row.reportDate}-${row.shopCode}`}
                    columns={financialColumns}
                    dataSource={financialData?.data ?? []}
                    loading={loading}
                    pagination={{ pageSize: 20, total: financialData?.pagination?.total }}
                    scroll={{ x: 900 }}
                  />
                </Space>
              )
            },
            {
              key: "fund",
              label: "Báo cáo quỹ",
              children: (
                <Space direction="vertical" size="middle" className="full-width">
                  <Row gutter={16}>
                    <Col span={12}>
                      <Card>
                        <Statistic
                          title="Tổng thu"
                          value={fundSummary.totalReceipts}
                          formatter={(v) => formatNumber(v as number)}
                        />
                      </Card>
                    </Col>
                    <Col span={12}>
                      <Card>
                        <Statistic
                          title="Dòng tiền ròng"
                          value={fundSummary.netCashFlow}
                          formatter={(v) => formatNumber(v as number)}
                        />
                      </Card>
                    </Col>
                  </Row>
                  <Table
                    rowKey={(row) => `${row.reportDate}-${row.shopCode}-${row.paymentMethodName}`}
                    columns={fundColumns}
                    dataSource={fundData?.data ?? []}
                    loading={loading}
                    pagination={{ pageSize: 20, total: fundData?.pagination?.total }}
                    scroll={{ x: 900 }}
                  />
                </Space>
              )
            }
          ]}
        />

        <Text type="secondary">
          Template đề xuất: Ant Design Dashboard (cards + tabs + table), dễ mở rộng thành module report thực tế.
        </Text>
      </Content>
    </Layout>
  );
}

export default App;
