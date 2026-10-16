import { useCallback, useEffect, useMemo, useState } from "react";
import { App as AntdApp, Layout, Tabs, Typography } from "antd";
import type { TablePaginationConfig } from "antd/es/table";
import dayjs from "dayjs";
import axios from "axios";
import { fetchReport, fetchFinancialDetailFilterOptions, fetchKdFilterOptions, type ReportFilter } from "./api/reportApi";
import { ReportDetailTab, type FilterFormValues } from "./components/ReportDetailTab";
import { ReportViewSlicer } from "./components/ReportViewSlicer";
import { SupersetEmbedTab } from "./components/SupersetEmbedTab";
import { getColumnsForView, rowKeyForView } from "./config/reportColumns";
import {
  DEFAULT_VIEW_KEY,
  getViewConfig,
  type FilterField,
  type ReportCategory,
  type ReportViewKey
} from "./config/reportViews";
import type { ListResponse, ReportRow } from "./types/report";
import { Form } from "antd";

const { Header, Content } = Layout;
const { Title, Text } = Typography;

const DEFAULT_PAGE = 1;
const DEFAULT_PAGE_SIZE = 20;
const PAGE_SIZE_ALL = 0;
const FILTER_ALL_VALUE = "__ALL__";
const PAGE_SIZE_OPTIONS = ["20", "50", "100"];

type ContentTabKey = "detail" | "bi";

function App() {
  const { message } = AntdApp.useApp();
  const [loading, setLoading] = useState(false);
  const [activeViewKey, setActiveViewKey] = useState<ReportViewKey>(DEFAULT_VIEW_KEY);
  const [contentTab, setContentTab] = useState<ContentTabKey>("detail");
  const [reportData, setReportData] = useState<ListResponse<ReportRow>>();
  const [detailFilterOptions, setDetailFilterOptions] = useState<{
    shops: { shopCode: string; shopName: string }[];
    products: { productId: string; productName: string }[];
    orderStatuses: string[];
  }>({ shops: [], products: [], orderStatuses: [] });
  const [page, setPage] = useState(DEFAULT_PAGE);
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
  const [form] = Form.useForm<FilterFormValues>();

  const viewConfig = useMemo(() => getViewConfig(activeViewKey), [activeViewKey]);
  const columns = useMemo(() => getColumnsForView(activeViewKey), [activeViewKey]);

  const isFinancialDetailView = activeViewKey === "financial:detail";
  const isKdView = viewConfig.category === "kd";

  const shopOptions = useMemo(() => {
    if (isFinancialDetailView || isKdView) {
      const dynamic = detailFilterOptions.shops.map((shop) => ({
        label: shop.shopName ? `${shop.shopCode} - ${shop.shopName}` : shop.shopCode,
        value: shop.shopCode
      }));
      return [{ label: "Tất cả cửa hàng", value: FILTER_ALL_VALUE }, ...dynamic];
    }
    const unique = new Set(
      (reportData?.data ?? [])
        .map((row) => row.shopCode)
        .filter((code): code is string => Boolean(code))
    );
    const dynamic = Array.from(unique).map((code) => ({ label: code, value: code }));
    return [{ label: "Tất cả cửa hàng", value: FILTER_ALL_VALUE }, ...dynamic];
  }, [isFinancialDetailView, isKdView, detailFilterOptions.shops, reportData?.data]);

  const productOptions = useMemo(() => {
    if (isFinancialDetailView) {
      const dynamic = detailFilterOptions.products.map((product) => ({
        label: product.productName
          ? `${product.productId} - ${product.productName}`
          : product.productId,
        value: product.productId
      }));
      return [{ label: "Tất cả sản phẩm", value: FILTER_ALL_VALUE }, ...dynamic];
    }
    const unique = new Map<string, string>();
    for (const row of reportData?.data ?? []) {
      if (!row.productId) continue;
      const label = row.productName ? `${row.productId} - ${row.productName}` : row.productId;
      unique.set(row.productId, label);
    }
    const dynamic = Array.from(unique.entries()).map(([value, label]) => ({ label, value }));
    return [{ label: "Tất cả sản phẩm", value: FILTER_ALL_VALUE }, ...dynamic];
  }, [isFinancialDetailView, detailFilterOptions.products, reportData?.data]);

  const orderStatusOptions = useMemo(() => {
    if (isKdView) {
      const dynamic = detailFilterOptions.orderStatuses.map((status) => ({
        label: status,
        value: status
      }));
      return [{ label: "Tất cả trạng thái", value: FILTER_ALL_VALUE }, ...dynamic];
    }
    const unique = new Set(
      (reportData?.data ?? [])
        .map((row) => row.orderStatus)
        .filter((status): status is string => Boolean(status))
    );
    const dynamic = Array.from(unique).map((status) => ({ label: status, value: status }));
    return [{ label: "Tất cả trạng thái", value: FILTER_ALL_VALUE }, ...dynamic];
  }, [isKdView, detailFilterOptions.orderStatuses, reportData?.data]);

  const paymentOptions = useMemo(() => {
    const unique = new Set(
      (reportData?.data ?? [])
        .map((row) => row.paymentMethodName)
        .filter((name): name is string => Boolean(name))
    );
    const dynamic = Array.from(unique).map((name) => ({ label: name, value: name }));
    return [{ label: "Tất cả PTTT", value: FILTER_ALL_VALUE }, ...dynamic];
  }, [reportData?.data]);

  const buildFilter = useCallback(
    (
      values?: FilterFormValues,
      paging?: { page?: number; pageSize?: number; view?: string }
    ): ReportFilter => {
      const formValues = values ?? form.getFieldsValue();
      const fromDate = (formValues.dateRange?.[0] ?? dayjs().subtract(30, "day")).format(
        "YYYY-MM-DD"
      );
      const toDate = (formValues.dateRange?.[1] ?? dayjs()).format("YYYY-MM-DD");
      const resolvedPageSize = paging?.pageSize ?? pageSize;
      const resolvedPage = resolvedPageSize === PAGE_SIZE_ALL ? DEFAULT_PAGE : (paging?.page ?? page);

      return {
        fromDate,
        toDate,
        view: paging?.view ?? viewConfig.view,
        shopCode:
          !formValues.shopCode || formValues.shopCode === FILTER_ALL_VALUE
            ? undefined
            : formValues.shopCode,
        productId:
          !formValues.productId || formValues.productId === FILTER_ALL_VALUE
            ? undefined
            : formValues.productId,
        productLineCode: formValues.productLineCode || undefined,
        orderId: formValues.orderId || undefined,
        documentType: formValues.documentType || undefined,
        orderStatus:
          !formValues.orderStatus || formValues.orderStatus === FILTER_ALL_VALUE
            ? undefined
            : formValues.orderStatus,
        paymentMethodName:
          !formValues.paymentMethodName || formValues.paymentMethodName === FILTER_ALL_VALUE
            ? undefined
            : formValues.paymentMethodName,
        page: resolvedPage,
        pageSize: resolvedPageSize
      };
    },
    [form, page, pageSize, viewConfig.view]
  );

  const loadDetailFilterOptions = useCallback(
    async (values?: FilterFormValues, viewKey: ReportViewKey = activeViewKey) => {
      if (viewKey !== "financial:detail" && viewKey !== "kd:order") {
        setDetailFilterOptions({ shops: [], products: [], orderStatuses: [] });
        return;
      }
      const formValues = values ?? form.getFieldsValue();
      const fromDate = (formValues.dateRange?.[0] ?? dayjs().subtract(30, "day")).format(
        "YYYY-MM-DD"
      );
      const toDate = (formValues.dateRange?.[1] ?? dayjs()).format("YYYY-MM-DD");
      if (viewKey === "financial:detail") {
        const opts = await fetchFinancialDetailFilterOptions(fromDate, toDate);
        setDetailFilterOptions({
          shops: opts.shops ?? [],
          products: opts.products ?? [],
          orderStatuses: []
        });
        return;
      }
      const opts = await fetchKdFilterOptions(fromDate, toDate);
      setDetailFilterOptions({
        shops: opts.shops ?? [],
        products: [],
        orderStatuses: opts.orderStatuses ?? []
      });
      // Clear stale selections when value not in refreshed options / Xóa lựa chọn không còn hợp lệ
      const currentShop = form.getFieldValue("shopCode");
      if (
        currentShop &&
        currentShop !== FILTER_ALL_VALUE &&
        !(opts.shops ?? []).some((s) => s.shopCode === currentShop)
      ) {
        form.setFieldValue("shopCode", undefined);
      }
      const currentStatus = form.getFieldValue("orderStatus");
      if (
        currentStatus &&
        currentStatus !== FILTER_ALL_VALUE &&
        !(opts.orderStatuses ?? []).includes(currentStatus)
      ) {
        form.setFieldValue("orderStatus", undefined);
      }
    },
    [form, activeViewKey]
  );

  const loadReport = useCallback(
    async (
      values?: FilterFormValues,
      paging?: { page?: number; pageSize?: number; view?: string; category?: ReportCategory }
    ) => {
      const category = paging?.category ?? viewConfig.category;
      const filter = buildFilter(values, paging);
      const data = await fetchReport(category, filter);
      setReportData(data);
      if (filter.pageSize === PAGE_SIZE_ALL) {
        setPage(DEFAULT_PAGE);
      } else if (filter.page) {
        setPage(filter.page);
      }
      if (filter.pageSize !== undefined) {
        setPageSize(filter.pageSize);
      }
    },
    [buildFilter, viewConfig.category]
  );

  const handleSearch = async () => {
    try {
      setLoading(true);
      await loadDetailFilterOptions(undefined, activeViewKey);
      await loadReport(undefined, { page: DEFAULT_PAGE, pageSize });
    } catch (error) {
      message.error(extractErrorMessage(error));
    } finally {
      setLoading(false);
    }
  };

  const handlePageChange = async (nextPage: number, nextPageSize: number) => {
    try {
      setLoading(true);
      await loadReport(undefined, { page: nextPage, pageSize: nextPageSize });
    } catch (error) {
      message.error(extractErrorMessage(error));
    } finally {
      setLoading(false);
    }
  };

  const handleViewChange = (nextKey: ReportViewKey) => {
    setActiveViewKey(nextKey);
    setContentTab("detail");
    setPage(DEFAULT_PAGE);
    form.resetFields([
      "shopCode",
      "productId",
      "productLineCode",
      "orderId",
      "documentType",
      "orderStatus",
      "paymentMethodName"
    ]);
    void (async () => {
      try {
        setLoading(true);
        const cfg = getViewConfig(nextKey);
        if (nextKey === "financial:detail" || nextKey === "kd:order") {
          await loadDetailFilterOptions(undefined, nextKey);
        } else {
          setDetailFilterOptions({ shops: [], products: [], orderStatuses: [] });
        }
        await loadReport(undefined, {
          page: DEFAULT_PAGE,
          pageSize,
          view: cfg.view,
          category: cfg.category
        });
      } catch (error) {
        message.error(extractErrorMessage(error));
      } finally {
        setLoading(false);
      }
    })();
  };

  useEffect(() => {
    const initial: FilterFormValues = {
      dateRange: [dayjs().subtract(30, "day"), dayjs()]
    };
    void (async () => {
      try {
        setLoading(true);
        if (activeViewKey === "financial:detail" || activeViewKey === "kd:order") {
          await loadDetailFilterOptions(initial, activeViewKey);
        }
        await loadReport(initial, { page: DEFAULT_PAGE, pageSize: DEFAULT_PAGE_SIZE });
      } catch (error) {
        message.error(extractErrorMessage(error));
      } finally {
        setLoading(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const summary = useMemo(() => {
    const rows = reportData?.data ?? [];
    if (viewConfig.category === "financial") {
      return rows.reduce(
        (acc, row) => {
          acc.netRevenue += row.netRevenue ?? 0;
          acc.grossProfit += row.grossProfit ?? 0;
          return acc;
        },
        {
          netRevenue: 0,
          grossProfit: 0,
          totalReceipts: 0,
          netCashFlow: 0,
          reconciliationDiff: 0,
          pendingOrderCount: 0
        }
      );
    }
    if (viewConfig.category === "kd") {
      return rows.reduce(
        (acc, row) => {
          acc.netRevenue += row.netRevenue ?? 0;
          acc.netCashFlow += row.netCashFlow ?? 0;
          acc.reconciliationDiff += row.reconciliationDiff ?? 0;
          if (row.pendingOrder === 1) acc.pendingOrderCount += 1;
          return acc;
        },
        {
          netRevenue: 0,
          grossProfit: 0,
          totalReceipts: 0,
          netCashFlow: 0,
          reconciliationDiff: 0,
          pendingOrderCount: 0
        }
      );
    }
    return rows.reduce(
      (acc, row) => {
        acc.totalReceipts += row.totalReceipts ?? 0;
        acc.netCashFlow += row.netCashFlow ?? 0;
        return acc;
      },
      {
        netRevenue: 0,
        grossProfit: 0,
        totalReceipts: 0,
        netCashFlow: 0,
        reconciliationDiff: 0,
        pendingOrderCount: 0
      }
    );
  }, [reportData?.data, viewConfig.category]);

  const showFilter = (field: FilterField) => viewConfig.filters.includes(field);

  const totalRows = reportData?.pagination?.total ?? 0;
  const tablePageSize =
    pageSize === PAGE_SIZE_ALL ? Math.max(reportData?.data?.length ?? 0, 1) : pageSize;

  const paginationConfig: TablePaginationConfig = {
    current: page,
    pageSize: tablePageSize,
    total: totalRows,
    showSizeChanger: true,
    pageSizeOptions: PAGE_SIZE_OPTIONS,
    showTotal: (total, range) => `${range[0]}-${range[1]} / ${total} dòng`,
    onChange: handlePageChange,
    hideOnSinglePage: false
  };

  return (
    <Layout className="page">
      <Header className="header">
        <Title level={3} style={{ color: "#fff", margin: 0 }}>
          Kaizen Report Dashboard
        </Title>
      </Header>
      <div className="page-body">
        <aside className="report-sider">
          <ReportViewSlicer activeKey={activeViewKey} onChange={handleViewChange} />
        </aside>
        <Content className="content">
          <div className="view-meta">
            <Title level={4} style={{ margin: 0 }}>
              {viewConfig.label}
            </Title>
            <Text type="secondary">{viewConfig.description}</Text>
          </div>

          <Tabs
            className="report-content-tabs"
            activeKey={contentTab}
            onChange={(key) => setContentTab(key as ContentTabKey)}
            destroyInactiveTabPane={false}
            items={[
              {
                key: "detail",
                label: "Bảng chi tiết",
                children: (
                  <ReportDetailTab
                    viewConfig={viewConfig}
                    form={form}
                    loading={loading}
                    shopOptions={shopOptions}
                    productOptions={productOptions}
                    paymentOptions={paymentOptions}
                    orderStatusOptions={orderStatusOptions}
                    showFilter={showFilter}
                    onSearch={handleSearch}
                    summary={summary}
                    columns={columns}
                    reportData={reportData}
                    paginationConfig={paginationConfig}
                    rowKey={(row, index) => rowKeyForView(activeViewKey, row, index ?? 0)}
                  />
                )
              },
              ...(viewConfig.supersetSlug
                ? [
                    {
                      key: "bi" as const,
                      label: "Phân tích BI",
                      children: (
                        <SupersetEmbedTab
                          slug={viewConfig.supersetSlug}
                          active={contentTab === "bi"}
                        />
                      )
                    }
                  ]
                : [])
            ]}
          />
        </Content>
      </div>
    </Layout>
  );
}

function extractErrorMessage(error: unknown): string {
  let detail = "Không tải được dữ liệu report từ backend.";
  if (axios.isAxiosError(error)) {
    const apiError = error.response?.data as { error?: string } | undefined;
    if (apiError?.error) {
      detail = apiError.error;
    } else if (error.message) {
      detail = error.message;
    }
  }
  return detail;
}

export default App;
