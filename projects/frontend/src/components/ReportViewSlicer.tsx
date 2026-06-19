import { Select } from "antd";
import type { ReportCategory, ReportViewKey } from "../config/reportViews";
import { getViewConfig, MENU_GROUP_LABELS, REPORT_VIEWS } from "../config/reportViews";

type ReportViewSlicerProps = {
  activeKey: ReportViewKey;
  onChange: (key: ReportViewKey) => void;
};

function childViewOptions(category: ReportCategory) {
  return REPORT_VIEWS.filter((v) => v.category === category).map((view) => ({
    label: view.label,
    value: view.key
  }));
}

export function ReportViewSlicer({ activeKey, onChange }: ReportViewSlicerProps) {
  const activeCategory = getViewConfig(activeKey).category;

  return (
    <nav className="report-nav" aria-label="Danh mục báo cáo">
      <div className="report-nav__heading">Danh mục báo cáo</div>

      <section
        className={`report-nav__group${
          activeCategory === "financial" ? " report-nav__group--active" : ""
        }`}
      >
        <h2 className="report-nav__category">{MENU_GROUP_LABELS.financial}</h2>
        <Select
          className="report-nav__select"
          value={activeCategory === "financial" ? activeKey : undefined}
          options={childViewOptions("financial")}
          onChange={(key) => onChange(key as ReportViewKey)}
          placeholder="Chọn chiều báo cáo"
          showSearch
          optionFilterProp="label"
        />
      </section>

      <section
        className={`report-nav__group${
          activeCategory === "fund" ? " report-nav__group--active" : ""
        }`}
      >
        <h2 className="report-nav__category">{MENU_GROUP_LABELS.fund}</h2>
        <Select
          className="report-nav__select"
          value={activeCategory === "fund" ? activeKey : undefined}
          options={childViewOptions("fund")}
          onChange={(key) => onChange(key as ReportViewKey)}
          placeholder="Chọn chiều báo cáo"
          showSearch
          optionFilterProp="label"
        />
      </section>
    </nav>
  );
}
