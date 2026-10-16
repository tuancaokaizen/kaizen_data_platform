import { Alert, Spin } from "antd";
import { useSupersetEmbed } from "../hooks/useSupersetEmbed";

type SupersetEmbedTabProps = {
  slug?: string;
  active: boolean;
};

/** Embedded Superset dashboard panel / Panel dashboard Superset nhúng */
export function SupersetEmbedTab({ slug, active }: SupersetEmbedTabProps) {
  const { mountRef, loading, error } = useSupersetEmbed({ slug: slug ?? "", active: active && Boolean(slug) });

  if (!active) {
    return null;
  }

  if (!slug) {
    return (
      <Alert
        type="info"
        showIcon
        message="Dashboard BI chưa được cấu hình cho báo cáo này"
        description="Chạy publish Superset: cd projects/superset && ./publish_superset.sh --tailscale --force --embed --dashboard kinh-doanh-realtime-theo-don"
      />
    );
  }

  if (!active) {
    return null;
  }

  return (
    <div className="superset-embed-wrap">
      {loading && (
        <div className="superset-embed-loading">
          <Spin tip="Đang tải dashboard BI..." />
        </div>
      )}
      {error && <Alert type="error" message={error} showIcon className="superset-embed-error" />}
      <div ref={mountRef} className="superset-embed-mount" />
    </div>
  );
}
