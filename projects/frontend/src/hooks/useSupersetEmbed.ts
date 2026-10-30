import { useCallback, useEffect, useRef, useState } from "react";
import { embedDashboard } from "@superset-ui/embedded-sdk";
import { fetchEmbedConfig, fetchGuestToken } from "../api/biApi";

/**
 * Dev (Vite): same origin so /embedded is proxied to Superset.
 * Prod: prefer same hostname as Report UI with Superset NodePort 32288
 * (LAN 192.168.x or Tailscale 100.x) so iframe matches how the user opened the UI.
 * Dev: cùng origin (Vite proxy). Prod: cùng hostname UI, port Superset 32288.
 */
export function resolveSupersetEmbedDomain(backendHint: string): string {
  // Vite dev always proxies /embedded on the page origin / Dev Vite luôn proxy /embedded trên origin trang
  if (import.meta.env.DEV) {
    return window.location.origin;
  }

  // Same host as UI (Tailscale or LAN), fixed Superset NodePort / Cùng host UI, NodePort Superset cố định
  const { protocol, hostname, port } = window.location;
  if (hostname && (port === "31090" || port === "8090" || port === "8092" || port === "")) {
    return `${protocol}//${hostname}:32288`;
  }

  const hint = (backendHint || "").trim().replace(/\/$/, "");
  if (hint && hint !== window.location.origin) {
    return hint;
  }
  return window.location.origin;
}

const DEFAULT_UI_CONFIG = {
  hideTitle: true,
  hideTab: true,
  filters: { expanded: true }
} as const;

export type UseSupersetEmbedOptions = {
  slug: string;
  active: boolean;
};

export type UseSupersetEmbedResult = {
  mountRef: React.RefObject<HTMLDivElement>;
  loading: boolean;
  error: string | null;
};

/**
 * Mount Superset embedded dashboard when tab is active.
 * Gắn dashboard Superset nhúng khi tab BI được chọn.
 */
export function useSupersetEmbed({ slug, active }: UseSupersetEmbedOptions): UseSupersetEmbedResult {
  const mountRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const resolveDashboard = useCallback(async (dashboardSlug: string) => {
    const config = await fetchEmbedConfig();
    if (!config.enabled) {
      throw new Error("Superset embed chưa được cấu hình trên backend.");
    }
    const dash = config.dashboards.find((d) => d.slug === dashboardSlug);
    if (!dash) {
      throw new Error(`Không tìm thấy dashboard BI: ${dashboardSlug}`);
    }
    return { config, dash };
  }, []);

  useEffect(() => {
    if (!active || !slug) {
      return;
    }

    let cancelled = false;
    let unmountDashboard: (() => void) | undefined;

    async function mount() {
      if (!mountRef.current) {
        return;
      }
      setLoading(true);
      setError(null);
      mountRef.current.innerHTML = "";

      try {
        const { config, dash } = await resolveDashboard(slug);
        const initialToken = await fetchGuestToken(slug);

        const embedded = await embedDashboard({
          id: dash.embeddedUuid,
          supersetDomain: resolveSupersetEmbedDomain(config.supersetDomain),
          mountPoint: mountRef.current,
          fetchGuestToken: async () => {
            const tokenResp = await fetchGuestToken(slug);
            return tokenResp.token;
          },
          dashboardUiConfig: {
            ...DEFAULT_UI_CONFIG,
            urlParams: { guest_token: initialToken.token }
          }
        });

        if (cancelled) {
          embedded.unmount();
          return;
        }
        unmountDashboard = () => embedded.unmount();
      } catch (err) {
        if (!cancelled) {
          const message = err instanceof Error ? err.message : "Không tải được dashboard BI";
          setError(message);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void mount();

    return () => {
      cancelled = true;
      unmountDashboard?.();
      if (mountRef.current) {
        mountRef.current.innerHTML = "";
      }
    };
  }, [slug, active, resolveDashboard]);

  return { mountRef, loading, error };
}
