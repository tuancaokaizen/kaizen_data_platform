import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import type { IncomingMessage } from "http";

// Strip headers that block iframe embed in dev / Gỡ header chặn nhúng iframe khi dev
function allowIframeEmbed(proxyRes: IncomingMessage) {
  delete proxyRes.headers["x-frame-options"];
  delete proxyRes.headers["X-Frame-Options"];

  const csp = proxyRes.headers["content-security-policy"];
  if (typeof csp === "string" && !csp.includes("frame-ancestors")) {
    proxyRes.headers["content-security-policy"] = `${csp}; frame-ancestors 'self'`;
  }
}

function configureSupersetProxy(proxy: {
  on(event: "proxyRes", handler: (proxyRes: IncomingMessage, req: IncomingMessage) => void): void;
}) {
  proxy.on("proxyRes", (proxyRes: IncomingMessage, _req: IncomingMessage) => {
    allowIframeEmbed(proxyRes);
  });
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const supersetUpstream = env.VITE_SUPERSET_UPSTREAM || "http://localhost:32288";
  const backendUpstream = env.VITE_BACKEND_UPSTREAM || "http://localhost:8090";

  const supersetProxy = {
    target: supersetUpstream,
    changeOrigin: true,
    ws: true,
    configure: configureSupersetProxy
  };

  return {
    plugins: [react()],
    server: {
      port: 8092,
      host: "0.0.0.0",
      // Proxy API + Superset in dev / Proxy API và Superset khi dev
      proxy: {
        // Go report API — must be before Superset /api/v1 catch-all
        "/api/v1/reports": {
          target: backendUpstream,
          changeOrigin: true
        },
        "/api/v1/bi": {
          target: backendUpstream,
          changeOrigin: true
        },
        // Superset REST + static + embedded iframe (same-origin via Vite proxy)
        "/api/v1": supersetProxy,
        "/static": supersetProxy,
        "/embedded": supersetProxy,
        "/superset": supersetProxy,
        "/healthz": {
          target: backendUpstream,
          changeOrigin: true
        },
        // Legacy prefix — prefer /embedded on origin / Tiền tố cũ, ưu tiên /embedded
        "/superset-proxy": {
          ...supersetProxy,
          rewrite: (path) => path.replace(/^\/superset-proxy/, "")
        }
      }
    }
  };
});
