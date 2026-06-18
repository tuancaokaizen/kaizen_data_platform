import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 8092,
    host: "0.0.0.0",
    // Proxy API in dev to avoid CORS / Proxy API khi dev để tránh lỗi CORS
    proxy: {
      "/api": {
        target: "http://localhost:8090",
        changeOrigin: true
      },
      "/healthz": {
        target: "http://localhost:8090",
        changeOrigin: true
      }
    }
  }
});
