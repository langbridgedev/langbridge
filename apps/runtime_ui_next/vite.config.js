import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  base: "/ui/",
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5177,
    proxy: {
      "/api": {
        target: process.env.LANGBRIDGE_RUNTIME_URL || "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    emptyOutDir: true,
    outDir: "../../langbridge/ui/static",
  },
});
