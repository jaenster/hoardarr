import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite bakes a sentinel string as the base path:
//   /__HOARDARR_BASE__/
// All asset URLs in the built HTML/CSS/JS — plus any
// `import.meta.env.BASE_URL` reference inside our TypeScript — end up
// containing this sentinel. At serve time, the Go server does a single
// in-memory string replace of the sentinel with the configured
// `URLBase + "/"` (or "/" when empty). The same binary then works at
// any reverse-proxy mount path with zero rebuild.
export default defineConfig({
  base: "/__HOARDARR_BASE__/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8085",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
