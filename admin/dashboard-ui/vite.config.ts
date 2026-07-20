import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  base: "/assets/",
  plugins: [react(), tailwindcss()],
  build: {
    outDir: "../dashboard",
    emptyOutDir: true,
    assetsDir: ".",
    cssCodeSplit: false,
    sourcemap: false,
    rollupOptions: {
      output: {
        entryFileNames: "app.js",
        chunkFileNames: "app.js",
        assetFileNames: (asset) => asset.name?.endsWith(".css") ? "style.css" : "[name][extname]",
      },
    },
  },
});
