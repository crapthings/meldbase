import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

/** Configures the React example build with source maps. */
export default defineConfig({
  plugins: [react()],
  build: { sourcemap: true },
});
