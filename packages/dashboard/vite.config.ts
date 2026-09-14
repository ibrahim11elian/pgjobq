import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    // 5400, deliberately away from the Vite default 5173: another local project commonly holds it, and
    // on Windows both can bind with the pre-existing process winning for localhost.
    port: 5400,
    strictPort: true,
    proxy: {
      // Proxying in dev means the browser makes same-origin requests, so CORS and
      // SSE both work without special-casing the dev environment.
      '/v1': { target: 'http://localhost:3001', changeOrigin: true },
      '/metrics': { target: 'http://localhost:3001', changeOrigin: true },
      '/health': { target: 'http://localhost:3001', changeOrigin: true },
      '/ready': { target: 'http://localhost:3001', changeOrigin: true },
    },
  },
  build: { outDir: 'dist', sourcemap: true },
});
