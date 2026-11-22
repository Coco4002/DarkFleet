import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// In dev the map-gateway runs on 8088 and Vessel Intelligence (login, analyst API) on 8086; in production
// Caddy routes /api the same way.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api/v1/map': { target: 'http://localhost:8088', changeOrigin: true },
      '/api/v1': { target: 'http://localhost:8086', changeOrigin: true },
    },
  },
  test: { environment: 'node' },
});
