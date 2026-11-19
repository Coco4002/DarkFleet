import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// In dev the map-gateway runs on 8088 (docker compose up map-gateway); in production Caddy routes /api.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api/v1/map': { target: 'http://localhost:8088', changeOrigin: true },
    },
  },
  test: { environment: 'node' },
});
