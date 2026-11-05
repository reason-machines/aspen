import { defineConfig } from 'vitest/config';
import { placeMock } from './mock/vite-plugin.ts';

// PLACE_API_PROXY=http://localhost:7070 sends /api to a real back end
// instead of the built-in mock.
const proxy = process.env.PLACE_API_PROXY;

export default defineConfig({
  plugins: [placeMock()],
  server: proxy ? { proxy: { '/api': { target: proxy, changeOrigin: true } } } : {},
  build: {
    target: 'es2022',
    sourcemap: true,
  },
  test: {
    include: ['test/**/*.test.ts'],
    environment: 'node',
  },
});
