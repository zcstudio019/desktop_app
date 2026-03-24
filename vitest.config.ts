import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'jsdom',
    include: ['src/**/*.test.ts', 'src/**/*.test.tsx'],
    setupFiles: ['./src/setupTests.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
    },
    // Property-based testing configuration
    // Minimum 100 iterations per property test
    testTimeout: 30000,
  },
  resolve: {
    alias: {
      '@': '/src',
    },
  },
});
