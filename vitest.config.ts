import { defineConfig } from 'vitest/config';

// Unit tests exercise the module/service wiring in isolation, with the
// underlying sqs-consumer/sqs-producer libraries mocked. No broker required.
export default defineConfig({
  test: {
    include: ['test/**/*.spec.ts'],
    environment: 'node',
    globals: false,
    root: './',
    testTimeout: 10000,
    coverage: {
      provider: 'v8',
      include: ['lib/**/*.ts'],
      exclude: ['lib/index.ts'],
    },
  },
});
