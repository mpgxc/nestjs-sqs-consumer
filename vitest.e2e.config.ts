import AutoImport from 'unplugin-auto-import/vite';
import { defineConfig } from 'vitest/config';

// End-to-end tests run against a live SQS-compatible broker (ElasticMQ).
// They are intentionally serialized to keep queue state deterministic.
export default defineConfig({
  test: {
    include: ['e2e/**/*.e2e-spec.ts'],
    environment: 'node',
    globals: true,
    root: './',
    testTimeout: 20000,
    hookTimeout: 30000,
    cache: false,
    reporters: ['verbose'],
    isolate: false,
    maxConcurrency: 1,
    pool: 'forks',
    poolOptions: {
      forks: {
        isolate: false,
        singleFork: true,
        minForks: 1,
        maxForks: 1,
      },
    },
  },
  plugins: [
    AutoImport({
      imports: ['vitest'],
    }),
  ],
});
