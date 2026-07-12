import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['lib/index.ts'],
  format: ['cjs', 'esm'],
  dts: true,
  clean: true,
  sourcemap: true,
  target: 'es2022',
  outDir: 'dist',
  splitting: false,
  treeshake: true,
  // DI in this library is fully explicit (see @Inject usage), so decorator
  // metadata emission is not required — esbuild can transform decorators directly.
  keepNames: true,
});
