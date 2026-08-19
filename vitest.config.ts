import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['packages/**/test/**/*.test.ts', 'apps/**/test/**/*.test.ts'],
    // The Docker-gated real-Kafka integration suite lives under
    // `test/integration` and runs via `npm run test:integration` (needs
    // Docker). Keep it out of the default `npm test` so the unit suite runs
    // anywhere with no Docker dependency.
    exclude: ['**/node_modules/**', '**/dist/**', '**/test/integration/**'],
    environment: 'node',
  },
});
