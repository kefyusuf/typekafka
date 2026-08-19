import { defineConfig } from 'vitest/config';

// Docker-gated real-Kafka integration suite. Run via `npm run test:integration`
// (requires Docker for Testcontainers). Kept separate from the default unit
// config so `npm test` runs with no Docker dependency.
export default defineConfig({
  test: {
    include: ['apps/**/test/integration/**/*.test.ts'],
    environment: 'node',
  },
});
