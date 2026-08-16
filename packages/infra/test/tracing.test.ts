import { describe, expect, it } from 'vitest';
import { initTracing } from '../src/tracing.js';

describe('initTracing', () => {
  it('returns a noop tracing when the endpoint is empty or whitespace', async () => {
    for (const endpoint of ['', '   ']) {
      const tracing = initTracing({ endpoint });
      await expect(tracing.shutdown()).resolves.toBeUndefined();
    }
  });
});
