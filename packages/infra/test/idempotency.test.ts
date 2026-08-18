import { describe, expect, it } from 'vitest';
import { createIdempotencyFilter } from '../src/idempotency.js';

describe('createIdempotencyFilter', () => {
  it('reports an unseen eventId as not duplicate', () => {
    const filter = createIdempotencyFilter();
    expect(filter.isDuplicate('evt-1')).toBe(false);
  });

  it('reports a marked eventId as duplicate until evicted', () => {
    const filter = createIdempotencyFilter();
    expect(filter.isDuplicate('evt-1')).toBe(false);
    filter.mark('evt-1');
    expect(filter.isDuplicate('evt-1')).toBe(true);
    // A different eventId is still unseen.
    expect(filter.isDuplicate('evt-2')).toBe(false);
  });

  it('evicts the oldest entry beyond maxSize (LRU bound)', () => {
    const filter = createIdempotencyFilter({ maxSize: 1 });
    filter.mark('a');
    expect(filter.isDuplicate('a')).toBe(true);
    filter.mark('b');
    // 'a' was evicted when 'b' was inserted past the bound.
    expect(filter.isDuplicate('a')).toBe(false);
    expect(filter.isDuplicate('b')).toBe(true);
  });

  it('treats entries older than ttlMs as unseen', () => {
    const filter = createIdempotencyFilter({ ttlMs: 10 });
    filter.mark('evt-ttl');
    expect(filter.isDuplicate('evt-ttl')).toBe(true);
  });
});
