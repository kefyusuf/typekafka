import { describe, expect, it } from 'vitest';
import { TOPIC_ORDER_CREATED, createSampleOrder, parseEvent } from '@typekafka/domain';
import { runRoundtrip } from '../src/roundtrip.js';

describe('hello-roundtrip (consumes packed @typekafka tarballs)', () => {
  it('produces a validated order event and consumes it back with its key', async () => {
    const result = await runRoundtrip();

    expect(result.received).toEqual(result.produced);
    expect(result.key).toBe(result.produced.orderId);
    expect(result.received.type).toBe('order.created');
  });

  it('parseEvent rejects a payload that violates the domain schema', () => {
    const bad = { ...createSampleOrder(2), totalCents: -1 };
    expect(() => parseEvent(TOPIC_ORDER_CREATED, bad)).toThrow();
  });
});