import { describe, expect, it } from 'vitest';
import { TelemetryEventSchema, TELEMETRY_TOPIC } from '../src/events/index.js';

const base = {
  type: 'produced',
  topic: 'orders.created',
  eventId: '11111111-1111-1111-1111-111111111111',
  orderId: 'ORD-00001',
  partition: 2,
  offset: '7',
  message: 'Order published to orders.created',
  concept: 'partition-key',
  occurredAt: '2026-08-15T00:00:00.000Z',
} as const;

describe('telemetry event schema', () => {
  it('parses a valid telemetry event', () => {
    const parsed = TelemetryEventSchema.parse(base);
    expect(parsed.type).toBe('produced');
    expect(parsed.offset).toBe('7');
  });

  it('rejects an unknown event type', () => {
    expect(() =>
      TelemetryEventSchema.parse({ ...base, type: 'exploded' }),
    ).toThrow();
  });

  it('accepts an optional attempt field (retry)', () => {
    const parsed = TelemetryEventSchema.parse({ ...base, attempt: 2 });
    expect(parsed.attempt).toBe(2);
  });

  it('exposes the telemetry topic constant', () => {
    expect(TELEMETRY_TOPIC).toBe('telemetry.events');
  });
});
