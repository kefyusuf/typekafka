import { describe, expect, it } from 'vitest';
import {
  parseEvent,
  createSampleOrder,
  createSamplePayment,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  type EventOf,
} from '../src/index.js';

describe('event schemas', () => {
  it('parses a valid order.created payload', () => {
    const order = createSampleOrder(1);
    const parsed = parseEvent(TOPIC_ORDER_CREATED, order);

    expect(parsed.type).toBe('order.created');
    expect(parsed.eventId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
  });

  it('rejects a payload that violates the schema', () => {
    expect(() =>
      parseEvent(TOPIC_ORDER_CREATED, { type: 'order.created' }),
    ).toThrow();
  });

  it('parses a valid payment.completed payload', () => {
    const order = createSampleOrder(1);
    const payment = createSamplePayment(order);

    const parsed = parseEvent(TOPIC_PAYMENT_COMPLETED, payment);
    expect(parsed.orderId).toBe(order.orderId);
    expect(parsed.amountCents).toBe(order.totalCents);
  });

  it('rejects an unknown event type for a topic', () => {
    expect(() =>
      parseEvent(TOPIC_ORDER_CREATED, {
        ...createSampleOrder(1),
        type: 'payment.completed',
      }),
    ).toThrow();
  });

  it('generates an oversized order for every 3rd sequence (retry/DLQ trigger)', () => {
    expect(createSampleOrder(3).totalCents).toBeGreaterThan(100_000);
    expect(createSampleOrder(1).totalCents).toBeLessThanOrEqual(100_000);
  });

  it('is fully type-safe: EventOf<topic> narrows to the right payload', () => {
    const order: EventOf<'orders.created'> = createSampleOrder(1);
    const payment: EventOf<'payments.completed'> = createSamplePayment(order);

    // Discriminated union property access is legal on both.
    expect(order.orderId).toBeDefined();
    expect(payment.paymentId).toBeDefined();
  });
});
