import { describe, expect, it } from 'vitest';
import {
  CUSTOMER_TOPIC,
  applyOrder,
  applyPayment,
  createSampleOrder,
  createSamplePayment,
  parseEvent,
  toCustomerUpdated,
  type CustomerState,
} from '../src/index.js';

describe('customer changelog', () => {
  it('parses a valid customer.updated payload', () => {
    const state = applyOrder(undefined, createSampleOrder(1));
    const parsed = parseEvent(CUSTOMER_TOPIC, toCustomerUpdated(state));

    expect(parsed.type).toBe('customer.updated');
    expect(parsed.customerId).toBe(state.customerId);
    expect(parsed.totalSpentCents).toBe(state.totalSpentCents);
  });

  it('rejects a payload with a wrong type discriminator', () => {
    const state = applyOrder(undefined, createSampleOrder(1));

    expect(() =>
      parseEvent(CUSTOMER_TOPIC, {
        ...toCustomerUpdated(state),
        type: 'order.created',
      }),
    ).toThrow();
  });

  it('applyOrder creates a fresh state from an undefined prior state', () => {
    const order = createSampleOrder(1);
    const state = applyOrder(undefined, order);

    expect(state.customerId).toBe(order.customerId);
    expect(state.orderCount).toBe(1);
    expect(state.totalSpentCents).toBe(0);
  });

  it('applyOrder accumulates across orders without mutating prior state', () => {
    const first = createSampleOrder(1);
    const second = createSampleOrder(2);
    const prior = applyOrder(undefined, first);
    const state = applyOrder(prior, second);

    expect(state).not.toBe(prior);
    expect(prior.totalSpentCents).toBe(0);
    expect(prior.orderCount).toBe(1);
    expect(state.orderCount).toBe(2);
    expect(state.totalSpentCents).toBe(0);
  });

  it('applyPayment increments totalSpentCents and leaves order fields unchanged', () => {
    const order = createSampleOrder(1);
    const payment = createSamplePayment(order);
    const state: CustomerState = applyOrder(undefined, order);
    const paid = applyPayment(state, payment);

    expect(paid).not.toBe(state);
    expect(paid.totalSpentCents).toBe(state.totalSpentCents + payment.amountCents);
    expect(paid.orderCount).toBe(state.orderCount);
    expect(paid.lastOrderAt).toBe(state.lastOrderAt);
    expect(paid.customerId).toBe(state.customerId);
  });

  it('sample orders draw from a deterministic customer pool', () => {
    expect(createSampleOrder(1).customerId).toBe(createSampleOrder(4).customerId);
    expect(createSampleOrder(1).customerId).not.toBe(createSampleOrder(2).customerId);
  });
});
