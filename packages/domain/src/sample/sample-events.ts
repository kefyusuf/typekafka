import { randomUUID } from 'node:crypto';
import type { OrderCreated } from '../events/order-created.js';
import type { PaymentCompleted } from '../events/payment-completed.js';

const SKUS = ['TSHIRT-BLACK', 'MUG-WHITE', 'HOODIE-GREY', 'STICKER-OG'] as const;
const PAYMENT_METHODS = ['card', 'bank_transfer', 'wallet'] as const;

const randomInt = (max: number) => Math.floor(Math.random() * max);
const pick = <T>(values: readonly T[]): T => values[randomInt(values.length)]!;

/**
 * Deterministic, spec-compliant order event for demos and tests.
 *
 * Every third order is deliberately oversized (> 100_000 cents) so the
 * notification handler's simulated provider timeout fires — demonstrating
 * the consumer's retry + DLQ pipeline.
 */
export function createSampleOrder(seq: number): OrderCreated {
  const oversized = seq % 3 === 0;

  const items = oversized
    ? [{ sku: 'TSHIRT-BLACK', quantity: 3, priceCents: 50_000 }]
    : Array.from({ length: 1 + randomInt(3) }, () => ({
        sku: pick(SKUS),
        quantity: 1 + randomInt(4),
        priceCents: 500 + randomInt(4) * 500,
      }));

  return {
    type: 'order.created',
    eventId: randomUUID(),
    occurredAt: new Date().toISOString(),
    orderId: `ORD-${String(seq).padStart(5, '0')}`,
    customerId: `CUST-100${(seq % 3) + 1}`,
    items,
    totalCents: items.reduce((sum, i) => sum + i.quantity * i.priceCents, 0),
  };
}

/** Companion payment event for a given order. */
export function createSamplePayment(order: OrderCreated): PaymentCompleted {
  return {
    type: 'payment.completed',
    eventId: randomUUID(),
    occurredAt: new Date().toISOString(),
    orderId: order.orderId,
    paymentId: `PAY-${randomUUID().slice(0, 8).toUpperCase()}`,
    amountCents: order.totalCents,
    method: pick(PAYMENT_METHODS),
  };
}
