import { randomUUID } from 'node:crypto';
import type { CustomerUpdated } from '../events/customer-updated.js';
import type { OrderCreated } from '../events/order-created.js';
import type { PaymentCompleted } from '../events/payment-completed.js';

export interface CustomerState {
  customerId: string;
  totalSpentCents: number;
  orderCount: number;
  lastOrderAt: string;
  updatedAt: string;
}

export function applyOrder(
  state: CustomerState | undefined,
  order: OrderCreated,
): CustomerState {
  return {
    customerId: order.customerId,
    totalSpentCents: (state?.totalSpentCents ?? 0) + order.totalCents,
    orderCount: (state?.orderCount ?? 0) + 1,
    lastOrderAt: order.occurredAt,
    updatedAt: order.occurredAt,
  };
}

export function applyPayment(
  state: CustomerState,
  payment: PaymentCompleted,
): CustomerState {
  return {
    ...state,
    totalSpentCents: state.totalSpentCents + payment.amountCents,
    updatedAt: payment.occurredAt,
  };
}

export function toCustomerUpdated(state: CustomerState): CustomerUpdated {
  return {
    type: 'customer.updated',
    eventId: randomUUID(),
    occurredAt: state.updatedAt,
    customerId: state.customerId,
    totalSpentCents: state.totalSpentCents,
    orderCount: state.orderCount,
    lastOrderAt: state.lastOrderAt,
    updatedAt: state.updatedAt,
  };
}
