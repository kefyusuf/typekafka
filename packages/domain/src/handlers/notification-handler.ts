import type { EventPayload } from '../events/index.js';
import type { OrderCreated } from '../events/order-created.js';

/**
 * A domain handler receives an already-validated payload and returns void.
 *
 * Domain code is pure: it never imports broker types. All Kafka concerns
 * (headers, offsets, commits) live in the adapter layer above.
 */
export type EventHandler<T extends EventPayload = EventPayload> = (
  payload: T,
) => Promise<void>;

/**
 * Example business handler: sends a "new order" notification.
 * In a real system this would call an email / push / SMS provider.
 */
export const createNotificationHandler = (
  log: { info(msg: string, fields?: Record<string, unknown>): void },
): EventHandler<OrderCreated> => {
  return async (order) => {
    log.info('notification: order placed', {
      eventId: order.eventId,
      orderId: order.orderId,
      customerId: order.customerId,
      itemCount: order.items.length,
      totalCents: order.totalCents,
    });

    // Simulate a transient side-effect failure that the retry pipeline handles.
    if (order.totalCents > 100_000) {
      throw new Error('notification provider timed out (simulated retry)');
    }
  };
};
