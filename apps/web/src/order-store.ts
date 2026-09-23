import { DatabaseSync, type StatementSync } from 'node:sqlite';
import {
  CUSTOMER_TOPIC,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  applyOrder,
  applyPayment,
  toCustomerUpdated,
  type CustomerState,
  type CustomerUpdated,
  type OrderCreated,
  type PaymentCompleted,
} from '@typekafka/domain';
import type { OutboxStore } from '@typekafka/infra';

export class OrderStore {
  private readonly insertStmt: StatementSync;
  // Process-local running per-customer aggregation so web-placed orders also
  // feed the customer-360 read model (mirrors the producer's aggregation).
  private readonly customerStates = new Map<string, CustomerState>();

  constructor(private readonly db: DatabaseSync) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL UNIQUE,
        event_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        total_cents INTEGER NOT NULL,
        items TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
    `);
    this.insertStmt = db.prepare(
      'INSERT INTO orders (order_id, event_id, customer_id, total_cents, items, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
    );
  }

  createOrderWithOutbox(order: OrderCreated, payment: PaymentCompleted, outbox: OutboxStore): void {
    this.db.exec('BEGIN');
    try {
      this.insertStmt.run(
        order.orderId,
        order.eventId,
        order.customerId,
        order.totalCents,
        JSON.stringify(order.items),
        'placed',
        new Date().toISOString(),
      );
      outbox.insertPending({ topic: TOPIC_ORDER_CREATED, payload: order, key: order.orderId });
      outbox.insertPending({
        topic: TOPIC_PAYMENT_COMPLETED,
        payload: payment,
        key: order.orderId,
      });

      // Aggregate this order into the per-customer state and publish a
      // customer.updated event to the compacted customers topic so the
      // customer-360 read model (customer-view) reflects web-placed orders too.
      const prev = this.customerStates.get(order.customerId);
      const nextState = applyPayment(applyOrder(prev, order), payment);
      this.customerStates.set(order.customerId, nextState);
      const customerEvent: CustomerUpdated = toCustomerUpdated(nextState);
      outbox.insertPending({
        topic: CUSTOMER_TOPIC,
        payload: customerEvent,
        key: customerEvent.customerId,
      });

      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }
}
