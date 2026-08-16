import { DatabaseSync, type StatementSync } from 'node:sqlite';
import { TOPIC_ORDER_CREATED, TOPIC_PAYMENT_COMPLETED } from '@nodejs-kafka/domain';
import type { OrderCreated, PaymentCompleted } from '@nodejs-kafka/domain';
import type { OutboxStore } from '@nodejs-kafka/infra';

export class OrderStore {
  private readonly insertStmt: StatementSync;

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
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }
}
