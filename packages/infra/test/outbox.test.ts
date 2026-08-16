import { describe, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { createBroker } from '@nodejs-kafka/broker';
import type {
  IMessageBroker,
  KafkaMessage,
  MessageTransaction,
  ProduceResult,
} from '@nodejs-kafka/broker';
import { OutboxRelay, OutboxStore } from '../src/outbox.js';
import type { OutboxRow } from '../src/outbox.js';
import { createLogger } from '../src/logger.js';

const logger = createLogger('silent');

function memoryStore() {
  return new OutboxStore(new DatabaseSync(':memory:'));
}

describe('OutboxStore', () => {
  it('insertPending/peekPending round-trips topic, key, and payload', () => {
    const store = memoryStore();
    const payload = { orderId: 'ORD-00001', items: ['a', 'b'], total: 12.5 };
    const id = store.insertPending({ topic: 'orders.created', payload, key: 'ORD-00001' });

    const rows = store.peekPending();
    expect(rows).toHaveLength(1);
    expect(rows[0]!.id).toBe(id);
    expect(rows[0]!.topic).toBe('orders.created');
    expect(rows[0]!.key).toBe('ORD-00001');
    expect(rows[0]!.payload).toEqual(payload);
    expect(Number.isNaN(Date.parse(rows[0]!.createdAt))).toBe(false);
    expect(store.countPending()).toBe(1);
  });

  it('markPublished clears rows from peekPending and updates countPending', () => {
    const store = memoryStore();
    const id1 = store.insertPending({ topic: 'orders.created', payload: { n: 1 } });
    const id2 = store.insertPending({ topic: 'payments.completed', payload: { n: 2 } });

    expect(store.countPending()).toBe(2);
    store.markPublished([id1]);

    const rows = store.peekPending();
    expect(rows.map((r) => r.id)).toEqual([id2]);
    expect(store.countPending()).toBe(1);

    store.markPublished([id2]);
    expect(store.peekPending()).toHaveLength(0);
    expect(store.countPending()).toBe(0);
  });

  it('markPublished with an empty id list is a no-op', () => {
    const store = memoryStore();
    store.insertPending({ topic: 'orders.created', payload: { n: 1 } });
    store.markPublished([]);
    expect(store.countPending()).toBe(1);
  });

  it('peekPending limits the batch to the given limit', () => {
    const store = memoryStore();
    for (let i = 1; i <= 3; i++) {
      store.insertPending({ topic: 'orders.created', payload: { n: i } });
    }

    const batch = store.peekPending(2);
    expect(batch).toHaveLength(2);
    expect(batch.map((r) => r.id)).toEqual([1, 2]);
  });

  it('peekPending skips a corrupt payload row without throwing', () => {
    const db = new DatabaseSync(':memory:');
    const store = new OutboxStore(db);
    db.prepare(
      'INSERT INTO outbox (topic, payload, key, created_at) VALUES (?, ?, ?, ?)',
    ).run('orders.created', '{ not json', 'BAD', new Date().toISOString());
    store.insertPending({ topic: 'payments.completed', payload: { ok: true } });

    const rows = store.peekPending();
    expect(rows).toHaveLength(1);
    expect(rows[0]!.topic).toBe('payments.completed');
    expect(store.countPending()).toBe(2);
    db.close();
  });
});

describe('OutboxRelay', () => {
  it('runOnce publishes the batch through a transaction and marks it published', async () => {
    const store = memoryStore();
    store.insertPending({
      topic: 'orders.created',
      payload: { orderId: 'ORD-00001' },
      key: 'ORD-00001',
    });
    store.insertPending({
      topic: 'payments.completed',
      payload: { paymentId: 'PAY-00001', orderId: 'ORD-00001' },
      key: 'ORD-00001',
    });

    const produced: Array<{ topic: string; value: unknown; key: string | null }> = [];
    let committed = 0;
    let aborted = 0;
    const tx: MessageTransaction = {
      produce: async (topic, value, options) => {
        produced.push({ topic, value, key: options?.key ?? null });
        return { topic, partition: 0, offset: String(produced.length) };
      },
      commit: async () => {
        committed += 1;
      },
      abort: async () => {
        aborted += 1;
      },
    };

    const fakeBroker: IMessageBroker = {
      isConnected: false,
      connect: async () => {
        throw new Error('unexpected');
      },
      disconnect: async () => {
        throw new Error('unexpected');
      },
      createTopics: async () => {
        throw new Error('unexpected');
      },
      listTopics: async () => {
        throw new Error('unexpected');
      },
      produce: async () => {
        throw new Error('unexpected');
      },
      consume: async () => {
        throw new Error('unexpected');
      },
      consumeFromNow: async () => {
        throw new Error('unexpected');
      },
      beginTransaction: async () => tx,
    };

    const publishedEvents: Array<{ row: OutboxRow; result: ProduceResult }> = [];
    const relay = new OutboxRelay({
      broker: fakeBroker,
      store,
      logger,
      onPublished: (p) => {
        publishedEvents.push(p);
      },
    });

    const count = await relay.runOnce();

    expect(count).toBe(2);
    expect(produced).toHaveLength(2);
    expect(produced[0]).toEqual({
      topic: 'orders.created',
      value: { orderId: 'ORD-00001' },
      key: 'ORD-00001',
    });
    expect(produced[1]).toEqual({
      topic: 'payments.completed',
      value: { paymentId: 'PAY-00001', orderId: 'ORD-00001' },
      key: 'ORD-00001',
    });
    expect(committed).toBe(1);
    expect(aborted).toBe(0);
    expect(store.countPending()).toBe(0);
    expect(publishedEvents).toHaveLength(2);
    expect(publishedEvents[0]!.result).toEqual({ topic: 'orders.created', partition: 0, offset: '1' });
    expect(publishedEvents[1]!.result).toEqual({ topic: 'payments.completed', partition: 0, offset: '2' });
  });

  it('runOnce on an empty store returns 0 and produces nothing', async () => {
    const store = memoryStore();
    const produced: Array<{ topic: string; value: unknown }> = [];
    const tx: MessageTransaction = {
      produce: async (topic, value) => {
        produced.push({ topic, value });
        return { topic, partition: 0, offset: '1' };
      },
      commit: async () => {},
      abort: async () => {},
    };
    const fakeBroker: IMessageBroker = {
      isConnected: false,
      connect: async () => {
        throw new Error('unexpected');
      },
      disconnect: async () => {
        throw new Error('unexpected');
      },
      createTopics: async () => {
        throw new Error('unexpected');
      },
      listTopics: async () => {
        throw new Error('unexpected');
      },
      produce: async () => {
        throw new Error('unexpected');
      },
      consume: async () => {
        throw new Error('unexpected');
      },
      consumeFromNow: async () => {
        throw new Error('unexpected');
      },
      beginTransaction: async () => tx,
    };

    const onPublished = (() => {}) as (p: { row: OutboxRow; result: ProduceResult }) => void;
    const relay = new OutboxRelay({ broker: fakeBroker, store, logger, onPublished });

    const count = await relay.runOnce();

    expect(count).toBe(0);
    expect(produced).toHaveLength(0);
  });

  it('runOnce leaves rows pending when beginTransaction rejects', async () => {
    const store = memoryStore();
    store.insertPending({ topic: 'orders.created', payload: { orderId: 'ORD-00001' } });

    const fakeBroker: IMessageBroker = {
      isConnected: false,
      connect: async () => {
        throw new Error('unexpected');
      },
      disconnect: async () => {
        throw new Error('unexpected');
      },
      createTopics: async () => {
        throw new Error('unexpected');
      },
      listTopics: async () => {
        throw new Error('unexpected');
      },
      produce: async () => {
        throw new Error('unexpected');
      },
      consume: async () => {
        throw new Error('unexpected');
      },
      consumeFromNow: async () => {
        throw new Error('unexpected');
      },
      beginTransaction: async () => {
        throw new Error('no transaction support');
      },
    };

    const publishedEvents: Array<{ row: OutboxRow; result: ProduceResult }> = [];
    const relay = new OutboxRelay({
      broker: fakeBroker,
      store,
      logger,
      onPublished: (p) => {
        publishedEvents.push(p);
      },
    });

    const count = await relay.runOnce();

    expect(count).toBe(0);
    expect(store.countPending()).toBe(1);
    expect(publishedEvents).toHaveLength(0);
  });

  it('runOnce with transactional: false publishes through the real in-memory broker', async () => {
    const store = memoryStore();
    store.insertPending({
      topic: 'orders.created',
      payload: { orderId: 'ORD-00002' },
      key: 'ORD-00002',
    });
    store.insertPending({
      topic: 'payments.completed',
      payload: { paymentId: 'PAY-00002' },
      key: 'ORD-00002',
    });

    const broker = createBroker({
      driver: 'in-memory',
      connection: { brokers: ['in-memory://'], clientId: 'outbox-relay-test' },
    });
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created' }, { name: 'payments.completed' }]);

    const received: KafkaMessage[] = [];
    await broker.consume(
      ['orders.created', 'payments.completed'],
      (message, ctx) => {
        received.push(message);
        return ctx.commit();
      },
      { fromBeginning: false },
    );

    const relay = new OutboxRelay({ broker, store, logger, transactional: false });
    const count = await relay.runOnce();
    await new Promise((r) => setTimeout(r, 10));

    expect(count).toBe(2);
    expect(store.countPending()).toBe(0);
    expect(received).toHaveLength(2);
    const orders = received.find((m) => m.topic === 'orders.created');
    const payments = received.find((m) => m.topic === 'payments.completed');
    expect(orders?.key).toBe('ORD-00002');
    expect(orders?.value).toEqual({ orderId: 'ORD-00002' });
    expect(payments?.key).toBe('ORD-00002');
    expect(payments?.value).toEqual({ paymentId: 'PAY-00002' });

    await broker.disconnect();
  });

  it('start publishes rows on subsequent polls and the disposer resolves', async () => {
    const store = memoryStore();
    const broker = createBroker({
      driver: 'in-memory',
      connection: { brokers: ['in-memory://'], clientId: 'outbox-relay-start' },
    });
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created' }]);

    const received: KafkaMessage[] = [];
    await broker.consume(
      ['orders.created'],
      (message, ctx) => {
        received.push(message);
        return ctx.commit();
      },
      { fromBeginning: false },
    );

    const relay = new OutboxRelay({
      broker,
      store,
      logger,
      transactional: false,
      pollIntervalMs: 25,
    });
    const disposer = await relay.start();

    store.insertPending({
      topic: 'orders.created',
      payload: { orderId: 'ORD-00003' },
      key: 'ORD-00003',
    });
    await new Promise((r) => setTimeout(r, 100));

    await disposer();
    expect(store.countPending()).toBe(0);
    expect(received).toHaveLength(1);
    expect(received[0]!.value).toEqual({ orderId: 'ORD-00003' });

    await broker.disconnect();
  });
});
