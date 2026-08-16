import { afterEach, describe, expect, it, vi, type Mock } from 'vitest';
import type { IMessageBroker, KafkaMessage } from '@nodejs-kafka/broker';
import { CUSTOMER_TOPIC, type CustomerUpdated } from '@nodejs-kafka/domain';
import { createLogger } from '@nodejs-kafka/infra';
import { createCustomerViewServer, type CustomerViewServer } from '../src/app.js';
import { CustomerStore } from '../src/customer-store.js';

const logger = createLogger('silent');

const sampleCustomer = (customerId: string): CustomerUpdated => {
  const now = new Date().toISOString();
  return {
    type: 'customer.updated',
    eventId: '00000000-0000-0000-0000-000000000000',
    occurredAt: now,
    customerId,
    totalSpentCents: 1500,
    orderCount: 1,
    lastOrderAt: now,
    updatedAt: now,
  };
};

const message = (
  key: string | null,
  value: CustomerUpdated | null,
): KafkaMessage<CustomerUpdated | null> => ({
  topic: CUSTOMER_TOPIC,
  key,
  value,
  partition: 0,
  offset: '0',
  timestamp: new Date().toISOString(),
});

function createMockBroker(): { broker: IMessageBroker; produce: Mock } {
  const produce = vi.fn(
    async (_topic: string, _value: unknown, _options?: { key?: string | null }) => ({
      topic: CUSTOMER_TOPIC,
      partition: 0,
      offset: '0',
    }),
  );
  const broker = {
    connect: vi.fn(async () => {}),
    disconnect: vi.fn(async () => {}),
    isConnected: false,
    createTopics: vi.fn(async () => {}),
    listTopics: vi.fn(async () => []),
    produce,
    consume: vi.fn(async () => async () => {}),
    consumeFromNow: vi.fn(async () => async () => {}),
    beginTransaction: vi.fn(async () => {
      throw new Error('not implemented');
    }),
  } as unknown as IMessageBroker;
  return { broker, produce };
}

interface RunningServer {
  ws: CustomerViewServer;
  port: number;
  broker: IMessageBroker;
  produce: Mock;
  store: CustomerStore;
  close: () => Promise<void>;
}

const setup = async (): Promise<RunningServer> => {
  const { broker, produce } = createMockBroker();
  const store = new CustomerStore();
  const ws = createCustomerViewServer({ broker, logger, store });
  const { port, close } = await ws.start(0);
  return { ws, port, broker, produce, store, close };
};

const running: RunningServer[] = [];

afterEach(async () => {
  for (const r of running.splice(0)) {
    await r.close();
  }
});

describe('CustomerStore', () => {
  it('upserts a changelog record by key', () => {
    const store = new CustomerStore();
    const record = sampleCustomer('CUST-1');
    store.apply(message('CUST-1', record));
    expect(store.get('CUST-1')).toBe(record);
    expect(store.size).toBe(1);
    store.apply(message('CUST-1', { ...record, orderCount: 2 }));
    expect(store.size).toBe(1);
    expect(store.get('CUST-1')?.orderCount).toBe(2);
  });

  it('deletes the key when a tombstone (null value) arrives', () => {
    const store = new CustomerStore();
    store.apply(message('CUST-1', sampleCustomer('CUST-1')));
    expect(store.size).toBe(1);
    store.apply(message('CUST-1', null));
    expect(store.get('CUST-1')).toBeUndefined();
    expect(store.size).toBe(0);
  });

  it('throws when applying an unkeyed record', () => {
    const store = new CustomerStore();
    expect(() => store.apply(message(null, sampleCustomer('CUST-1')))).toThrow(/keyed/);
  });
});

describe('customer view server', () => {
  it('GET /health reports ok', async () => {
    const s = await setup();
    running.push(s);

    const res = await fetch(`http://127.0.0.1:${s.port}/health`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  });

  it('GET /customers lists the store; GET /customers/:id returns the record or 404', async () => {
    const s = await setup();
    running.push(s);
    const record = sampleCustomer('CUST-42');
    s.store.apply(message('CUST-42', record));

    const all = await fetch(`http://127.0.0.1:${s.port}/customers`);
    expect(all.status).toBe(200);
    expect(await all.json()).toEqual([record]);

    const one = await fetch(`http://127.0.0.1:${s.port}/customers/CUST-42`);
    expect(one.status).toBe(200);
    expect(await one.json()).toEqual(record);

    const missing = await fetch(`http://127.0.0.1:${s.port}/customers/CUST-999`);
    expect(missing.status).toBe(404);
    expect(await missing.json()).toEqual({ error: 'Customer not found' });
  });

  it('DELETE /customers/:id produces a tombstone and evicts the key', async () => {
    const s = await setup();
    running.push(s);
    s.store.apply(message('CUST-7', sampleCustomer('CUST-7')));

    const res = await fetch(`http://127.0.0.1:${s.port}/customers/CUST-7`, {
      method: 'DELETE',
    });
    expect(res.status).toBe(204);
    expect(s.produce).toHaveBeenCalledWith(CUSTOMER_TOPIC, null, { key: 'CUST-7' });
    expect(s.store.get('CUST-7')).toBeUndefined();
  });

  it('start/close lifecycle is idempotent', async () => {
    const s = await setup();
    running.push(s);

    expect(typeof s.port).toBe('number');
    await s.close();
    await expect(s.close()).resolves.toBeUndefined();
  });
});
