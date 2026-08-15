import { describe, expect, it } from 'vitest';
import { createBroker } from '../src/index.js';
import type { BrokerConfig } from '../src/config.js';
import { BrokerStateError } from '../src/errors.js';

function makeBroker(): ReturnType<typeof createBroker> {
  return createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'test' },
  } satisfies BrokerConfig);
}

describe('InMemoryBrokerAdapter', () => {
  it('produces and consumes a roundtrip message', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created', numPartitions: 3 }]);

    const received: Array<{ key: string | null; value: unknown; offset: string }> = [];
    await broker.consume(['orders.created'], (message, ctx) => {
      received.push({ key: message.key, value: message.value, offset: message.offset });
      return ctx.commit();
    });

    const result = await broker.produce('orders.created', { orderId: 'ORD-00001' }, { key: 'ORD-00001' });

    await new Promise((r) => setTimeout(r, 10));

    expect(result.topic).toBe('orders.created');
    expect(received).toHaveLength(1);
    expect(received[0]?.value).toEqual({ orderId: 'ORD-00001' });
    expect(received[0]?.key).toBe('ORD-00001');
    expect(received[0]?.offset).toBe('0');

    await broker.disconnect();
  });

  it('assigns the same partition for the same key', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 't', numPartitions: 3 }]);

    const partitions = new Set<number>();
    for (let i = 0; i < 10; i++) {
      const r = await broker.produce('t', { i }, { key: 'stable-key' });
      partitions.add(r.partition);
    }

    expect(partitions.size).toBe(1);

    await broker.disconnect();
  });

  it('does not deliver messages written before consumeFromNow', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 't', numPartitions: 1 }]);

    await broker.produce('t', { seq: 1 });

    const received: unknown[] = [];
    await broker.consumeFromNow(['t'], (message) => {
      received.push(message.value);
    });

    await new Promise((r) => setTimeout(r, 10));

    expect(received).toHaveLength(0);

    await broker.produce('t', { seq: 2 });
    await new Promise((r) => setTimeout(r, 10));

    expect(received).toHaveLength(1);

    await broker.disconnect();
  });

  it('disposer stops future deliveries', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 't', numPartitions: 1 }]);

    let count = 0;
    const dispose = await broker.consume(['t'], () => {
      count++;
    });

    await broker.produce('t', { seq: 1 });
    await new Promise((r) => setTimeout(r, 10));
    expect(count).toBe(1);

    await dispose();
    await broker.produce('t', { seq: 2 });
    await new Promise((r) => setTimeout(r, 10));
    expect(count).toBe(1);

    await broker.disconnect();
  });

  it('throws when producing to a non-existent topic', async () => {
    const broker = makeBroker();
    await broker.connect();

    await expect(broker.produce('missing', {})).rejects.toThrow(BrokerStateError);

    await broker.disconnect();
  });

  it('lists topics after creation', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 'a', numPartitions: 1 }, { name: 'b', numPartitions: 2 }]);

    expect(await broker.listTopics()).toEqual(expect.arrayContaining(['a', 'b']));

    await broker.disconnect();
  });

  it('rejects beginTransaction with a driver-guidance error', async () => {
    const broker = makeBroker();
    await broker.connect();

    await expect(broker.beginTransaction()).rejects.toThrow(/confluent/);

    await broker.disconnect();
  });
});
