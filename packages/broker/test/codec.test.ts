import { describe, expect, it, vi } from 'vitest';
import { JsonCodec } from '../src/codec/index.js';
import { createBroker } from '../src/index.js';
import type { BrokerConfig } from '../src/config.js';
import type { MessageCodec } from '../src/codec/types.js';

function makeBroker(config: Partial<BrokerConfig> = {}) {
  return createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'test' },
    ...config,
  } satisfies BrokerConfig);
}

describe('JsonCodec', () => {
  it('serializes a value to its JSON string', async () => {
    const codec = new JsonCodec();
    expect(await codec.serialize('orders.created', { orderId: 'ORD-1' })).toBe(
      JSON.stringify({ orderId: 'ORD-1' }),
    );
  });

  it('returns null for undefined values', async () => {
    const codec = new JsonCodec();
    expect(await codec.serialize('t', undefined)).toBeNull();
  });

  it('deserializes a JSON string back to an object', async () => {
    const codec = new JsonCodec();
    expect(await codec.deserialize('t', JSON.stringify({ a: 1 }))).toEqual({ a: 1 });
  });

  it('deserializes a Buffer payload', async () => {
    const codec = new JsonCodec();
    expect(await codec.deserialize('t', Buffer.from('{"a":2}'))).toEqual({ a: 2 });
  });

  it('falls back to raw text when the payload is not JSON', async () => {
    const codec = new JsonCodec();
    expect(await codec.deserialize('t', 'not-json')).toBe('not-json');
  });

  it('returns null when the raw payload is nullish', async () => {
    const codec = new JsonCodec();
    expect(await codec.deserialize('t', null)).toBeNull();
    expect(await codec.deserialize('t', undefined)).toBeNull();
  });

  it('passes a null value through serialize as a tombstone (not the string "null")', async () => {
    const codec = new JsonCodec();
    expect(await codec.serialize('customers.deleted', null)).toBeNull();
    expect(await codec.deserialize('customers.deleted', null)).toBeNull();
    expect(await codec.deserialize('customers.deleted', undefined)).toBeNull();
  });
});

describe('codec wiring', () => {
  it('serializes on produce and deserializes on consume', async () => {
    const codec: MessageCodec = {
      kind: 'json',
      serialize: vi.fn(async (_t: string, v: unknown) => `wrapped:${JSON.stringify(v)}`),
      deserialize: vi.fn(async (_t: string, raw: unknown) => {
        const text = String(raw).replace(/^wrapped:/, '');
        return JSON.parse(text) as unknown;
      }),
    };

    const broker = makeBroker({ codec });
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created' }]);

    const received: unknown[] = [];
    await broker.consumeFromNow(['orders.created'], async (message) => {
      received.push(message.value);
    });

    await broker.produce('orders.created', { orderId: 'ORD-1' });
    await new Promise((r) => setTimeout(r, 10));

    expect(codec.serialize).toHaveBeenCalledWith('orders.created', { orderId: 'ORD-1' });
    expect(codec.deserialize).toHaveBeenCalled();
    expect(received).toEqual([{ orderId: 'ORD-1' }]);

    await broker.disconnect();
  });

  it('round-trips through the default JsonCodec', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created' }]);

    const received: unknown[] = [];
    await broker.consumeFromNow(['orders.created'], async (message) => {
      received.push(message.value);
    });

    await broker.produce('orders.created', { orderId: 'ORD-1' });
    await new Promise((r) => setTimeout(r, 10));

    expect(received).toEqual([{ orderId: 'ORD-1' }]);

    await broker.disconnect();
  });
});
