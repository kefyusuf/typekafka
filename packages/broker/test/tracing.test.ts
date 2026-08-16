import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SpanStatusCode, trace } from '@opentelemetry/api';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { createBroker } from '../src/index.js';
import { withSpan } from '../src/trace.js';
import type { BrokerConfig } from '../src/config.js';

function makeBroker(): ReturnType<typeof createBroker> {
  return createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'test' },
  } satisfies BrokerConfig);
}

describe('manual OTel spans', () => {
  let provider: BasicTracerProvider;
  let exporter: InMemorySpanExporter;

  beforeAll(() => {
    exporter = new InMemorySpanExporter();
    provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });
    trace.setGlobalTracerProvider(provider);
  });

  afterAll(async () => {
    await provider.forceFlush();
    await provider.shutdown();
    trace.disable();
  });

  beforeEach(() => {
    exporter.reset();
  });

  it('exports a produce span with messaging attributes', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created', numPartitions: 3 }]);

    await broker.produce('orders.created', { orderId: 'ORD-1' }, { key: 'k1' });

    await new Promise((r) => setTimeout(r, 10));

    const produce = exporter.getFinishedSpans().find((s) => s.name === 'produce');
    expect(produce).toBeDefined();
    expect(produce?.attributes['messaging.system']).toBe('kafka');
    expect(produce?.attributes['messaging.destination']).toBe('orders.created');
    expect(produce?.attributes).toHaveProperty('messaging.destination_partition');

    await broker.disconnect();
  });

  it('exports a consume span around the handler invocation', async () => {
    const broker = makeBroker();
    await broker.connect();
    await broker.createTopics([{ name: 'orders.created', numPartitions: 3 }]);

    await broker.consume(['orders.created'], async (message) => {
      expect(message.value).toEqual({ orderId: 'ORD-1' });
    });
    await broker.produce('orders.created', { orderId: 'ORD-1' }, { key: 'k1' });

    await new Promise((r) => setTimeout(r, 10));

    const consume = exporter.getFinishedSpans().find((s) => s.name === 'consume');
    expect(consume).toBeDefined();
    expect(consume?.attributes['messaging.destination']).toBe('orders.created');
    expect(consume?.attributes).toHaveProperty('partition');
    expect(consume?.attributes).toHaveProperty('offset');

    await broker.disconnect();
  });

  it('marks a failing span as error and records the exception', async () => {
    await expect(
      withSpan('test', { k: 'v' }, async () => {
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');

    const span = exporter.getFinishedSpans().find((s) => s.name === 'test');
    expect(span).toBeDefined();
    expect(span?.status.code).toBe(SpanStatusCode.ERROR);
    expect(span?.events.some((e) => e.name === 'exception')).toBe(true);
  });
});
