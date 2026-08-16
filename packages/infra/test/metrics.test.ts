import { describe, expect, it } from 'vitest';
import { Counter, Histogram, Registry } from 'prom-client';
import { createMetrics, startMetricsServer } from '../src/metrics.js';

describe('createMetrics', () => {
  it('exposes all six handles sharing one registry', () => {
    const metrics = createMetrics();

    expect(metrics.registry).toBeInstanceOf(Registry);
    const counters: ReadonlyArray<readonly [string, Counter<string>]> = [
      ['nodejs_kafka_messages_produced_total', metrics.messagesProduced],
      ['nodejs_kafka_messages_consumed_total', metrics.messagesConsumed],
      ['nodejs_kafka_retries_total', metrics.retriesTotal],
      ['nodejs_kafka_dlq_total', metrics.dlqTotal],
      ['nodejs_kafka_outbox_published_total', metrics.outboxPublishedTotal],
    ];
    for (const [name, handle] of counters) {
      expect(handle).toBeInstanceOf(Counter);
      expect(metrics.registry.getSingleMetric(name)).toBe(handle);
    }
    expect(metrics.handlerDurationMs).toBeInstanceOf(Histogram);
    expect(
      metrics.registry.getSingleMetric('nodejs_kafka_handler_duration_ms'),
    ).toBe(metrics.handlerDurationMs);
  });

  it('records a produced counter increment', async () => {
    const metrics = createMetrics();
    metrics.messagesProduced.labels({ topic: 'orders.created' }).inc();

    const json = await metrics.registry.getMetricsAsJSON();
    const counter = json.find(
      (m) => m.name === 'nodejs_kafka_messages_produced_total',
    );
    expect(counter?.values[0]?.value).toBe(1);
  });

  it('records a handler duration observation', async () => {
    const metrics = createMetrics();
    metrics.handlerDurationMs.labels({ topic: 'orders.created' }).observe(12);

    const json = await metrics.registry.getMetricsAsJSON();
    const histogram = json.find(
      (m) => m.name === 'nodejs_kafka_handler_duration_ms',
    );
    const sum = histogram?.values.find((v) => v.value === 12)?.value;
    expect(sum).toBe(12);
  });
});

describe('startMetricsServer', () => {
  it('serves the registry at /metrics', async () => {
    const metrics = createMetrics();
    metrics.messagesProduced.labels({ topic: 'orders.created' }).inc();
    const server = await startMetricsServer(0, metrics.registry);

    try {
      const res = await fetch(`http://127.0.0.1:${server.port}/metrics`);
      expect(res.status).toBe(200);
      expect((res.headers.get('content-type') ?? '').startsWith('text/plain')).toBe(
        true,
      );
      const body = await res.text();
      expect(body).toContain('nodejs_kafka_messages_produced_total');
    } finally {
      await server.close();
    }
  });

  it('returns 404 for unknown paths', async () => {
    const metrics = createMetrics();
    const server = await startMetricsServer(0, metrics.registry);

    try {
      const res = await fetch(`http://127.0.0.1:${server.port}/`);
      expect(res.status).toBe(404);
    } finally {
      await server.close();
    }
  });

  it('stops accepting connections after close()', async () => {
    const metrics = createMetrics();
    const server = await startMetricsServer(0, metrics.registry);

    await server.close();

    await expect(
      fetch(`http://127.0.0.1:${server.port}/metrics`),
    ).rejects.toThrow();
  });
});
