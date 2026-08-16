import { describe, expect, it } from 'vitest';
import { createBroker, type KafkaMessage } from '@nodejs-kafka/broker';
import { RETRY_TOPIC } from '@nodejs-kafka/domain';
import { RetryTopicScheduler } from '../src/retry-topic.js';

function brokerWithTopic() {
  const broker = createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'test' },
  });
  return broker;
}

describe('RetryTopicScheduler', () => {
  it('nextDelayMs clamps and honors custom policy', () => {
    const broker = brokerWithTopic();
    const def = new RetryTopicScheduler(broker);
    expect(def.nextDelayMs(0)).toBe(2_000);
    expect(def.nextDelayMs(1)).toBe(10_000);
    expect(def.nextDelayMs(2)).toBe(60_000);
    expect(def.nextDelayMs(5)).toBe(60_000);

    const custom = new RetryTopicScheduler(broker, { policy: { delaysMs: [50, 500] } });
    expect(custom.nextDelayMs(0)).toBe(50);
    expect(custom.nextDelayMs(1)).toBe(500);
    expect(custom.nextDelayMs(9)).toBe(500);
  });

  it('isMaxRetries boundary', () => {
    const broker = brokerWithTopic();
    const def = new RetryTopicScheduler(broker);
    expect(def.isMaxRetries(0)).toBe(false);
    expect(def.isMaxRetries(1)).toBe(false);
    expect(def.isMaxRetries(2)).toBe(false);
    expect(def.isMaxRetries(3)).toBe(true);

    const custom = new RetryTopicScheduler(broker, {
      policy: { delaysMs: [50, 100, 200], maxDeliveries: 4 },
    });
    expect(custom.isMaxRetries(3)).toBe(false);
    expect(custom.isMaxRetries(4)).toBe(true);
  });

  it('parseRetryHeaders defaults and values', () => {
    const broker = brokerWithTopic();
    const scheduler = new RetryTopicScheduler(broker);

    expect(scheduler.parseRetryHeaders(undefined)).toEqual({ retryCount: 0, nextDeliverAtMs: 0 });
    expect(scheduler.parseRetryHeaders({})).toEqual({ retryCount: 0, nextDeliverAtMs: 0 });
    expect(scheduler.parseRetryHeaders({ 'retry-count': 'abc' })).toEqual({ retryCount: 0, nextDeliverAtMs: 0 });
    expect(scheduler.parseRetryHeaders({ 'next-deliver-at': 'nope' })).toEqual({ retryCount: 0, nextDeliverAtMs: 0 });
    expect(scheduler.parseRetryHeaders({ 'retry-count': '2' })).toEqual({ retryCount: 2, nextDeliverAtMs: 0 });

    const iso = '2026-08-16T12:34:56.789Z';
    expect(scheduler.parseRetryHeaders({ 'next-deliver-at': iso })).toEqual({
      retryCount: 0,
      nextDeliverAtMs: Date.parse(iso),
    });
    expect(scheduler.parseRetryHeaders({ 'next-deliver-at': '1725000000000' })).toEqual({
      retryCount: 0,
      nextDeliverAtMs: 1_725_000_000_000,
    });
  });

  it('parkDelayMs past and future', () => {
    const broker = brokerWithTopic();
    const scheduler = new RetryTopicScheduler(broker);
    const now = 1_700_000_000_000;

    expect(scheduler.parkDelayMs(1, now - 5_000, now)).toBe(0);
    expect(scheduler.parkDelayMs(1, now, now)).toBe(0);
    expect(scheduler.parkDelayMs(2, now + 25_000, now)).toBe(25_000);
  });

  it('schedule produces to orders.retry with headers', async () => {
    const broker = brokerWithTopic();
    await broker.connect();

    const scheduler = new RetryTopicScheduler(broker);
    await scheduler.ensureTopic();

    const received: KafkaMessage[] = [];
    await broker.consume([RETRY_TOPIC], (message, ctx) => {
      received.push(message);
      return ctx.commit();
    }, { fromBeginning: false });

    const original: KafkaMessage = {
      topic: 'orders.created',
      key: 'ORD-00004',
      value: { orderId: 'ORD-00004', amount: 42 },
      partition: 0,
      offset: '1',
      timestamp: new Date().toISOString(),
    };

    const before = Date.now();
    await scheduler.schedule(original, new Error('boom'), 0);
    await new Promise((r) => setTimeout(r, 10));

    expect(received).toHaveLength(1);
    const message = received[0]!;
    expect(message.topic).toBe('orders.retry');
    expect(message.key).toBe('ORD-00004');
    expect(message.headers?.['retry-count']).toBe('1');
    expect(message.headers?.['retry.original-topic']).toBe('orders.created');

    const nextDeliverAt = Date.parse(message.headers?.['next-deliver-at'] as string);
    expect(nextDeliverAt).toBeGreaterThan(before + 1_900);
    expect(nextDeliverAt).toBeLessThan(before + 2_100);

    expect(message.value).toEqual({ orderId: 'ORD-00004', amount: 42 });

    await broker.disconnect();
  });

  it('ensureTopic creates topic and getters expose config', async () => {
    const broker = brokerWithTopic();
    await broker.connect();

    const scheduler = new RetryTopicScheduler(broker);
    expect(scheduler.topicName).toBe('orders.retry');
    expect(scheduler.maxDeliveries).toBe(3);

    await scheduler.ensureTopic();
    expect(await broker.listTopics()).toContain('orders.retry');

    await broker.disconnect();
  });
});
