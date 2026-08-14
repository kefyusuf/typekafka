import { describe, expect, it } from 'vitest';
import { createBroker, type KafkaMessage } from '@nodejs-kafka/broker';
import { DLQ_TOPIC } from '@nodejs-kafka/domain';
import { DlqManager } from '../src/dlq.js';

function brokerWithTopic() {
  const broker = createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'test' },
  });
  return broker;
}

describe('DlqManager', () => {
  it('writes a dead-letter entry with diagnostics', async () => {
    const broker = brokerWithTopic();
    await broker.connect();
    await broker.createTopics([{ name: DLQ_TOPIC, numPartitions: 3 }]);
    const dlq = new DlqManager(broker);
    await dlq.ensureTopic();

    const deadLettered: KafkaMessage[] = [];
    await broker.consume([DLQ_TOPIC], (message, ctx) => {
      deadLettered.push(message);
      return ctx.commit();
    });

    const original: KafkaMessage = {
      topic: 'orders.created',
      key: 'ORD-00003',
      value: { orderId: 'ORD-00003' },
      partition: 1,
      offset: '4',
      timestamp: new Date().toISOString(),
    };
    const error = new Error('provider timed out');

    await dlq.deadLetter(original, error, 3);
    await new Promise((r) => setTimeout(r, 10));

    expect(deadLettered).toHaveLength(1);
    const entry = deadLettered[0]!.value as {
      topic: string;
      offset: string;
      error: string;
      errorType: string;
      attempts: number;
      original: { orderId: string };
    };
    expect(entry.topic).toBe('orders.created');
    expect(entry.offset).toBe('4');
    expect(entry.error).toBe('provider timed out');
    expect(entry.errorType).toBe('Error');
    expect(entry.attempts).toBe(3);
    expect(entry.original).toEqual({ orderId: 'ORD-00003' });
    expect(deadLettered[0]!.headers?.['dlq.original-topic']).toBe('orders.created');

    await broker.disconnect();
  });
});
