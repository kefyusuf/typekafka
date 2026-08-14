import { describe, expect, it } from 'vitest';
import { createBroker, type IMessageBroker } from '@nodejs-kafka/broker';
import { TELEMETRY_TOPIC } from '@nodejs-kafka/domain';
import { createLogger } from '../src/logger.js';
import { createTelemetryClient } from '../src/telemetry.js';

const logger = createLogger('silent');

const setupBroker = async (): Promise<IMessageBroker> => {
  const broker = createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'test' },
  });
  await broker.connect();
  await broker.createTopics([{ name: TELEMETRY_TOPIC, numPartitions: 1 }]);
  return broker;
};

describe('createTelemetryClient', () => {
  it('publishes a schema-validated event to telemetry.events', async () => {
    const broker = await setupBroker();
    const seen: unknown[] = [];
    await broker.consume([TELEMETRY_TOPIC], (message) => {
      seen.push(message.value);
      return Promise.resolve();
    });

    const client = createTelemetryClient(broker, logger);
    expect(client.enabled).toBe(true);
    await client.emit({
      type: 'produced',
      topic: 'orders.created',
      eventId: 'event-1',
      orderId: 'ORD-1',
      partition: 0,
      offset: '0',
      message: 'order published',
      concept: 'partition-key',
    });

    await new Promise((r) => setTimeout(r, 30));
    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({ type: 'produced', orderId: 'ORD-1' });
    await broker.disconnect();
  });

  it('returns a no-op client when broker is null', async () => {
    const client = createTelemetryClient(null, logger);
    expect(client.enabled).toBe(false);
    await expect(client.emit({
      type: 'committed',
      topic: 'orders.created',
      eventId: 'e',
      orderId: 'o',
      partition: 0,
      offset: '0',
      message: 'committed',
      concept: 'offset-commit',
    })).resolves.toBeUndefined();
  });

  it('never throws when the payload is invalid', async () => {
    const broker = await setupBroker();
    const client = createTelemetryClient(broker, logger);
    await expect(client.emit({
      type: 'produced',
      topic: 'orders.created',
      eventId: 'event-1',
      orderId: 'ORD-1',
      partition: -1,          // invalid: negative
      offset: '0',
      message: 'order published',
      concept: 'partition-key',
    })).resolves.toBeUndefined();
    await broker.disconnect();
  });
});
