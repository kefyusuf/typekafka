import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { BrokerConfig } from '../src/config.js';
import { BrokerStateError } from '../src/errors.js';
import { createBroker } from '../src/index.js';

const mocks = vi.hoisted(() => {
  const kafkaInstances: Array<{ config: unknown }> = [];
  const producerCreate = vi.fn();
  const consumerCreate = vi.fn();
  const adminCreate = vi.fn();
  return { kafkaInstances, producerCreate, consumerCreate, adminCreate };
});

vi.mock('@confluentinc/kafka-javascript', () => {
  class FakeKafka {
    constructor(readonly config: unknown) {
      mocks.kafkaInstances.push({ config: this.config });
    }

    producer(...args: unknown[]) {
      return mocks.producerCreate(...args);
    }

    consumer(...args: unknown[]) {
      return mocks.consumerCreate(...args);
    }

    admin(...args: unknown[]) {
      return mocks.adminCreate(...args);
    }
  }

  return {
    KafkaJS: {
      Kafka: FakeKafka,
      logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 3, DEBUG: 4 },
    },
  };
});

function fakeProducer(overrides: Record<string, unknown> = {}) {
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    flush: vi.fn().mockResolvedValue(undefined),
    send: vi
      .fn()
      .mockResolvedValue([{ topicName: 'orders.created', partition: 1, baseOffset: '42' }]),
    ...overrides,
  };
}

function fakeAdmin(overrides: Record<string, unknown> = {}) {
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    createTopics: vi.fn().mockResolvedValue(true),
    listTopics: vi.fn().mockResolvedValue(['a', 'b']),
    ...overrides,
  };
}

function fakeConsumer(overrides: Record<string, unknown> = {}) {
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn().mockResolvedValue(undefined),
    commitOffsets: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

function makeBroker(config: Partial<BrokerConfig> = {}) {
  return createBroker({
    driver: 'confluent',
    connection: { brokers: ['kafka:9092'], clientId: 'test-app' },
    ...config,
  } satisfies BrokerConfig);
}

describe('ConfluentKafkaAdapter', () => {
  beforeEach(() => {
    mocks.kafkaInstances.length = 0;
    mocks.producerCreate.mockReset();
    mocks.consumerCreate.mockReset();
    mocks.adminCreate.mockReset();
  });

  it('maps connection config to the driver global config', async () => {
    const broker = makeBroker({
      connection: {
        brokers: ['k1:9092', 'k2:9092'],
        clientId: 'svc',
        sasl: { username: 'user', password: 'pass' },
      },
    });

    await broker.connect();

    expect(mocks.kafkaInstances).toHaveLength(1);
    expect(mocks.kafkaInstances[0]?.config).toMatchObject({
      'bootstrap.servers': 'k1:9092,k2:9092',
      'client.id': 'svc',
      'security.protocol': 'sasl_plaintext',
      'sasl.mechanisms': 'PLAIN',
      'sasl.username': 'user',
      'sasl.password': 'pass',
    });
    expect(mocks.kafkaInstances[0]?.config).not.toHaveProperty('kafkaJS.logger');

    await broker.disconnect();
  });

  it('uses plaintext when no SASL credentials are given', async () => {
    const broker = makeBroker();
    await broker.connect();

    expect(mocks.kafkaInstances[0]?.config).toMatchObject({
      'security.protocol': 'plaintext',
    });

    await broker.disconnect();
  });

  it('produces with idempotence and maps value/key/headers/partition', async () => {
    const producer = fakeProducer();
    mocks.producerCreate.mockReturnValue(producer);

    const broker = makeBroker();
    await broker.connect();

    const result = await broker.produce(
      'orders.created',
      { orderId: 'ORD-1' },
      {
        key: 'ORD-1',
        headers: { 'event.type': 'order.created' },
        partition: 1,
      },
    );

    const [producerConfig] = mocks.producerCreate.mock.calls[0] ?? [];
    expect(producerConfig).toMatchObject({
      kafkaJS: { idempotent: true, acks: -1, allowAutoTopicCreation: false },
    });
    expect(producer.connect).toHaveBeenCalledOnce();
    expect(producer.send).toHaveBeenCalledWith({
      topic: 'orders.created',
      messages: [
        {
          value: JSON.stringify({ orderId: 'ORD-1' }),
          key: 'ORD-1',
          headers: { 'event.type': 'order.created' },
          partition: 1,
        },
      ],
    });
    expect(result).toEqual({ topic: 'orders.created', partition: 1, offset: '42' });

    await broker.disconnect();
  });

  it('falls back to record.offset when baseOffset is absent', async () => {
    const producer = fakeProducer({
      send: vi
        .fn()
        .mockResolvedValue([{ topicName: 'orders.created', partition: 0, offset: '7' }]),
    });
    mocks.producerCreate.mockReturnValue(producer);

    const broker = makeBroker();
    await broker.connect();

    const result = await broker.produce('orders.created', { orderId: 'ORD-1' });
    expect(result.offset).toBe('7');

    await broker.disconnect();
  });

  it('throws a BrokerStateError when producing before connect', async () => {
    const broker = makeBroker();
    await expect(broker.produce('t', {})).rejects.toBeInstanceOf(BrokerStateError);
  });

  it('creates topics via the admin client', async () => {
    const admin = fakeAdmin();
    mocks.adminCreate.mockReturnValue(admin);

    const broker = makeBroker();
    await broker.connect();

    await broker.createTopics([
      {
        name: 'orders.created',
        numPartitions: 3,
        replicationFactor: 2,
        configEntries: { 'retention.ms': '60000' },
      },
      { name: 'payments.completed' },
    ]);

    expect(admin.connect).toHaveBeenCalledOnce();
    expect(admin.createTopics).toHaveBeenCalledWith({
      topics: [
        {
          topic: 'orders.created',
          numPartitions: 3,
          replicationFactor: 2,
          configEntries: [{ name: 'retention.ms', value: '60000' }],
        },
        {
          topic: 'payments.completed',
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: undefined,
        },
      ],
    });

    await broker.disconnect();
  });

  it('lists topics via the admin client', async () => {
    const admin = fakeAdmin({ listTopics: vi.fn().mockResolvedValue(['orders.created']) });
    mocks.adminCreate.mockReturnValue(admin);

    const broker = makeBroker();
    await broker.connect();

    expect(await broker.listTopics()).toEqual(['orders.created']);

    await broker.disconnect();
  });

  it('maps eachMessage payloads to KafkaMessage and commits offset + 1', async () => {
    const consumer = fakeConsumer();
    mocks.consumerCreate.mockReturnValue(consumer);

    const broker = makeBroker();
    await broker.connect();

    const received: Array<{
      key: string | null;
      value: unknown;
      headers?: Record<string, string | string[]>;
      offset: string;
      partition: number;
      timestamp: string;
    }> = [];

    const dispose = await broker.consume(
      ['orders.created'],
      async (message, context) => {
        received.push({
          key: message.key,
          value: message.value,
          headers: message.headers,
          offset: message.offset,
          partition: message.partition,
          timestamp: message.timestamp,
        });
        await context.commit();
      },
      { groupId: 'notification-service', manualCommit: true },
    );

    const [consumerConfig] = mocks.consumerCreate.mock.calls[0] ?? [];
    expect(consumerConfig).toMatchObject({
      kafkaJS: {
        groupId: 'notification-service',
        fromBeginning: true,
        autoCommit: false,
        allowAutoTopicCreation: false,
      },
    });
    expect(consumer.connect).toHaveBeenCalledOnce();
    expect(consumer.subscribe).toHaveBeenCalledWith({ topics: ['orders.created'] });

    const [runConfig] = consumer.run.mock.calls[0] ?? [];
    await runConfig.eachMessage({
      topic: 'orders.created',
      partition: 2,
      message: {
        key: Buffer.from('ORD-1'),
        value: Buffer.from(JSON.stringify({ orderId: 'ORD-1' })),
        headers: {
          'event.type': Buffer.from('order.created'),
          multi: [Buffer.from('a'), 'b'],
        },
        offset: '7',
        timestamp: '1234567890',
      },
    });

    expect(received[0]).toEqual({
      key: 'ORD-1',
      value: { orderId: 'ORD-1' },
      headers: { 'event.type': 'order.created', multi: ['a', 'b'] },
      offset: '7',
      partition: 2,
      timestamp: '1234567890',
    });
    expect(consumer.commitOffsets).toHaveBeenCalledWith([
      { topic: 'orders.created', partition: 2, offset: '8' },
    ]);

    await dispose();
    expect(consumer.disconnect).toHaveBeenCalledOnce();
  });

  it('leaves a failed JSON payload as raw text so Zod can route it to the DLQ', async () => {
    const consumer = fakeConsumer();
    mocks.consumerCreate.mockReturnValue(consumer);

    const broker = makeBroker();
    await broker.connect();

    let value: unknown;
    const dispose = await broker.consume(
      ['orders.created'],
      (message) => {
        value = message.value;
      },
      { groupId: 'g', manualCommit: true },
    );

    const [runConfig] = consumer.run.mock.calls[0] ?? [];
    await runConfig.eachMessage({
      topic: 'orders.created',
      partition: 0,
      message: {
        key: null,
        value: Buffer.from('not-json'),
        headers: undefined,
        offset: '1',
        timestamp: '1',
      },
    });

    expect(value).toBe('not-json');
    await dispose();
  });

  it('auto-commits when manualCommit is disabled (commit is a no-op)', async () => {
    const consumer = fakeConsumer();
    mocks.consumerCreate.mockReturnValue(consumer);

    const broker = makeBroker();
    await broker.connect();

    let committed = 0;
    await broker.consume(
      ['t'],
      async (_message, context) => {
        await context.commit();
        committed++;
      },
      { groupId: 'g' },
    );

    const [consumerConfig] = mocks.consumerCreate.mock.calls[0] ?? [];
    expect(consumerConfig).toMatchObject({
      kafkaJS: { autoCommit: true },
    });

    const [runConfig] = consumer.run.mock.calls[0] ?? [];
    await runConfig.eachMessage({
      topic: 't',
      partition: 0,
      message: {
        key: null,
        value: Buffer.from('{}'),
        headers: undefined,
        offset: '3',
        timestamp: '1',
      },
    });

    expect(committed).toBe(1);
    expect(consumer.commitOffsets).not.toHaveBeenCalled();
  });

  it('consumeFromNow forces fromBeginning = false', async () => {
    const consumer = fakeConsumer();
    mocks.consumerCreate.mockReturnValue(consumer);

    const broker = makeBroker();
    await broker.connect();

    await broker.consumeFromNow(['t'], async () => {}, { groupId: 'g' });

    const [consumerConfig] = mocks.consumerCreate.mock.calls[0] ?? [];
    expect(consumerConfig).toMatchObject({
      kafkaJS: { fromBeginning: false },
    });

    await broker.disconnect();
  });

  it('disconnect tears down consumers, producer and admin', async () => {
    const producer = fakeProducer();
    const admin = fakeAdmin();
    const consumer = fakeConsumer();
    mocks.producerCreate.mockReturnValue(producer);
    mocks.adminCreate.mockReturnValue(admin);
    mocks.consumerCreate.mockReturnValue(consumer);

    const broker = makeBroker();
    await broker.connect();

    await broker.produce('t', { a: 1 });
    await broker.createTopics([{ name: 't' }]);
    await broker.consume(['t'], async () => {}, { groupId: 'g' });

    await broker.disconnect();

    expect(consumer.disconnect).toHaveBeenCalledOnce();
    expect(producer.flush).toHaveBeenCalledOnce();
    expect(producer.disconnect).toHaveBeenCalledOnce();
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });

  it('defaults the consumer group id when none is provided', async () => {
    const consumer = fakeConsumer();
    mocks.consumerCreate.mockReturnValue(consumer);

    const broker = makeBroker();
    await broker.connect();

    await broker.consume(['t'], async () => {}, {});

    const [consumerConfig] = mocks.consumerCreate.mock.calls[0] ?? [];
    expect(consumerConfig).toMatchObject({ kafkaJS: { groupId: 'test-app-consumer' } });
  });

  describe('transactions', () => {
    it('begins a transaction on a transactional producer and commits', async () => {
      const tx = {
        send: vi
          .fn()
          .mockResolvedValue([
            { topicName: 'orders.created', partition: 1, baseOffset: '50' },
          ]),
        commit: vi.fn().mockResolvedValue(undefined),
        abort: vi.fn().mockResolvedValue(undefined),
      };
      const producer = fakeProducer({ transaction: vi.fn().mockResolvedValue(tx) });
      mocks.producerCreate.mockReturnValue(producer);

      const broker = makeBroker();
      await broker.connect();

      const transaction = await broker.beginTransaction();
      const result = await transaction.produce('orders.created', { orderId: 'ORD-1' }, { key: 'ORD-1' });
      await transaction.commit();

      const [producerConfig] = mocks.producerCreate.mock.calls[0] ?? [];
      expect(producerConfig).toMatchObject({
        kafkaJS: {
          idempotent: true,
          acks: -1,
          transactionalId: 'test-app-tx',
          allowAutoTopicCreation: false,
        },
      });
      expect(producer.transaction).toHaveBeenCalledOnce();
      expect(tx.send).toHaveBeenCalledWith({
        topic: 'orders.created',
        messages: [
          {
            value: JSON.stringify({ orderId: 'ORD-1' }),
            key: 'ORD-1',
            headers: undefined,
            partition: undefined,
          },
        ],
      });
      expect(result).toEqual({ topic: 'orders.created', partition: 1, offset: '50' });
      expect(tx.commit).toHaveBeenCalledOnce();
      expect(tx.abort).not.toHaveBeenCalled();

      await broker.disconnect();
    });

    it('allows the caller to abort instead of commit', async () => {
      const tx = {
        send: vi.fn().mockResolvedValue([]),
        commit: vi.fn().mockResolvedValue(undefined),
        abort: vi.fn().mockResolvedValue(undefined),
      };
      const producer = fakeProducer({ transaction: vi.fn().mockResolvedValue(tx) });
      mocks.producerCreate.mockReturnValue(producer);

      const broker = makeBroker();
      await broker.connect();

      const transaction = await broker.beginTransaction();
      await transaction.produce('orders.created', { orderId: 'ORD-1' });
      await transaction.abort();

      expect(tx.abort).toHaveBeenCalledOnce();
      expect(tx.commit).not.toHaveBeenCalled();

      await broker.disconnect();
    });

    it('surfaces a transaction produce failure and does not commit', async () => {
      const tx = {
        send: vi.fn().mockRejectedValue(new Error('broker down')),
        commit: vi.fn().mockResolvedValue(undefined),
        abort: vi.fn().mockResolvedValue(undefined),
      };
      const producer = fakeProducer({ transaction: vi.fn().mockResolvedValue(tx) });
      mocks.producerCreate.mockReturnValue(producer);

      const broker = makeBroker();
      await broker.connect();

      const transaction = await broker.beginTransaction();
      await expect(transaction.produce('orders.created', {})).rejects.toThrow(/failed/);

      await transaction.abort();
      expect(tx.send).toHaveBeenCalledOnce();
      expect(tx.commit).not.toHaveBeenCalled();

      await broker.disconnect();
    });
  });
});
