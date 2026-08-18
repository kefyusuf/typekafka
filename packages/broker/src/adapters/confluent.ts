import { KafkaJS } from '@confluentinc/kafka-javascript';
import type { IMessageBroker } from '../port.js';
import type {
  ConsumeContext,
  ConsumeHandler,
  ConsumeOptions,
  Disposer,
  KafkaMessage,
  MessageTransaction,
  ProduceOptions,
  ProduceResult,
  TopicConfig,
  TransactionOptions,
} from '../types.js';
import type { BrokerConfig, BrokerLogger } from '../config.js';
import { JsonCodec, type MessageCodec } from '../codec/index.js';
import { BrokerError, BrokerStateError } from '../errors.js';
import { extractParentContext, injectTraceContext, withSpan } from '../trace.js';

const SECURITY_PROTOCOL_PLAIN = 'plaintext';
const SECURITY_PROTOCOL_SASL = 'sasl_plaintext';
const SECURITY_PROTOCOL_SSL = 'ssl';
const SECURITY_PROTOCOL_SASL_SSL = 'sasl_ssl';
const PRODUCER_FLUSH_TIMEOUT_MS = 5000;

interface ConsumerHandle {
  consumer: KafkaJS.Consumer;
}

/** Kafka stores "committed offset" as the offset of the NEXT message. */
function nextOffset(offset: string): string {
  return String(Number(offset) + 1);
}

function keyToString(key: Buffer | string | null | undefined): string | null {
  if (key === null || key === undefined) return null;
  return Buffer.isBuffer(key) ? key.toString('utf8') : String(key);
}

function toKafkaHeaders(
  headers?: Record<string, string | string[]>,
): KafkaJS.IHeaders | undefined {
  if (!headers) return undefined;
  const out: KafkaJS.IHeaders = {};
  for (const [name, value] of Object.entries(headers)) out[name] = value;
  return out;
}

function fromKafkaHeaders(
  headers?: KafkaJS.IHeaders,
): Record<string, string | string[]> | undefined {
  if (!headers) return undefined;
  const out: Record<string, string | string[]> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      out[name] = value.map((v) => (Buffer.isBuffer(v) ? v.toString('utf8') : String(v)));
    } else {
      out[name] = Buffer.isBuffer(value) ? value.toString('utf8') : String(value);
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/** Adapt a pino-style logger to the driver's `Logger` interface. */
function toBrokerLogger(logger?: BrokerLogger): KafkaJS.Logger | undefined {
  if (!logger) return undefined;

  const forward =
    (level: 'info' | 'warn' | 'error' | 'debug') =>
    (message: string, extra?: object): void => {
      if (extra !== undefined) logger[level](extra, message);
      else logger[level](message);
    };

  const bridge: KafkaJS.Logger = {
    info: forward('info'),
    warn: forward('warn'),
    error: forward('error'),
    debug: forward('debug'),
    namespace: () => bridge,
    setLogLevel: () => {
      /* Level filtering happens on the app logger. */
    },
  };

  return bridge;
}

function toBrokerError(context: string, error: unknown): BrokerError {
  if (error instanceof BrokerError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new BrokerError(`${context}: ${message}`, error);
}

/**
 * Real Kafka driver built on top of @confluentinc/kafka-javascript (KafkaJS
 * compatibility facade).
 *
 * - Idempotent producer (`enable.idempotence` + `acks=all`).
 * - Consumer groups with manual offset commit (`offset + 1`, Kafka convention).
 * - Header / partition-key round-trips, rebalance + connection logs forwarded
 *   to the app logger.
 */
export class ConfluentKafkaAdapter implements IMessageBroker {
  private kafka?: KafkaJS.Kafka;
  private producer?: KafkaJS.Producer;
  private producerConnected = false;
  private txProducer?: KafkaJS.Producer;
  private txProducerConnected = false;
  private admin?: KafkaJS.Admin;
  private adminConnected = false;
  private consumers = new Set<ConsumerHandle>();
  private connected = false;

  private readonly codec: MessageCodec;

  constructor(private readonly config: BrokerConfig) {
    this.codec = config.codec ?? new JsonCodec();
  }

  get isConnected(): boolean {
    return this.connected;
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    this.kafka = new KafkaJS.Kafka(this.buildGlobalConfig());
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    const handles = [...this.consumers];
    this.consumers.clear();

    await Promise.allSettled(handles.map((h) => h.consumer.disconnect()));

    if (this.producer && this.producerConnected) {
      await Promise.allSettled([
        this.producer.flush({ timeout: PRODUCER_FLUSH_TIMEOUT_MS }),
        this.producer.disconnect(),
      ]);
      this.producerConnected = false;
    }

    if (this.txProducer && this.txProducerConnected) {
      await Promise.allSettled([
        this.txProducer.flush({ timeout: PRODUCER_FLUSH_TIMEOUT_MS }),
        this.txProducer.disconnect(),
      ]);
      this.txProducerConnected = false;
    }

    if (this.admin && this.adminConnected) {
      await Promise.allSettled([this.admin.disconnect()]);
      this.adminConnected = false;
    }

    this.connected = false;
  }

  async createTopics(topics: TopicConfig[]): Promise<void> {
    try {
      const admin = await this.getAdmin();
      await admin.createTopics({
        topics: topics.map((t) => ({
          topic: t.name,
          numPartitions: t.numPartitions ?? 1,
          replicationFactor: t.replicationFactor ?? 1,
          configEntries: t.configEntries
            ? Object.entries(t.configEntries).map(([name, value]) => ({ name, value }))
            : undefined,
        })),
      });
    } catch (error) {
      throw toBrokerError('createTopics failed', error);
    }
  }

  async listTopics(): Promise<string[]> {
    try {
      const admin = await this.getAdmin();
      return admin.listTopics();
    } catch (error) {
      throw toBrokerError('listTopics failed', error);
    }
  }

  async produce<T>(
    topic: string,
    value: T,
    options: ProduceOptions = {},
  ): Promise<ProduceResult> {
    try {
      return await withSpan(
        'produce',
        {
          'messaging.system': 'kafka',
          'messaging.destination': topic,
        },
        async () => {
          const producer = await this.getProducer();
          const [record] = await producer.send({
            topic,
            messages: [
              {
                value: await this.codec.serialize(topic, value),
                key: options.key ?? null,
                headers: toKafkaHeaders(injectTraceContext(options.headers)),
                partition: options.partition,
              },
            ],
          });

          if (!record) {
            throw new BrokerError(
              `produce to "${topic}" failed: no metadata returned`,
            );
          }

          // The driver's KafkaJS facade reports the produced offset in
          // `baseOffset` (it never sets `offset` on real Kafka).
          return {
            topic: record.topicName,
            partition: record.partition,
            offset: record.baseOffset?.toString() ?? record.offset ?? '',
          };
        },
      );
    } catch (error) {
      throw toBrokerError(`produce to "${topic}" failed`, error);
    }
  }

  async consume<T>(
    topics: string[],
    handler: ConsumeHandler<T>,
    options: ConsumeOptions = {},
  ): Promise<Disposer> {
    try {
      await this.ensureReady();
    } catch (error) {
      throw toBrokerError('consumer setup failed', error);
    }

    const manualCommit = options.manualCommit ?? false;
    const consumer = this.kafka!.consumer({
      'isolation.level': 'read_committed',
      kafkaJS: {
        groupId: options.groupId ?? this.defaultGroupId(),
        fromBeginning: options.fromBeginning ?? true,
        autoCommit: !manualCommit,
        allowAutoTopicCreation: false,
      },
    });

    const handle: ConsumerHandle = { consumer };
    this.consumers.add(handle);

    try {
      await consumer.connect();
      await consumer.subscribe({ topics });
    } catch (error) {
      this.consumers.delete(handle);
      await consumer.disconnect().catch(() => {});
      throw toBrokerError('consumer connect failed', error);
    }

    void consumer.run({
      partitionsConsumedConcurrently:
        options.concurrency && options.concurrency > 1 ? options.concurrency : 1,
      eachMessage: async ({ topic, partition, message }) => {
        const kafkaMessage: KafkaMessage<T> = {
          topic,
          partition,
          key: keyToString(message.key),
          value: (await this.codec.deserialize(topic, message.value)) as T,
          headers: fromKafkaHeaders(message.headers),
          offset: message.offset,
          timestamp: message.timestamp,
        };

        const context: ConsumeContext = {
          commit: async () => {
            if (!manualCommit) return;
            await consumer.commitOffsets([
              { topic, partition, offset: nextOffset(message.offset) },
            ]);
          },
        };

        // If the handler throws (e.g. the DLQ write itself failed), the driver
        // seeks back to this offset and reprocesses it — at-least-once delivery.
        // Continue the trace that crossed the Kafka boundary by parenting this
        // span on the `traceparent` header injected by the producer.
        const parentContext = extractParentContext(kafkaMessage.headers);
        await withSpan(
          'consume',
          {
            'messaging.destination': topic,
            topic,
            partition,
            offset: message.offset,
          },
          () => handler(kafkaMessage, context),
          parentContext,
        );
      },
    });

    return async () => {
      this.consumers.delete(handle);
      await consumer.disconnect();
    };
  }

  async consumeFromNow<T>(
    topics: string[],
    handler: ConsumeHandler<T>,
    options: ConsumeOptions = {},
  ): Promise<Disposer> {
    return this.consume(topics, handler, { ...options, fromBeginning: false });
  }

  async beginTransaction(_options?: TransactionOptions): Promise<MessageTransaction> {
    try {
      const producer = await this.getTransactionalProducer();
      const tx = await producer.transaction();
      return {
        produce: async <T>(topic: string, value: T, options: ProduceOptions = {}) => {
          try {
            const [record] = await tx.send({
              topic,
              messages: [
                {
                  value: await this.codec.serialize(topic, value),
                  key: options.key ?? null,
                  headers: toKafkaHeaders(injectTraceContext(options.headers)),
                  partition: options.partition,
                },
              ],
            });
            if (!record) {
              throw new BrokerError(
                `transaction produce to "${topic}" failed: no metadata returned`,
              );
            }

            // Mirrors `produce`: the driver's KafkaJS facade reports the
            // produced offset in `baseOffset`.
            return {
              topic: record.topicName,
              partition: record.partition,
              offset: record.baseOffset?.toString() ?? record.offset ?? '',
            };
          } catch (error) {
            throw toBrokerError(`transaction produce to "${topic}" failed`, error);
          }
        },
        commit: async () => {
          await tx.commit();
        },
        abort: async () => {
          await tx.abort();
        },
      };
    } catch (error) {
      throw toBrokerError('beginTransaction failed', error);
    }
  }

  private async getProducer(): Promise<KafkaJS.Producer> {
    await this.ensureReady();
    if (!this.producer) {
      this.producer = this.kafka!.producer({
        kafkaJS: {
          idempotent: true,
          acks: -1,
          allowAutoTopicCreation: false,
        },
      });
    }
    if (!this.producerConnected) {
      await this.producer.connect();
      this.producerConnected = true;
    }
    return this.producer;
  }

  private async getTransactionalProducer(): Promise<KafkaJS.Producer> {
    await this.ensureReady();
    if (!this.txProducer) {
      this.txProducer = this.kafka!.producer({
        kafkaJS: {
          idempotent: true,
          acks: -1,
          transactionalId: `${this.config.connection.clientId}-tx`,
          allowAutoTopicCreation: false,
        },
      });
    }
    if (!this.txProducerConnected) {
      await this.txProducer.connect();
      this.txProducerConnected = true;
    }
    return this.txProducer;
  }

  private async getAdmin(): Promise<KafkaJS.Admin> {
    await this.ensureReady();
    if (!this.admin) {
      this.admin = this.kafka!.admin();
    }
    if (!this.adminConnected) {
      await this.admin.connect();
      this.adminConnected = true;
    }
    return this.admin;
  }

  private async ensureReady(): Promise<void> {
    if (this.connected) return;
    if (!this.config.autoConnect) {
      throw new BrokerStateError('Broker is not connected. Call connect() first.');
    }
    await this.connect();
  }

  private defaultGroupId(): string {
    return `${this.config.connection.clientId}-consumer`;
  }

  private buildGlobalConfig(): KafkaJS.CommonConstructorConfig {
    const { connection } = this.config;
    const config: KafkaJS.CommonConstructorConfig = {
      'bootstrap.servers': connection.brokers.join(','),
      'client.id': connection.clientId,
      'security.protocol': connection.sasl
        ? connection.ssl
          ? SECURITY_PROTOCOL_SASL_SSL
          : SECURITY_PROTOCOL_SASL
        : connection.ssl
          ? SECURITY_PROTOCOL_SSL
          : SECURITY_PROTOCOL_PLAIN,
    };

    if (connection.sasl) {
      config['sasl.mechanisms'] = 'PLAIN';
      config['sasl.username'] = connection.sasl.username;
      config['sasl.password'] = connection.sasl.password;
    }

    if (connection.ssl) {
      if (connection.ssl.ca) config['ssl.ca.location'] = connection.ssl.ca;
      if (connection.ssl.cert) config['ssl.certificate.location'] = connection.ssl.cert;
      if (connection.ssl.key) config['ssl.key.location'] = connection.ssl.key;
    }

    // NOTE: never pass `logger: undefined` — the driver checks `hasOwn` and
    // would then call `setLogLevel()` on `undefined`.
    config.kafkaJS = {
      brokers: connection.brokers,
      ...(this.config.logger
        ? { logger: toBrokerLogger(this.config.logger) }
        : {}),
    };

    return config;
  }
}
