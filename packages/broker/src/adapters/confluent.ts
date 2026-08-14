import { KafkaJS } from '@confluentinc/kafka-javascript';
import type { IMessageBroker } from '../port.js';
import type {
  ConsumeContext,
  ConsumeHandler,
  ConsumeOptions,
  Disposer,
  KafkaMessage,
  ProduceOptions,
  ProduceResult,
  TopicConfig,
} from '../types.js';
import type { BrokerConfig, BrokerLogger } from '../config.js';
import { BrokerError, BrokerStateError } from '../errors.js';

const SECURITY_PROTOCOL_PLAIN = 'plaintext';
const SECURITY_PROTOCOL_SASL = 'sasl_plaintext';
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

/**
 * Deserialize the raw payload. Falls back to the raw text when the payload is
 * not valid JSON so the app-layer Zod validation still runs and can route the
 * message to the DLQ instead of reprocessing it forever.
 */
function deserializeValue(value: Buffer | string | null | undefined): unknown {
  if (value === null || value === undefined) return null;
  const text = Buffer.isBuffer(value) ? value.toString('utf8') : String(value);
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
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
  private admin?: KafkaJS.Admin;
  private adminConnected = false;
  private consumers = new Set<ConsumerHandle>();
  private connected = false;

  constructor(private readonly config: BrokerConfig) {}

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
      const producer = await this.getProducer();
      const [record] = await producer.send({
        topic,
        messages: [
          {
            value: JSON.stringify(value) ?? null,
            key: options.key ?? null,
            headers: toKafkaHeaders(options.headers),
            partition: options.partition,
          },
        ],
      });

      if (!record) {
        throw new BrokerError(`produce to "${topic}" failed: no metadata returned`);
      }

      return {
        topic: record.topicName,
        partition: record.partition,
        offset: record.offset ?? '',
      };
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
      eachMessage: async ({ topic, partition, message }) => {
        const kafkaMessage: KafkaMessage<T> = {
          topic,
          partition,
          key: keyToString(message.key),
          value: deserializeValue(message.value) as T,
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
          nack: async () => {
            /* DLQ handling lives at the app layer (infra/dlq.ts). */
          },
        };

        // If the handler throws (e.g. the DLQ write itself failed), the driver
        // seeks back to this offset and reprocesses it — at-least-once delivery.
        await handler(kafkaMessage, context);
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
        ? SECURITY_PROTOCOL_SASL
        : SECURITY_PROTOCOL_PLAIN,
    };

    if (connection.sasl) {
      config['sasl.mechanisms'] = 'PLAIN';
      config['sasl.username'] = connection.sasl.username;
      config['sasl.password'] = connection.sasl.password;
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
