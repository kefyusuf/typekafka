import type { MessageCodec } from './codec/index.js';

export type BrokerDriver = 'in-memory' | 'confluent';

export interface BrokerConnectionConfig {
  brokers: string[];
  clientId: string;
  sasl?: {
    username: string;
    password: string;
  };
}

/**
 * Minimal logger contract for drivers. Structurally compatible with pino's
 * `Logger` so apps can forward their structured logger (rebalance events,
 * connection notices, ...) into the Kafka client.
 */
export interface BrokerLogger {
  info(msg: string): void;
  info(obj: unknown, msg?: string): void;
  warn(msg: string): void;
  warn(obj: unknown, msg?: string): void;
  error(msg: string): void;
  error(obj: unknown, msg?: string): void;
  debug(msg: string): void;
  debug(obj: unknown, msg?: string): void;
}

export interface BrokerConfig {
  driver: BrokerDriver;
  connection: BrokerConnectionConfig;
  /**
   * When `false`, a produce() call throws if the broker isn't connected yet
   * (fail-fast). When `true`, produce() enqueues and auto-connects.
   */
  autoConnect?: boolean;
  /** For the in-memory driver: commit offsets automatically after handler resolve. */
  memoryAutoCommit?: boolean;
  /** Forwarded to the underlying driver for structured logging. */
  logger?: BrokerLogger;
  /** Message (de)serialization strategy. Defaults to `JsonCodec`. */
  codec?: MessageCodec;
}
