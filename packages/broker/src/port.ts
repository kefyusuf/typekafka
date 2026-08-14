import type {
  ConsumeHandler,
  ConsumeOptions,
  Disposer,
  ProduceOptions,
  ProduceResult,
  TopicConfig,
} from './types.js';

/**
 * The broker port (hexagonal architecture).
 *
 * The whole application talks to this interface — never to a concrete
 * Kafka client. Swap the implementation behind `createBroker()` and the
 * domain / apps stay untouched.
 */
export interface IMessageBroker {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  readonly isConnected: boolean;

  createTopics(topics: TopicConfig[]): Promise<void>;
  listTopics(): Promise<string[]>;

  produce<T>(
    topic: string,
    value: T,
    options?: ProduceOptions,
  ): Promise<ProduceResult>;

  consume<T>(
    topics: string[],
    handler: ConsumeHandler<T>,
    options?: ConsumeOptions,
  ): Promise<Disposer>;

  /** Subscribe, but only deliver messages written after subscription. */
  consumeFromNow<T>(
    topics: string[],
    handler: ConsumeHandler<T>,
    options?: ConsumeOptions,
  ): Promise<Disposer>;
}
