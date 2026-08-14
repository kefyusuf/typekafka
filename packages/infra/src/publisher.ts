import type {
  IMessageBroker,
  ProduceOptions,
  ProduceResult,
} from '@nodejs-kafka/broker';
import type {
  EventOf,
  EventTopic,
  OrderCreated,
} from '@nodejs-kafka/domain';
import { parseEvent } from '@nodejs-kafka/domain';

/**
 * Type-safe publish facade.
 *
 * Every topic is bound to its schema at compile time: `publish('orders.created', ...)`
 * accepts exactly `OrderCreated`, and the payload is validated at runtime before
 * it reaches the broker (fail-fast on the producer side).
 */
export class TypedPublisher {
  constructor(private readonly broker: IMessageBroker) {}

  async publish<Topic extends EventTopic>(
    topic: Topic,
    payload: EventOf<Topic>,
    options?: ProduceOptions,
  ): Promise<ProduceResult> {
    // Runtime validation -> guarantees the broker never sees an invalid envelope.
    const parsed = parseEvent(topic, payload);

    return this.broker.produce(topic, parsed, {
      key: options?.key ?? null,
      headers: options?.headers,
      partition: options?.partition,
    });
  }

  async publishOrder(order: OrderCreated, options?: ProduceOptions): Promise<ProduceResult> {
    return this.publish('orders.created', order, options);
  }
}
