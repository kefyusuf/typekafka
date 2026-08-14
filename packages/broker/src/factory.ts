import type { IMessageBroker } from './port.js';
import type { BrokerConfig } from './config.js';
import { BrokerError } from './errors.js';
import { InMemoryBrokerAdapter } from './adapters/in-memory.js';
import { ConfluentKafkaAdapter } from './adapters/confluent.js';

export function createBroker(config: BrokerConfig): IMessageBroker {
  switch (config.driver) {
    case 'in-memory':
      return new InMemoryBrokerAdapter(config);
    case 'confluent':
      return new ConfluentKafkaAdapter(config);
    default:
      throw new BrokerError(`Unknown broker driver: ${String(config.driver)}`);
  }
}

export { InMemoryBrokerAdapter, ConfluentKafkaAdapter };
