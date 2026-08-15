import { createAvroCodec, type BrokerConfig } from '@nodejs-kafka/broker';
import { topicToAvroSchema } from '@nodejs-kafka/domain';
import type { AppConfig } from './config.js';

export interface BuildBrokerOptions {
  /** App-specific client id suffix, e.g. "-consumer" / "-web". */
  clientIdSuffix?: string;
  /** In-memory driver auto-commit flag (consumer app uses false). */
  memoryAutoCommit?: boolean;
  /** Optional logger passed through to the broker. */
  logger?: BrokerConfig['logger'];
}

/** Build the broker config for an app. JSON codec unless Schema Registry is set. */
export function buildBrokerConfig(
  config: AppConfig,
  options: BuildBrokerOptions = {},
): BrokerConfig {
  return {
    driver: config.driver,
    connection: {
      brokers: config.brokers,
      clientId: `${config.clientId}${options.clientIdSuffix ?? ''}`,
      sasl: config.sasl,
    },
    memoryAutoCommit: options.memoryAutoCommit ?? config.memoryAutoCommit,
    logger: options.logger,
    ...(config.driver === 'confluent' && config.schemaRegistryUrl
      ? {
          codec: createAvroCodec({
            baseURLs: [config.schemaRegistryUrl],
            schemas: topicToAvroSchema,
          }),
        }
      : {}),
  };
}
