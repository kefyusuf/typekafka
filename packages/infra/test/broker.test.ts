import { describe, expect, it } from 'vitest';
import { buildBrokerConfig } from '../src/broker.js';
import type { AppConfig } from '../src/config.js';

const baseConfig: AppConfig = {
  driver: 'in-memory',
  brokers: ['kafka:9092'],
  clientId: 'nodejs-kafka-demo',
  memoryAutoCommit: true,
  consumerGroupId: 'notification-service',
  consumerFromBeginning: true,
  logLevel: 'info',
  webPort: 3000,
  telemetryGroupId: 'web-telemetry',
  schemaRegistryUrl: '',
};

describe('buildBrokerConfig', () => {
  it('keeps the JSON codec when the driver is in-memory even with a registry URL', () => {
    const cfg = buildBrokerConfig({
      ...baseConfig,
      driver: 'in-memory',
      schemaRegistryUrl: 'http://localhost:8081',
    });
    expect(cfg.codec).toBeUndefined();
  });

  it('keeps the JSON codec when no registry URL is set', () => {
    const cfg = buildBrokerConfig(baseConfig);
    expect(cfg.codec).toBeUndefined();
    expect(cfg.connection.brokers).toEqual(['kafka:9092']);
  });

  it('builds an Avro codec for the confluent driver with a registry URL', () => {
    const cfg = buildBrokerConfig({
      ...baseConfig,
      driver: 'confluent',
      schemaRegistryUrl: 'http://localhost:8081',
    });
    expect(cfg.codec?.kind).toBe('avro');
  });

  it('applies the client id suffix and memoryAutoCommit override', () => {
    const cfg = buildBrokerConfig(baseConfig, {
      clientIdSuffix: '-consumer',
      memoryAutoCommit: false,
    });
    expect(cfg.connection.clientId).toBe('nodejs-kafka-demo-consumer');
    expect(cfg.memoryAutoCommit).toBe(false);
  });
});
