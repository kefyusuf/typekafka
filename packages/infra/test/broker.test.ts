import { describe, expect, it } from 'vitest';
import { buildBrokerConfig } from '../src/broker.js';
import { loadConfig, type AppConfig } from '../src/config.js';

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
  otelEndpoint: '',
  otelServiceName: '',
  metricsPort: '',
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

  it('maps ssl paths from config into the connection', () => {
    const cfg = buildBrokerConfig({
      ...baseConfig,
      ssl: { ca: '/certs/ca.pem', cert: '/certs/cert.pem', key: '/certs/key.pem' },
    });
    expect(cfg.connection.ssl).toEqual({
      ca: '/certs/ca.pem',
      cert: '/certs/cert.pem',
      key: '/certs/key.pem',
    });
  });

  it('omits ssl from the connection when no ssl paths are set', () => {
    const cfg = buildBrokerConfig(baseConfig);
    expect(cfg.connection.ssl).toBeUndefined();
  });
});

describe('loadConfig observability envs', () => {
  it('defaults the OTel and metrics envs to disabled', () => {
    const cfg = loadConfig({});
    expect(cfg.otelEndpoint).toBe('');
    expect(cfg.otelServiceName).toBe('');
    expect(cfg.metricsPort).toBe('');
  });

  it('defaults all ssl envs to empty and leaves ssl undefined', () => {
    const cfg = loadConfig({});
    expect(cfg.ssl).toBeUndefined();
  });

  it('maps explicit ssl paths into config.ssl', () => {
    const cfg = loadConfig({
      BROKER_SSL_CA_PATH: '/certs/ca.pem',
      BROKER_SSL_CERT_PATH: '/certs/cert.pem',
      BROKER_SSL_KEY_PATH: '/certs/key.pem',
    });
    expect(cfg.ssl).toEqual({
      ca: '/certs/ca.pem',
      cert: '/certs/cert.pem',
      key: '/certs/key.pem',
    });
  });

  it('maps a valid METRICS_PORT through', () => {
    const cfg = loadConfig({ METRICS_PORT: '9464' });
    expect(cfg.metricsPort).toBe('9464');
  });

  it('rejects an invalid METRICS_PORT with a descriptive error', () => {
    expect(() => loadConfig({ METRICS_PORT: 'abc' })).toThrow(
      /METRICS_PORT must be empty or a positive integer/,
    );
  });

  it('rejects a non-positive METRICS_PORT', () => {
    expect(() => loadConfig({ METRICS_PORT: '-1' })).toThrow(
      /METRICS_PORT must be empty or a positive integer/,
    );
  });
});
