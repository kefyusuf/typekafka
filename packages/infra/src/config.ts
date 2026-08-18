import { z } from 'zod';

const nonEmpty = (label: string) =>
  z.string().trim().min(1, `${label} is required`);

export const BrokerEnvSchema = z.object({
  BROKER_DRIVER: z.enum(['in-memory', 'confluent']).default('in-memory'),
  BROKER_BROKERS: z.string().default('kafka:9092'),
  BROKER_CLIENT_ID: nonEmpty('BROKER_CLIENT_ID').default('nodejs-kafka-demo'),
  BROKER_SASL_USERNAME: z.string().optional(),
  BROKER_SASL_PASSWORD: z.string().optional(),
  BROKER_SSL_CA_PATH: z.string().default(''),
  BROKER_SSL_CERT_PATH: z.string().default(''),
  BROKER_SSL_KEY_PATH: z.string().default(''),
  SCHEMA_REGISTRY_URL: z.string().default(''),
  BROKER_MEMORY_AUTO_COMMIT: z
    .enum(['true', 'false'])
    .default('true')
    .transform((v) => v === 'true'),
  CONSUMER_GROUP_ID: nonEmpty('CONSUMER_GROUP_ID').default('notification-service'),
  CONSUMER_FROM_BEGINNING: z
    .enum(['true', 'false'])
    .default('true')
    .transform((v) => v === 'true'),
  LOG_LEVEL: z
    .enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
    .default('info'),
  WEB_PORT: z.coerce.number().int().positive().default(3000),
  TELEMETRY_GROUP_ID: z.string().trim().min(1).default('web-telemetry'),
  SERVICE_NAME: z.string().trim().default('nodejs-kafka'),
  CUSTOMER_VIEW_PORT: z.coerce.number().int().positive().default(3001),
  CUSTOMER_VIEW_GROUP_ID: z.string().trim().min(1).default('customer-view'),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().default(''),
  OTEL_SERVICE_NAME: z.string().default(''),
  METRICS_PORT: z
    .string()
    .refine((v) => v === '' || /^[1-9]\d*$/.test(v), {
      message: 'METRICS_PORT must be empty or a positive integer',
    })
    .default(''),
  // OPTIONAL demo-only HTTP basic-auth for the app UIs and /metrics surfaces.
  // Format "user:password" in plain text. OFF unless set. For production,
  // source credentials from a secrets manager instead of an env var.
  HTTP_BASIC_AUTH: z.string().optional(),
});

export type BrokerEnv = z.infer<typeof BrokerEnvSchema>;

export interface AppConfig {
  driver: BrokerEnv['BROKER_DRIVER'];
  brokers: string[];
  clientId: string;
  sasl?: { username: string; password: string };
  ssl?: { ca?: string; cert?: string; key?: string };
  /** Schema Registry URL (Avro codec). Empty -> JSON codec. */
  schemaRegistryUrl: string;
  memoryAutoCommit: boolean;
  consumerGroupId: string;
  consumerFromBeginning: boolean;
  logLevel: BrokerEnv['LOG_LEVEL'];
  webPort: number;
  telemetryGroupId: string;
  serviceName: string;
  customerViewPort: number;
  customerViewGroupId: string;
  otelEndpoint: string;
  otelServiceName: string;
  metricsPort: string;
  /** Demo-only basic-auth (`user:password`) for HTTP/metrics surfaces. Off unless set. */
  httpBasicAuth?: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const parsed = BrokerEnvSchema.safeParse(env);

  if (!parsed.success) {
    const details = parsed.error.issues
      .map((i) => `  - ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${details}`);
  }

  const e = parsed.data;
  return {
    driver: e.BROKER_DRIVER,
    brokers: e.BROKER_BROKERS.split(',').map((s) => s.trim()).filter(Boolean),
    clientId: e.BROKER_CLIENT_ID,
    sasl:
      e.BROKER_SASL_USERNAME && e.BROKER_SASL_PASSWORD
        ? { username: e.BROKER_SASL_USERNAME, password: e.BROKER_SASL_PASSWORD }
        : undefined,
    ssl:
      e.BROKER_SSL_CA_PATH || e.BROKER_SSL_CERT_PATH || e.BROKER_SSL_KEY_PATH
        ? {
            ...(e.BROKER_SSL_CA_PATH ? { ca: e.BROKER_SSL_CA_PATH } : {}),
            ...(e.BROKER_SSL_CERT_PATH ? { cert: e.BROKER_SSL_CERT_PATH } : {}),
            ...(e.BROKER_SSL_KEY_PATH ? { key: e.BROKER_SSL_KEY_PATH } : {}),
          }
        : undefined,
    schemaRegistryUrl: e.SCHEMA_REGISTRY_URL,
    memoryAutoCommit: e.BROKER_MEMORY_AUTO_COMMIT,
    consumerGroupId: e.CONSUMER_GROUP_ID,
    consumerFromBeginning: e.CONSUMER_FROM_BEGINNING,
    logLevel: e.LOG_LEVEL,
    webPort: e.WEB_PORT,
    telemetryGroupId: e.TELEMETRY_GROUP_ID,
    serviceName: e.SERVICE_NAME,
    customerViewPort: e.CUSTOMER_VIEW_PORT,
    customerViewGroupId: e.CUSTOMER_VIEW_GROUP_ID,
    otelEndpoint: e.OTEL_EXPORTER_OTLP_ENDPOINT,
    otelServiceName: e.OTEL_SERVICE_NAME,
    metricsPort: e.METRICS_PORT,
    httpBasicAuth: e.HTTP_BASIC_AUTH,
  };
}
