import { z } from 'zod';

const nonEmpty = (label: string) =>
  z.string().trim().min(1, `${label} is required`);

export const BrokerEnvSchema = z.object({
  BROKER_DRIVER: z.enum(['in-memory', 'confluent']).default('in-memory'),
  BROKER_BROKERS: z.string().default('kafka:9092'),
  BROKER_CLIENT_ID: nonEmpty('BROKER_CLIENT_ID').default('nodejs-kafka-demo'),
  BROKER_SASL_USERNAME: z.string().optional(),
  BROKER_SASL_PASSWORD: z.string().optional(),
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
});

export type BrokerEnv = z.infer<typeof BrokerEnvSchema>;

export interface AppConfig {
  driver: BrokerEnv['BROKER_DRIVER'];
  brokers: string[];
  clientId: string;
  sasl?: { username: string; password: string };
  memoryAutoCommit: boolean;
  consumerGroupId: string;
  consumerFromBeginning: boolean;
  logLevel: BrokerEnv['LOG_LEVEL'];
  webPort: number;
  telemetryGroupId: string;
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
    memoryAutoCommit: e.BROKER_MEMORY_AUTO_COMMIT,
    consumerGroupId: e.CONSUMER_GROUP_ID,
    consumerFromBeginning: e.CONSUMER_FROM_BEGINNING,
    logLevel: e.LOG_LEVEL,
    webPort: e.WEB_PORT,
    telemetryGroupId: e.TELEMETRY_GROUP_ID,
  };
}
