import type { IMessageBroker } from '@nodejs-kafka/broker';
import {
  TELEMETRY_TOPIC,
  TelemetryEventSchema,
  type TelemetryEvent,
  type TelemetryEventType,
} from '@nodejs-kafka/domain';
import type { AppLogger } from './logger.js';

export interface TelemetryInput {
  type: TelemetryEventType;
  topic: string;
  eventId: string;
  orderId: string;
  partition: number;
  offset: string;
  attempt?: number;
  message: string;
  concept: string;
}

export interface TelemetryClient {
  readonly enabled: boolean;
  emit(input: TelemetryInput): Promise<void>;
}

export function createTelemetryClient(
  broker: IMessageBroker | null,
  logger: AppLogger,
): TelemetryClient {
  if (!broker) {
    return { enabled: false, emit: async () => {} };
  }

  return {
    enabled: true,
    async emit(input) {
      const event: TelemetryEvent = { ...input, occurredAt: new Date().toISOString() };
      try {
        const parsed = TelemetryEventSchema.parse(event);
        await broker.produce(TELEMETRY_TOPIC, parsed, { key: parsed.orderId });
      } catch (error) {
        logger.warn({ err: error, type: input.type }, 'telemetry emit failed (ignored)');
      }
    },
  };
}
