import { z } from 'zod';
import { OrderCreatedSchema, type OrderCreated } from './order-created.js';
import { PaymentCompletedSchema, type PaymentCompleted } from './payment-completed.js';

export const TOPIC_ORDER_CREATED = 'orders.created';
export const TOPIC_PAYMENT_COMPLETED = 'payments.completed';

export const DLQ_TOPIC = 'orders.dlq';
export const RETRY_TOPIC = 'orders.retry';

export type { OrderCreated, OrderItem } from './order-created.js';
export type { PaymentCompleted } from './payment-completed.js';

export { TELEMETRY_TOPIC, TelemetryEventSchema } from './telemetry.js';
export type { TelemetryEvent, TelemetryEventType } from './telemetry.js';

/** Discriminated union of every event in the system (keyed by `type`). */
export type EventPayload = OrderCreated | PaymentCompleted;

/** Maps each topic to the `type` discriminator of the event it carries. */
export const topicToType = {
  [TOPIC_ORDER_CREATED]: 'order.created',
  [TOPIC_PAYMENT_COMPLETED]: 'payment.completed',
} as const;

export type EventTopic = keyof typeof topicToType;

/** The concrete event type that a given topic carries. */
export type EventOf<Topic extends EventTopic> = Extract<
  EventPayload,
  { type: (typeof topicToType)[Topic] }
>;

/** A topic plus its already-validated payload. */
export interface EventEnvelope<Topic extends EventTopic = EventTopic> {
  topic: Topic;
  payload: EventOf<Topic>;
}

/**
 * Zod schema registry: each topic -> the schema for the event it carries.
 * Types are statically bound, so parse() returns exactly the right payload.
 */
type EventSchemas = { [K in EventTopic]: z.ZodType<EventOf<K>> };

export const eventSchemas: EventSchemas = {
  [TOPIC_ORDER_CREATED]: OrderCreatedSchema,
  [TOPIC_PAYMENT_COMPLETED]: PaymentCompletedSchema,
};

/** Strictly parse an unknown payload against the topic's schema. */
export function parseEvent<Topic extends EventTopic>(
  topic: Topic,
  value: unknown,
): EventOf<Topic> {
  return eventSchemas[topic].parse(value);
}

export { avroSubject, topicToAvroSchema } from './avro-schemas.js';
