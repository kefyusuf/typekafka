import type { EventTopic } from './index.js';

/**
 * Curated Avro schemas (Confluent wire format) for every event topic.
 *
 * Authored beside the Zod schemas (order-created.ts / payment-completed.ts) so the
 * validation schema (Zod) and the wire schema (Avro) stay in the same place.
 * The AvroCodec registers them under the `{topic}-value` subject at startup.
 */
export const avroSubject = (topic: string): string => `${topic}-value`;

const orderCreatedAvroSchema = {
  type: 'record',
  name: 'OrderCreated',
  namespace: 'com.nodejs.kafka.events',
  doc: 'Mirrors OrderCreatedSchema (packages/domain/src/events/order-created.ts).',
  fields: [
    { name: 'type', type: 'string' },
    { name: 'eventId', type: 'string' },
    { name: 'occurredAt', type: 'string' },
    { name: 'orderId', type: 'string' },
    { name: 'customerId', type: 'string' },
    {
      name: 'items',
      type: {
        type: 'array',
        items: {
          type: 'record',
          name: 'OrderItem',
          fields: [
            { name: 'sku', type: 'string' },
            { name: 'quantity', type: 'int' },
            { name: 'priceCents', type: 'long' },
          ],
        },
      },
    },
    { name: 'totalCents', type: 'long' },
  ],
} as const;

const paymentCompletedAvroSchema = {
  type: 'record',
  name: 'PaymentCompleted',
  namespace: 'com.nodejs.kafka.events',
  doc: 'Mirrors PaymentCompletedSchema (packages/domain/src/events/payment-completed.ts).',
  fields: [
    { name: 'type', type: 'string' },
    { name: 'eventId', type: 'string' },
    { name: 'occurredAt', type: 'string' },
    { name: 'orderId', type: 'string' },
    { name: 'paymentId', type: 'string' },
    { name: 'amountCents', type: 'long' },
    {
      name: 'method',
      type: {
        type: 'enum',
        name: 'PaymentMethod',
        symbols: ['card', 'bank_transfer', 'wallet'],
      },
    },
  ],
} as const;

const customerUpdatedAvroSchema = {
  type: 'record',
  name: 'CustomerUpdated',
  namespace: 'com.nodejs.kafka.events',
  doc: 'Mirrors CustomerUpdatedSchema (packages/domain/src/events/customer-updated.ts).',
  fields: [
    { name: 'type', type: 'string' },
    { name: 'eventId', type: 'string' },
    { name: 'occurredAt', type: 'string' },
    { name: 'customerId', type: 'string' },
    { name: 'totalSpentCents', type: 'long' },
    { name: 'orderCount', type: 'int' },
    { name: 'lastOrderAt', type: 'string' },
    { name: 'updatedAt', type: 'string' },
  ],
} as const;

/** Topic -> curated Avro schema (record JSON), mirroring `eventSchemas`. */
export const topicToAvroSchema: Record<EventTopic, Record<string, unknown>> = {
  ['orders.created']: orderCreatedAvroSchema,
  ['payments.completed']: paymentCompletedAvroSchema,
  ['customers']: customerUpdatedAvroSchema,
};
