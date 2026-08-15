import { describe, expect, it } from 'vitest';
import {
  eventSchemas,
  topicToAvroSchema,
  avroSubject,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  type EventTopic,
} from '../src/index.js';

describe('avro schemas', () => {
  it('covers every event topic and only event topics', () => {
    const eventTopics = Object.keys(eventSchemas).sort();
    expect(Object.keys(topicToAvroSchema).sort()).toEqual(eventTopics);
  });

  it('names the registry subject with the {topic}-value convention', () => {
    expect(avroSubject('orders.created')).toBe('orders.created-value');
    expect(avroSubject(TOPIC_PAYMENT_COMPLETED)).toBe('payments.completed-value');
  });

  it('keeps Avro record fields in sync with the Zod schema keys', () => {
    for (const [topic, schema] of Object.entries(topicToAvroSchema)) {
      const avroFields = (schema.fields as Array<{ name: string }>).map((f) => f.name);
      const zodKeys = Object.keys(eventSchemas[topic as EventTopic].shape);
      expect(avroFields, `fields for ${topic}`).toEqual(zodKeys);
    }
  });
});
