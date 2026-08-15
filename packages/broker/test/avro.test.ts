import { describe, expect, it } from 'vitest';
import { MockClient } from '@confluentinc/schemaregistry';
import { BrokerError, MessageParseError } from '../src/errors.js';
import { AvroCodec, createAvroCodec } from '../src/codec/avro.js';

const orderSchema = {
  type: 'record',
  name: 'OrderCreated',
  fields: [
    { name: 'orderId', type: 'string' },
    { name: 'totalCents', type: 'long' },
  ],
};

const schemas = { 'orders.created': orderSchema };

const makeClient = () => new MockClient({});

describe('AvroCodec', () => {
  it('registers every curated schema and pins BACKWARD compatibility', async () => {
    const client = makeClient();
    const codec = new AvroCodec({ client, schemas });
    await codec.init();
    const subject = 'orders.created-value';
    expect(await client.getLatestSchemaMetadata(subject)).toBeDefined();
    expect(await client.getCompatibility(subject)).toBe('BACKWARD');
  });

  it('round-trips an event through the Confluent wire format', async () => {
    const codec = new AvroCodec({ client: makeClient(), schemas });
    const wire = await codec.serialize('orders.created', {
      orderId: 'ord_1',
      totalCents: 2500,
    });
    expect(Buffer.isBuffer(wire)).toBe(true);
    expect((wire as Buffer)[0]).toBe(0x00); // magic byte
    const decoded = await codec.deserialize('orders.created', wire as Buffer);
    expect(decoded).toMatchObject({ orderId: 'ord_1', totalCents: 2500 });
  });

  it('fails serialization with a MessageParseError when the value does not match', async () => {
    const codec = new AvroCodec({ client: makeClient(), schemas });
    await expect(
      codec.serialize('orders.created', { nope: true }),
    ).rejects.toBeInstanceOf(MessageParseError);
  });

  it('rejects non-Buffer avro payloads with a MessageParseError', async () => {
    const codec = new AvroCodec({ client: makeClient(), schemas });
    await expect(
      codec.deserialize('orders.created', '{}'),
    ).rejects.toBeInstanceOf(MessageParseError);
  });

  it('falls back to JSON for topics without an Avro schema', async () => {
    const codec = new AvroCodec({ client: makeClient(), schemas });
    const wire = await codec.serialize('app.dlq', { orderId: 'ord_1' });
    expect(Buffer.isBuffer(wire)).toBe(false);
    expect(JSON.parse(wire as string)).toEqual({ orderId: 'ord_1' });
    expect(await codec.deserialize('app.dlq', wire as string)).toEqual({
      orderId: 'ord_1',
    });
  });

  it('surfaces registration failures as BrokerError', async () => {
    const client = makeClient();
    client.register = async () => {
      throw new Error('registry down');
    };
    const codec = new AvroCodec({ client, schemas });
    await expect(codec.init()).rejects.toBeInstanceOf(BrokerError);
  });

  it('createAvroCodec builds a codec with an internal SchemaRegistryClient', async () => {
    const codec = createAvroCodec({
      baseURLs: ['http://localhost:8081'],
      schemas,
    });
    expect(codec.kind).toBe('avro');
    // JSON fallback path works without touching the registry (no network in tests).
    await expect(codec.serialize('app.dlq', { ok: true })).resolves.toBe(
      '{"ok":true}',
    );
  });
});
