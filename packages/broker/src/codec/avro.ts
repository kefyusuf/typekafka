import {
  AvroDeserializer,
  AvroSerializer,
  Compatibility,
  SchemaRegistryClient,
  SerdeType,
  type Client,
} from '@confluentinc/schemaregistry';
import { BrokerError, MessageParseError } from '../errors.js';
import { JsonCodec } from './json.js';
import type { MessageCodec } from './types.js';

/** Confluent subject convention for value payloads. */
const avroSubject = (topic: string): string => `${topic}-value`;

export interface AvroCodecOptions {
  /** Schema Registry client (real `SchemaRegistryClient` or `MockClient` in tests). */
  client: Client;
  /** Per-topic curated Avro schemas (see `topicToAvroSchema` in @typekafka/domain). */
  schemas: Record<string, Record<string, unknown>>;
}

/**
 * Avro + Schema Registry codec behind the `MessageCodec` seam.
 *
 * Topics listed in `schemas` use the Confluent wire format (magic byte + schema id
 * + Avro payload) and are registered under the `{topic}-value` subject with
 * `BACKWARD` compatibility on first use. Every other topic keeps the exact
 * `JsonCodec` behavior (DLQ / retry / telemetry payloads are plain JSON).
 */
export class AvroCodec implements MessageCodec {
  readonly kind = 'avro' as const;

  private readonly json = new JsonCodec();
  private readonly serializer: AvroSerializer;
  private readonly deserializer: AvroDeserializer;
  private initPromise?: Promise<void>;

  constructor(private readonly options: AvroCodecOptions) {
    this.serializer = new AvroSerializer(this.options.client, SerdeType.VALUE, {
      autoRegisterSchemas: false,
      useLatestVersion: true,
    });
    this.deserializer = new AvroDeserializer(this.options.client, SerdeType.VALUE, {});
  }

  /** Register every curated schema and pin `BACKWARD` compatibility. Idempotent. */
  init(): Promise<void> {
    this.initPromise ??= this.registerAll();
    return this.initPromise;
  }

  private async registerAll(): Promise<void> {
    for (const [topic, schema] of Object.entries(this.options.schemas)) {
      const subject = avroSubject(topic);
      try {
        await this.options.client.register(subject, {
          schema: JSON.stringify(schema),
          schemaType: 'AVRO',
        }, true);
        await this.options.client.updateCompatibility(subject, Compatibility.BACKWARD);
      } catch (error) {
        throw new BrokerError(
          `schema registry registration failed for subject "${subject}"`,
          error,
        );
      }
    }
  }

  async serialize(topic: string, value: unknown): Promise<Buffer | string | null> {
    if (topic in this.options.schemas) {
      await this.init();
      try {
        return await this.serializer.serialize(topic, value);
      } catch (error) {
        throw new MessageParseError(
          `avro serialization failed for "${topic}"`,
          topic,
          value,
          error,
        );
      }
    }
    return this.json.serialize(topic, value);
  }

  async deserialize(
    topic: string,
    raw: Buffer | string | null | undefined,
  ): Promise<unknown> {
    if (topic in this.options.schemas) {
      if (!Buffer.isBuffer(raw)) {
        throw new MessageParseError(
          `avro deserialization failed for "${topic}": expected a Buffer (Confluent wire format)`,
          topic,
          raw,
        );
      }
      try {
        return await this.deserializer.deserialize(topic, raw);
      } catch (error) {
        throw new MessageParseError(
          `avro deserialization failed for "${topic}"`,
          topic,
          raw,
          error,
        );
      }
    }
    return this.json.deserialize(topic, raw);
  }
}

/** Build an AvroCodec from registry URLs (owns the SR client construction). */
export function createAvroCodec(options: {
  baseURLs: string[];
  schemas: Record<string, Record<string, unknown>>;
}): AvroCodec {
  return new AvroCodec({
    client: new SchemaRegistryClient({ baseURLs: options.baseURLs }),
    schemas: options.schemas,
  });
}
