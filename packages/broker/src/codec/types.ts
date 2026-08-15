export type CodecKind = 'json' | 'avro';

/**
 * Pluggable message (de)serialization behind the broker port.
 *
 * JSON is the default (`JsonCodec`). An Avro + Schema Registry codec plugs in
 * here in Phase 2 without touching the port or the apps.
 */
export interface MessageCodec {
  readonly kind: CodecKind;

  /** Serialize a validated value before it is handed to the broker. */
  serialize(topic: string, value: unknown): Promise<Buffer | string | null>;

  /** Deserialize a raw broker payload back into an object. */
  deserialize(topic: string, raw: Buffer | string | null | undefined): Promise<unknown>;
}
