import type { MessageCodec } from './types.js';

/**
 * Default JSON codec — reproduces the adapters' original behavior exactly.
 *
 * When the payload is not valid JSON, `deserialize` falls back to the raw text
 * so the app-layer Zod validation still runs and can route the message to the
 * DLQ instead of reprocessing it forever.
 */
export class JsonCodec implements MessageCodec {
  readonly kind = 'json' as const;

  async serialize(_topic: string, value: unknown): Promise<Buffer | string | null> {
    return JSON.stringify(value) ?? null;
  }

  async deserialize(
    _topic: string,
    raw: Buffer | string | null | undefined,
  ): Promise<unknown> {
    if (raw === null || raw === undefined) return null;
    const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
    try {
      return JSON.parse(text) as unknown;
    } catch {
      return text;
    }
  }
}
