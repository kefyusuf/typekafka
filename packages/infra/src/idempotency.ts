/**
 * In-process idempotency filter for at-least-once consumers.
 *
 * Kafka delivery is at-least-once: a message can be redelivered after a commit
 * failure, and the transactional outbox can re-publish the same logical event
 * if its `markPublished` write fails. A consumer must therefore tolerate
 * seeing the same `eventId` more than once and produce the same effect.
 *
 * This filter records successfully processed event ids and skips any later
 * delivery of an already-completed one. It is intentionally **only marked on
 * success**, so a transient in-flight retry (which has not completed yet) is
 * never suppressed — only a true re-delivery of a finished event is skipped.
 *
 * The store is in-memory and bounded (LRU eviction). That is enough for a
 * single consumer instance within its uptime, but it does not survive a
 * restart: for cross-restart safety, back this with a durable store (e.g. a
 * unique constraint on `eventId` in the downstream write, or Redis/SQLite).
 */
export interface IdempotencyFilter {
  /** True if `eventId` has already been successfully processed. */
  isDuplicate(eventId: string): boolean;
  /** Record a successfully processed `eventId`. */
  mark(eventId: string): void;
}

export interface IdempotencyFilterOptions {
  /** Max number of remembered ids before the oldest is evicted. Default 100k. */
  maxSize?: number;
  /** Optional TTL (ms); entries older than this are treated as unseen. */
  ttlMs?: number;
}

export function createIdempotencyFilter(
  options: IdempotencyFilterOptions = {},
): IdempotencyFilter {
  const maxSize = options.maxSize ?? 100_000;
  const ttlMs = options.ttlMs ?? 0;
  const seen = new Map<string, number>();

  const isDuplicate = (eventId: string): boolean => {
    const ts = seen.get(eventId);
    if (ts === undefined) return false;
    if (ttlMs > 0 && Date.now() - ts > ttlMs) {
      seen.delete(eventId);
      return false;
    }
    return true;
  };

  const mark = (eventId: string): void => {
    seen.set(eventId, Date.now());
    if (seen.size > maxSize) {
      // Evict the oldest inserted entry (insertion order == Map iteration).
      const oldest = seen.keys().next().value;
      if (typeof oldest === 'string') seen.delete(oldest);
    }
  };

  return { isDuplicate, mark };
}
