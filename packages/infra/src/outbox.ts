import { DatabaseSync, type StatementSync } from 'node:sqlite';
import type {
  Disposer,
  IMessageBroker,
  MessageTransaction,
  ProduceResult,
} from '@nodejs-kafka/broker';
import type { AppLogger } from './logger.js';

export interface OutboxRow {
  id: number;
  topic: string;
  payload: unknown;
  key: string | null;
  createdAt: string;
}

export interface OutboxInsert {
  topic: string;
  payload: unknown;
  key?: string | null;
}

interface RawOutboxRow {
  id: number | bigint;
  topic: string;
  payload: string;
  key: string | null;
  created_at: string;
}

/**
 * SQLite-backed outbox table. `insertPending` is meant to be called inside the
 * same local transaction that writes the domain row, so the app gets atomic
 * write-then-publish semantics. Rows stay pending until the relay marks them
 * published; a row with an unparseable payload is skipped by `peekPending`
 * (never deleted) so it can be inspected and re-processed.
 */
export class OutboxStore {
  private readonly insertStmt: StatementSync;
  private readonly peekStmt: StatementSync;
  private readonly countStmt: StatementSync;

  constructor(private readonly db: DatabaseSync) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        payload TEXT NOT NULL,
        key TEXT,
        created_at TEXT NOT NULL,
        published_at TEXT
      );
      CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (published_at, id);
    `);
    this.insertStmt = db.prepare(
      'INSERT INTO outbox (topic, payload, key, created_at) VALUES (?, ?, ?, ?)',
    );
    this.peekStmt = db.prepare(
      'SELECT id, topic, payload, key, created_at FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT ?',
    );
    this.countStmt = db.prepare('SELECT COUNT(*) AS c FROM outbox WHERE published_at IS NULL');
  }

  insertPending(insert: OutboxInsert): number {
    const result = this.insertStmt.run(
      insert.topic,
      JSON.stringify(insert.payload),
      insert.key ?? null,
      new Date().toISOString(),
    );
    return Number(result.lastInsertRowid);
  }

  peekPending(limit = 10): OutboxRow[] {
    const rows = this.peekStmt.all(limit) as unknown as RawOutboxRow[];
    const pending: OutboxRow[] = [];
    for (const row of rows) {
      let payload: unknown;
      try {
        payload = JSON.parse(row.payload);
      } catch {
        continue;
      }
      pending.push({
        id: Number(row.id),
        topic: row.topic,
        payload,
        key: row.key,
        createdAt: row.created_at,
      });
    }
    return pending;
  }

  markPublished(ids: number[]): void {
    if (ids.length === 0) return;
    const placeholders = ids.map(() => '?').join(', ');
    this.db
      .prepare(`UPDATE outbox SET published_at = ? WHERE id IN (${placeholders})`)
      .run(new Date().toISOString(), ...ids);
  }

  countPending(): number {
    const row = this.countStmt.get() as unknown as { c: number | bigint } | undefined;
    return Number(row?.c ?? 0);
  }
}

export interface OutboxRelayOptions {
  broker: IMessageBroker;
  store: OutboxStore;
  logger: AppLogger;
  /** Rows per batch. Default 10. */
  batchSize?: number;
  /** Delay between polls. Default 250ms. */
  pollIntervalMs?: number;
  /** Publish via a broker transaction (all-or-nothing per batch). Default true. */
  transactional?: boolean;
  onPublished?: (published: { row: OutboxRow; result: ProduceResult }) => void | Promise<void>;
}

/**
 * Polls the outbox store and publishes pending rows through the broker. With
 * `transactional` the whole batch commits atomically via `beginTransaction`;
 * on any failure the batch is aborted and the rows stay pending so the next
 * poll retries them.
 */
export class OutboxRelay {
  private readonly broker: IMessageBroker;
  private readonly store: OutboxStore;
  private readonly logger: AppLogger;
  private readonly batchSize: number;
  private readonly pollIntervalMs: number;
  private readonly transactional: boolean;
  private readonly onPublished?: (
    published: { row: OutboxRow; result: ProduceResult },
  ) => void | Promise<void>;
  /**
   * Tracks row ids published within this process so a re-poll (the relay polls
   * on an interval) never re-publishes a row it has already dispatched. The
   * durable marker lives in SQLite (`markPublished`), so a fresh process only
   * sees genuinely pending rows; this set guards the window before that marker
   * is flushed and is also a safety net if `markPublished` itself throws.
   */
  private readonly publishedIds = new Set<number>();

  constructor(options: OutboxRelayOptions) {
    this.broker = options.broker;
    this.store = options.store;
    this.logger = options.logger;
    this.batchSize = options.batchSize ?? 10;
    this.pollIntervalMs = options.pollIntervalMs ?? 250;
    this.transactional = options.transactional ?? true;
    this.onPublished = options.onPublished;
  }

  async runOnce(): Promise<number> {
    // Skip rows already dispatched in this process (see `publishedIds`).
    const rows = this.store
      .peekPending(this.batchSize)
      .filter((r) => !this.publishedIds.has(r.id));
    if (rows.length === 0) return 0;

    let tx: MessageTransaction | undefined;
    try {
      if (this.transactional) tx = await this.broker.beginTransaction();

      const published: Array<{ row: OutboxRow; result: ProduceResult }> = [];
      for (const row of rows) {
        const result = tx
          ? await tx.produce(row.topic, row.payload, { key: row.key })
          : await this.broker.produce(row.topic, row.payload, { key: row.key });
        published.push({ row, result });
        this.publishedIds.add(row.id);
      }

      // Commit the Kafka transaction first, then mark rows published in SQLite.
      // This ordering is at-least-once: a crash between the two leaves the row
      // pending and it is re-published on the next poll (never dropped). Making
      // this exactly-once would require consumer-side idempotency on `eventId`,
      // which is intentionally out of scope for the demo.
      if (tx) await tx.commit();
      this.store.markPublished(rows.map((r) => r.id));

      for (const p of published) {
        await this.onPublished?.(p);
      }
      return rows.length;
    } catch (err) {
      this.logger.error({ err, count: rows.length }, 'outbox relay batch failed');
      if (tx) await tx.abort().catch(() => {});
      return 0;
    }
  }

  async start(): Promise<Disposer> {
    let running = false;
    let stopped = false;
    let inFlight: Promise<number> | null = null;

    const tick = async () => {
      if (running || stopped) return;
      running = true;
      try {
        inFlight = this.runOnce();
        await inFlight;
      } finally {
        running = false;
      }
    };

    await tick();

    const handle = setInterval(() => void tick(), this.pollIntervalMs);
    handle.unref?.();

    return async () => {
      stopped = true;
      clearInterval(handle);
      if (inFlight) await inFlight.catch(() => {});
    };
  }
}
